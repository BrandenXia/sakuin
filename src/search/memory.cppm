export module sakuin.search.memory;

import std;

import sakuin.classification;
import sakuin.core.ids;
import sakuin.core.result;
import sakuin.model.torrent;
import sakuin.search.index;

namespace sakuin::search {
class MemoryRebuildSession;
class MemoryUpdateSession;
} // namespace sakuin::search

export namespace sakuin::search {

// A deterministic reference backend for tests and small self-hosted datasets.
// It intentionally uses simple ASCII folding; production Unicode tokenization
// belongs in a replaceable backend rather than the canonical model.
class InMemorySearchIndex final : public SearchIndex {
public:
  explicit InMemorySearchIndex(SearchClassificationOptions options = {});

  core::Result<std::unique_ptr<SearchRebuildSession>>
  begin_rebuild(std::uint64_t source_generation) override;
  core::Result<std::unique_ptr<SearchUpdateSession>>
  begin_update(std::uint64_t source_generation) override;
  core::Result<SearchResult> search(const SearchQuery &query) const override;
  std::uint64_t source_generation() const noexcept override;
  ClassificationIndexStats classification_stats() const noexcept override;

private:
  friend class MemoryRebuildSession;
  friend class MemoryUpdateSession;

  struct IndexedRecord {
    model::TorrentRecord record;
    classification::Classification deterministic;
    classification::Classification classification;
    std::vector<classification::MediaCategory> categories;
  };

  struct State {
    std::uint64_t source_generation{};
    std::vector<IndexedRecord> records;
    // Disposable candidate lookup. Exact substring matching and scoring remain
    // authoritative after candidate selection.
    std::unordered_map<std::uint32_t, std::vector<std::size_t>> trigrams;
    ClassificationIndexStats classification;
  };

  core::Result<void> publish(std::shared_ptr<const State> replacement);
  core::Result<void>
  publish_updates(std::uint64_t source_generation,
                  std::span<const model::TorrentRecord> updates);
  void rebuild_classification(State &state) const;
  void rebuild_text_candidates(State &state) const;
  ClassificationIndexStats summarize(
      std::span<const IndexedRecord> records,
      const classification::LearnedClassifierStats &learned) const noexcept;

  mutable std::shared_mutex mutex_;
  SearchClassificationOptions options_;
  std::shared_ptr<const State> state_;
};

} // namespace sakuin::search

namespace sakuin::search {
namespace {

core::Error invalid(std::string message) {
  return {core::ErrorCode::InvalidQuery, std::move(message)};
}

std::string folded(std::string_view input) {
  std::string result;
  result.reserve(input.size());
  for (const unsigned char value : input)
    result.push_back(value >= 'A' && value <= 'Z'
                         ? static_cast<char>(value - 'A' + 'a')
                         : static_cast<char>(value));
  return result;
}

std::vector<std::string> terms(std::string_view input) {
  std::vector<std::string> result;
  std::string current;
  for (const unsigned char value : input) {
    if (std::isspace(value)) {
      if (!current.empty()) {
        result.push_back(std::move(current));
        current.clear();
      }
    } else {
      current.push_back(value >= 'A' && value <= 'Z'
                            ? static_cast<char>(value - 'A' + 'a')
                            : static_cast<char>(value));
    }
  }
  if (!current.empty())
    result.push_back(std::move(current));
  return result;
}

std::uint32_t trigram(std::string_view value, std::size_t offset) {
  return (static_cast<std::uint32_t>(static_cast<unsigned char>(value[offset]))
          << 16U) |
         (static_cast<std::uint32_t>(
              static_cast<unsigned char>(value[offset + 1]))
          << 8U) |
         static_cast<std::uint32_t>(
             static_cast<unsigned char>(value[offset + 2]));
}

void add_trigrams(std::string_view value,
                  std::unordered_set<std::uint32_t> &result) {
  if (value.size() < 3)
    return;
  for (std::size_t offset = 0; offset + 2 < value.size(); ++offset)
    result.insert(trigram(value, offset));
}

std::optional<std::vector<std::size_t>> candidate_ordinals(
    const std::unordered_map<std::uint32_t, std::vector<std::size_t>> &index,
    std::span<const std::string> query_terms) {
  std::unordered_set<std::uint32_t> required;
  for (const auto &term : query_terms)
    add_trigrams(term, required);
  if (required.empty())
    return std::nullopt;

  std::vector<const std::vector<std::size_t> *> postings;
  postings.reserve(required.size());
  for (const auto key : required) {
    const auto found = index.find(key);
    if (found == index.end())
      return std::vector<std::size_t>{};
    postings.push_back(&found->second);
  }
  std::ranges::sort(postings, {},
                    [](const auto *posting) { return posting->size(); });
  auto result = *postings.front();
  for (std::size_t posting_index = 1;
       posting_index < postings.size() && !result.empty(); ++posting_index) {
    const auto &posting = *postings[posting_index];
    std::erase_if(result, [&](const auto ordinal) {
      return !std::ranges::binary_search(posting, ordinal);
    });
  }
  return result;
}

std::string hex(const core::InfoHash &hash) {
  constexpr std::string_view digits{"0123456789abcdef"};
  std::string result;
  result.reserve(hash.bytes.size() * 2);
  for (const auto byte : hash.bytes) {
    result.push_back(digits[byte >> 4]);
    result.push_back(digits[byte & 0x0f]);
  }
  return result;
}

std::optional<std::uint32_t>
match_score(const model::TorrentRecord &record,
            std::span<const std::string> query_terms) {
  if (query_terms.empty())
    return 0;
  std::vector<std::string> fields;
  fields.reserve(record.files.size() + 2);
  fields.push_back(hex(record.info_hash));
  if (record.name)
    fields.push_back(folded(*record.name));
  for (const auto &file : record.files)
    fields.push_back(folded(file.path));

  std::uint32_t score{};
  for (const auto &term : query_terms) {
    bool found{};
    for (std::size_t index = 0; index < fields.size(); ++index) {
      if (fields[index].contains(term)) {
        found = true;
        score += index == 1 && record.name ? 4U : 1U;
      }
    }
    if (!found)
      return std::nullopt;
  }
  return score;
}

bool contains_category(
    std::span<const classification::MediaCategory> categories,
    classification::MediaCategory expected) {
  return std::ranges::find(categories, expected) != categories.end();
}

bool contains_labels(const classification::Classification &classified,
                     std::span<const classification::ContentLabel> labels,
                     classification::Confidence minimum) {
  return std::ranges::all_of(labels, [&](const auto label) {
    const auto confidence = classification::label_confidence(classified, label);
    return confidence && classification::at_least(*confidence, minimum);
  });
}

} // namespace

class MemoryRebuildSession final : public SearchRebuildSession {
public:
  MemoryRebuildSession(InMemorySearchIndex &owner, std::uint64_t generation)
      : owner_(&owner), generation_(generation) {}
  ~MemoryRebuildSession() override { abort(); }

  core::Result<void> append(const model::TorrentRecord &record) override {
    if (!active_)
      return std::unexpected(
          core::Error{core::ErrorCode::InvalidArgument,
                      "Search rebuild session is no longer active"});
    records_.push_back(record);
    return {};
  }

  core::Result<void> commit() override;

  void abort() noexcept override {
    if (active_) {
      active_ = false;
      records_.clear();
    }
  }

private:
  InMemorySearchIndex *owner_;
  std::uint64_t generation_;
  std::vector<model::TorrentRecord> records_;
  bool active_{true};
};

class MemoryUpdateSession final : public SearchUpdateSession {
public:
  MemoryUpdateSession(InMemorySearchIndex &owner, std::uint64_t generation)
      : owner_(&owner), generation_(generation) {}
  ~MemoryUpdateSession() override { abort(); }

  core::Result<void> upsert(const model::TorrentRecord &record) override {
    if (!active_)
      return std::unexpected(
          core::Error{core::ErrorCode::InvalidArgument,
                      "Search update session is no longer active"});
    updates_.push_back(record);
    return {};
  }

  core::Result<void> commit() override {
    if (!active_)
      return std::unexpected(
          core::Error{core::ErrorCode::InvalidArgument,
                      "Search update session is no longer active"});
    active_ = false;
    return owner_->publish_updates(generation_, updates_);
  }

  void abort() noexcept override {
    if (active_) {
      active_ = false;
      updates_.clear();
    }
  }

private:
  InMemorySearchIndex *owner_;
  std::uint64_t generation_;
  std::vector<model::TorrentRecord> updates_;
  bool active_{true};
};

InMemorySearchIndex::InMemorySearchIndex(SearchClassificationOptions options)
    : options_(std::move(options)) {
  auto initial = std::make_shared<State>();
  initial->classification.enabled = options_.enabled;
  initial->classification.learned_enabled =
      options_.enabled && options_.learned.enabled;
  state_ = std::move(initial);
}

core::Result<std::unique_ptr<SearchRebuildSession>>
InMemorySearchIndex::begin_rebuild(std::uint64_t source_generation) {
  return std::unique_ptr<SearchRebuildSession>{
      std::make_unique<MemoryRebuildSession>(*this, source_generation)};
}

core::Result<std::unique_ptr<SearchUpdateSession>>
InMemorySearchIndex::begin_update(std::uint64_t source_generation) {
  return std::unique_ptr<SearchUpdateSession>{
      std::make_unique<MemoryUpdateSession>(*this, source_generation)};
}

core::Result<void>
InMemorySearchIndex::publish(std::shared_ptr<const State> replacement) {
  std::unique_lock lock{mutex_};
  if (replacement->source_generation < state_->source_generation)
    return std::unexpected(core::Error{
        core::ErrorCode::Conflict,
        "Search rebuild cannot replace a newer canonical generation"});
  state_ = std::move(replacement);
  return {};
}

core::Result<void> InMemorySearchIndex::publish_updates(
    std::uint64_t source_generation,
    std::span<const model::TorrentRecord> updates) {
  std::unique_lock lock{mutex_};
  if (source_generation < state_->source_generation)
    return std::unexpected(core::Error{
        core::ErrorCode::Conflict,
        "Search update cannot replace a newer canonical generation"});
  auto replacement = std::make_shared<State>();
  replacement->source_generation = source_generation;
  replacement->records = state_->records;
  for (const auto &update : updates) {
    const auto existing =
        std::ranges::find_if(replacement->records, [&](const auto &indexed) {
          return indexed.record.info_hash == update.info_hash;
        });
    IndexedRecord indexed{.record = update};
    if (existing == replacement->records.end())
      replacement->records.push_back(std::move(indexed));
    else
      *existing = std::move(indexed);
  }
  rebuild_classification(*replacement);
  rebuild_text_candidates(*replacement);
  state_ = std::move(replacement);
  return {};
}

core::Result<void> MemoryRebuildSession::commit() {
  if (!active_)
    return std::unexpected(
        core::Error{core::ErrorCode::InvalidArgument,
                    "Search rebuild session is no longer active"});
  active_ = false;
  auto state = std::make_shared<InMemorySearchIndex::State>();
  state->source_generation = generation_;
  state->records.reserve(records_.size());
  for (auto &record : records_) {
    state->records.push_back(
        InMemorySearchIndex::IndexedRecord{.record = std::move(record)});
  }
  owner_->rebuild_classification(*state);
  owner_->rebuild_text_candidates(*state);
  return owner_->publish(std::move(state));
}

void InMemorySearchIndex::rebuild_classification(State &state) const {
  std::vector<classification::LearnedTrainingExample> examples;
  examples.reserve(state.records.size());
  for (auto &record : state.records) {
    record.deterministic =
        classification::Classification{.info_hash = record.record.info_hash};
    if (options_.enabled)
      record.deterministic =
          classification::classify(record.record, options_.classifier);
    examples.push_back(
        {.record = &record.record, .classification = &record.deterministic});
  }
  auto learned_options = options_.learned;
  learned_options.enabled = options_.enabled && learned_options.enabled;
  const auto learned = classification::LearnedContentClassifier::train(
      examples, options_.classifier, learned_options);
  for (auto &record : state.records) {
    record.classification = learned.apply(record.record, record.deterministic);
    record.categories = classification::media_categories(
        record.classification, options_.category_minimum,
        options_.adult_minimum);
  }
  state.classification = summarize(state.records, learned.stats());
}

void InMemorySearchIndex::rebuild_text_candidates(State &state) const {
  state.trigrams.clear();
  std::unordered_set<std::uint32_t> record_trigrams;
  for (std::size_t ordinal = 0; ordinal < state.records.size(); ++ordinal) {
    record_trigrams.clear();
    const auto &record = state.records[ordinal].record;
    add_trigrams(hex(record.info_hash), record_trigrams);
    if (record.name)
      add_trigrams(folded(*record.name), record_trigrams);
    for (const auto &file : record.files)
      add_trigrams(folded(file.path), record_trigrams);
    for (const auto key : record_trigrams)
      state.trigrams[key].push_back(ordinal);
  }
}

ClassificationIndexStats InMemorySearchIndex::summarize(
    std::span<const IndexedRecord> records,
    const classification::LearnedClassifierStats &learned) const noexcept {
  ClassificationIndexStats result{
      .enabled = options_.enabled,
      .total_records = records.size(),
      .learned_enabled = learned.enabled,
      .learned_ready = learned.ready,
      .learned_training_records = learned.training_records,
      .learned_eligible_kinds = learned.eligible_kinds,
      .learned_vocabulary_size = learned.vocabulary_size};
  for (const auto &record : records) {
    ++result.states[std::to_underlying(record.classification.state)];
    if (record.classification.input_truncated)
      ++result.input_truncated;
    if (classification::label_confidence(record.classification,
                                         classification::ContentLabel::Adult))
      ++result.adult_labeled;
    if (std::ranges::contains(record.classification.evidence,
                              classification::EvidenceCode::LearnedContentModel,
                              &classification::Evidence::code))
      ++result.learned_classified_records;
    for (const auto category : record.categories)
      ++result.categories[std::to_underlying(category)];
  }
  return result;
}

core::Result<SearchResult>
InMemorySearchIndex::search(const SearchQuery &query) const {
  if (query.limit == 0 || query.limit > 1'000)
    return std::unexpected(
        invalid("Search result limit must be between 1 and 1000"));
  if (query.minimum_size && query.maximum_size &&
      *query.minimum_size > *query.maximum_size)
    return std::unexpected(invalid("Search minimum size exceeds maximum size"));
  if (query.minimum_file_count && query.maximum_file_count &&
      *query.minimum_file_count > *query.maximum_file_count)
    return std::unexpected(
        invalid("Search minimum file count exceeds maximum file count"));
  if (query.first_seen_at_or_after && query.last_seen_at_or_before &&
      *query.first_seen_at_or_after > *query.last_seen_at_or_before)
    return std::unexpected(
        invalid("Search first-seen lower bound exceeds last-seen upper bound"));
  const auto query_terms = terms(query.text);
  std::shared_ptr<const State> state;
  {
    std::shared_lock lock{mutex_};
    state = state_;
  }

  std::vector<SearchHit> matches;
  const auto candidates = candidate_ordinals(state->trigrams, query_terms);
  const auto visit = [&](std::size_t ordinal) {
    const auto &indexed = state->records[ordinal];
    const auto &record = indexed.record;
    // Observation materialization creates records before BEP 9 metadata is
    // available. Keep those placeholders in the derived index for later
    // enrichment and operational accounting, but do not expose an unknown
    // size as a real zero-byte torrent through native or Torznab search.
    if (!record.name || record.files.empty())
      return;
    const bool adult = contains_category(indexed.categories,
                                         classification::MediaCategory::Adult);
    if ((query.minimum_size && record.total_size < *query.minimum_size) ||
        (query.maximum_size && record.total_size > *query.maximum_size) ||
        (query.minimum_file_count &&
         record.files.size() < *query.minimum_file_count) ||
        (query.maximum_file_count &&
         record.files.size() > *query.maximum_file_count) ||
        (query.first_seen_at_or_after &&
         record.first_seen < *query.first_seen_at_or_after) ||
        (query.last_seen_at_or_before &&
         record.last_seen > *query.last_seen_at_or_before) ||
        (query.classification_state &&
         indexed.classification.state != *query.classification_state) ||
        (query.content_kind &&
         indexed.classification.kind != *query.content_kind) ||
        (query.minimum_kind_confidence &&
         !classification::at_least(indexed.classification.kind_confidence,
                                   *query.minimum_kind_confidence)) ||
        !contains_labels(indexed.classification, query.labels,
                         query.minimum_label_confidence) ||
        (query.adult_content == AdultContentMode::Exclude && adult) ||
        (query.adult_content == AdultContentMode::Only && !adult) ||
        (!query.categories.empty() &&
         !std::ranges::any_of(query.categories, [&](const auto category) {
           return contains_category(indexed.categories, category);
         })))
      return;
    const auto score = match_score(record, query_terms);
    if (!score)
      return;
    matches.push_back(SearchHit{.info_hash = record.info_hash,
                                .name = record.name,
                                .total_size = record.total_size,
                                .file_count = record.files.size(),
                                .first_seen = record.first_seen,
                                .last_seen = record.last_seen,
                                .score = *score,
                                .classification = indexed.classification,
                                .categories = indexed.categories});
  };
  if (candidates) {
    for (const auto ordinal : *candidates)
      visit(ordinal);
  } else {
    for (std::size_t ordinal = 0; ordinal < state->records.size(); ++ordinal)
      visit(ordinal);
  }
  std::ranges::sort(matches, [](const auto &left, const auto &right) {
    if (left.score != right.score)
      return left.score > right.score;
    if (left.last_seen != right.last_seen)
      return left.last_seen > right.last_seen;
    return left.info_hash.bytes < right.info_hash.bytes;
  });
  SearchResult result{.total_matches = matches.size(),
                      .source_generation = state->source_generation};
  if (query.offset < matches.size()) {
    const auto count = std::min(query.limit, matches.size() - query.offset);
    result.hits.insert(
        result.hits.end(),
        matches.begin() + static_cast<std::ptrdiff_t>(query.offset),
        matches.begin() + static_cast<std::ptrdiff_t>(query.offset + count));
  }
  return result;
}

std::uint64_t InMemorySearchIndex::source_generation() const noexcept {
  std::shared_lock lock{mutex_};
  return state_->source_generation;
}

ClassificationIndexStats
InMemorySearchIndex::classification_stats() const noexcept {
  std::shared_lock lock{mutex_};
  return state_->classification;
}

} // namespace sakuin::search

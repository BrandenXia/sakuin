export module sakuin.search.memory;

import std;

import sakuin.classification;
import sakuin.core.ids;
import sakuin.core.result;
import sakuin.core.time;
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
    core::InfoHash info_hash;
    core::Timestamp first_seen;
    core::Timestamp last_seen;
    std::optional<std::string> name;
    std::uint64_t total_size{};
    std::size_t file_count{};
    // Folded fields are separated by NUL. The first field is the info hash,
    // the optional second field is the display name, and the remainder are
    // file paths. This keeps one allocation instead of retaining the full
    // canonical file vector and one allocation per path.
    std::string search_fields;
    std::array<std::uint64_t, 2> trigram_filter{};
    classification::Classification classification;
    std::uint32_t category_mask{};
  };

  struct State {
    std::uint64_t source_generation{};
    std::vector<IndexedRecord> records;
    classification::LearnedContentClassifier learned;
    ClassificationIndexStats classification;
  };

  core::Result<void> publish(std::unique_ptr<State> replacement);
  core::Result<void>
  publish_updates(std::uint64_t source_generation,
                  std::span<const model::TorrentRecord> updates);
  IndexedRecord compact(const model::TorrentRecord &record,
                        classification::Classification classified) const;
  void
  rebuild_classification(State &state,
                         std::span<const model::TorrentRecord> records) const;
  ClassificationIndexStats summarize(
      std::span<const IndexedRecord> records,
      const classification::LearnedClassifierStats &learned) const noexcept;

  mutable std::shared_mutex mutex_;
  SearchClassificationOptions options_;
  std::unique_ptr<State> state_;
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
                  std::array<std::uint64_t, 2> &filter) {
  if (value.size() < 3)
    return;
  for (std::size_t offset = 0; offset + 2 < value.size(); ++offset) {
    const auto key = static_cast<std::uint64_t>(trigram(value, offset));
    const auto mixed = key * 0x9e3779b97f4a7c15ULL;
    for (const auto bit : {mixed & 127U, (mixed >> 17U) & 127U})
      filter[bit / 64U] |= std::uint64_t{1} << (bit % 64U);
  }
}

bool maybe_contains(const std::array<std::uint64_t, 2> &filter,
                    std::span<const std::string> query_terms) {
  for (const auto &term : query_terms) {
    std::array<std::uint64_t, 2> required{};
    add_trigrams(term, required);
    if ((filter[0] & required[0]) != required[0] ||
        (filter[1] & required[1]) != required[1])
      return false;
  }
  return true;
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
match_score(std::string_view fields, bool has_name,
            std::span<const std::string> query_terms) {
  if (query_terms.empty())
    return 0;

  std::uint32_t score{};
  for (const auto &term : query_terms) {
    bool found{};
    std::size_t index{};
    std::size_t offset{};
    while (offset < fields.size()) {
      const auto end = fields.find('\0', offset);
      const auto field = fields.substr(offset, end == std::string_view::npos
                                                   ? fields.size() - offset
                                                   : end - offset);
      if (field.contains(term)) {
        found = true;
        score += index == 1 && has_name ? 4U : 1U;
      }
      ++index;
      if (end == std::string_view::npos)
        break;
      offset = end + 1;
    }
    if (!found)
      return std::nullopt;
  }
  return score;
}

constexpr std::uint32_t
category_bit(classification::MediaCategory category) noexcept {
  return std::uint32_t{1} << std::to_underlying(category);
}

bool contains_category(std::uint32_t categories,
                       classification::MediaCategory expected) {
  return (categories & category_bit(expected)) != 0;
}

std::vector<classification::MediaCategory>
categories_from_mask(std::uint32_t mask) {
  std::vector<classification::MediaCategory> result;
  for (std::size_t value = 0; value < MediaCategoryCount; ++value) {
    const auto category = static_cast<classification::MediaCategory>(value);
    if (contains_category(mask, category))
      result.push_back(category);
  }
  return result;
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
    // Observation-only materializations cannot be searched or classified.
    // Exclude them before copying so a rebuild's working set is proportional
    // to metadata-complete records rather than the full DHT observation set.
    if (!record.name || record.files.empty())
      return {};
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
  auto initial = std::make_unique<State>();
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
InMemorySearchIndex::publish(std::unique_ptr<State> replacement) {
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
  for (const auto &update : updates) {
    const auto existing =
        std::ranges::find_if(state_->records, [&](const auto &indexed) {
          return indexed.info_hash == update.info_hash;
        });
    if (!update.name || update.files.empty()) {
      if (existing != state_->records.end())
        state_->records.erase(existing);
      continue;
    }
    auto deterministic =
        classification::Classification{.info_hash = update.info_hash};
    if (options_.enabled)
      deterministic = classification::classify(update, options_.classifier);
    auto classified = state_->learned.apply(update, std::move(deterministic));
    auto indexed = compact(update, std::move(classified));
    if (existing == state_->records.end())
      state_->records.push_back(std::move(indexed));
    else
      *existing = std::move(indexed);
  }
  state_->source_generation = source_generation;
  state_->classification = summarize(state_->records, state_->learned.stats());
  return {};
}

core::Result<void> MemoryRebuildSession::commit() {
  if (!active_)
    return std::unexpected(
        core::Error{core::ErrorCode::InvalidArgument,
                    "Search rebuild session is no longer active"});
  active_ = false;
  auto state = std::make_unique<InMemorySearchIndex::State>();
  state->source_generation = generation_;
  owner_->rebuild_classification(*state, records_);
  return owner_->publish(std::move(state));
}

InMemorySearchIndex::IndexedRecord
InMemorySearchIndex::compact(const model::TorrentRecord &record,
                             classification::Classification classified) const {
  IndexedRecord result{.info_hash = record.info_hash,
                       .first_seen = record.first_seen,
                       .last_seen = record.last_seen,
                       .name = record.name,
                       .total_size = record.total_size,
                       .file_count = record.files.size(),
                       .classification = std::move(classified)};
  std::size_t text_bytes = record.info_hash.bytes.size() * 2U + 1U;
  if (record.name)
    text_bytes += record.name->size() + 1U;
  for (const auto &file : record.files)
    text_bytes += file.path.size() + 1U;
  result.search_fields.reserve(text_bytes);
  const auto append_field = [&](std::string_view value) {
    const auto normalized = folded(value);
    add_trigrams(normalized, result.trigram_filter);
    result.search_fields.append(normalized);
    result.search_fields.push_back('\0');
  };
  append_field(hex(record.info_hash));
  if (record.name)
    append_field(*record.name);
  for (const auto &file : record.files)
    append_field(file.path);
  const auto categories = classification::media_categories(
      result.classification, options_.category_minimum, options_.adult_minimum);
  for (const auto category : categories)
    result.category_mask |= category_bit(category);
  return result;
}

void InMemorySearchIndex::rebuild_classification(
    State &state, std::span<const model::TorrentRecord> records) const {
  std::vector<classification::Classification> deterministic;
  deterministic.reserve(records.size());
  std::vector<classification::LearnedTrainingExample> examples;
  examples.reserve(records.size());
  for (const auto &record : records) {
    auto classified =
        classification::Classification{.info_hash = record.info_hash};
    if (options_.enabled)
      classified = classification::classify(record, options_.classifier);
    deterministic.push_back(std::move(classified));
    examples.push_back(
        {.record = &record, .classification = &deterministic.back()});
  }
  auto learned_options = options_.learned;
  learned_options.enabled = options_.enabled && learned_options.enabled;
  state.learned = classification::LearnedContentClassifier::train(
      examples, options_.classifier, learned_options);
  state.records.reserve(records.size());
  for (std::size_t index = 0; index < records.size(); ++index) {
    if (!records[index].name || records[index].files.empty())
      continue;
    auto classified =
        state.learned.apply(records[index], std::move(deterministic[index]));
    state.records.push_back(compact(records[index], std::move(classified)));
  }
  state.classification = summarize(state.records, state.learned.stats());
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
      .learned_vocabulary_size = learned.vocabulary_size,
      .estimated_memory_bytes =
          static_cast<std::uint64_t>(records.size()) * sizeof(IndexedRecord) +
          static_cast<std::uint64_t>(learned.vocabulary_size) *
              (64U + static_cast<std::uint64_t>(learned.eligible_kinds) * 64U)};
  for (const auto &record : records) {
    result.estimated_memory_bytes += record.search_fields.capacity();
    if (record.name)
      result.estimated_memory_bytes += record.name->capacity();
    result.estimated_memory_bytes += record.classification.labels.capacity() *
                                     sizeof(classification::LabelAssessment);
    result.estimated_memory_bytes += record.classification.evidence.capacity() *
                                     sizeof(classification::Evidence);
    for (const auto &evidence : record.classification.evidence)
      if (evidence.rule_id)
        result.estimated_memory_bytes += evidence.rule_id->capacity();
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
    for (std::size_t value = 0; value < MediaCategoryCount; ++value)
      if (record.category_mask & (std::uint32_t{1} << value))
        ++result.categories[value];
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
  std::shared_lock lock{mutex_};
  const auto &state = *state_;

  struct Match {
    std::size_t ordinal{};
    std::uint32_t score{};
  };
  std::vector<Match> matches;
  const auto visit = [&](std::size_t ordinal) {
    const auto &indexed = state.records[ordinal];
    // The projection excludes observation-only placeholders. Keep this guard
    // defensive so an invalid incremental record cannot expose an unknown
    // size as a real zero-byte torrent through native or Torznab search.
    if (!indexed.name || indexed.file_count == 0 ||
        !maybe_contains(indexed.trigram_filter, query_terms))
      return;
    const bool adult = contains_category(indexed.category_mask,
                                         classification::MediaCategory::Adult);
    if ((query.minimum_size && indexed.total_size < *query.minimum_size) ||
        (query.maximum_size && indexed.total_size > *query.maximum_size) ||
        (query.minimum_file_count &&
         indexed.file_count < *query.minimum_file_count) ||
        (query.maximum_file_count &&
         indexed.file_count > *query.maximum_file_count) ||
        (query.first_seen_at_or_after &&
         indexed.first_seen < *query.first_seen_at_or_after) ||
        (query.last_seen_at_or_before &&
         indexed.last_seen > *query.last_seen_at_or_before) ||
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
           return contains_category(indexed.category_mask, category);
         })))
      return;
    const auto score = match_score(indexed.search_fields,
                                   indexed.name.has_value(), query_terms);
    if (!score)
      return;
    matches.push_back({.ordinal = ordinal, .score = *score});
  };
  for (std::size_t ordinal = 0; ordinal < state.records.size(); ++ordinal)
    visit(ordinal);
  std::ranges::sort(matches, [&](const auto &left, const auto &right) {
    if (left.score != right.score)
      return left.score > right.score;
    const auto &left_record = state.records[left.ordinal];
    const auto &right_record = state.records[right.ordinal];
    if (left_record.last_seen != right_record.last_seen)
      return left_record.last_seen > right_record.last_seen;
    return left_record.info_hash.bytes < right_record.info_hash.bytes;
  });
  SearchResult result{.total_matches = matches.size(),
                      .source_generation = state.source_generation};
  if (query.offset < matches.size()) {
    const auto count = std::min(query.limit, matches.size() - query.offset);
    result.hits.reserve(count);
    for (std::size_t match = query.offset; match < query.offset + count;
         ++match) {
      const auto &found = matches[match];
      const auto &indexed = state.records[found.ordinal];
      result.hits.push_back(
          {.info_hash = indexed.info_hash,
           .name = indexed.name,
           .total_size = indexed.total_size,
           .file_count = indexed.file_count,
           .first_seen = indexed.first_seen,
           .last_seen = indexed.last_seen,
           .score = found.score,
           .classification = indexed.classification,
           .categories = categories_from_mask(indexed.category_mask)});
    }
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

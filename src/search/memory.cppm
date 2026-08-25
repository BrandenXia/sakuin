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
    classification::Classification classification;
    std::vector<classification::MediaCategory> categories;
  };

  struct State {
    std::uint64_t source_generation{};
    std::vector<IndexedRecord> records;
    ClassificationIndexStats classification;
  };

  core::Result<void> publish(std::shared_ptr<const State> replacement);
  core::Result<void>
  publish_updates(std::uint64_t source_generation,
                  std::span<const model::TorrentRecord> updates);
  ClassificationIndexStats
  summarize(std::span<const IndexedRecord> records) const noexcept;

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
  auto replacement = std::make_shared<State>(*state_);
  replacement->source_generation = source_generation;
  for (const auto &update : updates) {
    const auto existing =
        std::ranges::find_if(replacement->records, [&](const auto &indexed) {
          return indexed.record.info_hash == update.info_hash;
        });
    classification::Classification classified{.info_hash = update.info_hash};
    if (options_.enabled)
      classified = classification::classify(update, options_.classifier);
    IndexedRecord indexed{.record = update,
                          .classification = std::move(classified)};
    indexed.categories = classification::media_categories(
        indexed.classification, options_.category_minimum,
        options_.adult_minimum);
    if (existing == replacement->records.end())
      replacement->records.push_back(std::move(indexed));
    else
      *existing = std::move(indexed);
  }
  replacement->classification = summarize(replacement->records);
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
    classification::Classification classified{.info_hash = record.info_hash};
    if (owner_->options_.enabled)
      classified =
          classification::classify(record, owner_->options_.classifier);
    InMemorySearchIndex::IndexedRecord indexed{
        .record = std::move(record), .classification = std::move(classified)};
    indexed.categories = classification::media_categories(
        indexed.classification, owner_->options_.category_minimum,
        owner_->options_.adult_minimum);
    state->records.push_back(std::move(indexed));
  }
  state->classification = owner_->summarize(state->records);
  return owner_->publish(std::move(state));
}

ClassificationIndexStats InMemorySearchIndex::summarize(
    std::span<const IndexedRecord> records) const noexcept {
  ClassificationIndexStats result{.enabled = options_.enabled,
                                  .total_records = records.size()};
  for (const auto &record : records) {
    ++result.states[std::to_underlying(record.classification.state)];
    if (record.classification.input_truncated)
      ++result.input_truncated;
    if (classification::label_confidence(record.classification,
                                         classification::ContentLabel::Adult))
      ++result.adult_labeled;
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
  for (const auto &indexed : state->records) {
    const auto &record = indexed.record;
    // Observation materialization creates records before BEP 9 metadata is
    // available. Keep those placeholders in the derived index for later
    // enrichment and operational accounting, but do not expose an unknown
    // size as a real zero-byte torrent through native or Torznab search.
    if (!record.name || record.files.empty())
      continue;
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
      continue;
    const auto score = match_score(record, query_terms);
    if (!score)
      continue;
    matches.push_back(SearchHit{.info_hash = record.info_hash,
                                .name = record.name,
                                .total_size = record.total_size,
                                .file_count = record.files.size(),
                                .first_seen = record.first_seen,
                                .last_seen = record.last_seen,
                                .score = *score,
                                .classification = indexed.classification,
                                .categories = indexed.categories});
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

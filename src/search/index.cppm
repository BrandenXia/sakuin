export module sakuin.search.index;

import std;

import sakuin.classification;
import sakuin.core.ids;
import sakuin.core.result;
import sakuin.core.time;
import sakuin.model.torrent;

export namespace sakuin::search {

enum class AdultContentMode { Include, Exclude, Only };

struct SearchClassificationOptions {
  bool enabled{true};
  classification::ClassifierOptions classifier;
  classification::LearnedClassifierOptions learned;
  classification::Confidence category_minimum{
      classification::Confidence::Medium};
  classification::Confidence adult_minimum{classification::Confidence::High};
};

struct SearchQuery {
  std::string text;
  std::optional<std::uint64_t> minimum_size;
  std::optional<std::uint64_t> maximum_size;
  std::optional<std::size_t> minimum_file_count;
  std::optional<std::size_t> maximum_file_count;
  std::optional<core::Timestamp> first_seen_at_or_after;
  std::optional<core::Timestamp> last_seen_at_or_before;
  std::optional<classification::ClassificationState> classification_state;
  std::optional<classification::ContentKind> content_kind;
  std::optional<classification::Confidence> minimum_kind_confidence;
  // Every requested label must be present at the configured minimum. These
  // facets describe classifier output and never override Adult visibility.
  std::vector<classification::ContentLabel> labels;
  classification::Confidence minimum_label_confidence{
      classification::Confidence::Low};
  // Categories are ORed. Adult visibility is intentionally separate from
  // classification and defaults to retaining all matching records.
  std::vector<classification::MediaCategory> categories;
  AdultContentMode adult_content{AdultContentMode::Include};
  std::size_t offset{};
  std::size_t limit{50};
};

struct SearchHit {
  core::InfoHash info_hash;
  std::optional<std::string> name;
  std::uint64_t total_size{};
  std::size_t file_count{};
  core::Timestamp first_seen;
  core::Timestamp last_seen;
  std::uint32_t score{};
  classification::Classification classification;
  std::vector<classification::MediaCategory> categories;
};

struct SearchResult {
  std::vector<SearchHit> hits;
  std::uint64_t total_matches{};
  std::uint64_t source_generation{};
};

inline constexpr std::size_t ClassificationStateCount =
    std::to_underlying(classification::ClassificationState::Unknown) + 1U;
inline constexpr std::size_t MediaCategoryCount =
    std::to_underlying(classification::MediaCategory::Other) + 1U;

struct ClassificationIndexStats {
  bool enabled{};
  std::uint32_t algorithm_version{classification::AlgorithmVersion};
  std::uint64_t total_records{};
  std::array<std::uint64_t, ClassificationStateCount> states{};
  std::uint64_t input_truncated{};
  // Counts every Adult label produced by the classifier. The Adult semantic
  // category below applies the configured confidence threshold separately.
  std::uint64_t adult_labeled{};
  bool learned_enabled{};
  bool learned_ready{};
  std::uint64_t learned_training_records{};
  std::uint64_t learned_classified_records{};
  std::uint64_t learned_eligible_kinds{};
  std::uint64_t learned_vocabulary_size{};
  std::array<std::uint64_t, MediaCategoryCount> categories{};

  std::uint64_t
  state_count(classification::ClassificationState state) const noexcept {
    return states[std::to_underlying(state)];
  }

  std::uint64_t
  category_count(classification::MediaCategory category) const noexcept {
    return categories[std::to_underlying(category)];
  }
};

class SearchRebuildSession {
public:
  virtual ~SearchRebuildSession() = default;
  virtual core::Result<void> append(const model::TorrentRecord &record) = 0;
  virtual core::Result<void> commit() = 0;
  virtual void abort() noexcept = 0;
};

class SearchUpdateSession {
public:
  virtual ~SearchUpdateSession() = default;
  virtual core::Result<void> upsert(const model::TorrentRecord &record) = 0;
  virtual core::Result<void> commit() = 0;
  virtual void abort() noexcept = 0;
};

class SearchIndex {
public:
  virtual ~SearchIndex() = default;

  SearchIndex(const SearchIndex &) = delete;
  SearchIndex &operator=(const SearchIndex &) = delete;

  // Rebuild sessions publish replacement state atomically. Search indexes are
  // derived views; source_generation identifies the canonical snapshot from
  // which the view can be reproduced.
  virtual core::Result<std::unique_ptr<SearchRebuildSession>>
  begin_rebuild(std::uint64_t source_generation) = 0;
  // Update sessions atomically advance a derived view using the latest values
  // from an append-only canonical change stream.
  virtual core::Result<std::unique_ptr<SearchUpdateSession>>
  begin_update(std::uint64_t source_generation) = 0;
  virtual core::Result<SearchResult> search(const SearchQuery &query) const = 0;
  virtual std::uint64_t source_generation() const noexcept = 0;
  virtual ClassificationIndexStats classification_stats() const noexcept = 0;

protected:
  SearchIndex() = default;
};

} // namespace sakuin::search

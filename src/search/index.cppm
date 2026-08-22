export module sakuin.search.index;

import std;

import sakuin.core.ids;
import sakuin.core.result;
import sakuin.core.time;
import sakuin.model.torrent;

export namespace sakuin::search {

struct SearchQuery {
  std::string text;
  std::optional<std::uint64_t> minimum_size;
  std::optional<std::uint64_t> maximum_size;
  std::optional<std::size_t> minimum_file_count;
  std::optional<std::size_t> maximum_file_count;
  std::optional<core::Timestamp> first_seen_at_or_after;
  std::optional<core::Timestamp> last_seen_at_or_before;
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
};

struct SearchResult {
  std::vector<SearchHit> hits;
  std::uint64_t total_matches{};
  std::uint64_t source_generation{};
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

protected:
  SearchIndex() = default;
};

} // namespace sakuin::search

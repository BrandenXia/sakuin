export module sakuin.search.rebuild;

import std;

import sakuin.core.result;
import sakuin.model.torrent;
import sakuin.search.index;
import sakuin.storage.dataset.snapshot;
import sakuin.storage.dataset.torrents;

export namespace sakuin::search {

struct SearchRebuildResult {
  std::uint64_t source_generation{};
  std::uint64_t records_indexed{};
};

struct SearchSynchronizationResult {
  std::uint64_t source_generation{};
  std::uint64_t records_indexed{};
  bool full_rebuild{};
  storage::TorrentChangeCursor cursor;
};

core::Result<SearchRebuildResult> rebuild(
    const storage::Snapshot<model::TorrentRecord, storage::AllTorrents> &source,
    SearchIndex &destination);

// Applies only newly appended keyed values when the cursor prefix is still
// present. Compaction or cursor loss transparently selects an atomic rebuild.
core::Result<SearchSynchronizationResult>
synchronize(const storage::TorrentDataset &source, SearchIndex &destination,
            storage::TorrentChangeCursor cursor = {});

} // namespace sakuin::search

namespace sakuin::search {

core::Result<SearchRebuildResult> rebuild(
    const storage::Snapshot<model::TorrentRecord, storage::AllTorrents> &source,
    SearchIndex &destination) {
  auto session = destination.begin_rebuild(source.id().generation);
  if (!session)
    return std::unexpected(session.error());
  auto records = source.scan({});
  if (!records)
    return std::unexpected(records.error());
  SearchRebuildResult result{.source_generation = source.id().generation};
  while (true) {
    auto next = (*records)->next();
    if (!next)
      return std::unexpected(next.error());
    if (!*next)
      break;
    if (auto appended = (*session)->append(**next); !appended)
      return std::unexpected(appended.error());
    ++result.records_indexed;
  }
  if (auto committed = (*session)->commit(); !committed)
    return std::unexpected(committed.error());
  return result;
}

core::Result<SearchSynchronizationResult>
synchronize(const storage::TorrentDataset &source, SearchIndex &destination,
            storage::TorrentChangeCursor cursor) {
  auto changes = source.changes_since(cursor);
  if (!changes)
    return std::unexpected(changes.error());
  SearchSynchronizationResult result{
      .source_generation = changes->cursor.source_generation,
      .full_rebuild = changes->full_rebuild_required,
      .cursor = changes->cursor};

  if (changes->full_rebuild_required) {
    auto session = destination.begin_rebuild(result.source_generation);
    if (!session)
      return std::unexpected(session.error());
    while (true) {
      auto next = changes->records->next();
      if (!next)
        return std::unexpected(next.error());
      if (!*next)
        break;
      if (auto appended = (*session)->append(**next); !appended)
        return std::unexpected(appended.error());
      ++result.records_indexed;
    }
    if (auto committed = (*session)->commit(); !committed)
      return std::unexpected(committed.error());
  } else {
    auto session = destination.begin_update(result.source_generation);
    if (!session)
      return std::unexpected(session.error());
    while (true) {
      auto next = changes->records->next();
      if (!next)
        return std::unexpected(next.error());
      if (!*next)
        break;
      if (auto updated = (*session)->upsert(**next); !updated)
        return std::unexpected(updated.error());
      ++result.records_indexed;
    }
    if (auto committed = (*session)->commit(); !committed)
      return std::unexpected(committed.error());
  }
  return result;
}

} // namespace sakuin::search

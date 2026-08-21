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

core::Result<SearchRebuildResult>
rebuild(const storage::Snapshot<model::TorrentRecord, storage::AllTorrents> &source,
        SearchIndex &destination);

} // namespace sakuin::search

namespace sakuin::search {

core::Result<SearchRebuildResult>
rebuild(const storage::Snapshot<model::TorrentRecord, storage::AllTorrents> &source,
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

} // namespace sakuin::search

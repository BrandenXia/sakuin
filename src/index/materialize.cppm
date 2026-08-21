export module sakuin.index.materialize;

import std;

import sakuin.core.ids;
import sakuin.core.result;
import sakuin.core.time;
import sakuin.model.observation;
import sakuin.model.torrent;
import sakuin.storage.dataset.observations;
import sakuin.storage.dataset.snapshot;
import sakuin.storage.dataset.torrents;

export namespace sakuin::index {

struct MaterializationResult {
  std::uint64_t observations_read{};
  std::uint64_t torrents_written{};
  std::uint64_t generation{};
};

class ObservationMaterializer {
public:
  // Rebuilds a derived torrent dataset from canonical observations. Requiring
  // an empty destination makes replacement atomic at the directory/catalog
  // level and avoids mixing results produced by different algorithms.
  static core::Result<MaterializationResult>
  rebuild(const storage::Snapshot<model::ObservationRecord,
                                  storage::AllObservations> &observations,
          storage::TorrentDataset &destination);
};

} // namespace sakuin::index

namespace sakuin::index {
namespace {

struct InfoHashHasher {
  std::size_t operator()(const core::InfoHash &hash) const noexcept {
    std::size_t value = 1469598103934665603ULL;
    for (const auto byte : hash.bytes) {
      value ^= byte;
      value *= 1099511628211ULL;
    }
    return value;
  }
};

struct SeenRange {
  core::Timestamp first;
  core::Timestamp last;
};

} // namespace

core::Result<MaterializationResult> ObservationMaterializer::rebuild(
    const storage::Snapshot<model::ObservationRecord,
                            storage::AllObservations> &observations,
    storage::TorrentDataset &destination) {
  auto destination_snapshot = destination.keyed_snapshot();
  if (!destination_snapshot)
    return std::unexpected(destination_snapshot.error());
  if ((*destination_snapshot)->id().generation != 0)
    return std::unexpected(core::Error{
        core::ErrorCode::Conflict,
        "Torrent materialization rebuild requires an empty destination"});

  auto stream = observations.scan({});
  if (!stream)
    return std::unexpected(stream.error());
  std::unordered_map<core::InfoHash, SeenRange, InfoHashHasher> ranges;
  MaterializationResult result{};
  while (true) {
    auto next = (*stream)->next();
    if (!next)
      return std::unexpected(next.error());
    if (!*next)
      break;
    ++result.observations_read;
    const auto [entry, inserted] = ranges.try_emplace(
        (*next)->info_hash,
        SeenRange{.first = (*next)->observed_at,
                  .last = (*next)->observed_at});
    if (!inserted) {
      entry->second.first =
          std::min(entry->second.first, (*next)->observed_at);
      entry->second.last =
          std::max(entry->second.last, (*next)->observed_at);
    }
  }

  std::vector<std::pair<core::InfoHash, SeenRange>> ordered{ranges.begin(),
                                                            ranges.end()};
  std::ranges::sort(ordered, {}, [](const auto &entry) {
    return entry.first.bytes;
  });

  auto write = destination.begin_write();
  if (!write)
    return std::unexpected(write.error());
  for (const auto &[hash, range] : ordered) {
    model::TorrentRecord torrent{.info_hash = hash,
                                 .first_seen = range.first,
                                 .last_seen = range.last,
                                 .name = std::nullopt,
                                 .total_size = 0,
                                 .files = {}};
    if (auto appended = (*write)->append(torrent); !appended)
      return std::unexpected(appended.error());
  }
  auto committed = (*write)->commit();
  if (!committed)
    return std::unexpected(committed.error());
  result.torrents_written = ordered.size();
  result.generation = committed->generation;
  return result;
}

} // namespace sakuin::index

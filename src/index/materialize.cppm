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

struct MaterializationCheckpoint {
  std::uint64_t observations_processed{};
  std::uint64_t source_generation{};
  std::uint64_t destination_generation{};
  friend bool operator==(const MaterializationCheckpoint &,
                         const MaterializationCheckpoint &) = default;
};

struct IncrementalMaterializationResult {
  std::uint64_t observations_read{};
  std::uint64_t observations_skipped{};
  std::uint64_t torrents_updated{};
  MaterializationCheckpoint checkpoint;
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

  // Advances from a logical observation ordinal. RowV1 compaction preserves
  // record order, so the ordinal remains stable when physical segments are
  // replaced. The returned checkpoint must be durably published only after
  // this call succeeds; replaying an older checkpoint is idempotent.
  static core::Result<IncrementalMaterializationResult>
  advance(const storage::Snapshot<model::ObservationRecord,
                                  storage::AllObservations> &observations,
          storage::TorrentDataset &destination,
          MaterializationCheckpoint checkpoint = {});
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
    const storage::Snapshot<model::ObservationRecord, storage::AllObservations>
        &observations,
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
        SeenRange{.first = (*next)->observed_at, .last = (*next)->observed_at});
    if (!inserted) {
      entry->second.first = std::min(entry->second.first, (*next)->observed_at);
      entry->second.last = std::max(entry->second.last, (*next)->observed_at);
    }
  }

  std::vector<std::pair<core::InfoHash, SeenRange>> ordered{ranges.begin(),
                                                            ranges.end()};
  std::ranges::sort(ordered, {},
                    [](const auto &entry) { return entry.first.bytes; });

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

core::Result<IncrementalMaterializationResult> ObservationMaterializer::advance(
    const storage::Snapshot<model::ObservationRecord, storage::AllObservations>
        &observations,
    storage::TorrentDataset &destination,
    MaterializationCheckpoint checkpoint) {
  if (observations.id().generation < checkpoint.source_generation)
    return std::unexpected(core::Error{
        core::ErrorCode::Conflict,
        "Observation materialization checkpoint is ahead of its source"});
  auto stream = observations.scan({});
  if (!stream)
    return std::unexpected(stream.error());

  IncrementalMaterializationResult result{.checkpoint = checkpoint};
  std::unordered_map<core::InfoHash, SeenRange, InfoHashHasher> ranges;
  std::uint64_t ordinal{};
  while (true) {
    auto next = (*stream)->next();
    if (!next)
      return std::unexpected(next.error());
    if (!*next)
      break;
    if (ordinal++ < checkpoint.observations_processed) {
      ++result.observations_skipped;
      continue;
    }
    ++result.observations_read;
    const auto [entry, inserted] = ranges.try_emplace(
        (*next)->info_hash,
        SeenRange{.first = (*next)->observed_at, .last = (*next)->observed_at});
    if (!inserted) {
      entry->second.first = std::min(entry->second.first, (*next)->observed_at);
      entry->second.last = std::max(entry->second.last, (*next)->observed_at);
    }
  }
  if (ordinal < checkpoint.observations_processed)
    return std::unexpected(core::Error{
        core::ErrorCode::CorruptSegment,
        "Observation history is shorter than its materialization checkpoint"});

  std::vector<storage::TorrentObservationRange> updates;
  updates.reserve(ranges.size());
  for (const auto &[hash, range] : ranges)
    updates.push_back({.info_hash = hash,
                       .first_seen = range.first,
                       .last_seen = range.last});
  std::ranges::sort(updates, {},
                    [](const auto &entry) { return entry.info_hash.bytes; });
  // Metadata enrichment and maintenance publish to the same optimistic
  // catalog. Rebase the already-aggregated observation ranges when one of
  // those writers wins the generation race; rescanning the observation
  // history is unnecessary and an older checkpoint remains safe to replay.
  constexpr std::size_t maximum_conflict_attempts = 8;
  core::Result<storage::TorrentObservationMergeResult> committed =
      std::unexpected(core::Error{core::ErrorCode::Internal,
                                  "Materialization retry loop did not run"});
  for (std::size_t attempt = 0; attempt < maximum_conflict_attempts;
       ++attempt) {
    committed = destination.merge_observations(updates);
    if (committed || committed.error().code != core::ErrorCode::Conflict ||
        attempt + 1 == maximum_conflict_attempts)
      break;
    std::this_thread::yield();
  }
  if (!committed)
    return std::unexpected(committed.error());
  result.torrents_updated = committed->records_written;
  result.checkpoint = {.observations_processed = ordinal,
                       .source_generation = observations.id().generation,
                       .destination_generation = committed->commit.generation};
  return result;
}

} // namespace sakuin::index

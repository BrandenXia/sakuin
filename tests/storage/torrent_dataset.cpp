import std;

import sakuin.core;
import sakuin.model.torrent;
import sakuin.storage;
import sakuin.storage.dataset.torrents;

namespace {

struct TemporaryDirectory {
  std::filesystem::path path;
  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path, ignored);
  }
};

sakuin::core::InfoHash hash(std::uint8_t value) {
  sakuin::core::InfoHash result;
  result.bytes.fill(value);
  return result;
}

sakuin::model::TorrentRecord torrent(std::uint8_t key, std::string name,
                                     std::int64_t last_seen) {
  using namespace sakuin;
  return {.info_hash = hash(key),
          .first_seen = core::Timestamp{core::Timestamp::duration{10}},
          .last_seen = core::Timestamp{core::Timestamp::duration{last_seen}},
          .name = std::move(name),
          .total_size = 42,
          .files = {{"file.bin", 42}}};
}

} // namespace

int main() {
  using namespace sakuin;
  const auto nonce = std::to_string(
      std::chrono::steady_clock::now().time_since_epoch().count());
  TemporaryDirectory directory{std::filesystem::temp_directory_path() /
                               ("sakuin-torrents-" + nonce)};
  storage::LocalBlobStore blobs{directory.path / "blobs"};
  auto catalog =
      storage::LocalManifestCatalog::open(directory.path / "catalog", blobs);
  if (!catalog)
    return 1;
  storage::TorrentDataset dataset{blobs, **catalog};

  auto empty = dataset.keyed_snapshot();
  if (!empty)
    return 2;
  auto missing = (*empty)->get(hash(1));
  if (!missing || missing->has_value())
    return 3;

  auto first_write = dataset.begin_write();
  const auto first_a = torrent(1, "first-a", 20);
  const auto first_b = torrent(2, "first-b", 30);
  const std::array initial{first_a, first_b};
  if (!first_write || !(*first_write)->append(initial))
    return 4;
  auto first_commit = (*first_write)->commit();
  if (!first_commit || first_commit->generation != 1)
    return 5;

  auto first_snapshot = dataset.keyed_snapshot();
  auto first_value = (*first_snapshot)->get(hash(1));
  if (!first_value || !*first_value || (*first_value)->name != "first-a")
    return 6;

  auto second_write = dataset.begin_write();
  if (!second_write || !(*second_write)->append(torrent(1, "second-a", 40)) ||
      !(*second_write)->append(torrent(1, "latest-a", 50)))
    return 7;
  auto second_commit = (*second_write)->commit();
  if (!second_commit || second_commit->generation != 2)
    return 8;

  auto current = dataset.keyed_snapshot();
  auto latest = (*current)->get(hash(1));
  auto unchanged = (*current)->get(hash(2));
  if (!latest || !*latest || (*latest)->name != "latest-a" || !unchanged ||
      !*unchanged || (*unchanged)->name != "first-b")
    return 9;
  auto still_old = (*first_snapshot)->get(hash(1));
  if (!still_old || !*still_old || (*still_old)->name != "first-a")
    return 10;

  auto stream = (*current)->scan({});
  std::unordered_map<std::uint8_t, std::string> values;
  while (true) {
    auto next = (*stream)->next();
    if (!next)
      return 11;
    if (!*next)
      break;
    values.emplace((*next)->info_hash.bytes[0], *(*next)->name);
  }
  if (values.size() != 2 || values[1] != "latest-a" || values[2] != "first-b")
    return 12;

  auto stale = dataset.begin_write();
  auto winner = dataset.begin_write();
  if (!stale || !winner || !(*stale)->append(torrent(3, "stale", 60)) ||
      !(*winner)->append(torrent(4, "winner", 60)) || !(*winner)->commit())
    return 13;
  auto conflict = (*stale)->commit();
  if (conflict || conflict.error().code != core::ErrorCode::Conflict)
    return 14;

  auto placeholder_write = dataset.begin_write();
  model::TorrentRecord placeholder{
      .info_hash = hash(5),
      .first_seen = core::Timestamp{core::Timestamp::duration{5}},
      .last_seen = core::Timestamp{core::Timestamp::duration{80}},
      .name = std::nullopt,
      .total_size = 0,
      .files = {}};
  if (!placeholder_write || !(*placeholder_write)->append(placeholder) ||
      !(*placeholder_write)->commit())
    return 15;
  auto enriched = torrent(5, "metadata", 40);
  enriched.first_seen = core::Timestamp{core::Timestamp::duration{20}};
  auto enrichment = dataset.enrich(std::move(enriched));
  if (!enrichment)
    return 16;
  auto enriched_snapshot = dataset.keyed_snapshot();
  auto enriched_value = (*enriched_snapshot)->get(hash(5));
  if (!enriched_value || !*enriched_value ||
      (*enriched_value)->name != "metadata" ||
      (*enriched_value)->first_seen != placeholder.first_seen ||
      (*enriched_value)->last_seen != placeholder.last_seen ||
      (*enriched_value)->total_size != 42 ||
      (*enriched_value)->files.size() != 1)
    return 17;

  auto reopened =
      storage::LocalManifestCatalog::open(directory.path / "catalog", blobs);
  if (!reopened)
    return 18;
  storage::TorrentDataset restarted{blobs, **reopened};
  auto restarted_snapshot = restarted.keyed_snapshot();
  auto restarted_value = (*restarted_snapshot)->get(hash(1));
  if (!restarted_value || !*restarted_value ||
      (*restarted_value)->name != "latest-a")
    return 19;

  auto compacted =
      restarted.compact({.minimum_segment_count = 2,
                         .target_block_size = 96,
                         .compression = storage::CompressionCodec::Zstd,
                         .compression_level = 5});
  if (!compacted || compacted->segments_created != 1 ||
      compacted->segments_removed < 2 || compacted->bytes_after == 0)
    return 20;
  auto compacted_manifest = (*reopened)->pin_current();
  if (!compacted_manifest ||
      (*compacted_manifest)->manifest().segments.size() != 1 ||
      (*compacted_manifest)->manifest().segments[0].tier !=
          storage::SegmentTier::Warm ||
      (*compacted_manifest)->manifest().segments[0].format_version !=
          storage::StorageFormatVersion{1, 2})
    return 21;
  auto compacted_snapshot = restarted.keyed_snapshot();
  auto compacted_latest = (*compacted_snapshot)->get(hash(1));
  auto compacted_unchanged = (*compacted_snapshot)->get(hash(2));
  auto compacted_enriched = (*compacted_snapshot)->get(hash(5));
  if (!compacted_latest || !*compacted_latest ||
      (*compacted_latest)->name != "latest-a" || !compacted_unchanged ||
      !*compacted_unchanged || (*compacted_unchanged)->name != "first-b" ||
      !compacted_enriched || !*compacted_enriched ||
      (*compacted_enriched)->name != "metadata")
    return 22;
  auto historical = (*first_snapshot)->get(hash(1));
  if (!historical || !*historical || (*historical)->name != "first-a")
    return 23;

  auto post_compaction_write = restarted.begin_write();
  if (!post_compaction_write ||
      !(*post_compaction_write)->append(torrent(1, "post-warm", 90)) ||
      !(*post_compaction_write)->commit())
    return 24;
  auto mixed_snapshot = restarted.keyed_snapshot();
  auto mixed_latest = (*mixed_snapshot)->get(hash(1));
  auto mixed_warm = (*mixed_snapshot)->get(hash(2));
  if (!mixed_latest || !*mixed_latest || (*mixed_latest)->name != "post-warm" ||
      !mixed_warm || !*mixed_warm || (*mixed_warm)->name != "first-b")
    return 25;

  const auto deferred_generation = (*reopened)->current_id().generation;
  auto deferred =
      restarted.compact({.minimum_segment_count = 2, .target_block_size = 96});
  auto deferred_manifest = (*reopened)->pin_current();
  if (!deferred || deferred->segments_created != 0 || !deferred_manifest ||
      (*reopened)->current_id().generation != deferred_generation ||
      (*deferred_manifest)->manifest().segments.size() != 2)
    return 26;

  auto second_delta = restarted.begin_write();
  if (!second_delta ||
      !(*second_delta)->append(torrent(2, "post-warm-b", 100)) ||
      !(*second_delta)->commit())
    return 27;
  auto incremental = restarted.compact({.minimum_segment_count = 2,
                                        .maximum_warm_segment_count = 3,
                                        .target_block_size = 96});
  auto incremental_manifest = (*reopened)->pin_current();
  if (!incremental || incremental->segments_created != 1 ||
      incremental->segments_removed != 2 || !incremental_manifest ||
      (*incremental_manifest)->manifest().segments.size() != 2 ||
      !std::ranges::all_of((*incremental_manifest)->manifest().segments,
                           [](const storage::SegmentDescriptor &segment) {
                             return segment.tier == storage::SegmentTier::Warm;
                           }))
    return 28;

  for (const auto &record :
       {torrent(1, "cap-a", 110), torrent(1, "cap-b", 120)}) {
    auto write = restarted.begin_write();
    if (!write || !(*write)->append(record) || !(*write)->commit())
      return 29;
  }
  auto consolidated = restarted.compact({.minimum_segment_count = 2,
                                         .maximum_warm_segment_count = 2,
                                         .target_block_size = 96});
  auto consolidated_manifest = (*reopened)->pin_current();
  if (!consolidated || consolidated->segments_created != 1 ||
      consolidated->segments_removed != 4 || !consolidated_manifest ||
      (*consolidated_manifest)->manifest().segments.size() != 1 ||
      (*consolidated_manifest)->manifest().segments[0].tier !=
          storage::SegmentTier::Warm)
    return 30;
  auto final_snapshot = restarted.keyed_snapshot();
  auto final_latest = (*final_snapshot)->get(hash(1));
  auto final_second = (*final_snapshot)->get(hash(2));
  if (!final_latest || !*final_latest || (*final_latest)->name != "cap-b" ||
      !final_second || !*final_second || (*final_second)->name != "post-warm-b")
    return 31;

  return 0;
}

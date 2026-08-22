import std;

import sakuin.core;
import sakuin.model.observation;
import sakuin.storage;
import sakuin.storage.dataset.observations;

namespace {

struct TemporaryDirectory {
  std::filesystem::path path;
  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path, ignored);
  }
};

sakuin::model::ObservationRecord observation(std::uint8_t marker,
                                             std::int64_t seconds) {
  sakuin::model::ObservationRecord result;
  result.info_hash.bytes.fill(marker);
  result.observed_at = sakuin::core::Timestamp{std::chrono::seconds{seconds}};
  return result;
}

sakuin::core::Result<std::vector<std::uint8_t>>
markers(const sakuin::storage::ObservationDataset &dataset) {
  auto snapshot = dataset.snapshot();
  if (!snapshot)
    return std::unexpected(snapshot.error());
  auto stream = (*snapshot)->scan({});
  if (!stream)
    return std::unexpected(stream.error());
  std::vector<std::uint8_t> result;
  while (true) {
    auto next = (*stream)->next();
    if (!next)
      return std::unexpected(next.error());
    if (!*next)
      return result;
    result.push_back((*next)->info_hash.bytes.front());
  }
}

} // namespace

int main() {
  using namespace sakuin;
  const auto nonce = std::to_string(
      std::chrono::steady_clock::now().time_since_epoch().count());
  TemporaryDirectory directory{std::filesystem::temp_directory_path() /
                               ("sakuin-retention-" + nonce)};
  storage::LocalBlobStore blobs{directory.path / "blobs"};
  auto catalog =
      storage::LocalManifestCatalog::open(directory.path / "catalog", blobs);
  if (!catalog)
    return 1;
  storage::SegmentHeader hot_header{
      .compression = storage::CompressionCodec::None, .target_block_size = 64};
  storage::ObservationDataset dataset{blobs, **catalog, hot_header};

  for (const auto &record :
       {observation(1, 10), observation(2, 50), observation(3, 100)}) {
    auto write = dataset.begin_write();
    if (!write || !(*write)->append(record) || !(*write)->commit())
      return 2;
  }
  auto old_snapshot = dataset.snapshot();
  auto old_manifest = (*catalog)->pin_current();
  if (!old_snapshot || !old_manifest ||
      (*old_manifest)->manifest().segments.size() != 3)
    return 3;
  const std::array old_objects{(*old_manifest)->manifest().segments[0].object,
                               (*old_manifest)->manifest().segments[1].object};

  const storage::RetentionPolicy policy{
      .cold_before = core::Timestamp{std::chrono::seconds{75}},
      .expire_before = core::Timestamp{std::chrono::seconds{25}},
      .cold_target_block_size = 128,
      .cold_compression = storage::CompressionCodec::None,
      .cold_compression_level = 3};
  auto retained = storage::RowV1DatasetMaintenance::retain_unkeyed(
      blobs, **catalog, policy);
  if (!retained || retained->source_generation != 4 ||
      retained->segments_archived != 1 || retained->segments_expired != 1 ||
      retained->records_expired != 1 || retained->bytes_before == 0 ||
      retained->bytes_after == 0)
    return 4;

  auto current_manifest = (*catalog)->pin_current();
  if (!current_manifest ||
      (*current_manifest)->manifest().segments.size() != 2 ||
      (*current_manifest)->manifest().segments[0].tier !=
          storage::SegmentTier::Cold ||
      (*current_manifest)->manifest().segments[0].format_version !=
          storage::StorageFormatVersion{1, 3} ||
      (*current_manifest)->manifest().segments[1].tier !=
          storage::SegmentTier::Hot)
    return 5;
  auto current_markers = markers(dataset);
  if (!current_markers || *current_markers != std::vector<std::uint8_t>{2, 3})
    return 6;

  auto old_stream = (*old_snapshot)->scan({});
  if (!old_stream)
    return 7;
  std::vector<std::uint8_t> old_markers;
  while (true) {
    auto next = (*old_stream)->next();
    if (!next)
      return 8;
    if (!*next)
      break;
    old_markers.push_back((*next)->info_hash.bytes.front());
  }
  if (old_markers != std::vector<std::uint8_t>{1, 2, 3})
    return 9;

  auto no_op = storage::RowV1DatasetMaintenance::retain_unkeyed(
      blobs, **catalog, policy);
  if (!no_op || no_op->source_generation != 4 ||
      no_op->segments_archived != 0 || no_op->segments_expired != 0)
    return 10;
  auto invalid = storage::RowV1DatasetMaintenance::retain_unkeyed(
      blobs, **catalog,
      storage::RetentionPolicy{
          .cold_before = core::Timestamp{std::chrono::seconds{70}},
          .expire_before = core::Timestamp{std::chrono::seconds{80}}});
  if (invalid || invalid.error().code != core::ErrorCode::InvalidArgument)
    return 11;

  auto pinned_gc = storage::RowV1DatasetMaintenance::garbage_collect(**catalog);
  if (!pinned_gc || pinned_gc->objects_deleted != 0)
    return 12;
  old_stream->reset();
  old_snapshot->reset();
  old_manifest->reset();
  auto reclaimed = storage::RowV1DatasetMaintenance::garbage_collect(**catalog);
  if (!reclaimed || reclaimed->objects_deleted != 2 ||
      reclaimed->bytes_reclaimed == 0)
    return 13;
  for (const auto &object : old_objects) {
    auto exists = blobs.exists(object);
    if (!exists || *exists)
      return 14;
  }

  for (const auto &record : {observation(4, 110), observation(5, 120)}) {
    auto write = dataset.begin_write();
    if (!write || !(*write)->append(record) || !(*write)->commit())
      return 15;
  }
  auto compacted = storage::RowV1DatasetMaintenance::compact(
      blobs, **catalog,
      storage::CompactionPolicy{.minimum_segment_count = 2,
                                .target_block_size = 64,
                                .compression =
                                    storage::CompressionCodec::None});
  if (!compacted || compacted->segments_created != 1 ||
      compacted->segments_removed != 3)
    return 16;
  auto tiered_manifest = (*catalog)->pin_current();
  if (!tiered_manifest || (*tiered_manifest)->manifest().segments.size() != 2 ||
      (*tiered_manifest)->manifest().segments[0].tier !=
          storage::SegmentTier::Cold ||
      (*tiered_manifest)->manifest().segments[1].tier !=
          storage::SegmentTier::Hot)
    return 17;
  current_markers = markers(dataset);
  if (!current_markers ||
      *current_markers != std::vector<std::uint8_t>{2, 3, 4, 5})
    return 18;

  auto verified = storage::RowV1DatasetMaintenance::verify(blobs, **catalog);
  if (!verified || verified->segments_checked != 2 ||
      verified->records_checked != 4)
    return 19;
  auto reopened =
      storage::LocalManifestCatalog::open(directory.path / "catalog", blobs);
  if (!reopened)
    return 20;
  auto restart_verification =
      storage::RowV1DatasetMaintenance::verify(blobs, **reopened);
  if (!restart_verification || restart_verification->segments_checked != 2 ||
      restart_verification->records_checked != 4)
    return 21;
  return 0;
}

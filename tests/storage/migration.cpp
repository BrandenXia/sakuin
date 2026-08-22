import std;

import sakuin.core;
import sakuin.model.observation;
import sakuin.storage;
import sakuin.storage.codec.model_records;
import sakuin.storage.dataset.observations;

namespace {

struct TemporaryDirectory {
  std::filesystem::path path;
  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path, ignored);
  }
};

sakuin::model::ObservationRecord observation(std::uint8_t marker) {
  sakuin::model::ObservationRecord result;
  result.info_hash.bytes.fill(marker);
  result.observed_at = sakuin::core::Timestamp{std::chrono::seconds{marker}};
  return result;
}

sakuin::core::Result<std::vector<std::uint8_t>>
scan(const sakuin::storage::ObservationDataset &dataset) {
  auto snapshot = dataset.snapshot();
  if (!snapshot)
    return std::unexpected(snapshot.error());
  auto stream = (*snapshot)->scan({});
  if (!stream)
    return std::unexpected(stream.error());
  std::vector<std::uint8_t> markers;
  while (true) {
    auto next = (*stream)->next();
    if (!next)
      return std::unexpected(next.error());
    if (!*next)
      return markers;
    markers.push_back((*next)->info_hash.bytes.front());
  }
}

} // namespace

int main() {
  using namespace sakuin;
  const auto nonce = std::to_string(
      std::chrono::steady_clock::now().time_since_epoch().count());
  TemporaryDirectory directory{std::filesystem::temp_directory_path() /
                               ("sakuin-migration-" + nonce)};
  storage::LocalBlobStore source_blobs{directory.path / "source-objects"};
  storage::LocalBlobStore destination_blobs{directory.path /
                                            "destination-objects"};
  auto source_catalog = storage::LocalManifestCatalog::open(
      directory.path / "source-catalog", source_blobs);
  auto destination_catalog = storage::LocalManifestCatalog::open(
      directory.path / "destination-catalog", destination_blobs);
  if (!source_catalog || !destination_catalog)
    return 1;
  storage::SegmentHeader header{.compression = storage::CompressionCodec::None,
                                .target_block_size = 64};
  storage::ObservationDataset source{source_blobs, **source_catalog, header};
  storage::ObservationDataset destination{destination_blobs,
                                          **destination_catalog, header};

  for (const auto &record : {observation(1), observation(2)}) {
    auto write = source.begin_write();
    if (!write || !(*write)->append(record) || !(*write)->commit())
      return 2;
  }
  auto destination_write = destination.begin_write();
  if (!destination_write || !(*destination_write)->append(observation(9)) ||
      !(*destination_write)->commit())
    return 3;
  auto old_destination = destination.snapshot();
  auto old_stream = old_destination
                        ? (*old_destination)->scan({})
                        : core::Result<std::unique_ptr<
                              storage::RecordStream<model::ObservationRecord>>>{
                              std::unexpected(old_destination.error())};
  auto source_snapshot = (*source_catalog)->pin_current();
  if (!old_stream || !source_snapshot)
    return 4;

  auto migrated =
      storage::migrate_snapshot(source_blobs, **source_snapshot,
                                destination_blobs, **destination_catalog, 17);
  if (!migrated || migrated->destination_generation != 2 ||
      migrated->segments_published != 2 || migrated->objects_copied != 2 ||
      migrated->objects_reused != 0 || migrated->bytes_copied == 0)
    return 5;
  auto destination_records = scan(destination);
  if (!destination_records ||
      *destination_records != std::vector<std::uint8_t>{1, 2})
    return 6;
  auto old_record = (*old_stream)->next();
  auto old_end = (*old_stream)->next();
  if (!old_record || !*old_record ||
      (*old_record)->info_hash.bytes.front() != 9 || !old_end || *old_end)
    return 7;

  auto repeated =
      storage::migrate_snapshot(source_blobs, **source_snapshot,
                                destination_blobs, **destination_catalog, 17);
  if (!repeated || repeated->destination_generation != 2 ||
      repeated->objects_copied != 0 || repeated->objects_reused != 2)
    return 8;
  auto invalid =
      storage::migrate_snapshot(source_blobs, **source_snapshot,
                                destination_blobs, **destination_catalog, 0);
  if (invalid || invalid.error().code != core::ErrorCode::InvalidArgument)
    return 9;

  auto verified = storage::RowV1DatasetMaintenance::verify(
      destination_blobs, **destination_catalog);
  if (!verified || verified->segments_checked != 2 ||
      verified->records_checked != 2)
    return 10;
  auto pinned_gc =
      storage::RowV1DatasetMaintenance::garbage_collect(**destination_catalog);
  if (!pinned_gc || pinned_gc->objects_deleted != 0)
    return 11;
  old_stream->reset();
  old_destination->reset();
  auto reclaimed =
      storage::RowV1DatasetMaintenance::garbage_collect(**destination_catalog);
  if (!reclaimed || reclaimed->objects_deleted != 1 ||
      reclaimed->bytes_reclaimed == 0)
    return 12;

  auto legacy_catalog = storage::LocalManifestCatalog::open(
      directory.path / "legacy-catalog", source_blobs);
  if (!legacy_catalog)
    return 13;
  storage::ObservationDataset legacy_dataset{source_blobs, **legacy_catalog,
                                             header};
  auto legacy_write = legacy_dataset.begin_write();
  if (!legacy_write || !(*legacy_write)->append(observation(7)) ||
      !(*legacy_write)->commit())
    return 14;
  auto modern_pin = (*legacy_catalog)->pin_current();
  if (!modern_pin || (*modern_pin)->manifest().segments.size() != 1)
    return 15;
  const auto modern = (*modern_pin)->manifest().segments.front();
  auto modern_reader = source_blobs.open(modern.object);
  if (!modern_reader)
    return 16;
  core::ByteBuffer legacy_bytes((*modern_reader)->size());
  std::size_t offset{};
  while (offset < legacy_bytes.size()) {
    auto read = (*modern_reader)
                    ->read(core::MutableByteView{legacy_bytes}.subspan(offset));
    if (!read || *read == 0)
      return 17;
    offset += *read;
  }
  // RowV1 1.0 used the same block/footer encoding but did not persist the
  // four-byte schema ID introduced by 1.1.
  legacy_bytes[10] = std::byte{};
  legacy_bytes[11] = std::byte{};
  legacy_bytes.erase(legacy_bytes.begin() + 12, legacy_bytes.begin() + 16);
  auto legacy_object_writer = source_blobs.create();
  if (!legacy_object_writer || !(*legacy_object_writer)->write(legacy_bytes))
    return 18;
  auto legacy_object = (*legacy_object_writer)->finalize();
  if (!legacy_object)
    return 19;
  auto legacy = modern;
  legacy.object = *legacy_object;
  std::copy_n(legacy.object.bytes.begin(), legacy.id.bytes.size(),
              legacy.id.bytes.begin());
  legacy.physical_size = legacy_bytes.size();
  legacy.format_version = {1, 0};
  legacy.schema_id = {};
  auto legacy_published =
      (*legacy_catalog)->publish((*modern_pin)->manifest().id, {legacy});
  if (!legacy_published || legacy_published->generation != 2)
    return 20;

  auto missing_schema =
      storage::migrate_row_v1_format(source_blobs, **legacy_catalog);
  if (missing_schema ||
      missing_schema.error().code != core::ErrorCode::InvalidArgument)
    return 21;
  auto upgraded = storage::migrate_row_v1_format(
      source_blobs, **legacy_catalog,
      storage::RowV1FormatMigrationPolicy{.legacy_schema_id =
                                              storage::ObservationRecordSchema,
                                          .compression_level = 3});
  if (!upgraded || upgraded->source_generation != 3 ||
      upgraded->segments_migrated != 1 || upgraded->bytes_before == 0 ||
      upgraded->bytes_after == 0)
    return 22;
  auto upgraded_pin = (*legacy_catalog)->pin_current();
  if (!upgraded_pin ||
      (*upgraded_pin)->manifest().segments.front().format_version !=
          storage::StorageFormatVersion{1, 1} ||
      (*upgraded_pin)->manifest().segments.front().schema_id !=
          storage::ObservationRecordSchema)
    return 23;
  auto upgraded_records = scan(legacy_dataset);
  if (!upgraded_records || *upgraded_records != std::vector<std::uint8_t>{7})
    return 24;
  auto format_no_op = storage::migrate_row_v1_format(
      source_blobs, **legacy_catalog,
      storage::RowV1FormatMigrationPolicy{
          .legacy_schema_id = storage::ObservationRecordSchema});
  if (!format_no_op || format_no_op->source_generation != 3 ||
      format_no_op->segments_migrated != 0)
    return 25;
  return 0;
}

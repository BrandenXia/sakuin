export module sakuin.storage.admin.migration;

import std;

import sakuin.core.bytes;
import sakuin.core.result;
import sakuin.storage.blob.store;
import sakuin.storage.catalog.manifest;
import sakuin.storage.format.row_v1;
import sakuin.storage.format.segment;

export namespace sakuin::storage {

struct MigrationResult {
  std::uint64_t destination_generation{};
  std::uint64_t segments_published{};
  std::uint64_t objects_copied{};
  std::uint64_t objects_reused{};
  std::uint64_t bytes_copied{};
};

struct RowV1FormatMigrationPolicy {
  // Format 1.0 predates persisted schema IDs. Callers migrating such a
  // dataset must supply the schema identity established by its catalog.
  std::optional<SchemaId> legacy_schema_id;
  int compression_level{3};
};

struct FormatMigrationResult {
  std::uint64_t source_generation{};
  std::uint64_t segments_migrated{};
  std::uint64_t bytes_before{};
  std::uint64_t bytes_after{};
};

// Copies every object reachable from one pinned manifest before atomically
// publishing the equivalent segment set in the destination catalog. Existing
// content-addressed objects are reused. The destination's previous generation
// remains readable while pinned and becomes GC-eligible only after publication.
core::Result<MigrationResult>
migrate_snapshot(BlobStore &source, const ManifestPin &snapshot,
                 BlobStore &destination, ManifestCatalog &destination_catalog,
                 std::size_t copy_buffer_bytes = 1024U * 1024U);

// Rewrites supported older RowV1 segments into their current tier-specific
// representation, verifies the replacements, then publishes one generation.
// Currently this upgrades legacy HOT 1.0 to schema-aware HOT 1.1.
core::Result<FormatMigrationResult>
migrate_row_v1_format(BlobStore &blobs, ManifestCatalog &catalog,
                      const RowV1FormatMigrationPolicy &policy = {});

} // namespace sakuin::storage

namespace sakuin::storage {
namespace {

bool same_descriptor(const SegmentDescriptor &left,
                     const SegmentDescriptor &right) {
  return left.id == right.id && left.object == right.object &&
         left.tier == right.tier && left.record_count == right.record_count &&
         left.logical_size == right.logical_size &&
         left.physical_size == right.physical_size &&
         left.min_timestamp == right.min_timestamp &&
         left.max_timestamp == right.max_timestamp &&
         left.format_version == right.format_version &&
         left.schema_id == right.schema_id &&
         left.schema_version == right.schema_version &&
         left.encoding == right.encoding &&
         left.compression == right.compression;
}

bool same_segments(std::span<const SegmentDescriptor> left,
                   std::span<const SegmentDescriptor> right) {
  return left.size() == right.size() &&
         std::ranges::equal(left, right, same_descriptor);
}

StorageFormatVersion current_format(SegmentTier tier) {
  switch (tier) {
  case SegmentTier::Hot:
    return {1, 1};
  case SegmentTier::Warm:
    return {1, 2};
  case SegmentTier::Cold:
    return {1, 3};
  }
  std::unreachable();
}

core::Result<std::unique_ptr<RowV1SegmentReader>>
open_segment(BlobStore &blobs, const SegmentDescriptor &descriptor) {
  if (descriptor.encoding != SegmentEncoding::RowV1)
    return std::unexpected(core::Error{core::ErrorCode::UnsupportedFormat,
                                       "Format migration supports RowV1 only"});
  auto reader = RowV1SegmentReader::open(blobs, descriptor.object);
  if (!reader)
    return std::unexpected(reader.error());
  const auto &header = (*reader)->header();
  if (header.format_version != descriptor.format_version ||
      header.schema_id != descriptor.schema_id ||
      header.schema_version != descriptor.schema_version ||
      header.encoding != descriptor.encoding ||
      header.tier != descriptor.tier ||
      header.compression != descriptor.compression ||
      (*reader)->record_count() != descriptor.record_count)
    return std::unexpected(core::Error{
        core::ErrorCode::CorruptSegment,
        "Manifest metadata does not match its migration source segment"});
  return reader;
}

core::Result<void> validate_object(BlobStore &store,
                                   const SegmentDescriptor &segment) {
  auto reader = store.open(segment.object);
  if (!reader)
    return std::unexpected(reader.error());
  if ((*reader)->size() != segment.physical_size)
    return std::unexpected(core::Error{
        core::ErrorCode::CorruptSegment,
        "Manifest object size does not match its segment descriptor"});
  return {};
}

core::Result<void> copy_object(BlobStore &source, BlobStore &destination,
                               const SegmentDescriptor &segment,
                               std::size_t copy_buffer_bytes) {
  auto input = source.open(segment.object);
  if (!input)
    return std::unexpected(input.error());
  if ((*input)->size() != segment.physical_size)
    return std::unexpected(core::Error{
        core::ErrorCode::CorruptSegment,
        "Source object size does not match its segment descriptor"});
  auto output = destination.create();
  if (!output)
    return std::unexpected(output.error());
  core::ByteBuffer buffer(
      std::min<std::uint64_t>(copy_buffer_bytes, (*input)->size()));
  while (true) {
    auto read = (*input)->read(buffer);
    if (!read) {
      (*output)->abort();
      return std::unexpected(read.error());
    }
    if (*read == 0)
      break;
    if (auto written = (*output)->write(core::ByteView{buffer}.first(*read));
        !written) {
      (*output)->abort();
      return std::unexpected(written.error());
    }
  }
  auto object = (*output)->finalize();
  if (!object)
    return std::unexpected(object.error());
  if (*object != segment.object)
    return std::unexpected(core::Error{
        core::ErrorCode::ChecksumMismatch,
        "Migrated object content ID differs from its source descriptor"});
  return {};
}

} // namespace

core::Result<MigrationResult>
migrate_snapshot(BlobStore &source, const ManifestPin &snapshot,
                 BlobStore &destination, ManifestCatalog &destination_catalog,
                 std::size_t copy_buffer_bytes) {
  if (copy_buffer_bytes == 0)
    return std::unexpected(core::Error{core::ErrorCode::InvalidArgument,
                                       "Migration buffer must be nonzero"});
  auto destination_pin = destination_catalog.pin_current();
  if (!destination_pin)
    return std::unexpected(destination_pin.error());
  const auto &source_manifest = snapshot.manifest();
  const auto &destination_manifest = (*destination_pin)->manifest();
  MigrationResult result{.destination_generation =
                             destination_manifest.id.generation,
                         .segments_published = source_manifest.segments.size()};

  for (const auto &segment : source_manifest.segments) {
    auto exists = destination.exists(segment.object);
    if (!exists)
      return std::unexpected(exists.error());
    if (*exists) {
      if (auto valid = validate_object(destination, segment); !valid)
        return std::unexpected(valid.error());
      ++result.objects_reused;
      continue;
    }
    if (auto copied =
            copy_object(source, destination, segment, copy_buffer_bytes);
        !copied)
      return std::unexpected(copied.error());
    ++result.objects_copied;
    result.bytes_copied += segment.physical_size;
  }

  if (same_segments(source_manifest.segments, destination_manifest.segments))
    return result;
  auto published = destination_catalog.publish(destination_manifest.id,
                                               source_manifest.segments);
  if (!published)
    return std::unexpected(published.error());
  result.destination_generation = published->generation;
  return result;
}

core::Result<FormatMigrationResult>
migrate_row_v1_format(BlobStore &blobs, ManifestCatalog &catalog,
                      const RowV1FormatMigrationPolicy &policy) {
  auto pin = catalog.pin_current();
  if (!pin)
    return std::unexpected(pin.error());
  const auto &manifest = (*pin)->manifest();
  FormatMigrationResult result{.source_generation = manifest.id.generation};
  auto replacements = manifest.segments;

  for (std::size_t index = 0; index < manifest.segments.size(); ++index) {
    const auto &segment = manifest.segments[index];
    if (segment.encoding != SegmentEncoding::RowV1)
      return std::unexpected(core::Error{
          core::ErrorCode::UnsupportedFormat,
          "Format migration generation contains a non-RowV1 segment"});
    if (segment.format_version == current_format(segment.tier))
      continue;
    if (segment.tier != SegmentTier::Hot ||
        segment.format_version != StorageFormatVersion{1, 0})
      return std::unexpected(core::Error{
          core::ErrorCode::UnsupportedFormat,
          "No migration path exists for this RowV1 format and tier"});
    if (!policy.legacy_schema_id)
      return std::unexpected(
          core::Error{core::ErrorCode::InvalidArgument,
                      "Migrating RowV1 1.0 requires its legacy schema ID"});

    auto reader = open_segment(blobs, segment);
    if (!reader)
      return std::unexpected(reader.error());
    SegmentHeader header{.format_version = current_format(segment.tier),
                         .schema_id = *policy.legacy_schema_id,
                         .schema_version = segment.schema_version,
                         .encoding = segment.encoding,
                         .tier = segment.tier,
                         .compression = segment.compression,
                         .target_block_size =
                             (*reader)->header().target_block_size};
    auto writer = RowV1SegmentWriter::create(
        blobs, header,
        RowV1WriterOptions{.compression_level = policy.compression_level});
    if (!writer)
      return std::unexpected(writer.error());
    for (std::uint64_t ordinal = 0; ordinal < (*reader)->record_count();
         ++ordinal) {
      auto location = (*reader)->location(ordinal);
      if (!location) {
        (*writer)->abort();
        return std::unexpected(location.error());
      }
      auto record = (*reader)->read(*location);
      if (!record) {
        (*writer)->abort();
        return std::unexpected(record.error());
      }
      if (auto appended = (*writer)->append(*record); !appended) {
        (*writer)->abort();
        return std::unexpected(appended.error());
      }
    }
    auto replacement = (*writer)->finalize();
    if (!replacement)
      return std::unexpected(replacement.error());
    replacement->min_timestamp = segment.min_timestamp;
    replacement->max_timestamp = segment.max_timestamp;
    auto verified = open_segment(blobs, *replacement);
    if (!verified)
      return std::unexpected(verified.error());
    if (auto integrity = (*verified)->verify(); !integrity)
      return std::unexpected(integrity.error());
    ++result.segments_migrated;
    result.bytes_before += segment.physical_size;
    result.bytes_after += replacement->physical_size;
    replacements[index] = std::move(*replacement);
  }

  if (result.segments_migrated == 0)
    return result;
  auto published = catalog.publish(manifest.id, std::move(replacements));
  if (!published)
    return std::unexpected(published.error());
  result.source_generation = published->generation;
  return result;
}

} // namespace sakuin::storage

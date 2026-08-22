export module sakuin.storage.admin.row_v1;

import std;

import sakuin.core.result;
import sakuin.core.time;
import sakuin.storage.admin;
import sakuin.storage.admin.compaction;
import sakuin.storage.admin.retention;
import sakuin.storage.blob.store;
import sakuin.storage.catalog.manifest;
import sakuin.storage.format.row_v1;
import sakuin.storage.format.segment;

export namespace sakuin::storage {

// Model-independent administration for RowV1 datasets. Compaction preserves
// physical record order, so append-only history and newest-value-wins keyed
// semantics remain unchanged without decoding logical records.
class RowV1DatasetMaintenance {
public:
  static core::Result<CompactionResult>
  compact(BlobStore &blobs, ManifestCatalog &catalog,
          const CompactionPolicy &policy = {});

  static core::Result<VerifyResult> verify(BlobStore &blobs,
                                           ManifestCatalog &catalog);

  // Applies segment-granular retention to an unkeyed timestamped RowV1
  // dataset. Partially eligible segments are deliberately retained so a
  // policy pass never discards a record newer than its cutoff.
  static core::Result<RetentionResult>
  retain_unkeyed(BlobStore &blobs, ManifestCatalog &catalog,
                 const RetentionPolicy &policy);

  static core::Result<GcResult> garbage_collect(ManifestCatalog &catalog);
};

} // namespace sakuin::storage

namespace sakuin::storage {
namespace {

core::Result<std::unique_ptr<RowV1SegmentReader>>
open_checked(BlobStore &blobs, const SegmentDescriptor &descriptor) {
  if (descriptor.encoding != SegmentEncoding::RowV1)
    return std::unexpected(core::Error{core::ErrorCode::UnsupportedFormat,
                                       "Maintenance supports RowV1 only"});
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
    return std::unexpected(
        core::Error{core::ErrorCode::CorruptSegment,
                    "Manifest metadata does not match its RowV1 segment"});
  return reader;
}

std::optional<core::Timestamp>
minimum_timestamp(std::span<const SegmentDescriptor> segments) {
  std::optional<core::Timestamp> result;
  for (const auto &segment : segments) {
    if (segment.min_timestamp)
      result = result ? std::min(*result, *segment.min_timestamp)
                      : segment.min_timestamp;
  }
  return result;
}

std::optional<core::Timestamp>
maximum_timestamp(std::span<const SegmentDescriptor> segments) {
  std::optional<core::Timestamp> result;
  for (const auto &segment : segments) {
    if (segment.max_timestamp)
      result = result ? std::max(*result, *segment.max_timestamp)
                      : segment.max_timestamp;
  }
  return result;
}

} // namespace

core::Result<CompactionResult>
RowV1DatasetMaintenance::compact(BlobStore &blobs, ManifestCatalog &catalog,
                                 const CompactionPolicy &policy) {
  if (policy.minimum_segment_count < 2 || policy.target_block_size == 0)
    return std::unexpected(core::Error{
        core::ErrorCode::InvalidArgument,
        "Compaction requires at least two segments and a nonzero block size"});

  auto pin = catalog.pin_current();
  if (!pin)
    return std::unexpected(pin.error());
  const auto &manifest = (*pin)->manifest();
  std::size_t first_compacted{};
  std::size_t compacted_count{};
  while (first_compacted < manifest.segments.size()) {
    while (first_compacted < manifest.segments.size() &&
           manifest.segments[first_compacted].tier == SegmentTier::Cold)
      ++first_compacted;
    auto end = first_compacted;
    while (end < manifest.segments.size() &&
           manifest.segments[end].tier != SegmentTier::Cold)
      ++end;
    if (end - first_compacted >= policy.minimum_segment_count) {
      compacted_count = end - first_compacted;
      break;
    }
    first_compacted = end;
  }
  if (compacted_count == 0)
    return CompactionResult{.source_generation = manifest.id.generation};

  const auto selected = std::span<const SegmentDescriptor>{
      manifest.segments.data() + first_compacted, compacted_count};

  const auto &first = selected.front();
  for (const auto &segment : selected) {
    if (segment.encoding != SegmentEncoding::RowV1 ||
        segment.schema_id != first.schema_id ||
        segment.schema_version != first.schema_version)
      return std::unexpected(core::Error{
          core::ErrorCode::UnsupportedFormat,
          "Compaction generation contains mixed encodings or schemas"});
  }

  SegmentHeader output_header{.schema_id = first.schema_id,
                              .schema_version = first.schema_version,
                              .encoding = SegmentEncoding::RowV1,
                              .tier = SegmentTier::Hot,
                              .compression = policy.compression,
                              .target_block_size = policy.target_block_size};
  auto writer = RowV1SegmentWriter::create(
      blobs, output_header,
      RowV1WriterOptions{.compression_level = policy.compression_level});
  if (!writer)
    return std::unexpected(writer.error());

  for (const auto &segment : selected) {
    auto reader = open_checked(blobs, segment);
    if (!reader) {
      (*writer)->abort();
      return std::unexpected(reader.error());
    }
    for (std::uint64_t ordinal = 0; ordinal < (*reader)->record_count();
         ++ordinal) {
      auto location = (*reader)->location(ordinal);
      if (!location) {
        (*writer)->abort();
        return std::unexpected(location.error());
      }
      auto encoded = (*reader)->read(*location);
      if (!encoded) {
        (*writer)->abort();
        return std::unexpected(encoded.error());
      }
      if (auto appended = (*writer)->append(*encoded); !appended) {
        (*writer)->abort();
        return std::unexpected(appended.error());
      }
    }
  }

  auto replacement = (*writer)->finalize();
  if (!replacement)
    return std::unexpected(replacement.error());
  replacement->min_timestamp = minimum_timestamp(selected);
  replacement->max_timestamp = maximum_timestamp(selected);
  auto verified = open_checked(blobs, *replacement);
  if (!verified)
    return std::unexpected(verified.error());
  if (auto integrity = (*verified)->verify(); !integrity)
    return std::unexpected(integrity.error());

  const auto bytes_before = std::transform_reduce(
      selected.begin(), selected.end(), std::uint64_t{}, std::plus{},
      [](const SegmentDescriptor &segment) { return segment.physical_size; });
  auto segments = manifest.segments;
  segments.erase(
      segments.begin() + static_cast<std::ptrdiff_t>(first_compacted),
      segments.begin() +
          static_cast<std::ptrdiff_t>(first_compacted + compacted_count));
  segments.insert(segments.begin() +
                      static_cast<std::ptrdiff_t>(first_compacted),
                  *replacement);
  auto published = catalog.publish(manifest.id, std::move(segments));
  if (!published)
    return std::unexpected(published.error());
  return CompactionResult{
      .source_generation = published->generation,
      .segments_created = 1,
      .segments_removed = compacted_count,
      .bytes_before = bytes_before,
      .bytes_after = replacement->physical_size,
  };
}

core::Result<VerifyResult>
RowV1DatasetMaintenance::verify(BlobStore &blobs, ManifestCatalog &catalog) {
  auto pin = catalog.pin_current();
  if (!pin)
    return std::unexpected(pin.error());
  VerifyResult result{};
  for (const auto &segment : (*pin)->manifest().segments) {
    auto reader = open_checked(blobs, segment);
    if (!reader)
      return std::unexpected(reader.error());
    if (auto verified = (*reader)->verify(); !verified)
      return std::unexpected(verified.error());
    ++result.segments_checked;
    result.records_checked += (*reader)->record_count();
  }
  return result;
}

core::Result<RetentionResult> RowV1DatasetMaintenance::retain_unkeyed(
    BlobStore &blobs, ManifestCatalog &catalog, const RetentionPolicy &policy) {
  if (policy.expire_before > policy.cold_before ||
      policy.cold_target_block_size == 0)
    return std::unexpected(core::Error{
        core::ErrorCode::InvalidArgument,
        "Retention requires an expiration cutoff no newer than its COLD "
        "cutoff and a nonzero COLD block size"});

  auto pin = catalog.pin_current();
  if (!pin)
    return std::unexpected(pin.error());
  const auto &manifest = (*pin)->manifest();
  RetentionResult result{.source_generation = manifest.id.generation};
  std::vector<SegmentDescriptor> retained;
  retained.reserve(manifest.segments.size());

  for (const auto &segment : manifest.segments) {
    if (!segment.max_timestamp)
      return std::unexpected(core::Error{
          core::ErrorCode::InvalidManifest,
          "Timestamp retention requires maximum timestamp statistics"});

    if (*segment.max_timestamp <= policy.expire_before) {
      ++result.segments_expired;
      result.records_expired += segment.record_count;
      result.bytes_before += segment.physical_size;
      continue;
    }

    if (segment.tier == SegmentTier::Cold ||
        *segment.max_timestamp > policy.cold_before) {
      retained.push_back(segment);
      continue;
    }
    if (segment.tier == SegmentTier::Warm)
      return std::unexpected(
          core::Error{core::ErrorCode::UnsupportedFormat,
                      "Unkeyed retention cannot archive a keyed WARM segment"});

    auto reader = open_checked(blobs, segment);
    if (!reader)
      return std::unexpected(reader.error());
    SegmentHeader header{.format_version = {1, 3},
                         .schema_id = segment.schema_id,
                         .schema_version = segment.schema_version,
                         .encoding = SegmentEncoding::RowV1,
                         .tier = SegmentTier::Cold,
                         .compression = policy.cold_compression,
                         .target_block_size = policy.cold_target_block_size};
    auto writer = RowV1SegmentWriter::create(
        blobs, header,
        RowV1WriterOptions{.compression_level = policy.cold_compression_level});
    if (!writer)
      return std::unexpected(writer.error());
    for (std::uint64_t ordinal = 0; ordinal < (*reader)->record_count();
         ++ordinal) {
      auto location = (*reader)->location(ordinal);
      if (!location) {
        (*writer)->abort();
        return std::unexpected(location.error());
      }
      auto encoded = (*reader)->read(*location);
      if (!encoded) {
        (*writer)->abort();
        return std::unexpected(encoded.error());
      }
      if (auto appended = (*writer)->append(*encoded); !appended) {
        (*writer)->abort();
        return std::unexpected(appended.error());
      }
    }
    auto archived = (*writer)->finalize();
    if (!archived)
      return std::unexpected(archived.error());
    archived->min_timestamp = segment.min_timestamp;
    archived->max_timestamp = segment.max_timestamp;
    auto verified = open_checked(blobs, *archived);
    if (!verified)
      return std::unexpected(verified.error());
    if (auto integrity = (*verified)->verify(); !integrity)
      return std::unexpected(integrity.error());

    ++result.segments_archived;
    result.bytes_before += segment.physical_size;
    result.bytes_after += archived->physical_size;
    retained.push_back(std::move(*archived));
  }

  if (result.segments_archived == 0 && result.segments_expired == 0)
    return result;
  auto published = catalog.publish(manifest.id, std::move(retained));
  if (!published)
    return std::unexpected(published.error());
  result.source_generation = published->generation;
  return result;
}

core::Result<GcResult>
RowV1DatasetMaintenance::garbage_collect(ManifestCatalog &catalog) {
  return catalog.garbage_collect();
}

} // namespace sakuin::storage

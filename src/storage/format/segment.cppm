export module sakuin.storage.format.segment;

import std;

import sakuin.core.ids;
import sakuin.core.time;
import sakuin.storage.format.block;

export namespace sakuin::storage {

inline constexpr std::uint32_t DefaultTargetBlockSize = 2U * 1024U * 1024U;

enum class SegmentTier : std::uint8_t { Hot, Warm, Cold };

enum class SegmentEncoding : std::uint8_t {
  RowV1,
  // Reserved for a future columnar COLD encoding.
};

struct StorageFormatVersion {
  std::uint16_t major{1};
  std::uint16_t minor{1};

  friend bool operator==(const StorageFormatVersion &,
                         const StorageFormatVersion &) = default;
};

// Identifies the logical record schema independently from its version. The
// storage engine treats this as an opaque, project-assigned persistent value.
struct SchemaId {
  std::uint32_t value{};

  friend bool operator==(const SchemaId &, const SchemaId &) = default;
};

struct SchemaVersion {
  std::uint32_t value{1};

  friend bool operator==(const SchemaVersion &,
                         const SchemaVersion &) = default;
};

// HOT is append-oriented and unsorted. WARM is physically key-sorted, indexed,
// and normally uses stronger compression. COLD RowV1 is scan-oriented and
// unindexed; a future columnar encoding can be introduced independently.
struct SegmentHeader {
  StorageFormatVersion format_version{};
  SchemaId schema_id{};
  SchemaVersion schema_version{};
  SegmentEncoding encoding{SegmentEncoding::RowV1};
  SegmentTier tier{SegmentTier::Hot};
  CompressionCodec compression{CompressionCodec::Zstd};
  std::uint32_t target_block_size{DefaultTargetBlockSize};
};

struct SegmentFooter {
  std::uint64_t sparse_index_offset{};
  std::uint64_t sparse_index_size{};
  std::uint64_t bloom_filter_offset{};
  std::uint64_t bloom_filter_size{};
  std::uint32_t block_count{};
  std::uint64_t record_count{};
  std::uint32_t checksum{};
};

struct SegmentDescriptor {
  core::SegmentId id;
  core::ObjectId object;

  SegmentTier tier;

  std::uint64_t record_count;
  std::uint64_t logical_size;
  std::uint64_t physical_size;

  std::optional<core::Timestamp> min_timestamp;
  std::optional<core::Timestamp> max_timestamp;

  StorageFormatVersion format_version{};
  SchemaId schema_id{};
  SchemaVersion schema_version{};
  SegmentEncoding encoding{SegmentEncoding::RowV1};
  CompressionCodec compression{CompressionCodec::Zstd};
};

} // namespace sakuin::storage

export module sakuin.storage.format.segment;

import std;

import sakuin.core.ids;
import sakuin.core.time;
import sakuin.storage.format.block;

export namespace sakuin::storage {

enum class SegmentTier : std::uint8_t { Hot, Warm, Cold };

enum class SegmentEncoding : std::uint8_t {
  RowV1,
  // Reserved for a future columnar COLD encoding.
};

struct StorageFormatVersion {
  std::uint16_t major{1};
  std::uint16_t minor{0};

  friend bool operator==(const StorageFormatVersion &,
                         const StorageFormatVersion &) = default;
};

struct SchemaVersion {
  std::uint32_t value{1};

  friend bool operator==(const SchemaVersion &, const SchemaVersion &) = default;
};

// HOT is append-oriented and unsorted. WARM is physically key-sorted, indexed,
// and normally uses stronger compression. COLD remains a compatibility tier;
// its columnar representation is intentionally not defined yet.
struct SegmentHeader {
  StorageFormatVersion format_version{};
  SchemaVersion schema_version{};
  SegmentEncoding encoding{SegmentEncoding::RowV1};
  SegmentTier tier{SegmentTier::Hot};
  CompressionCodec compression{CompressionCodec::None};
  std::uint32_t target_block_size{};
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

  core::Timestamp min_timestamp;
  core::Timestamp max_timestamp;

  StorageFormatVersion format_version{};
  SchemaVersion schema_version{};
  SegmentEncoding encoding{SegmentEncoding::RowV1};
  CompressionCodec compression{CompressionCodec::None};
};

} // namespace sakuin::storage

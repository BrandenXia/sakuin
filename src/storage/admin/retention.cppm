export module sakuin.storage.admin.retention;

import std;

import sakuin.core.result;
import sakuin.core.time;
import sakuin.storage.format.block;
import sakuin.storage.format.segment;

export namespace sakuin::storage {

// Timestamp cutoffs are supplied by the caller so policy evaluation is
// deterministic and independently testable. A segment is expired or archived
// only when every record is known to be at or before the corresponding cutoff.
struct RetentionPolicy {
  core::Timestamp cold_before;
  core::Timestamp expire_before;
  std::uint32_t cold_target_block_size{DefaultTargetBlockSize};
  CompressionCodec cold_compression{CompressionCodec::Zstd};
  int cold_compression_level{9};
};

struct RetentionResult {
  std::uint64_t source_generation{};
  std::uint64_t segments_archived{};
  std::uint64_t segments_expired{};
  std::uint64_t records_expired{};
  std::uint64_t bytes_before{};
  std::uint64_t bytes_after{};
};

} // namespace sakuin::storage

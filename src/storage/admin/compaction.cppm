export module sakuin.storage.admin.compaction;

import std;

import sakuin.core.result;
import sakuin.storage.format.block;
import sakuin.storage.format.segment;

export namespace sakuin::storage {

struct CompactionPolicy {
  std::size_t minimum_segment_count{2};
  std::uint32_t target_block_size{DefaultTargetBlockSize};
  CompressionCodec compression{CompressionCodec::Zstd};
  int compression_level{3};
};

struct CompactionResult {
  std::uint64_t source_generation{};
  std::uint64_t segments_created{};
  std::uint64_t segments_removed{};

  std::uint64_t bytes_before{};
  std::uint64_t bytes_after{};
};

} // namespace sakuin::storage

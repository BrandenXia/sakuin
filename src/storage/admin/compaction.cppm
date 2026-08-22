export module sakuin.storage.admin.compaction;

import std;

import sakuin.core.result;
import sakuin.storage.format.block;
import sakuin.storage.format.segment;

export namespace sakuin::storage {

struct CompactionPolicy {
  // For keyed tiered datasets, this is the number of HOT suffix segments to
  // fold into a new WARM segment. Existing WARM segments remain immutable
  // until the cap below requires a consolidation pass.
  std::size_t minimum_segment_count{2};
  std::size_t maximum_warm_segment_count{8};
  std::uint32_t target_block_size{DefaultTargetBlockSize};
  // Keyed HOT-to-WARM compaction may use a smaller random-access unit while
  // append/history compaction retains target_block_size.
  std::optional<std::uint32_t> warm_target_block_size;
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

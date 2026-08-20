export module sakuin.storage.admin.compaction;

import std;

import sakuin.core.result;

export namespace sakuin::storage {

struct CompactionPolicy {
  // Deliberately abstract for now.
  //
  // Later this can contain:
  //   tier transitions
  //   target segment sizes
  //   compression levels
  //   age thresholds
  //   etc.
};

struct CompactionResult {
  std::uint64_t segments_created;
  std::uint64_t segments_removed;

  std::uint64_t bytes_before;
  std::uint64_t bytes_after;
};

} // namespace sakuin::storage

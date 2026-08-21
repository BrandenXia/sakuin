export module sakuin.storage.admin;

import std;

import sakuin.core.result;

import sakuin.storage.admin.compaction;
import sakuin.storage.catalog.manifest;

export namespace sakuin::storage {

struct VerifyResult {
  std::uint64_t segments_checked;
  std::uint64_t records_checked;
};

class DatasetAdmin {
public:
  virtual ~DatasetAdmin() = default;

  virtual core::Result<CompactionResult>
  compact(const CompactionPolicy &policy) = 0;

  // Removes only objects unreachable from the current manifest and every live
  // ManifestPin. Replacement segments must be published before old ones qualify.
  virtual core::Result<GcResult> garbage_collect() = 0;

  virtual core::Result<VerifyResult> verify() = 0;
};

} // namespace sakuin::storage

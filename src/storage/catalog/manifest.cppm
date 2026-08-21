export module sakuin.storage.catalog.manifest;

import std;

import sakuin.core.result;
import sakuin.storage.format.segment;

export namespace sakuin::storage {

struct SnapshotId {
  std::uint64_t generation{};
  friend bool operator==(const SnapshotId &, const SnapshotId &) = default;
};

struct Manifest {
  std::uint32_t format_version{2};
  SnapshotId id;
  std::vector<SegmentDescriptor> segments;
};

struct GcResult {
  std::uint64_t objects_deleted{};
  std::uint64_t bytes_reclaimed{};
};

// Search and duplicate indexes are rebuildable projections of these canonical
// manifests and segments; they are never authoritative storage.

// Holding a pin keeps its immutable manifest generation and every referenced
// segment reachable. Garbage collection must exclude all pinned generations.
class ManifestPin {
public:
  virtual ~ManifestPin() = default;
  virtual const Manifest &manifest() const noexcept = 0;
};

class ManifestCatalog {
public:
  virtual ~ManifestCatalog() = default;

  virtual core::Result<std::shared_ptr<const ManifestPin>>
  pin_current() const = 0;

  virtual SnapshotId current_id() const noexcept = 0;

  virtual bool is_pinned(SnapshotId id) const noexcept = 0;

  // Prunes non-current manifest generations that have no live pin and removes
  // segment objects no longer reachable from any retained generation.
  virtual core::Result<GcResult> garbage_collect() = 0;

  // Atomically makes `manifest` current. Every newly referenced segment must
  // already be durable and integrity-verified before this call.
  virtual core::Result<SnapshotId>
  publish(SnapshotId expected_current,
          std::vector<SegmentDescriptor> segments) = 0;
};

} // namespace sakuin::storage

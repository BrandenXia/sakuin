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
  SnapshotId id;
  std::vector<SegmentDescriptor> segments;
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

  // Atomically makes `manifest` current. Every newly referenced segment must
  // already be durable and integrity-verified before this call.
  virtual core::Result<SnapshotId> publish(Manifest manifest) = 0;
};

} // namespace sakuin::storage

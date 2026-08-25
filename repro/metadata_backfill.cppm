export module sakuin.integration.metadata_backfill;

import std;

import sakuin.core.result;
import sakuin.core.time;

export namespace sakuin::integration {

struct MetadataDiscoveryBackfillStep {
  std::size_t records_scanned{};
  std::size_t targets_offered{};
  std::size_t records_with_metadata{};
  std::uint64_t source_generation{};
  bool full_rebuild{};
  bool scan_in_progress{};
  std::optional<core::Timestamp> next_wakeup;
};

class MetadataDiscoveryBackfill {
public:
  core::Result<MetadataDiscoveryBackfillStep> poll(core::Timestamp now);
};

} // namespace sakuin::integration

import std;

import sakuin.config;
import sakuin.core;
import sakuin.model.observation;
import sakuin.service.maintenance;
import sakuin.service.storage;

namespace {

struct TemporaryDirectory {
  TemporaryDirectory() {
    path = std::filesystem::temp_directory_path() /
           ("sakuin-maintenance-service-" +
            std::to_string(
                std::chrono::steady_clock::now().time_since_epoch().count()));
  }
  ~TemporaryDirectory() { std::filesystem::remove_all(path); }
  std::filesystem::path path;
};

class Observer final : public sakuin::service::StorageMaintenanceObserver {
public:
  void
  on_maintenance_completed(sakuin::service::MaintenanceEvent event) override {
    std::lock_guard lock{mutex};
    events.push_back(event);
    changed.notify_all();
  }

  void on_maintenance_error(sakuin::service::LocalDataset,
                            sakuin::service::MaintenanceOperation,
                            sakuin::core::Error error) override {
    std::lock_guard lock{mutex};
    errors.push_back(std::move(error));
    changed.notify_all();
  }

  bool wait_for_verification() {
    std::unique_lock lock{mutex};
    return changed.wait_for(lock, std::chrono::seconds{2}, [&] {
      return std::ranges::any_of(events, [](const auto &event) {
        return event.dataset == sakuin::service::LocalDataset::Observations &&
               event.operation ==
                   sakuin::service::MaintenanceOperation::Verification;
      });
    });
  }

  std::mutex mutex;
  std::condition_variable changed;
  std::vector<sakuin::service::MaintenanceEvent> events;
  std::vector<sakuin::core::Error> errors;
};

} // namespace

int main() {
  using namespace sakuin;
  TemporaryDirectory temporary;
  auto configuration = config::defaults().storage;
  configuration.local_root = temporary.path;
  configuration.block_target_bytes = 64;
  configuration.segment_target_bytes = 32;
  configuration.compression = config::CompressionCodec::None;
  configuration.compaction_minimum_segments = 2;
  configuration.maintenance.interval = std::chrono::milliseconds{10};
  configuration.maintenance.verification_interval =
      std::chrono::milliseconds{20};

  auto storage = service::LocalCanonicalStorage::open(configuration);
  if (!storage)
    return 1;
  for (std::uint8_t marker = 0; marker < 4; ++marker) {
    model::ObservationRecord record;
    record.info_hash.bytes.front() = marker;
    record.observed_at = core::Timestamp{std::chrono::seconds{marker + 1}};
    if (!(*storage)->observations().observe(record))
      return 2;
  }
  if (!(*storage)->flush())
    return 3;

  Observer observer;
  std::atomic<std::uint64_t> observation_generation{};
  service::StorageMaintenanceCoordinator coordinator{
      **storage, configuration.maintenance, &observer,
      [&](service::LocalDataset dataset, std::uint64_t generation) {
        if (dataset == service::LocalDataset::Observations)
          observation_generation = generation;
      }};
  if (!coordinator.start() || !observer.wait_for_verification())
    return 4;
  coordinator.stop();
  if (coordinator.running())
    return 5;
  if (observation_generation == 0)
    return 6;

  std::lock_guard lock{observer.mutex};
  if (!observer.errors.empty() ||
      !std::ranges::any_of(
          observer.events,
          [](const auto &event) {
            return event.dataset == service::LocalDataset::Observations &&
                   event.operation ==
                       service::MaintenanceOperation::Compaction &&
                   event.segments_affected >= 2;
          }) ||
      !std::ranges::any_of(observer.events, [](const auto &event) {
        return event.dataset == service::LocalDataset::Observations &&
               event.operation ==
                   service::MaintenanceOperation::GarbageCollection;
      }))
    return 7;
  return 0;
}

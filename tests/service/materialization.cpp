import std;

import sakuin.config;
import sakuin.core;
import sakuin.index.materialize;
import sakuin.model.observation;
import sakuin.service.materialization;
import sakuin.service.storage;

namespace {

struct TemporaryDirectory {
  TemporaryDirectory() {
    path = std::filesystem::temp_directory_path() /
           ("sakuin-materialization-service-" +
            std::to_string(
                std::chrono::steady_clock::now().time_since_epoch().count()));
  }
  ~TemporaryDirectory() { std::filesystem::remove_all(path); }
  std::filesystem::path path;
};

class Observer final : public sakuin::service::MaterializationObserver {
public:
  void on_materialization_completed(
      sakuin::index::IncrementalMaterializationResult result) override {
    std::lock_guard lock{mutex};
    results.push_back(std::move(result));
    changed.notify_all();
  }

  void on_materialization_error(sakuin::core::Error error) override {
    std::lock_guard lock{mutex};
    errors.push_back(std::move(error));
    changed.notify_all();
  }

  bool wait_for_processed(std::uint64_t count) {
    std::unique_lock lock{mutex};
    return changed.wait_for(lock, std::chrono::seconds{2}, [&] {
      return std::ranges::any_of(results, [&](const auto &result) {
        return result.checkpoint.observations_processed == count;
      });
    });
  }

  std::mutex mutex;
  std::condition_variable changed;
  std::vector<sakuin::index::IncrementalMaterializationResult> results;
  std::vector<sakuin::core::Error> errors;
};

sakuin::model::ObservationRecord observation(std::uint8_t marker) {
  sakuin::model::ObservationRecord result;
  result.info_hash.bytes.fill(marker);
  result.observed_at =
      sakuin::core::Timestamp{sakuin::core::Timestamp::duration{marker + 1}};
  return result;
}

} // namespace

int main() {
  using namespace sakuin;
  TemporaryDirectory temporary;
  auto configuration = config::defaults().storage;
  configuration.local_root = temporary.path;
  configuration.segment_target_bytes = 32;
  configuration.materialization.interval = std::chrono::hours{1};
  auto storage = service::LocalCanonicalStorage::open(configuration);
  if (!storage || !(*storage)->observations().observe(observation(1)) ||
      !(*storage)->observations().observe(observation(2)))
    return 1;

  Observer first_observer;
  std::atomic<std::uint64_t> notified_generation{};
  {
    service::TorrentMaterializationCoordinator coordinator{
        **storage, configuration.materialization, &first_observer,
        [&](std::uint64_t generation) {
          notified_generation = generation;
          throw std::runtime_error{"simulated search refresh failure"};
        }};
    if (!coordinator.start() || !first_observer.wait_for_processed(2))
      return 2;
    coordinator.stop();
  }
  if (notified_generation == 0 || !first_observer.errors.empty())
    return 3;

  service::LocalMaterializationCheckpointStore checkpoints{
      temporary.path / "operational" / "materialization.checkpoint"};
  auto checkpoint = checkpoints.load();
  if (!checkpoint || checkpoint->observations_processed != 2)
    return 4;
  if (!(*storage)->observations().observe(observation(3)))
    return 5;

  Observer second_observer;
  {
    service::TorrentMaterializationCoordinator coordinator{
        **storage, configuration.materialization, &second_observer};
    if (!coordinator.start() || !second_observer.wait_for_processed(3))
      return 6;
    coordinator.stop();
  }
  std::lock_guard lock{second_observer.mutex};
  if (!second_observer.errors.empty() || second_observer.results.empty() ||
      second_observer.results.back().observations_skipped != 2 ||
      second_observer.results.back().observations_read != 1)
    return 7;
  return 0;
}

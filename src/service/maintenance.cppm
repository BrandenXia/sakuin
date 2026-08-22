export module sakuin.service.maintenance;

import std;

import sakuin.config.model;
import sakuin.core.result;
import sakuin.service.storage;

export namespace sakuin::service {

enum class MaintenanceOperation {
  Retention,
  Compaction,
  Verification,
  GarbageCollection
};

struct MaintenanceEvent {
  LocalDataset dataset;
  MaintenanceOperation operation;
  std::uint64_t segments_affected{};
  std::uint64_t records_checked{};
  std::uint64_t records_expired{};
  std::uint64_t segments_archived{};
  std::uint64_t segments_expired{};
  std::uint64_t objects_deleted{};
  std::uint64_t bytes_before{};
  std::uint64_t bytes_after{};
  std::uint64_t bytes_reclaimed{};
};

class StorageMaintenanceObserver {
public:
  virtual ~StorageMaintenanceObserver() = default;
  virtual void on_maintenance_completed(MaintenanceEvent event) = 0;
  virtual void on_maintenance_error(LocalDataset dataset,
                                    MaintenanceOperation operation,
                                    core::Error error) = 0;
};

// Sakuin-owned lifetime and cancellation boundary for local maintenance. The
// current implementation uses jthread; callers do not depend on that choice,
// leaving the coordinator replaceable by a stdexec scheduler later.
class StorageMaintenanceCoordinator final {
public:
  StorageMaintenanceCoordinator(
      LocalCanonicalStorage &storage,
      config::StorageConfig::MaintenanceConfig configuration,
      StorageMaintenanceObserver *observer = nullptr,
      std::function<void(LocalDataset, std::uint64_t)> on_dataset_committed =
          {});
  ~StorageMaintenanceCoordinator();

  StorageMaintenanceCoordinator(const StorageMaintenanceCoordinator &) = delete;
  StorageMaintenanceCoordinator &
  operator=(const StorageMaintenanceCoordinator &) = delete;

  core::Result<void> start();
  void stop() noexcept;
  bool running() const noexcept { return running_.load(); }

  // Runs a synchronous maintenance pass. Verification is optional because it
  // is substantially more expensive than threshold-based compaction and GC.
  void run_once(bool verify);

private:
  void worker(std::stop_token stop);
  void notify(MaintenanceEvent event) noexcept;
  void notify_error(LocalDataset dataset, MaintenanceOperation operation,
                    core::Error error) noexcept;

  LocalCanonicalStorage &storage_;
  config::StorageConfig::MaintenanceConfig configuration_;
  StorageMaintenanceObserver *observer_{};
  std::function<void(LocalDataset, std::uint64_t)> on_dataset_committed_;
  std::atomic<bool> running_{};
  std::mutex wait_mutex_;
  std::condition_variable_any wake_;
  std::jthread worker_;
};

} // namespace sakuin::service

namespace sakuin::service {

StorageMaintenanceCoordinator::StorageMaintenanceCoordinator(
    LocalCanonicalStorage &storage,
    config::StorageConfig::MaintenanceConfig configuration,
    StorageMaintenanceObserver *observer,
    std::function<void(LocalDataset, std::uint64_t)> on_dataset_committed)
    : storage_(storage), configuration_(configuration), observer_(observer),
      on_dataset_committed_(std::move(on_dataset_committed)) {}

StorageMaintenanceCoordinator::~StorageMaintenanceCoordinator() { stop(); }

core::Result<void> StorageMaintenanceCoordinator::start() {
  if (running_.exchange(true))
    return std::unexpected(core::Error{
        core::ErrorCode::Conflict, "Storage maintenance is already running"});
  try {
    worker_ =
        std::jthread{[this](std::stop_token stop) { worker(std::move(stop)); }};
  } catch (const std::exception &exception) {
    running_ = false;
    return std::unexpected(
        core::Error{core::ErrorCode::Internal,
                    "Unable to start storage maintenance worker: " +
                        std::string{exception.what()}});
  }
  return {};
}

void StorageMaintenanceCoordinator::stop() noexcept {
  if (!running_.exchange(false))
    return;
  worker_.request_stop();
  wake_.notify_all();
  if (worker_.joinable())
    worker_.join();
}

void StorageMaintenanceCoordinator::notify(MaintenanceEvent event) noexcept {
  if (!observer_)
    return;
  try {
    observer_->on_maintenance_completed(event);
  } catch (...) {
  }
}

void StorageMaintenanceCoordinator::notify_error(LocalDataset dataset,
                                                 MaintenanceOperation operation,
                                                 core::Error error) noexcept {
  if (!observer_)
    return;
  try {
    observer_->on_maintenance_error(dataset, operation, std::move(error));
  } catch (...) {
  }
}

void StorageMaintenanceCoordinator::run_once(bool perform_verification) {
  constexpr std::array datasets{LocalDataset::Observations,
                                LocalDataset::Torrents};
  for (const auto dataset : datasets) {
    if (dataset == LocalDataset::Observations) {
      auto retained =
          storage_.retain_observations(std::chrono::system_clock::now());
      if (!retained)
        notify_error(dataset, MaintenanceOperation::Retention,
                     retained.error());
      else
        notify({.dataset = dataset,
                .operation = MaintenanceOperation::Retention,
                .segments_affected =
                    retained->segments_archived + retained->segments_expired,
                .records_expired = retained->records_expired,
                .segments_archived = retained->segments_archived,
                .segments_expired = retained->segments_expired,
                .bytes_before = retained->bytes_before,
                .bytes_after = retained->bytes_after});
      if (retained &&
          (retained->segments_archived != 0 ||
           retained->segments_expired != 0) &&
          on_dataset_committed_) {
        try {
          on_dataset_committed_(dataset, retained->source_generation);
        } catch (...) {
          // The canonical commit is durable; derived views recover from a
          // missed notification on their next canonical update.
        }
      }
    }

    auto compacted = storage_.compact(dataset);
    if (!compacted)
      notify_error(dataset, MaintenanceOperation::Compaction,
                   compacted.error());
    else
      notify({.dataset = dataset,
              .operation = MaintenanceOperation::Compaction,
              .segments_affected = compacted->segments_removed,
              .bytes_before = compacted->bytes_before,
              .bytes_after = compacted->bytes_after});
    if (compacted && compacted->segments_removed != 0 &&
        on_dataset_committed_) {
      try {
        on_dataset_committed_(dataset, compacted->source_generation);
      } catch (...) {
        // Maintenance has already committed. Derived views independently
        // recover from missed notifications on their next canonical update.
      }
    }

    auto collected = storage_.garbage_collect(dataset);
    if (!collected)
      notify_error(dataset, MaintenanceOperation::GarbageCollection,
                   collected.error());
    else
      notify({.dataset = dataset,
              .operation = MaintenanceOperation::GarbageCollection,
              .objects_deleted = collected->objects_deleted,
              .bytes_reclaimed = collected->bytes_reclaimed});

    if (!perform_verification)
      continue;
    auto verified = storage_.verify(dataset);
    if (!verified)
      notify_error(dataset, MaintenanceOperation::Verification,
                   verified.error());
    else
      notify({.dataset = dataset,
              .operation = MaintenanceOperation::Verification,
              .segments_affected = verified->segments_checked,
              .records_checked = verified->records_checked});
  }
}

void StorageMaintenanceCoordinator::worker(std::stop_token stop) {
  auto next_verification =
      std::chrono::steady_clock::now() + configuration_.verification_interval;
  std::unique_lock lock{wait_mutex_};
  while (!stop.stop_requested()) {
    wake_.wait_for(lock, stop, configuration_.interval, [] { return false; });
    if (stop.stop_requested())
      break;
    lock.unlock();
    const auto now = std::chrono::steady_clock::now();
    const bool verify = now >= next_verification;
    run_once(verify);
    if (verify)
      next_verification = now + configuration_.verification_interval;
    lock.lock();
  }
}

} // namespace sakuin::service

export module sakuin.service.local;

import std;

import sakuin.config.model;
import sakuin.api.status;
import sakuin.core.ids;
import sakuin.core.result;
import sakuin.index.duplicates;
import sakuin.runtime.datagram;
import sakuin.runtime.stream;
import sakuin.runtime.traffic;
import sakuin.scheduler.traffic;
import sakuin.scheduler.recovery;
import sakuin.scheduler.work;
import sakuin.search.rebuild;
import sakuin.service.api;
import sakuin.service.duplicates;
import sakuin.service.distributed;
import sakuin.service.maintenance;
import sakuin.service.materialization;
import sakuin.service.runtime;
import sakuin.service.storage;
import sakuin.service.traffic;

export namespace sakuin::service {

// Local single-node DHT vertical slice. Storage is constructed first and
// destroyed last; stop() joins all producers before publishing the final
// partial observation segment.
class LocalAsioDhtService final {
public:
  static core::Result<std::unique_ptr<LocalAsioDhtService>>
  create(const config::AppConfig &configuration, DhtRuntimeObserver &observer,
         DhtRuntimeExternalAddresses external_addresses = {});

  ~LocalAsioDhtService();

  LocalAsioDhtService(const LocalAsioDhtService &) = delete;
  LocalAsioDhtService &operator=(const LocalAsioDhtService &) = delete;

  core::Result<void> start();
  core::Result<void> stop();
  bool running() const noexcept { return runtime_->running(); }

  LocalCanonicalStorage &storage() noexcept { return *storage_; }
  const LocalCanonicalStorage &storage() const noexcept { return *storage_; }
  std::optional<runtime::DatagramEndpoint>
  local_endpoint(runtime::AddressFamily family) const noexcept {
    return runtime_->local_endpoint(family);
  }

private:
  LocalAsioDhtService(std::unique_ptr<LocalCanonicalStorage> storage,
                      std::unique_ptr<scheduler::WorkCoordinator> work,
                      std::unique_ptr<AsioDhtRuntime> runtime)
      : storage_(std::move(storage)), work_(std::move(work)),
        runtime_(std::move(runtime)) {}

  std::unique_ptr<LocalCanonicalStorage> storage_;
  std::unique_ptr<scheduler::WorkCoordinator> work_;
  std::unique_ptr<AsioDhtRuntime> runtime_;
};

// Complete local node composition. The optional API is constructed before the
// DHT runtime so metadata commit notifications can safely target its refresh
// queue. Shutdown reverses producer order before flushing canonical storage.
class LocalSakuinService final {
public:
  static core::Result<std::unique_ptr<LocalSakuinService>>
  create(const config::AppConfig &configuration,
         DhtRuntimeObserver &dht_observer, ApiServiceObserver &api_observer,
         DhtRuntimeExternalAddresses external_addresses = {},
         StorageMaintenanceObserver *maintenance_observer = nullptr,
         MaterializationObserver *materialization_observer = nullptr,
         DuplicateIndexObserver *duplicate_index_observer = nullptr,
         DistributedWorkServiceObserver *distributed_observer = nullptr,
         api::StatusProvider *status = nullptr);

  ~LocalSakuinService();

  LocalSakuinService(const LocalSakuinService &) = delete;
  LocalSakuinService &operator=(const LocalSakuinService &) = delete;

  core::Result<void> start();
  core::Result<void> stop();
  bool running() const noexcept;

  LocalCanonicalStorage &storage() noexcept { return *storage_; }
  const LocalCanonicalStorage &storage() const noexcept { return *storage_; }
  std::optional<runtime::DatagramEndpoint>
  dht_endpoint(runtime::AddressFamily family) const noexcept {
    return runtime_->local_endpoint(family);
  }
  std::optional<runtime::DatagramEndpoint> api_endpoint() const noexcept {
    return api_ ? std::optional{api_->local_endpoint()} : std::nullopt;
  }
  std::optional<runtime::StreamEndpoint> coordinator_endpoint() const noexcept {
    return distributed_ ? std::optional{distributed_->local_endpoint()}
                        : std::nullopt;
  }
  core::Result<void> reload_api_credentials();
  core::Result<search::SearchRebuildResult> refresh_search();
  core::Result<index::DuplicateSynchronizationResult> refresh_duplicates();
  core::Result<std::vector<index::DuplicateGroup>>
  duplicate_groups(index::DuplicateFingerprintAlgorithm algorithm,
                   std::size_t minimum_members = 2) const;
  std::vector<index::DuplicateGroup>
  duplicate_matches(const core::InfoHash &torrent) const;

private:
  LocalSakuinService(
      std::unique_ptr<LocalCanonicalStorage> storage,
      std::unique_ptr<LocalApiService> api,
      std::unique_ptr<TorrentMaterializationCoordinator> materialization,
      std::unique_ptr<DuplicateIndexCoordinator> duplicates,
      std::unique_ptr<StorageMaintenanceCoordinator> maintenance,
      std::unique_ptr<scheduler::WorkCoordinator> work,
      std::unique_ptr<runtime::TrafficGovernor> aggregate_traffic,
      std::unique_ptr<scheduler::GovernorTrafficGrantSource> traffic_grants,
      std::unique_ptr<scheduler::GrantedTrafficGovernor> local_traffic,
      std::unique_ptr<DistributedWorkService> distributed,
      std::unique_ptr<AsioDhtRuntime> runtime)
      : storage_(std::move(storage)), api_(std::move(api)),
        materialization_(std::move(materialization)),
        duplicates_(std::move(duplicates)),
        maintenance_(std::move(maintenance)), work_(std::move(work)),
        aggregate_traffic_(std::move(aggregate_traffic)),
        traffic_grants_(std::move(traffic_grants)),
        local_traffic_(std::move(local_traffic)),
        distributed_(std::move(distributed)), runtime_(std::move(runtime)) {}

  std::unique_ptr<LocalCanonicalStorage> storage_;
  std::unique_ptr<LocalApiService> api_;
  std::unique_ptr<TorrentMaterializationCoordinator> materialization_;
  std::unique_ptr<DuplicateIndexCoordinator> duplicates_;
  std::unique_ptr<StorageMaintenanceCoordinator> maintenance_;
  std::unique_ptr<scheduler::WorkCoordinator> work_;
  std::unique_ptr<runtime::TrafficGovernor> aggregate_traffic_;
  std::unique_ptr<scheduler::GovernorTrafficGrantSource> traffic_grants_;
  std::unique_ptr<scheduler::GrantedTrafficGovernor> local_traffic_;
  std::unique_ptr<DistributedWorkService> distributed_;
  std::unique_ptr<AsioDhtRuntime> runtime_;
};

} // namespace sakuin::service

namespace sakuin::service {
namespace {

core::Result<std::unique_ptr<scheduler::WorkCoordinator>>
create_local_work_coordinator(const config::AppConfig &configuration) {
  const scheduler::WorkCoordinatorOptions options{
      .maximum_work_items = configuration.distributed.maximum_work_items,
      .maximum_payload_bytes = configuration.distributed.maximum_payload_bytes,
      .worker_timeout = configuration.distributed.worker_timeout,
      .lease_duration = configuration.distributed.lease_duration,
      .maximum_terminal_work_items =
          configuration.distributed.coordinator.maximum_terminal_work_items,
      .terminal_work_retention =
          configuration.distributed.coordinator.terminal_work_retention};
  const auto &recovery = configuration.distributed.coordinator;
  if (!recovery.recovery_enabled) {
    auto created = scheduler::LocalWorkCoordinator::create(options);
    if (!created)
      return std::unexpected(created.error());
    return std::unique_ptr<scheduler::WorkCoordinator>{std::move(*created)};
  }
  auto path = recovery.recovery_file.value_or(configuration.storage.local_root /
                                              "operational" / "scheduler" /
                                              "work.checkpoint");
  auto created = scheduler::RecoveringWorkCoordinator::open_local(
      options, std::move(path), recovery.recovery_maximum_bytes);
  if (!created)
    return std::unexpected(created.error());
  return std::unique_ptr<scheduler::WorkCoordinator>{std::move(*created)};
}

} // namespace

core::Result<std::unique_ptr<LocalAsioDhtService>>
LocalAsioDhtService::create(const config::AppConfig &configuration,
                            DhtRuntimeObserver &observer,
                            DhtRuntimeExternalAddresses external_addresses) {
  if (auto valid = config::validate(configuration); !valid)
    return std::unexpected(valid.error());
  auto storage = LocalCanonicalStorage::open(configuration.storage);
  if (!storage)
    return std::unexpected(storage.error());
  auto work = create_local_work_coordinator(configuration);
  if (!work)
    return std::unexpected(work.error());
  auto runtime =
      AsioDhtRuntime::create(configuration.network,
                             {.observations = &(*storage)->observations(),
                              .torrents = &(*storage)->torrents(),
                              .observer = &observer,
                              .work = work->get(),
                              .worker_heartbeat_interval =
                                  configuration.distributed.heartbeat_interval},
                             std::move(external_addresses));
  if (!runtime)
    return std::unexpected(runtime.error());
  return std::unique_ptr<LocalAsioDhtService>{new LocalAsioDhtService{
      std::move(*storage), std::move(*work), std::move(*runtime)}};
}

LocalAsioDhtService::~LocalAsioDhtService() {
  if (runtime_)
    runtime_->stop();
  if (storage_)
    static_cast<void>(storage_->flush());
}

core::Result<void> LocalAsioDhtService::start() { return runtime_->start(); }

core::Result<void> LocalAsioDhtService::stop() {
  runtime_->stop();
  return storage_->flush();
}

core::Result<std::unique_ptr<LocalSakuinService>>
LocalSakuinService::create(const config::AppConfig &configuration,
                           DhtRuntimeObserver &dht_observer,
                           ApiServiceObserver &api_observer,
                           DhtRuntimeExternalAddresses external_addresses,
                           StorageMaintenanceObserver *maintenance_observer,
                           MaterializationObserver *materialization_observer,
                           DuplicateIndexObserver *duplicate_index_observer,
                           DistributedWorkServiceObserver *distributed_observer,
                           api::StatusProvider *status) {
  if (auto valid = config::validate(configuration); !valid)
    return std::unexpected(valid.error());
  auto storage = LocalCanonicalStorage::open(configuration.storage);
  if (!storage)
    return std::unexpected(storage.error());
  auto work = create_local_work_coordinator(configuration);
  if (!work)
    return std::unexpected(work.error());

  std::unique_ptr<DuplicateIndexCoordinator> duplicates;
  if (configuration.indexing.duplicates.enabled) {
    auto created = DuplicateIndexCoordinator::create(
        **storage, configuration.indexing.duplicates, duplicate_index_observer);
    if (!created)
      return std::unexpected(created.error());
    duplicates = std::move(*created);
  }
  std::unique_ptr<LocalApiService> api;
  if (configuration.api.enabled) {
    auto created = LocalApiService::create(
        configuration.api, (*storage)->torrents(), api_observer,
        (*storage)->root() / "derived" / "search", duplicates.get(), status);
    if (!created)
      return std::unexpected(created.error());
    api = std::move(*created);
  }
  auto *api_refresh = api.get();
  (*storage)->set_work_result_torrent_callback(
      api_refresh
          ? std::function<void(std::uint64_t)>{[api_refresh](
                                                   std::uint64_t generation) {
              api_refresh->request_search_refresh(generation);
            }}
          : std::function<void(std::uint64_t)>{});
  std::unique_ptr<runtime::TrafficGovernor> aggregate_traffic;
  std::unique_ptr<scheduler::GovernorTrafficGrantSource> traffic_grants;
  std::unique_ptr<scheduler::GrantedTrafficGovernor> local_traffic;
  std::unique_ptr<DistributedWorkService> distributed;
  if (configuration.distributed.coordinator.enabled) {
    auto aggregate = create_traffic_governor(configuration.network.traffic);
    if (!aggregate)
      return std::unexpected(aggregate.error());
    aggregate_traffic = std::move(*aggregate);
    traffic_grants = std::make_unique<scheduler::GovernorTrafficGrantSource>(
        *aggregate_traffic);
    auto granted = scheduler::GrantedTrafficGovernor::create(
        *traffic_grants, "coordinator-local",
        configuration.network.traffic.grant_bytes);
    if (!granted)
      return std::unexpected(granted.error());
    local_traffic = std::move(*granted);
    auto created = DistributedWorkService::create(
        configuration.distributed, **work, distributed_observer,
        &(*storage)->work_results(), traffic_grants.get());
    if (!created)
      return std::unexpected(created.error());
    distributed = std::move(*created);
  }
  std::unique_ptr<StorageMaintenanceCoordinator> maintenance;
  if (configuration.storage.maintenance.enabled)
    maintenance = std::make_unique<StorageMaintenanceCoordinator>(
        **storage, configuration.storage.maintenance, maintenance_observer,
        api_refresh
            ? std::function<void(LocalDataset,
                                 std::uint64_t)>{[api_refresh](
                                                     LocalDataset dataset,
                                                     std::uint64_t generation) {
                if (dataset == LocalDataset::Torrents)
                  api_refresh->request_search_refresh(generation);
              }}
            : std::function<void(LocalDataset, std::uint64_t)>{});
  if (api && maintenance) {
    auto configured = api->set_maintenance_requester(
        [coordinator = maintenance.get()](bool verify) {
          return coordinator->request_run(verify);
        });
    if (!configured)
      return std::unexpected(configured.error());
  }
  std::unique_ptr<TorrentMaterializationCoordinator> materialization;
  if (configuration.storage.materialization.enabled)
    materialization = std::make_unique<TorrentMaterializationCoordinator>(
        **storage, configuration.storage.materialization,
        materialization_observer,
        api_refresh
            ? std::function<void(std::uint64_t)>{[api_refresh](
                                                     std::uint64_t generation) {
                api_refresh->request_search_refresh(generation);
              }}
            : std::function<void(std::uint64_t)>{});
  auto runtime = AsioDhtRuntime::create(
      configuration.network,
      {.observations = &(*storage)->observations(),
       .torrents = &(*storage)->torrents(),
       .observer = &dht_observer,
       .work = work->get(),
       .traffic = local_traffic.get(),
       .worker_heartbeat_interval =
           configuration.distributed.heartbeat_interval,
       .on_torrent_committed =
           api_refresh
               ? std::function<void(
                     std::uint64_t)>{[api_refresh](std::uint64_t generation) {
                   api_refresh->request_search_refresh(generation);
                 }}
               : std::function<void(std::uint64_t)>{}},
      std::move(external_addresses));
  if (!runtime)
    return std::unexpected(runtime.error());
  return std::unique_ptr<LocalSakuinService>{new LocalSakuinService{
      std::move(*storage), std::move(api), std::move(materialization),
      std::move(duplicates), std::move(maintenance), std::move(*work),
      std::move(aggregate_traffic), std::move(traffic_grants),
      std::move(local_traffic), std::move(distributed), std::move(*runtime)}};
}

LocalSakuinService::~LocalSakuinService() { static_cast<void>(stop()); }

core::Result<void> LocalSakuinService::start() {
  if (api_) {
    auto started = api_->start();
    if (!started)
      return std::unexpected(started.error());
  }
  if (materialization_) {
    auto started = materialization_->start();
    if (!started) {
      if (api_)
        api_->stop();
      return std::unexpected(started.error());
    }
  }
  if (duplicates_) {
    auto started = duplicates_->start();
    if (!started) {
      if (materialization_)
        materialization_->stop();
      if (api_)
        api_->stop();
      return std::unexpected(started.error());
    }
  }
  if (maintenance_) {
    auto started = maintenance_->start();
    if (!started) {
      if (materialization_)
        materialization_->stop();
      if (duplicates_)
        duplicates_->stop();
      if (api_)
        api_->stop();
      return std::unexpected(started.error());
    }
  }
  if (distributed_) {
    auto started = distributed_->start();
    if (!started) {
      if (maintenance_)
        maintenance_->stop();
      if (materialization_)
        materialization_->stop();
      if (duplicates_)
        duplicates_->stop();
      if (api_)
        api_->stop();
      return std::unexpected(started.error());
    }
  }
  auto started = runtime_->start();
  if (!started) {
    if (distributed_)
      distributed_->stop();
    if (maintenance_)
      maintenance_->stop();
    if (materialization_)
      materialization_->stop();
    if (duplicates_)
      duplicates_->stop();
    if (api_)
      api_->stop();
    return std::unexpected(started.error());
  }
  return {};
}

core::Result<void> LocalSakuinService::stop() {
  runtime_->stop();
  if (distributed_)
    distributed_->stop();
  std::optional<core::Error> materialization_error;
  if (materialization_) {
    materialization_->stop();
    if (auto materialized = materialization_->run_once(); !materialized)
      materialization_error = materialized.error();
  }
  if (maintenance_)
    maintenance_->stop();
  std::optional<core::Error> duplicate_error;
  if (duplicates_) {
    duplicates_->stop();
    if (auto synchronized = duplicates_->run_once(); !synchronized)
      duplicate_error = synchronized.error();
  }
  if (api_)
    api_->stop();
  auto flushed = storage_->flush();
  if (materialization_error)
    return std::unexpected(std::move(*materialization_error));
  if (duplicate_error)
    return std::unexpected(std::move(*duplicate_error));
  return flushed;
}

bool LocalSakuinService::running() const noexcept {
  return runtime_->running() && (!api_ || api_->running()) &&
         (!distributed_ || distributed_->running()) &&
         (!maintenance_ || maintenance_->running()) &&
         (!materialization_ || materialization_->running()) &&
         (!duplicates_ || duplicates_->running());
}

core::Result<void> LocalSakuinService::reload_api_credentials() {
  if (!api_)
    return std::unexpected(
        core::Error{core::ErrorCode::NotFound, "API service is disabled"});
  return api_->reload_credentials();
}

core::Result<search::SearchRebuildResult> LocalSakuinService::refresh_search() {
  if (!api_)
    return std::unexpected(
        core::Error{core::ErrorCode::NotFound, "API service is disabled"});
  return api_->refresh_search();
}

core::Result<index::DuplicateSynchronizationResult>
LocalSakuinService::refresh_duplicates() {
  if (!duplicates_)
    return std::unexpected(
        core::Error{core::ErrorCode::NotFound, "Duplicate index is disabled"});
  return duplicates_->run_once();
}

core::Result<std::vector<index::DuplicateGroup>>
LocalSakuinService::duplicate_groups(
    index::DuplicateFingerprintAlgorithm algorithm,
    std::size_t minimum_members) const {
  if (!duplicates_)
    return std::unexpected(
        core::Error{core::ErrorCode::NotFound, "Duplicate index is disabled"});
  return duplicates_->groups(algorithm, minimum_members);
}

std::vector<index::DuplicateGroup>
LocalSakuinService::duplicate_matches(const core::InfoHash &torrent) const {
  return duplicates_ ? duplicates_->matches(torrent)
                     : std::vector<index::DuplicateGroup>{};
}

} // namespace sakuin::service

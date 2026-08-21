export module sakuin.service.local;

import std;

import sakuin.config.model;
import sakuin.core.result;
import sakuin.runtime.datagram;
import sakuin.search.rebuild;
import sakuin.service.api;
import sakuin.service.runtime;
import sakuin.service.storage;

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
                      std::unique_ptr<AsioDhtRuntime> runtime)
      : storage_(std::move(storage)), runtime_(std::move(runtime)) {}

  std::unique_ptr<LocalCanonicalStorage> storage_;
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
         DhtRuntimeExternalAddresses external_addresses = {});

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
  core::Result<void> reload_api_credentials();
  core::Result<search::SearchRebuildResult> refresh_search();

private:
  LocalSakuinService(std::unique_ptr<LocalCanonicalStorage> storage,
                     std::unique_ptr<LocalApiService> api,
                     std::unique_ptr<AsioDhtRuntime> runtime)
      : storage_(std::move(storage)), api_(std::move(api)),
        runtime_(std::move(runtime)) {}

  std::unique_ptr<LocalCanonicalStorage> storage_;
  std::unique_ptr<LocalApiService> api_;
  std::unique_ptr<AsioDhtRuntime> runtime_;
};

} // namespace sakuin::service

namespace sakuin::service {

core::Result<std::unique_ptr<LocalAsioDhtService>>
LocalAsioDhtService::create(const config::AppConfig &configuration,
                            DhtRuntimeObserver &observer,
                            DhtRuntimeExternalAddresses external_addresses) {
  if (auto valid = config::validate(configuration); !valid)
    return std::unexpected(valid.error());
  auto storage = LocalCanonicalStorage::open(configuration.storage);
  if (!storage)
    return std::unexpected(storage.error());
  auto runtime =
      AsioDhtRuntime::create(configuration.network,
                             {.observations = &(*storage)->observations(),
                              .torrents = &(*storage)->torrents(),
                              .observer = &observer},
                             std::move(external_addresses));
  if (!runtime)
    return std::unexpected(runtime.error());
  return std::unique_ptr<LocalAsioDhtService>{
      new LocalAsioDhtService{std::move(*storage), std::move(*runtime)}};
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
                           DhtRuntimeExternalAddresses external_addresses) {
  if (auto valid = config::validate(configuration); !valid)
    return std::unexpected(valid.error());
  auto storage = LocalCanonicalStorage::open(configuration.storage);
  if (!storage)
    return std::unexpected(storage.error());

  std::unique_ptr<LocalApiService> api;
  if (configuration.api.enabled) {
    auto created = LocalApiService::create(
        configuration.api, (*storage)->torrents(), api_observer);
    if (!created)
      return std::unexpected(created.error());
    api = std::move(*created);
  }
  auto *api_refresh = api.get();
  auto runtime = AsioDhtRuntime::create(
      configuration.network,
      {.observations = &(*storage)->observations(),
       .torrents = &(*storage)->torrents(),
       .observer = &dht_observer,
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
      std::move(*storage), std::move(api), std::move(*runtime)}};
}

LocalSakuinService::~LocalSakuinService() {
  if (runtime_)
    runtime_->stop();
  if (api_)
    api_->stop();
  if (storage_)
    static_cast<void>(storage_->flush());
}

core::Result<void> LocalSakuinService::start() {
  if (api_) {
    auto started = api_->start();
    if (!started)
      return std::unexpected(started.error());
  }
  auto started = runtime_->start();
  if (!started) {
    if (api_)
      api_->stop();
    return std::unexpected(started.error());
  }
  return {};
}

core::Result<void> LocalSakuinService::stop() {
  runtime_->stop();
  if (api_)
    api_->stop();
  return storage_->flush();
}

bool LocalSakuinService::running() const noexcept {
  return runtime_->running() && (!api_ || api_->running());
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

} // namespace sakuin::service

export module sakuin.service.local;

import std;

import sakuin.config.model;
import sakuin.core.result;
import sakuin.runtime.datagram;
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

} // namespace sakuin::service

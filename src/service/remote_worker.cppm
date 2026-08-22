export module sakuin.service.remote_worker;

import std;

import sakuin.config.model;
import sakuin.core.result;
import sakuin.runtime.datagram;
import sakuin.service.runtime;

export namespace sakuin::service {

// Disposable remote DHT process composition. Observations and fetched logical
// metadata cross the mutually authenticated result channel; canonical storage
// remains coordinator-owned.
class RemoteDhtWorkerService final {
public:
  static core::Result<std::unique_ptr<RemoteDhtWorkerService>>
  create(const config::AppConfig &configuration, DhtRuntimeObserver &observer,
         DhtRuntimeExternalAddresses external_addresses = {});

  ~RemoteDhtWorkerService();

  RemoteDhtWorkerService(const RemoteDhtWorkerService &) = delete;
  RemoteDhtWorkerService &operator=(const RemoteDhtWorkerService &) = delete;

  core::Result<void> start();
  core::Result<void> stop();
  bool running() const noexcept;
  std::optional<runtime::DatagramEndpoint>
  local_endpoint(runtime::AddressFamily family) const noexcept;

private:
  struct Impl;
  explicit RemoteDhtWorkerService(std::unique_ptr<Impl> impl);

  std::unique_ptr<Impl> impl_;
};

} // namespace sakuin::service

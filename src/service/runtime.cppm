export module sakuin.service.runtime;

import std;

import sakuin.config.model;
import sakuin.core.result;
import sakuin.core.time;
import sakuin.dht.identity;
import sakuin.dht.krpc;
import sakuin.dht.metadata_fetch;
import sakuin.dht.observation;
import sakuin.integration.dht_worker;
import sakuin.runtime.asio_resolver;
import sakuin.runtime.datagram;
import sakuin.runtime.traffic;
import sakuin.scheduler.work;
import sakuin.service.dht;
import sakuin.service.traffic;
import sakuin.storage.dataset.torrents;

export namespace sakuin::service {

struct DhtRuntimeExternalAddresses {
  std::optional<runtime::IpAddress> ipv4;
  std::optional<runtime::IpAddress> ipv6;
};

// Callbacks may arrive concurrently from the IPv4, IPv6, and identity
// supervisor threads. Implementations must serialize any shared state.
class DhtRuntimeObserver {
public:
  virtual ~DhtRuntimeObserver() = default;
  virtual void on_family_cycle(runtime::AddressFamily family,
                               integration::DhtRuntimeCycle cycle) = 0;
  virtual void on_family_error(runtime::AddressFamily family,
                               core::Error error) = 0;
  virtual void on_identity_reconfigured(runtime::AddressFamily,
                                        const dht::krpc::NodeId &,
                                        const dht::krpc::NodeId &,
                                        runtime::IpAddress) {}
};

struct AsioDhtRuntimeDependencies {
  dht::ObservationSink *observations{};
  storage::TorrentDataset *torrents{};
  dht::MetadataFetchObserver *metadata_results{};
  DhtRuntimeObserver *observer{};
  scheduler::WorkCoordinator *work{};
  runtime::TrafficGovernor *traffic{};
  std::string worker_namespace;
  core::Duration worker_heartbeat_interval{std::chrono::seconds{10}};
  std::function<void(std::uint64_t)> on_torrent_committed;
};

// Owns the enabled address-family workers and their one shared traffic budget.
// BEP 42 replacement happens on a supervisor thread, never inside a family
// owner callback, and replaces the whole node/runtime before policy commit.
class AsioDhtRuntime final {
public:
  static core::Result<std::unique_ptr<AsioDhtRuntime>>
  create(const config::NetworkConfig &configuration,
         AsioDhtRuntimeDependencies dependencies,
         DhtRuntimeExternalAddresses external_addresses = {});

  ~AsioDhtRuntime();

  AsioDhtRuntime(const AsioDhtRuntime &) = delete;
  AsioDhtRuntime &operator=(const AsioDhtRuntime &) = delete;

  core::Result<void> start();
  void stop() noexcept;
  bool running() const noexcept {
    return running_.load(std::memory_order_acquire);
  }
  std::optional<runtime::DatagramEndpoint>
  local_endpoint(runtime::AddressFamily family) const noexcept;

private:
  class FamilyObserver;

  AsioDhtRuntime(config::NetworkConfig configuration,
                 AsioDhtRuntimeDependencies dependencies,
                 std::unique_ptr<runtime::TrafficGovernor> traffic);

  core::Result<std::unique_ptr<AsioDhtFamilyRuntime>>
  create_family(runtime::AddressFamily family, std::uint16_t listen_port,
                std::optional<runtime::IpAddress> external);
  void request_reconfiguration(runtime::AddressFamily family,
                               runtime::IpAddress external) noexcept;
  void run_supervisor(std::stop_token stop) noexcept;
  void reconfigure(runtime::AddressFamily family,
                   runtime::IpAddress external) noexcept;
  void restore_family(runtime::AddressFamily family, std::uint16_t listen_port,
                      std::optional<runtime::IpAddress> external,
                      core::Error cause) noexcept;
  void report(runtime::AddressFamily family, core::Error error) noexcept;

  config::NetworkConfig configuration_;
  AsioDhtRuntimeDependencies dependencies_;
  std::unique_ptr<runtime::TrafficGovernor> owned_traffic_;
  runtime::TrafficGovernor *traffic_{};
  std::vector<runtime::DatagramEndpoint> ipv4_bootstrap_;
  std::vector<runtime::DatagramEndpoint> ipv6_bootstrap_;
  std::unique_ptr<FamilyObserver> ipv4_observer_;
  std::unique_ptr<FamilyObserver> ipv6_observer_;
  std::unique_ptr<AsioDhtFamilyRuntime> ipv4_;
  std::unique_ptr<AsioDhtFamilyRuntime> ipv6_;
  std::mutex requests_mutex_;
  std::condition_variable_any requests_changed_;
  std::deque<std::pair<runtime::AddressFamily, runtime::IpAddress>> requests_;
  std::jthread supervisor_;
  std::atomic<bool> running_{};
};

} // namespace sakuin::service

namespace sakuin::service {

class AsioDhtRuntime::FamilyObserver final
    : public integration::DhtRuntimeWorkerObserver {
public:
  FamilyObserver(AsioDhtRuntime &owner, runtime::AddressFamily family)
      : owner_(&owner), family_(family) {}

  void on_cycle(integration::DhtRuntimeCycle cycle) override {
    const auto proposal = cycle.poll.identity_reconfiguration;
    if (proposal)
      owner_->request_reconfiguration(family_, proposal->external_address);
    owner_->dependencies_.observer->on_family_cycle(family_, std::move(cycle));
  }

  void on_worker_error(core::Error error) override {
    owner_->report(family_, std::move(error));
  }

private:
  AsioDhtRuntime *owner_;
  runtime::AddressFamily family_;
};

AsioDhtRuntime::AsioDhtRuntime(
    config::NetworkConfig configuration,
    AsioDhtRuntimeDependencies dependencies,
    std::unique_ptr<runtime::TrafficGovernor> traffic)
    : configuration_(std::move(configuration)), dependencies_(dependencies),
      owned_traffic_(std::move(traffic)),
      traffic_(dependencies_.traffic ? dependencies_.traffic
                                     : owned_traffic_.get()) {}

core::Result<std::unique_ptr<AsioDhtRuntime>>
AsioDhtRuntime::create(const config::NetworkConfig &configuration,
                       AsioDhtRuntimeDependencies dependencies,
                       DhtRuntimeExternalAddresses external_addresses) {
  if (!dependencies.observations || !dependencies.observer)
    return std::unexpected(
        core::Error{core::ErrorCode::InvalidArgument,
                    "DHT runtime requires observation and runtime observers"});
  if (configuration.dht.metadata.enabled && !dependencies.torrents &&
      !dependencies.metadata_results)
    return std::unexpected(
        core::Error{core::ErrorCode::InvalidArgument,
                    "Enabled metadata acquisition requires a canonical or "
                    "remote result sink"});
  if (dependencies.torrents && dependencies.metadata_results)
    return std::unexpected(core::Error{
        core::ErrorCode::InvalidArgument,
        "Metadata acquisition must use exactly one completion sink"});
  if (!configuration.enable_ipv4 && !configuration.enable_ipv6)
    return std::unexpected(core::Error{
        core::ErrorCode::InvalidArgument,
        "DHT runtime requires at least one enabled address family"});

  std::unique_ptr<runtime::TrafficGovernor> traffic;
  if (!dependencies.traffic) {
    auto created = create_traffic_governor(configuration.traffic);
    if (!created)
      return std::unexpected(created.error());
    traffic = std::move(*created);
  }
  auto result = std::unique_ptr<AsioDhtRuntime>{
      new AsioDhtRuntime{configuration, dependencies, std::move(traffic)}};

  runtime::AsioDatagramEndpointResolver resolver;
  if (configuration.enable_ipv4) {
    auto bootstrap = resolve_dht_bootstrap(
        configuration.dht.bootstrap, runtime::AddressFamily::IPv4, resolver);
    if (!bootstrap)
      return std::unexpected(bootstrap.error());
    result->ipv4_bootstrap_ = std::move(*bootstrap);
    result->ipv4_observer_ =
        std::make_unique<FamilyObserver>(*result, runtime::AddressFamily::IPv4);
    auto family = result->create_family(runtime::AddressFamily::IPv4,
                                        configuration.listen_port,
                                        external_addresses.ipv4);
    if (!family)
      return std::unexpected(family.error());
    result->ipv4_ = std::move(*family);
  }
  if (configuration.enable_ipv6) {
    auto bootstrap = resolve_dht_bootstrap(
        configuration.dht.bootstrap, runtime::AddressFamily::IPv6, resolver);
    if (!bootstrap)
      return std::unexpected(bootstrap.error());
    result->ipv6_bootstrap_ = std::move(*bootstrap);
    result->ipv6_observer_ =
        std::make_unique<FamilyObserver>(*result, runtime::AddressFamily::IPv6);
    auto family = result->create_family(runtime::AddressFamily::IPv6,
                                        configuration.listen_port,
                                        external_addresses.ipv6);
    if (!family)
      return std::unexpected(family.error());
    result->ipv6_ = std::move(*family);
  }
  return result;
}

core::Result<std::unique_ptr<AsioDhtFamilyRuntime>>
AsioDhtRuntime::create_family(runtime::AddressFamily family,
                              std::uint16_t listen_port,
                              std::optional<runtime::IpAddress> external) {
  auto material = secure_dht_family_runtime_material(
      configuration_.dht.identity, family, external);
  if (!material)
    return std::unexpected(material.error());
  auto &bootstrap = family == runtime::AddressFamily::IPv4 ? ipv4_bootstrap_
                                                           : ipv6_bootstrap_;
  auto *observer = family == runtime::AddressFamily::IPv4
                       ? ipv4_observer_.get()
                       : ipv6_observer_.get();
  return AsioDhtFamilyRuntime::create(
      configuration_.dht, family, listen_port, bootstrap, std::move(*material),
      {.observations = dependencies_.observations,
       .torrents = dependencies_.torrents,
       .metadata_results = dependencies_.metadata_results,
       .observer = observer,
       .traffic = traffic_,
       .work = dependencies_.work,
       .worker_namespace = dependencies_.worker_namespace,
       .worker_heartbeat_interval = dependencies_.worker_heartbeat_interval,
       .on_torrent_committed = dependencies_.on_torrent_committed});
}

AsioDhtRuntime::~AsioDhtRuntime() { stop(); }

core::Result<void> AsioDhtRuntime::start() {
  bool expected = false;
  if (!running_.compare_exchange_strong(expected, true,
                                        std::memory_order_acq_rel))
    return std::unexpected(
        core::Error{core::ErrorCode::Conflict, "DHT runtime already started"});
  if (ipv4_) {
    auto started = ipv4_->start();
    if (!started) {
      running_.store(false, std::memory_order_release);
      return std::unexpected(started.error());
    }
  }
  if (ipv6_) {
    auto started = ipv6_->start();
    if (!started) {
      if (ipv4_)
        ipv4_->stop();
      running_.store(false, std::memory_order_release);
      return std::unexpected(started.error());
    }
  }
  try {
    supervisor_ =
        std::jthread{[this](std::stop_token stop) { run_supervisor(stop); }};
  } catch (const std::exception &exception) {
    if (ipv6_)
      ipv6_->stop();
    if (ipv4_)
      ipv4_->stop();
    running_.store(false, std::memory_order_release);
    return std::unexpected(
        core::Error{core::ErrorCode::Internal,
                    std::string{"Failed to start DHT identity supervisor: "} +
                        exception.what()});
  }
  return {};
}

void AsioDhtRuntime::stop() noexcept {
  running_.store(false, std::memory_order_release);
  if (supervisor_.joinable()) {
    supervisor_.request_stop();
    requests_changed_.notify_all();
    if (supervisor_.get_id() != std::this_thread::get_id())
      supervisor_.join();
  }
  if (ipv6_)
    ipv6_->stop();
  if (ipv4_)
    ipv4_->stop();
}

std::optional<runtime::DatagramEndpoint>
AsioDhtRuntime::local_endpoint(runtime::AddressFamily family) const noexcept {
  const auto *family_runtime =
      family == runtime::AddressFamily::IPv4 ? ipv4_.get() : ipv6_.get();
  if (!family_runtime)
    return std::nullopt;
  return family_runtime->local_endpoint();
}

void AsioDhtRuntime::request_reconfiguration(
    runtime::AddressFamily family, runtime::IpAddress external) noexcept {
  if (!running())
    return;
  {
    std::lock_guard lock{requests_mutex_};
    std::erase_if(requests_, [family](const auto &request) {
      return request.first == family;
    });
    requests_.emplace_back(family, external);
  }
  requests_changed_.notify_all();
}

void AsioDhtRuntime::run_supervisor(std::stop_token stop) noexcept {
  while (!stop.stop_requested()) {
    std::pair<runtime::AddressFamily, runtime::IpAddress> request;
    {
      std::unique_lock lock{requests_mutex_};
      requests_changed_.wait(lock, stop, [&] { return !requests_.empty(); });
      if (stop.stop_requested())
        break;
      request = requests_.front();
      requests_.pop_front();
    }
    reconfigure(request.first, request.second);
  }
}

void AsioDhtRuntime::reconfigure(runtime::AddressFamily family,
                                 runtime::IpAddress external) noexcept {
  auto &slot = family == runtime::AddressFamily::IPv4 ? ipv4_ : ipv6_;
  if (!slot || !slot->identity_policy() ||
      slot->identity_policy()->proposed_external() != external)
    return;

  const auto old_id = slot->node_id();
  const auto old_external = slot->identity_policy()->current_external();
  const auto bound_port = slot->local_endpoint().port;
  slot->stop();
  auto replacement = create_family(family, bound_port, external);
  if (!replacement) {
    restore_family(family, bound_port, old_external, replacement.error());
    return;
  }
  if (auto started = (*replacement)->start(); !started) {
    (*replacement)->stop();
    (*replacement).reset();
    restore_family(family, bound_port, old_external, started.error());
    return;
  }
  if (auto committed = slot->identity_policy()->commit(external); !committed) {
    (*replacement)->stop();
    (*replacement).reset();
    restore_family(family, bound_port, old_external, committed.error());
    return;
  }
  const auto new_id = (*replacement)->node_id();
  slot = std::move(*replacement);
  try {
    dependencies_.observer->on_identity_reconfigured(family, old_id, new_id,
                                                     external);
  } catch (...) {
    report(family,
           {core::ErrorCode::Internal,
            "DHT identity-reconfiguration observer threw an exception"});
  }
}

void AsioDhtRuntime::restore_family(runtime::AddressFamily family,
                                    std::uint16_t listen_port,
                                    std::optional<runtime::IpAddress> external,
                                    core::Error cause) noexcept {
  report(family, std::move(cause));
  auto &slot = family == runtime::AddressFamily::IPv4 ? ipv4_ : ipv6_;
  auto restored = create_family(family, listen_port, external);
  if (!restored) {
    report(family, restored.error());
    slot.reset();
    return;
  }
  if (auto started = (*restored)->start(); !started) {
    report(family, started.error());
    slot.reset();
    return;
  }
  slot = std::move(*restored);
}

void AsioDhtRuntime::report(runtime::AddressFamily family,
                            core::Error error) noexcept {
  try {
    dependencies_.observer->on_family_error(family, std::move(error));
  } catch (...) {
    // Runtime observers cannot unwind through worker/supervisor threads.
  }
}

} // namespace sakuin::service

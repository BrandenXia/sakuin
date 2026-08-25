export module sakuin.service.dht;

import std;

import sakuin.config.model;
import sakuin.core.bytes;
import sakuin.core.random;
import sakuin.core.result;
import sakuin.core.time;
import sakuin.dht.bootstrap;
import sakuin.dht.discovery;
import sakuin.dht.identity;
import sakuin.dht.krpc;
import sakuin.dht.metadata;
import sakuin.dht.metadata_controller;
import sakuin.dht.metadata_fetch;
import sakuin.dht.node;
import sakuin.dht.observation;
import sakuin.dht.peer_discovery;
import sakuin.dht.routing_maintenance;
import sakuin.dht.runtime;
import sakuin.dht.token;
import sakuin.integration.dht_config;
import sakuin.integration.dht_runtime;
import sakuin.integration.dht_worker;
import sakuin.integration.metadata_backfill;
import sakuin.integration.metadata_config;
import sakuin.runtime.asio;
import sakuin.runtime.asio_resolver;
import sakuin.runtime.asio_stream;
import sakuin.runtime.datagram;
import sakuin.runtime.traffic;
import sakuin.scheduler.work;
import sakuin.storage.dataset.torrents;

export namespace sakuin::service {

struct DhtFamilyRuntimeMaterial {
  dht::krpc::NodeId node_id;
  dht::PeerId peer_id;
  dht::AnnounceTokenSecret token_secret;
  std::optional<runtime::IpAddress> current_external;
};

core::Result<DhtFamilyRuntimeMaterial> secure_dht_family_runtime_material(
    const config::DhtIdentityConfig &identity,
    runtime::AddressFamily address_family,
    std::optional<runtime::IpAddress> current_external = std::nullopt);

core::Result<std::vector<runtime::DatagramEndpoint>>
resolve_dht_bootstrap(std::span<const std::string> configured,
                      runtime::AddressFamily address_family,
                      runtime::DatagramEndpointResolver &resolver);

struct AsioDhtFamilyRuntimeDependencies {
  dht::ObservationSink *observations{};
  storage::TorrentDataset *torrents{};
  dht::MetadataFetchObserver *metadata_results{};
  integration::DhtRuntimeWorkerObserver *observer{};
  runtime::TrafficGovernor *traffic{};
  scheduler::WorkCoordinator *work{};
  std::string_view worker_namespace;
  core::Duration worker_heartbeat_interval{std::chrono::seconds{10}};
  std::function<void(std::uint64_t)> on_torrent_committed;
};

// Owns one IPv4 or IPv6 DHT execution graph. Standalone Asio is confined to
// construction of the UDP and TCP transports; the protocol, action pump, and
// worker remain expressed entirely in Sakuin interfaces.
class AsioDhtFamilyRuntime final {
public:
  static core::Result<std::unique_ptr<AsioDhtFamilyRuntime>>
  create(const config::DhtConfig &configuration,
         runtime::AddressFamily address_family, std::uint16_t listen_port,
         DhtFamilyRuntimeMaterial material,
         AsioDhtFamilyRuntimeDependencies dependencies);
  static core::Result<std::unique_ptr<AsioDhtFamilyRuntime>>
  create(const config::DhtConfig &configuration,
         runtime::AddressFamily address_family, std::uint16_t listen_port,
         std::span<const runtime::DatagramEndpoint> bootstrap_endpoints,
         DhtFamilyRuntimeMaterial material,
         AsioDhtFamilyRuntimeDependencies dependencies);

  ~AsioDhtFamilyRuntime();

  AsioDhtFamilyRuntime(const AsioDhtFamilyRuntime &) = delete;
  AsioDhtFamilyRuntime &operator=(const AsioDhtFamilyRuntime &) = delete;

  core::Result<void> start();
  void stop() noexcept;
  bool running() const noexcept;
  runtime::DatagramEndpoint local_endpoint() const noexcept;
  const dht::krpc::NodeId &node_id() const noexcept;
  dht::Bep42IdentityPolicy *identity_policy() noexcept {
    return identity_.get();
  }

private:
  AsioDhtFamilyRuntime() = default;

  std::unique_ptr<runtime::DatagramTransport> datagrams_;
  runtime::AsioTcpStreamTransportFactory streams_;
  std::unique_ptr<dht::RotatingAnnounceTokenProvider> tokens_;
  std::unique_ptr<dht::DhtNode> node_;
  std::unique_ptr<dht::Bep42IdentityPolicy> identity_;
  std::unique_ptr<dht::BootstrapPlanner> bootstrap_;
  std::unique_ptr<dht::RoutingMaintenancePlanner> routing_;
  std::unique_ptr<dht::RoutingDiscoveryPlanner> discovery_;
  std::unique_ptr<dht::PeerDiscoveryPlanner> peer_discovery_;
  std::unique_ptr<integration::MetadataDiscoveryBackfill> metadata_backfill_;
  integration::DhtRuntimeWakeup wakeup_;
  std::unique_ptr<integration::TorrentMetadataAcquisition> metadata_;
  std::unique_ptr<dht::MetadataAcquisitionController> external_metadata_;
  std::unique_ptr<integration::DhtRuntimeActionPump> pump_;
  std::unique_ptr<dht::DhtRuntimeDriver> driver_;
  std::unique_ptr<integration::DhtRuntimeWorker> worker_;
};

} // namespace sakuin::service

namespace sakuin::service {
namespace {

struct BootstrapHost {
  std::string_view host;
  std::uint16_t port{};
};

core::Result<BootstrapHost> parse_bootstrap_host(std::string_view configured) {
  std::string_view host;
  std::string_view port_text;
  if (configured.starts_with('[')) {
    const auto close = configured.find(']');
    if (close == std::string_view::npos || close == 1 ||
        close + 1 >= configured.size() || configured[close + 1] != ':')
      return std::unexpected(core::Error{
          core::ErrorCode::InvalidArgument,
          "Bracketed DHT bootstrap addresses must use [host]:port"});
    host = configured.substr(1, close - 1);
    port_text = configured.substr(close + 2);
  } else {
    const auto separator = configured.rfind(':');
    if (separator == std::string_view::npos || separator == 0 ||
        configured.find(':') != separator)
      return std::unexpected(core::Error{
          core::ErrorCode::InvalidArgument,
          "DHT bootstrap addresses must use host:port or [IPv6]:port"});
    host = configured.substr(0, separator);
    port_text = configured.substr(separator + 1);
  }
  std::uint32_t port{};
  const auto parsed = std::from_chars(
      port_text.data(), port_text.data() + port_text.size(), port);
  if (port_text.empty() || parsed.ec != std::errc{} ||
      parsed.ptr != port_text.data() + port_text.size() || port == 0 ||
      port > std::numeric_limits<std::uint16_t>::max())
    return std::unexpected(core::Error{
        core::ErrorCode::InvalidArgument,
        "DHT bootstrap port must be an integer between 1 and 65535"});
  return BootstrapHost{.host = host, .port = static_cast<std::uint16_t>(port)};
}

std::string dht_worker_id(runtime::AddressFamily family,
                          const dht::krpc::NodeId &node_id,
                          std::string_view worker_namespace) {
  if (!worker_namespace.empty())
    return std::string{worker_namespace} +
           (family == runtime::AddressFamily::IPv4 ? ":v4" : ":v6");
  constexpr std::string_view hexadecimal{"0123456789abcdef"};
  std::string result =
      family == runtime::AddressFamily::IPv4 ? "dht-v4-" : "dht-v6-";
  result.reserve(result.size() + node_id.bytes.size() * 2);
  for (const auto byte : node_id.bytes) {
    result.push_back(hexadecimal[byte >> 4]);
    result.push_back(hexadecimal[byte & 0x0f]);
  }
  return result;
}

} // namespace

core::Result<std::vector<runtime::DatagramEndpoint>>
resolve_dht_bootstrap(std::span<const std::string> configured,
                      runtime::AddressFamily address_family,
                      runtime::DatagramEndpointResolver &resolver) {
  std::vector<runtime::DatagramEndpoint> result;
  for (const auto &entry : configured) {
    auto host = parse_bootstrap_host(entry);
    if (!host)
      return std::unexpected(host.error());
    auto endpoints = resolver.resolve(host->host, host->port, address_family);
    if (!endpoints && endpoints.error().code == core::ErrorCode::NotFound)
      continue;
    if (!endpoints)
      return std::unexpected(endpoints.error());
    for (auto &endpoint : *endpoints)
      if (std::ranges::find(result, endpoint) == result.end())
        result.push_back(std::move(endpoint));
  }
  return result;
}

core::Result<DhtFamilyRuntimeMaterial> secure_dht_family_runtime_material(
    const config::DhtIdentityConfig &identity,
    runtime::AddressFamily address_family,
    std::optional<runtime::IpAddress> current_external) {
  auto node_id = integration::secure_dht_node_id(identity, address_family,
                                                 current_external);
  if (!node_id)
    return std::unexpected(node_id.error());

  DhtFamilyRuntimeMaterial result{.node_id = *node_id,
                                  .current_external = current_external};
  if (auto generated = core::secure_random(
          std::as_writable_bytes(std::span{result.peer_id}));
      !generated)
    return std::unexpected(generated.error());
  if (auto generated = core::secure_random(
          std::as_writable_bytes(std::span{result.token_secret.bytes}));
      !generated)
    return std::unexpected(generated.error());
  return result;
}

core::Result<std::unique_ptr<AsioDhtFamilyRuntime>>
AsioDhtFamilyRuntime::create(const config::DhtConfig &configuration,
                             runtime::AddressFamily address_family,
                             std::uint16_t listen_port,
                             DhtFamilyRuntimeMaterial material,
                             AsioDhtFamilyRuntimeDependencies dependencies) {
  runtime::AsioDatagramEndpointResolver resolver;
  auto bootstrap =
      resolve_dht_bootstrap(configuration.bootstrap, address_family, resolver);
  if (!bootstrap)
    return std::unexpected(bootstrap.error());
  return create(configuration, address_family, listen_port, *bootstrap,
                std::move(material), dependencies);
}

core::Result<std::unique_ptr<AsioDhtFamilyRuntime>>
AsioDhtFamilyRuntime::create(
    const config::DhtConfig &configuration,
    runtime::AddressFamily address_family, std::uint16_t listen_port,
    std::span<const runtime::DatagramEndpoint> bootstrap_endpoints,
    DhtFamilyRuntimeMaterial material,
    AsioDhtFamilyRuntimeDependencies dependencies) {
  if (!dependencies.observations || !dependencies.observer)
    return std::unexpected(core::Error{
        core::ErrorCode::InvalidArgument,
        "DHT family runtime requires observation and worker observers"});
  if (configuration.metadata.enabled && !dependencies.torrents &&
      !dependencies.metadata_results)
    return std::unexpected(
        core::Error{core::ErrorCode::InvalidArgument,
                    "Enabled metadata acquisition requires a canonical or "
                    "remote result sink"});
  if (dependencies.torrents && dependencies.metadata_results)
    return std::unexpected(core::Error{
        core::ErrorCode::InvalidArgument,
        "Metadata acquisition must use exactly one completion sink"});
  if (material.current_external &&
      material.current_external->family != address_family)
    return std::unexpected(core::Error{
        core::ErrorCode::InvalidArgument,
        "DHT current external address has the wrong address family"});
  if (configuration.identity.mode == config::DhtIdentityMode::Bep42 &&
      material.current_external &&
      !dht::bep42_exempt(*material.current_external) &&
      !dht::bep42_compliant(material.node_id, *material.current_external))
    return std::unexpected(core::Error{
        core::ErrorCode::InvalidArgument,
        "DHT node ID is not BEP 42 compliant with its external address"});

  runtime::IpAddress bind_address;
  bind_address.family = address_family;
  auto datagrams = runtime::AsioDatagramTransport::create(
      {.bind_to = {.address = bind_address, .port = listen_port}});
  if (!datagrams)
    return std::unexpected(datagrams.error());
  auto tokens =
      dht::RotatingAnnounceTokenProvider::create(material.token_secret);
  if (!tokens)
    return std::unexpected(tokens.error());

  auto result = std::unique_ptr<AsioDhtFamilyRuntime>{new AsioDhtFamilyRuntime};
  result->datagrams_ = std::move(*datagrams);
  result->tokens_ =
      std::make_unique<dht::RotatingAnnounceTokenProvider>(std::move(*tokens));
  result->node_ = std::make_unique<dht::DhtNode>(
      material.node_id, *result->tokens_,
      integration::dht_node_options(configuration, address_family));

  if (auto options =
          integration::bep42_identity_policy_options(configuration.identity)) {
    auto identity = dht::Bep42IdentityPolicy::create(
        address_family, material.current_external, *options);
    if (!identity)
      return std::unexpected(identity.error());
    result->identity_ = std::move(*identity);
  }
  if (!bootstrap_endpoints.empty()) {
    auto bootstrap = dht::BootstrapPlanner::create(
        *result->node_, bootstrap_endpoints,
        integration::bootstrap_options(configuration));
    if (!bootstrap)
      return std::unexpected(bootstrap.error());
    result->bootstrap_ = std::move(*bootstrap);
  }
  auto routing = dht::RoutingMaintenancePlanner::create(
      *result->node_,
      integration::routing_maintenance_options(configuration.routing));
  if (!routing)
    return std::unexpected(routing.error());
  result->routing_ = std::move(*routing);
  auto discovery = dht::RoutingDiscoveryPlanner::create(*result->node_);
  if (!discovery)
    return std::unexpected(discovery.error());
  result->discovery_ = std::move(*discovery);

  if (configuration.metadata.enabled &&
      configuration.metadata.discovery.enabled) {
    auto peer_discovery = dht::PeerDiscoveryPlanner::create(
        *result->node_,
        integration::peer_discovery_options(configuration.metadata.discovery));
    if (!peer_discovery)
      return std::unexpected(peer_discovery.error());
    result->peer_discovery_ = std::move(*peer_discovery);
    if (configuration.metadata.discovery.backfill.enabled &&
        dependencies.torrents) {
      auto backfill = integration::MetadataDiscoveryBackfill::create(
          *dependencies.torrents, *result->peer_discovery_,
          integration::metadata_discovery_backfill_options(
              configuration.metadata.discovery.backfill));
      if (!backfill)
        return std::unexpected(backfill.error());
      result->metadata_backfill_ = std::move(*backfill);
    }
  }

  const auto wake_owner = [owner = result.get()] { owner->wakeup_.notify(); };
  if (configuration.metadata.enabled) {
    if (dependencies.metadata_results) {
      auto runtime =
          integration::metadata_runtime_config(configuration.metadata);
      runtime.controller.wake_owner = wake_owner;
      auto metadata = dht::MetadataAcquisitionController::create(
          material.peer_id, result->streams_, *dependencies.metadata_results,
          std::move(runtime.controller));
      if (!metadata)
        return std::unexpected(metadata.error());
      result->external_metadata_ = std::move(*metadata);
    } else {
      auto metadata = integration::TorrentMetadataAcquisition::create(
          material.peer_id, result->streams_, *dependencies.torrents,
          configuration.metadata, wake_owner,
          dependencies.on_torrent_committed);
      if (!metadata)
        return std::unexpected(metadata.error());
      result->metadata_ = std::move(*metadata);
    }
  }
  auto *metadata_controller = result->metadata_
                                  ? result->metadata_->controller()
                                  : result->external_metadata_.get();
  auto pump = integration::DhtRuntimeActionPump::create(
      *dependencies.observations,
      {.metadata = metadata_controller,
       .node = result->node_.get(),
       .bootstrap = result->bootstrap_.get(),
       .routing = result->routing_.get(),
       .discovery = result->discovery_.get(),
       .peer_discovery = result->peer_discovery_.get(),
       .metadata_backfill = result->metadata_backfill_.get(),
       .identity = result->identity_.get()},
      {.wake_owner = wake_owner});
  if (!pump)
    return std::unexpected(pump.error());
  result->pump_ = std::move(*pump);

  if (dependencies.traffic) {
    result->driver_ = std::make_unique<dht::DhtRuntimeDriver>(
        *result->node_, *result->datagrams_, *result->pump_,
        *dependencies.traffic);
  } else {
    result->driver_ = std::make_unique<dht::DhtRuntimeDriver>(
        *result->node_, *result->datagrams_, *result->pump_);
  }
  result->worker_ = std::make_unique<integration::DhtRuntimeWorker>(
      *result->driver_, *result->pump_, result->wakeup_, *dependencies.observer,
      integration::DhtRuntimeWorkerScheduling{
          .coordinator = dependencies.work,
          .worker = {.id = dht_worker_id(address_family, material.node_id,
                                         dependencies.worker_namespace),
                     .capabilities = {scheduler::WorkClass::DhtCrawl}},
          .heartbeat_interval = dependencies.worker_heartbeat_interval});
  return result;
}

AsioDhtFamilyRuntime::~AsioDhtFamilyRuntime() { stop(); }

core::Result<void> AsioDhtFamilyRuntime::start() { return worker_->start(); }

void AsioDhtFamilyRuntime::stop() noexcept {
  if (worker_)
    worker_->stop();
  if (metadata_)
    metadata_->stop();
  if (external_metadata_)
    external_metadata_->stop();
}

bool AsioDhtFamilyRuntime::running() const noexcept {
  return worker_ && worker_->running();
}

runtime::DatagramEndpoint
AsioDhtFamilyRuntime::local_endpoint() const noexcept {
  return datagrams_ ? datagrams_->local_endpoint()
                    : runtime::DatagramEndpoint{};
}

const dht::krpc::NodeId &AsioDhtFamilyRuntime::node_id() const noexcept {
  return node_->routing_table().local_id();
}

} // namespace sakuin::service

import std;

import sakuin.config;
import sakuin.core;
import sakuin.dht;
import sakuin.integration.dht_config;
import sakuin.integration.dht_runtime;
import sakuin.integration.metadata_config;
import sakuin.model.observation;
import sakuin.model.torrent;
import sakuin.runtime;

namespace {

class ObservationSink final : public sakuin::dht::ObservationSink {
public:
  sakuin::core::Result<void>
  observe(const sakuin::model::ObservationRecord &observation) override {
    if (fail_once) {
      fail_once = false;
      return std::unexpected(
          sakuin::core::Error{sakuin::core::ErrorCode::StorageUnavailable,
                              "Synthetic observation storage outage"});
    }
    records.push_back(observation);
    return {};
  }

  bool fail_once{true};
  std::vector<sakuin::model::ObservationRecord> records;
};

class PassiveTransport final : public sakuin::runtime::StreamTransport {
public:
  explicit PassiveTransport(sakuin::runtime::StreamEndpoint remote)
      : remote_(remote) {}

  sakuin::core::Result<void> start(sakuin::runtime::StreamReceiver &) override {
    started = true;
    return {};
  }
  sakuin::core::Result<void> send(sakuin::core::ByteBuffer) override {
    return {};
  }
  sakuin::runtime::StreamEndpoint remote_endpoint() const noexcept override {
    return remote_;
  }
  void stop() noexcept override { stopped = true; }

  bool started{};
  bool stopped{};

private:
  sakuin::runtime::StreamEndpoint remote_;
};

class FailingDatagramTransport final
    : public sakuin::runtime::DatagramTransport {
public:
  sakuin::core::Result<void>
  start(sakuin::runtime::DatagramReceiver &) override {
    started = true;
    return {};
  }
  sakuin::core::Result<void> send(sakuin::runtime::DatagramEndpoint,
                                  sakuin::core::ByteBuffer) override {
    return std::unexpected(sakuin::core::Error{sakuin::core::ErrorCode::IoError,
                                               "Synthetic UDP send failure"});
  }
  sakuin::runtime::DatagramEndpoint local_endpoint() const noexcept override {
    return {};
  }
  void stop() noexcept override { started = false; }

  bool started{};
};

class TransportFactory final : public sakuin::runtime::StreamTransportFactory {
public:
  sakuin::core::Result<std::shared_ptr<sakuin::runtime::StreamTransport>>
  create(sakuin::runtime::StreamTransportOptions options) override {
    auto transport = std::make_shared<PassiveTransport>(options.remote);
    last = transport;
    return std::shared_ptr<sakuin::runtime::StreamTransport>{
        std::move(transport)};
  }

  std::shared_ptr<PassiveTransport> last;
};

class MetadataSink final : public sakuin::dht::MetadataFetchObserver {
public:
  sakuin::core::Result<void>
  on_metadata_fetched(sakuin::model::TorrentRecord) override {
    return {};
  }
  void on_metadata_fetch_failed(sakuin::core::Error error) override {
    errors.push_back(std::move(error));
  }
  std::vector<sakuin::core::Error> errors;
};

sakuin::core::InfoHash hash(std::uint8_t seed) {
  sakuin::core::InfoHash result;
  std::ranges::iota(result.bytes, seed);
  return result;
}

} // namespace

int main() {
  using namespace sakuin;

  auto configured = config::defaults().network.dht.metadata;
  configured.enabled = false;
  configured.maximum_in_flight = 7;
  configured.maximum_queued = 99;
  configured.maximum_attempts_per_peer = 5;
  configured.maximum_metadata_bytes = 2U * 1024U * 1024U;
  configured.maximum_outstanding_requests = 6;
  configured.maximum_queued_write_bytes = 12'345;
  configured.storage_conflict_attempts = 4;
  configured.storage_retry_delay = std::chrono::milliseconds{750};
  const auto mapped = integration::metadata_runtime_config(configured);
  if (mapped.enabled || mapped.controller.queue.maximum_in_flight != 7 ||
      mapped.controller.queue.maximum_queued != 99 ||
      mapped.controller.queue.maximum_attempts_per_peer != 5 ||
      mapped.controller.fetch.exchange.maximum_metadata_bytes !=
          2U * 1024U * 1024U ||
      mapped.controller.fetch.metainfo.maximum_metadata_bytes !=
          2U * 1024U * 1024U ||
      mapped.controller.fetch.exchange.maximum_outstanding_requests != 6 ||
      mapped.controller.transport.maximum_queued_write_bytes != 12'345 ||
      mapped.storage_conflict_attempts != 4 ||
      mapped.controller.storage_retry_delay != std::chrono::milliseconds{750})
    return 1;

  auto dht_config = config::defaults().network.dht;
  dht_config.query_timeout = std::chrono::seconds{9};
  dht_config.bootstrap_maximum_in_flight = 3;
  dht_config.bootstrap_maximum_attempts = 5;
  dht_config.bootstrap_retry_delay = std::chrono::seconds{7};
  dht_config.routing.maximum_queued = 222;
  dht_config.routing.maximum_in_flight = 4;
  dht_config.routing.maximum_attempts = 6;
  dht_config.routing.retry_delay = std::chrono::seconds{8};
  dht_config.metadata.discovery.maximum_pending = 333;
  dht_config.metadata.discovery.maximum_in_flight = 9;
  dht_config.metadata.discovery.parallelism_per_hash = 3;
  dht_config.metadata.discovery.maximum_queries_per_hash = 27;
  dht_config.metadata.discovery.retry_delay = std::chrono::seconds{70};
  dht_config.metadata.discovery.backfill.maximum_records_per_poll = 123;
  dht_config.metadata.discovery.backfill.refresh_interval =
      std::chrono::seconds{80};
  dht_config.metadata.discovery.backfill.full_rescan_interval =
      std::chrono::minutes{20};
  dht_config.metadata.discovery.backfill.retry_delay =
      std::chrono::milliseconds{900};
  dht_config.identity.observation_quorum = 5;
  dht_config.identity.vote_window = std::chrono::seconds{45};
  const auto node_options =
      integration::dht_node_options(dht_config, runtime::AddressFamily::IPv6);
  const auto configured_bootstrap = integration::bootstrap_options(dht_config);
  const auto configured_routing =
      integration::routing_maintenance_options(dht_config.routing);
  const auto configured_peer_discovery =
      integration::peer_discovery_options(dht_config.metadata.discovery);
  const auto configured_backfill =
      integration::metadata_discovery_backfill_options(
          dht_config.metadata.discovery.backfill);
  const auto configured_identity =
      integration::bep42_identity_policy_options(dht_config.identity);
  if (node_options.query_timeout != std::chrono::seconds{9} ||
      node_options.address_family != runtime::AddressFamily::IPv6 ||
      configured_bootstrap.maximum_in_flight != 3 ||
      configured_bootstrap.maximum_attempts != 5 ||
      configured_bootstrap.retry_delay != std::chrono::seconds{7} ||
      configured_routing.maximum_queued != 222 ||
      configured_routing.maximum_in_flight != 4 ||
      configured_routing.maximum_attempts != 6 ||
      configured_routing.retry_delay != std::chrono::seconds{8} ||
      configured_peer_discovery.maximum_pending != 333 ||
      configured_peer_discovery.maximum_in_flight != 9 ||
      configured_peer_discovery.parallelism_per_hash != 3 ||
      configured_peer_discovery.maximum_queries_per_hash != 27 ||
      configured_peer_discovery.retry_delay != std::chrono::seconds{70} ||
      configured_backfill.maximum_records_per_poll != 123 ||
      configured_backfill.refresh_interval != std::chrono::seconds{80} ||
      configured_backfill.full_rescan_interval != std::chrono::minutes{20} ||
      configured_backfill.backpressure_retry_delay !=
          std::chrono::milliseconds{900} ||
      !configured_identity || configured_identity->observation_quorum != 5 ||
      configured_identity->vote_window != std::chrono::seconds{45})
    return 32;
  dht_config.identity.mode = config::DhtIdentityMode::Fixed;
  if (integration::bep42_identity_policy_options(dht_config.identity))
    return 36;
  dht::krpc::NodeId identity_entropy;
  std::ranges::iota(identity_entropy.bytes, std::uint8_t{1});
  auto unbound_id = integration::dht_node_id(
      config::defaults().network.dht.identity, runtime::AddressFamily::IPv4,
      std::nullopt, identity_entropy);
  runtime::IpAddress public_address = runtime::IpAddress::loopback_v4();
  public_address.bytes = {203, 0, 113, 9};
  auto bound_id = integration::dht_node_id(
      config::defaults().network.dht.identity, runtime::AddressFamily::IPv4,
      public_address, identity_entropy);
  auto wrong_family = public_address;
  wrong_family.family = runtime::AddressFamily::IPv6;
  auto invalid_family = integration::dht_node_id(
      config::defaults().network.dht.identity, runtime::AddressFamily::IPv4,
      wrong_family, identity_entropy);
  if (!unbound_id || *unbound_id != identity_entropy || !bound_id ||
      !dht::bep42_compliant(*bound_id, public_address) ||
      bound_id->bytes.back() != identity_entropy.bytes.back() ||
      invalid_family ||
      invalid_family.error().code != core::ErrorCode::InvalidArgument)
    return 42;
  config::DhtIdentityConfig fixed_identity{
      .mode = config::DhtIdentityMode::Fixed,
      .fixed_ipv4_node_id = "000102030405060708090a0B0c0D0e0F10111213"};
  auto fixed_id = integration::secure_dht_node_id(fixed_identity,
                                                  runtime::AddressFamily::IPv4);
  auto missing_fixed_id = integration::secure_dht_node_id(
      fixed_identity, runtime::AddressFamily::IPv6);
  if (!fixed_id || fixed_id->bytes.front() != 0 ||
      fixed_id->bytes.back() != 0x13 || missing_fixed_id ||
      missing_fixed_id.error().code != core::ErrorCode::InvalidArgument)
    return 43;

  TransportFactory factory;
  MetadataSink metadata_sink;
  dht::PeerId peer_id{};
  std::ranges::iota(peer_id, std::uint8_t{1});
  auto controller = dht::MetadataAcquisitionController::create(
      peer_id, factory, metadata_sink, mapped.controller);
  if (!controller)
    return 2;

  ObservationSink observations;
  dht::AnnounceTokenSecret token_secret;
  auto tokens = dht::RotatingAnnounceTokenProvider::create(token_secret);
  dht::krpc::NodeId local_node{};
  dht::DhtNode routing_node{local_node,
                            *tokens,
                            {.query_timeout = std::chrono::seconds{2},
                             .address_family = runtime::AddressFamily::IPv4}};
  const auto routing_contact = [](std::uint8_t suffix) {
    dht::NodeContact result{
        .endpoint = {.address = runtime::IpAddress::loopback_v4(),
                     .port = static_cast<std::uint16_t>(6'000 + suffix)},
        .last_seen = core::Timestamp{std::chrono::seconds{1}}};
    result.id.bytes.front() = 0x80;
    result.id.bytes.back() = suffix;
    result.endpoint.address.bytes[3] = static_cast<std::uint8_t>(suffix + 1);
    return result;
  };
  for (std::uint8_t index = 0; index < dht::k_bucket_size; ++index)
    routing_node.routing_table().observe(routing_contact(index));
  auto routing_update =
      routing_node.routing_table().observe(routing_contact(20));
  auto routing = dht::RoutingMaintenancePlanner::create(
      routing_node, {.maximum_queued = 2,
                     .maximum_in_flight = 1,
                     .maximum_attempts = 1,
                     .retry_delay = std::chrono::seconds{1}});
  auto peer_discovery = dht::PeerDiscoveryPlanner::create(
      routing_node, {.maximum_pending = 16,
                     .maximum_in_flight = 2,
                     .parallelism_per_hash = 2,
                     .maximum_queries_per_hash = 4,
                     .retry_delay = std::chrono::minutes{5}});
  if (!routing_update.probe || !routing || !peer_discovery)
    return 20;
  auto pump = integration::DhtRuntimeActionPump::create(
      observations, {.metadata = controller->get(),
                     .node = &routing_node,
                     .routing = routing->get(),
                     .peer_discovery = peer_discovery->get()});
  auto invalid_pump = integration::DhtRuntimeActionPump::create(
      observations, {}, {.maximum_pending_actions = 0});
  auto missing_owner = integration::DhtRuntimeActionPump::create(
      observations, {.routing = routing->get()});
  auto missing_metadata = integration::DhtRuntimeActionPump::create(
      observations,
      {.node = &routing_node, .peer_discovery = peer_discovery->get()});
  if (!pump || invalid_pump ||
      invalid_pump.error().code != core::ErrorCode::InvalidArgument ||
      missing_owner ||
      missing_owner.error().code != core::ErrorCode::InvalidArgument ||
      missing_metadata ||
      missing_metadata.error().code != core::ErrorCode::InvalidArgument)
    return 3;
  std::size_t owner_wakeups{};
  auto bounded_pump = integration::DhtRuntimeActionPump::create(
      observations, {},
      {.maximum_pending_actions = 1,
       .maximum_pending_errors = 1,
       .wake_owner = [&] { ++owner_wakeups; }});
  if (!bounded_pump)
    return 4;
  (*bounded_pump)->on_actions({});
  (*bounded_pump)->on_actions({});
  (*bounded_pump)->on_error({core::ErrorCode::IoError, "First bounded error"});
  (*bounded_pump)->on_error({core::ErrorCode::IoError, "Second bounded error"});
  auto bounded = (*bounded_pump)->poll({});
  if (bounded.errors.size() != 3 || (*bounded_pump)->pending() != 0 ||
      owner_wakeups != 4)
    return 5;

  (*pump)->on_actions(
      dht::DhtActions{.probes_required = {*routing_update.probe}});
  const auto routing_now = core::Timestamp{std::chrono::seconds{10}};
  auto probe_started = (*pump)->poll(routing_now);
  if (probe_started.routing_probes_accepted != 1 || !probe_started.routing ||
      probe_started.routing->sends.size() != 1 ||
      (*routing)->in_flight() != 1 || probe_started.routing->next_wakeup ||
      probe_started.next_wakeup != routing_now + std::chrono::seconds{2})
    return 21;
  auto probe_query =
      dht::krpc::decode(probe_started.routing->sends.front().payload);
  const auto *typed_probe =
      probe_query ? std::get_if<dht::krpc::Query>(&*probe_query) : nullptr;
  if (!typed_probe)
    return 22;
  auto probe_response = dht::krpc::encode(
      dht::krpc::Response{.transaction = typed_probe->transaction,
                          .sender = routing_update.probe->incumbent.id});
  auto probe_completion =
      probe_response ? routing_node.handle(
                           {.source = routing_update.probe->incumbent.endpoint,
                            .payload = std::move(*probe_response)},
                           routing_now + std::chrono::seconds{1})
                     : core::Result<dht::DhtActions>{
                           std::unexpected(probe_response.error())};
  if (!probe_completion)
    return 23;
  (*pump)->on_actions(std::move(*probe_completion));
  auto probe_finished = (*pump)->poll(routing_now + std::chrono::seconds{1});
  if (!probe_finished.actions.empty() || !probe_finished.routing ||
      (*routing)->in_flight() != 0 || (*routing)->queued() != 0)
    return 24;

  auto unowned_query =
      routing_node.get_peers(routing_update.probe->incumbent.endpoint, hash(90),
                             routing_now + std::chrono::seconds{2});
  if (!unowned_query)
    return 25;
  auto expired = (*pump)->poll(routing_now + std::chrono::seconds{4});
  if (expired.queries_expired != 1 || expired.unhandled_timeouts.size() != 1 ||
      expired.unhandled_timeouts.front().kind != dht::krpc::QueryKind::GetPeers)
    return 26;

  dht::DhtNode bootstrap_node{
      local_node, *tokens, {.query_timeout = std::chrono::seconds{2}}};
  const std::array bootstrap_endpoints{
      routing_update.probe->incumbent.endpoint};
  auto bootstrap =
      dht::BootstrapPlanner::create(bootstrap_node, bootstrap_endpoints,
                                    {.maximum_in_flight = 1,
                                     .maximum_attempts = 2,
                                     .retry_delay = std::chrono::seconds{1}});
  if (!bootstrap)
    return 27;
  auto bootstrap_pump = integration::DhtRuntimeActionPump::create(
      observations, {.node = &bootstrap_node, .bootstrap = bootstrap->get()});
  if (!bootstrap_pump)
    return 28;
  const auto bootstrap_now = core::Timestamp{std::chrono::seconds{200}};
  auto bootstrap_started = (*bootstrap_pump)->poll(bootstrap_now);
  if (!bootstrap_started.bootstrap ||
      bootstrap_started.bootstrap->sends.size() != 1 ||
      bootstrap_started.bootstrap->next_wakeup ||
      bootstrap_started.next_wakeup != bootstrap_now + std::chrono::seconds{2})
    return 29;
  auto bootstrap_timed_out =
      (*bootstrap_pump)->poll(bootstrap_now + std::chrono::seconds{2});
  if (bootstrap_timed_out.queries_expired != 1 ||
      !bootstrap_timed_out.unhandled_timeouts.empty() ||
      !bootstrap_timed_out.bootstrap ||
      bootstrap_timed_out.bootstrap->next_wakeup !=
          bootstrap_now + std::chrono::seconds{3})
    return 30;
  auto bootstrap_retry =
      (*bootstrap_pump)->poll(bootstrap_now + std::chrono::seconds{3});
  if (!bootstrap_retry.bootstrap ||
      bootstrap_retry.bootstrap->sends.size() != 1)
    return 31;
  FailingDatagramTransport failing_datagrams;
  dht::DhtRuntimeDriver bootstrap_driver{
      bootstrap_node, failing_datagrams, **bootstrap_pump,
      [bootstrap_now] { return bootstrap_now + std::chrono::seconds{3}; }};
  if (!bootstrap_driver.start())
    return 33;
  auto dispatch = integration::dispatch_dht_runtime(
      bootstrap_retry, bootstrap_driver, **bootstrap_pump,
      bootstrap_now + std::chrono::seconds{3});
  if (dispatch.attempted != 1 || dispatch.accepted != 0 ||
      dispatch.failed != 1 || dispatch.errors.size() != 1 ||
      !bootstrap_retry.bootstrap->sends.empty() ||
      bootstrap_node.outstanding_queries() != 0)
    return 34;
  auto bootstrap_complete =
      (*bootstrap_pump)->poll(bootstrap_now + std::chrono::seconds{3});
  if (!bootstrap_complete.bootstrap || bootstrap_complete.bootstrap->complete ||
      !bootstrap_complete.bootstrap->exhausted)
    return 35;
  bootstrap_driver.stop();

  runtime::IpAddress peer_address = runtime::IpAddress::loopback_v4();
  peer_address.bytes[3] = 9;
  const auto wanted = hash(10);
  const auto observed_at = core::Timestamp{std::chrono::seconds{50}};
  dht::DhtActions actions{
      .observation = model::ObservationRecord{.info_hash = wanted,
                                              .observed_at = observed_at},
      .metadata_candidate =
          dht::PeerMetadataCandidate{
              .info_hash = wanted,
              .peer = {.address = peer_address, .port = 51'413},
              .observed_at = observed_at},
      .observed_address =
          dht::ObservedAddressReport{
              .reporter = {.address = peer_address, .port = 6'881},
              .observed = {.address = runtime::IpAddress::loopback_v4(),
                           .port = 6'881}},
      .inbound_message = dht::InboundMessageReport{
          .type = dht::InboundMessageType::Query,
          .query_kind = dht::krpc::QueryKind::GetPeers,
          .received_at = observed_at}};
  std::thread producer{[&] {
    (*pump)->on_actions(std::move(actions));
    (*pump)->on_error(core::Error{core::ErrorCode::QuotaExceeded,
                                  "Synthetic runtime quota report"});
  }};
  producer.join();
  if ((*pump)->pending() != 1)
    return 6;

  const auto now = core::Timestamp{std::chrono::seconds{100}};
  auto first = (*pump)->poll(now);
  if (first.observations_stored != 0 ||
      first.metadata_candidates_accepted != 1 || first.actions.size() != 1 ||
      first.inbound_messages != 1 || first.inbound_queries != 1 ||
      first.inbound_get_peers_queries != 1 ||
      first.last_inbound_query != observed_at ||
      !first.actions.front().observed_address || first.errors.size() != 2 ||
      controller->get()->in_flight() != 1 || !factory.last ||
      !factory.last->started || (*pump)->pending() != 1 ||
      first.next_wakeup != now + std::chrono::seconds{1})
    return 7;

  auto second = (*pump)->poll(now + std::chrono::seconds{1});
  if (second.observations_stored != 1 ||
      second.metadata_candidates_accepted != 0 || !second.actions.empty() ||
      second.inbound_messages != 0 || second.inbound_queries != 0 ||
      !second.errors.empty() || observations.records.size() != 1 ||
      observations.records.front().info_hash != wanted ||
      (*pump)->pending() != 0 || !second.peer_discovery ||
      second.peer_discovery->queries_started != 2 ||
      second.peer_discovery->active != 1 ||
      second.peer_discovery->sends.size() != 2)
    return 8;

  auto active_query =
      dht::krpc::decode(second.peer_discovery->sends.front().payload);
  const auto *typed_active_query =
      active_query ? std::get_if<dht::krpc::Query>(&*active_query) : nullptr;
  auto active_peer = routing_contact(60).endpoint;
  auto compact_active_peer = dht::krpc::encode_compact_endpoint(active_peer);
  auto active_response =
      typed_active_query && compact_active_peer
          ? dht::krpc::encode(dht::krpc::Response{
                .transaction = typed_active_query->transaction,
                .sender = routing_contact(1).id,
                .values = {{"values",
                            dht::bencode::Value{
                                dht::bencode::Value::List{dht::bencode::Value{
                                    std::move(*compact_active_peer)}}}}}})
          : core::Result<core::ByteBuffer>{std::unexpected(core::Error{
                core::ErrorCode::Internal,
                "Unable to construct active peer-discovery response"})};
  auto active_completion =
      active_response
          ? routing_node.handle(
                {.source = second.peer_discovery->sends.front().destination,
                 .payload = std::move(*active_response)},
                now + std::chrono::seconds{2})
          : core::Result<dht::DhtActions>{
                std::unexpected(active_response.error())};
  if (!active_completion)
    return 44;
  (*pump)->on_actions(std::move(*active_completion));
  auto active_completed = (*pump)->poll(now + std::chrono::seconds{2});
  if (!active_completed.peer_discovery ||
      active_completed.peer_discovery_peers_found != 1 ||
      active_completed.metadata_candidates_accepted != 1 ||
      active_completed.peer_discovery->pending != 0 ||
      active_completed.peer_discovery->active != 0 ||
      routing_node.outstanding_queries() != 0)
    return 45;

  auto identity = dht::Bep42IdentityPolicy::create(
      runtime::AddressFamily::IPv4, std::nullopt,
      {.observation_quorum = 2, .vote_window = std::chrono::seconds{30}});
  if (!identity)
    return 37;
  auto identity_pump = integration::DhtRuntimeActionPump::create(
      observations, {.identity = identity->get()});
  if (!identity_pump)
    return 37;
  runtime::IpAddress external = runtime::IpAddress::loopback_v4();
  external.bytes = {203, 0, 113, 21};
  const auto identity_report = [&](std::uint8_t reporter) {
    runtime::IpAddress address = runtime::IpAddress::loopback_v4();
    address.bytes = {198, 51, 100, reporter};
    (*identity_pump)
        ->on_actions(dht::DhtActions{
            .observed_address = dht::ObservedAddressReport{
                .reporter = {.address = address, .port = 6'881},
                .observed = {.address = external, .port = 50'000}}});
  };
  identity_report(1);
  auto first_identity = (*identity_pump)->poll(now);
  if (!first_identity.actions.empty() ||
      first_identity.identity_reconfiguration ||
      (*identity_pump)->pending() != 0)
    return 38;
  identity_report(2);
  auto proposed_identity = (*identity_pump)->poll(now);
  if (!proposed_identity.actions.empty() ||
      !proposed_identity.identity_reconfiguration ||
      proposed_identity.identity_reconfiguration->external_address !=
          external ||
      (*identity_pump)->pending() != 0)
    return 39;
  auto repeated_identity = (*identity_pump)->poll(now);
  if (!repeated_identity.identity_reconfiguration ||
      !(*identity)->commit(external))
    return 40;
  if ((*identity_pump)->poll(now).identity_reconfiguration)
    return 41;

  controller->get()->stop();
  if (!factory.last->stopped)
    return 9;
  return 0;
}

export module sakuin.integration.dht_config;

import std;

import sakuin.config.model;
import sakuin.core.bytes;
import sakuin.core.random;
import sakuin.core.result;
import sakuin.dht.bootstrap;
import sakuin.dht.identity;
import sakuin.dht.krpc;
import sakuin.dht.node;
import sakuin.dht.peer_discovery;
import sakuin.dht.routing_maintenance;
import sakuin.integration.metadata_backfill;
import sakuin.runtime.datagram;

export namespace sakuin::integration {

dht::DhtNodeOptions
dht_node_options(const config::DhtConfig &config,
                 runtime::AddressFamily address_family) noexcept;
dht::BootstrapOptions
bootstrap_options(const config::DhtConfig &config) noexcept;
dht::RoutingMaintenanceOptions routing_maintenance_options(
    const config::RoutingMaintenanceConfig &config) noexcept;
dht::PeerDiscoveryOptions
peer_discovery_options(const config::MetadataDiscoveryConfig &config) noexcept;
MetadataDiscoveryBackfillOptions metadata_discovery_backfill_options(
    const config::MetadataDiscoveryBackfillConfig &config) noexcept;
std::optional<dht::Bep42IdentityPolicyOptions>
bep42_identity_policy_options(const config::DhtIdentityConfig &config) noexcept;
core::Result<dht::krpc::NodeId>
dht_node_id(const config::DhtIdentityConfig &config,
            runtime::AddressFamily address_family,
            std::optional<runtime::IpAddress> external_address,
            dht::krpc::NodeId entropy);
core::Result<dht::krpc::NodeId> secure_dht_node_id(
    const config::DhtIdentityConfig &config,
    runtime::AddressFamily address_family,
    std::optional<runtime::IpAddress> external_address = std::nullopt);

} // namespace sakuin::integration

namespace sakuin::integration {
namespace {

std::optional<std::uint8_t> hex_digit(char value) noexcept {
  if (value >= '0' && value <= '9')
    return static_cast<std::uint8_t>(value - '0');
  if (value >= 'a' && value <= 'f')
    return static_cast<std::uint8_t>(value - 'a' + 10);
  if (value >= 'A' && value <= 'F')
    return static_cast<std::uint8_t>(value - 'A' + 10);
  return std::nullopt;
}

core::Result<dht::krpc::NodeId> parse_node_id(std::string_view encoded) {
  dht::krpc::NodeId result;
  if (encoded.size() != result.bytes.size() * 2)
    return std::unexpected(core::Error{
        core::ErrorCode::InvalidArgument,
        "Fixed DHT node IDs must contain exactly 40 hexadecimal digits"});
  for (std::size_t index = 0; index < result.bytes.size(); ++index) {
    const auto high = hex_digit(encoded[index * 2]);
    const auto low = hex_digit(encoded[index * 2 + 1]);
    if (!high || !low)
      return std::unexpected(core::Error{
          core::ErrorCode::InvalidArgument,
          "Fixed DHT node IDs must contain exactly 40 hexadecimal digits"});
    result.bytes[index] = static_cast<std::uint8_t>((*high << 4U) | *low);
  }
  return result;
}

} // namespace

dht::DhtNodeOptions
dht_node_options(const config::DhtConfig &config,
                 runtime::AddressFamily address_family) noexcept {
  return {.query_timeout = config.query_timeout,
          .address_family = address_family};
}

dht::BootstrapOptions
bootstrap_options(const config::DhtConfig &config) noexcept {
  return {.maximum_in_flight = config.bootstrap_maximum_in_flight,
          .maximum_attempts = config.bootstrap_maximum_attempts,
          .retry_delay = config.bootstrap_retry_delay};
}

dht::RoutingMaintenanceOptions routing_maintenance_options(
    const config::RoutingMaintenanceConfig &config) noexcept {
  return {.maximum_queued = config.maximum_queued,
          .maximum_in_flight = config.maximum_in_flight,
          .maximum_attempts = config.maximum_attempts,
          .retry_delay = config.retry_delay};
}

dht::PeerDiscoveryOptions
peer_discovery_options(const config::MetadataDiscoveryConfig &config) noexcept {
  return {.maximum_pending = config.maximum_pending,
          .maximum_in_flight = config.maximum_in_flight,
          .parallelism_per_hash = config.parallelism_per_hash,
          .maximum_queries_per_hash = config.maximum_queries_per_hash,
          .retry_delay = config.retry_delay};
}

MetadataDiscoveryBackfillOptions metadata_discovery_backfill_options(
    const config::MetadataDiscoveryBackfillConfig &config) noexcept {
  return {.maximum_records_per_poll = config.maximum_records_per_poll,
          .refresh_interval = config.refresh_interval,
          .full_rescan_interval = config.full_rescan_interval,
          .backpressure_retry_delay = config.retry_delay};
}

std::optional<dht::Bep42IdentityPolicyOptions> bep42_identity_policy_options(
    const config::DhtIdentityConfig &config) noexcept {
  if (config.mode != config::DhtIdentityMode::Bep42)
    return std::nullopt;
  return dht::Bep42IdentityPolicyOptions{.observation_quorum =
                                             config.observation_quorum,
                                         .vote_window = config.vote_window};
}

core::Result<dht::krpc::NodeId>
dht_node_id(const config::DhtIdentityConfig &configuration,
            runtime::AddressFamily address_family,
            std::optional<runtime::IpAddress> external_address,
            dht::krpc::NodeId entropy) {
  if (configuration.mode == config::DhtIdentityMode::Fixed) {
    const auto &configured = address_family == runtime::AddressFamily::IPv4
                                 ? configuration.fixed_ipv4_node_id
                                 : configuration.fixed_ipv6_node_id;
    if (!configured)
      return std::unexpected(core::Error{
          core::ErrorCode::InvalidArgument,
          "Fixed DHT identity is missing a node ID for the address family"});
    return parse_node_id(*configured);
  }
  if (external_address && external_address->family != address_family)
    return std::unexpected(core::Error{
        core::ErrorCode::InvalidArgument,
        "BEP 42 external address has the wrong family for the DHT node"});
  if (!external_address)
    return entropy;
  return dht::bep42_node_id(*external_address, entropy.bytes.back(), entropy);
}

core::Result<dht::krpc::NodeId>
secure_dht_node_id(const config::DhtIdentityConfig &configuration,
                   runtime::AddressFamily address_family,
                   std::optional<runtime::IpAddress> external_address) {
  dht::krpc::NodeId entropy;
  if (configuration.mode == config::DhtIdentityMode::Bep42) {
    auto bytes = std::as_writable_bytes(std::span{entropy.bytes});
    if (auto generated = core::secure_random(bytes); !generated)
      return std::unexpected(generated.error());
  }
  return dht_node_id(configuration, address_family, external_address, entropy);
}

} // namespace sakuin::integration

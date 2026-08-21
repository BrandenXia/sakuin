export module sakuin.integration.dht_config;

import sakuin.config.model;
import sakuin.dht.bootstrap;
import sakuin.dht.node;
import sakuin.dht.routing_maintenance;
import sakuin.runtime.datagram;

export namespace sakuin::integration {

dht::DhtNodeOptions
dht_node_options(const config::DhtConfig &config,
                 runtime::AddressFamily address_family) noexcept;
dht::BootstrapOptions
bootstrap_options(const config::DhtConfig &config) noexcept;
dht::RoutingMaintenanceOptions routing_maintenance_options(
    const config::RoutingMaintenanceConfig &config) noexcept;

} // namespace sakuin::integration

namespace sakuin::integration {

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

} // namespace sakuin::integration

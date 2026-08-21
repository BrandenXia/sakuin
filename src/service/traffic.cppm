export module sakuin.service.traffic;

import std;

import sakuin.config.model;
import sakuin.core.result;
import sakuin.runtime.traffic;
import sakuin.scheduler.traffic;

export namespace sakuin::service {

core::Result<std::unique_ptr<runtime::TrafficGovernor>>
create_traffic_governor(const config::PeriodicTrafficConfig &configuration);

} // namespace sakuin::service

namespace sakuin::service {
namespace {

scheduler::TrafficBudgetPolicy
traffic_budget_policy(const config::PeriodicTrafficConfig &configuration) {
  scheduler::TrafficBudgetPolicy result;
  if (configuration.inbound_bytes)
    result.global.inbound = scheduler::PeriodicByteQuota{
        .maximum_bytes = *configuration.inbound_bytes,
        .period = configuration.window};
  if (configuration.outbound_bytes)
    result.global.outbound = scheduler::PeriodicByteQuota{
        .maximum_bytes = *configuration.outbound_bytes,
        .period = configuration.window};
  return result;
}

} // namespace

core::Result<std::unique_ptr<runtime::TrafficGovernor>>
create_traffic_governor(const config::PeriodicTrafficConfig &configuration) {
  auto governor = scheduler::FixedWindowTrafficGovernor::create(
      traffic_budget_policy(configuration));
  if (!governor)
    return std::unexpected(governor.error());
  return std::unique_ptr<runtime::TrafficGovernor>{std::move(*governor)};
}

} // namespace sakuin::service

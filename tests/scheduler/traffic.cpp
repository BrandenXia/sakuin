import std;

import sakuin.core;
import sakuin.runtime;
import sakuin.scheduler;

namespace {

sakuin::core::Timestamp seconds(std::int64_t value) {
  return sakuin::core::Timestamp{
      std::chrono::duration_cast<sakuin::core::Timestamp::duration>(
          std::chrono::seconds{value})};
}

} // namespace

int main() {
  using namespace sakuin;

  scheduler::TrafficBudgetPolicy policy{
      .global = {.inbound = scheduler::PeriodicByteQuota{
                     .maximum_bytes = 50,
                     .period = std::chrono::seconds{10}},
                 .outbound = scheduler::PeriodicByteQuota{
                     .maximum_bytes = 100,
                     .period = std::chrono::seconds{10}}},
      .classes = {{.traffic_class = 7,
                   .limits = {.outbound = scheduler::PeriodicByteQuota{
                                  .maximum_bytes = 60,
                                  .period = std::chrono::seconds{5}}}}}};
  auto governor = scheduler::FixedWindowTrafficGovernor::create(policy);
  if (!governor)
    return 1;

  const auto outbound = [](std::uint16_t traffic_class, std::uint64_t bytes) {
    return runtime::TrafficRequest{
        .direction = runtime::TrafficDirection::Outbound,
        .traffic_class = traffic_class,
        .bytes = bytes};
  };
  auto first = (*governor)->admit(outbound(7, 40), seconds(0));
  auto class_denied = (*governor)->admit(outbound(7, 30), seconds(0));
  if (!first.allowed || first.remaining_bytes != 20 || class_denied.allowed ||
      class_denied.request_too_large ||
      class_denied.retry_after != std::chrono::seconds{5})
    return 2;

  auto after_class_reset = (*governor)->admit(outbound(7, 30), seconds(5));
  auto global_denied = (*governor)->admit(outbound(7, 31), seconds(5));
  if (!after_class_reset.allowed || after_class_reset.remaining_bytes != 30 ||
      global_denied.allowed ||
      global_denied.retry_after != std::chrono::seconds{5})
    return 3;

  auto after_global_reset = (*governor)->admit(outbound(7, 60), seconds(10));
  auto other_class = (*governor)->admit(outbound(8, 40), seconds(10));
  auto exhausted_global = (*governor)->admit(outbound(8, 1), seconds(10));
  if (!after_global_reset.allowed || !other_class.allowed ||
      exhausted_global.allowed)
    return 4;

  auto inbound = (*governor)->admit(
      {.direction = runtime::TrafficDirection::Inbound,
       .traffic_class = 7,
       .bytes = 50},
      seconds(10));
  if (!inbound.allowed || inbound.remaining_bytes != 0)
    return 5;

  auto too_large = (*governor)->admit(outbound(7, 61), seconds(20));
  if (too_large.allowed || !too_large.request_too_large ||
      too_large.retry_after)
    return 6;

  scheduler::TrafficBudgetPolicy duplicate{
      .classes = {{.traffic_class = 1}, {.traffic_class = 1}}};
  if (scheduler::FixedWindowTrafficGovernor::create(std::move(duplicate)))
    return 7;
  scheduler::TrafficBudgetPolicy invalid{
      .global = {.outbound = scheduler::PeriodicByteQuota{
                     .maximum_bytes = 1,
                     .period = core::Duration::zero()}}};
  if (scheduler::FixedWindowTrafficGovernor::create(std::move(invalid)))
    return 8;

  scheduler::TrafficBudgetPolicy concurrent_policy{
      .global = {.outbound = scheduler::PeriodicByteQuota{
                     .maximum_bytes = 1'000,
                     .period = std::chrono::seconds{1}}}};
  auto concurrent = scheduler::FixedWindowTrafficGovernor::create(
      std::move(concurrent_policy));
  std::atomic<std::uint64_t> accepted{};
  std::vector<std::jthread> threads;
  for (int index = 0; index < 8; ++index) {
    threads.emplace_back([&] {
      for (int attempt = 0; attempt < 200; ++attempt) {
        if ((*concurrent)->admit(outbound(0, 1), seconds(0)).allowed)
          accepted.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }
  threads.clear();
  if (accepted.load(std::memory_order_relaxed) != 1'000)
    return 9;
  return 0;
}

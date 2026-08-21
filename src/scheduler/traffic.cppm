export module sakuin.scheduler.traffic;

import std;

import sakuin.core.result;
import sakuin.core.time;
import sakuin.runtime.traffic;

export namespace sakuin::scheduler {

struct PeriodicByteQuota {
  std::uint64_t maximum_bytes{};
  core::Duration period{};
};

struct DirectionalTrafficPolicy {
  std::optional<PeriodicByteQuota> inbound;
  std::optional<PeriodicByteQuota> outbound;
};

struct ClassifiedTrafficPolicy {
  runtime::TrafficClassId traffic_class{};
  DirectionalTrafficPolicy limits;
};

struct TrafficBudgetPolicy {
  DirectionalTrafficPolicy global;
  std::vector<ClassifiedTrafficPolicy> classes;
};

class FixedWindowTrafficGovernor final : public runtime::TrafficGovernor {
public:
  static core::Result<std::unique_ptr<FixedWindowTrafficGovernor>>
  create(TrafficBudgetPolicy policy);

  runtime::TrafficDecision admit(runtime::TrafficRequest request,
                                 core::Timestamp now) override;

private:
  struct Counter {
    std::int64_t window{};
    std::uint64_t used{};
    bool initialized{};
  };

  struct DirectionState {
    std::optional<PeriodicByteQuota> quota;
    Counter counter;
  };

  struct PolicyState {
    std::array<DirectionState, 2> directions;
  };

  explicit FixedWindowTrafficGovernor(TrafficBudgetPolicy policy);

  static std::size_t direction_index(runtime::TrafficDirection direction) {
    return direction == runtime::TrafficDirection::Inbound ? 0 : 1;
  }

  mutable std::mutex mutex_;
  PolicyState global_;
  std::unordered_map<runtime::TrafficClassId, PolicyState> classes_;
};

} // namespace sakuin::scheduler

namespace sakuin::scheduler {
namespace {

core::Result<void> validate_quota(const std::optional<PeriodicByteQuota> &quota,
                                  std::string_view name) {
  if (quota && quota->period <= core::Duration::zero())
    return std::unexpected(core::Error{
        core::ErrorCode::InvalidArgument,
        std::string{name} + " traffic quota period must be positive"});
  return {};
}

std::int64_t floor_divide(std::int64_t numerator,
                          std::int64_t denominator) noexcept {
  const auto quotient = numerator / denominator;
  const auto remainder = numerator % denominator;
  return quotient - (remainder < 0 ? 1 : 0);
}

} // namespace

FixedWindowTrafficGovernor::FixedWindowTrafficGovernor(
    TrafficBudgetPolicy policy) {
  global_.directions[0].quota = policy.global.inbound;
  global_.directions[1].quota = policy.global.outbound;
  for (auto &entry : policy.classes) {
    PolicyState state;
    state.directions[0].quota = entry.limits.inbound;
    state.directions[1].quota = entry.limits.outbound;
    classes_.emplace(entry.traffic_class, std::move(state));
  }
}

core::Result<std::unique_ptr<FixedWindowTrafficGovernor>>
FixedWindowTrafficGovernor::create(TrafficBudgetPolicy policy) {
  if (auto valid = validate_quota(policy.global.inbound, "Global inbound");
      !valid)
    return std::unexpected(valid.error());
  if (auto valid = validate_quota(policy.global.outbound, "Global outbound");
      !valid)
    return std::unexpected(valid.error());

  std::unordered_set<runtime::TrafficClassId> seen;
  for (const auto &entry : policy.classes) {
    if (!seen.insert(entry.traffic_class).second)
      return std::unexpected(core::Error{
          core::ErrorCode::InvalidArgument,
          "Traffic policy contains a duplicate class id"});
    if (auto valid = validate_quota(entry.limits.inbound, "Class inbound");
        !valid)
      return std::unexpected(valid.error());
    if (auto valid = validate_quota(entry.limits.outbound, "Class outbound");
        !valid)
      return std::unexpected(valid.error());
  }
  return std::unique_ptr<FixedWindowTrafficGovernor>{
      new FixedWindowTrafficGovernor{std::move(policy)}};
}

runtime::TrafficDecision
FixedWindowTrafficGovernor::admit(runtime::TrafficRequest request,
                                  core::Timestamp now) {
  std::scoped_lock lock{mutex_};
  const auto direction = direction_index(request.direction);
  std::array<DirectionState *, 2> applicable{};
  std::size_t count{};
  if (global_.directions[direction].quota)
    applicable[count++] = &global_.directions[direction];
  if (const auto found = classes_.find(request.traffic_class);
      found != classes_.end() && found->second.directions[direction].quota)
    applicable[count++] = &found->second.directions[direction];

  if (count == 0)
    return {};

  const auto elapsed = std::chrono::duration_cast<core::Duration>(
                           now.time_since_epoch())
                           .count();
  runtime::TrafficDecision decision;
  decision.remaining_bytes = std::numeric_limits<std::uint64_t>::max();

  for (std::size_t index = 0; index < count; ++index) {
    auto &state = *applicable[index];
    const auto &quota = *state.quota;
    const auto window = floor_divide(elapsed, quota.period.count());
    if (!state.counter.initialized || state.counter.window != window) {
      state.counter = {.window = window, .used = 0, .initialized = true};
    }
    const auto available = quota.maximum_bytes - state.counter.used;
    decision.remaining_bytes = std::min(decision.remaining_bytes, available);
    if (request.bytes <= available)
      continue;

    decision.allowed = false;
    if (request.bytes > quota.maximum_bytes) {
      decision.request_too_large = true;
      continue;
    }
    const auto elapsed_in_window =
        elapsed - window * quota.period.count();
    const auto wait = quota.period - core::Duration{elapsed_in_window};
    decision.retry_after =
        std::max(decision.retry_after.value_or(core::Duration::zero()), wait);
  }

  if (!decision.allowed)
    return decision;
  for (std::size_t index = 0; index < count; ++index)
    applicable[index]->counter.used += request.bytes;
  decision.remaining_bytes -= request.bytes;
  return decision;
}

} // namespace sakuin::scheduler

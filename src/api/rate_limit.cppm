export module sakuin.api.rate_limit;

import std;

import sakuin.core.result;
import sakuin.core.time;

export namespace sakuin::api {

struct RequestAdmission {
  bool allowed{true};
  std::uint64_t remaining{};
  std::optional<core::Duration> retry_after;
};

class ApiRequestGovernor {
public:
  virtual ~ApiRequestGovernor() = default;
  virtual core::Result<RequestAdmission> admit(std::string_view key_id,
                                               core::Timestamp now) = 0;
};

struct FixedWindowRequestPolicy {
  std::uint64_t maximum_requests{};
  core::Duration period{};
};

class FixedWindowRequestGovernor final : public ApiRequestGovernor {
public:
  static core::Result<std::unique_ptr<FixedWindowRequestGovernor>>
  create(FixedWindowRequestPolicy policy);

  core::Result<RequestAdmission> admit(std::string_view key_id,
                                       core::Timestamp now) override;

private:
  struct Counter {
    std::int64_t window{};
    std::uint64_t used{};
    bool initialized{};
  };

  explicit FixedWindowRequestGovernor(FixedWindowRequestPolicy policy)
      : policy_(policy) {}

  FixedWindowRequestPolicy policy_;
  std::mutex mutex_;
  std::unordered_map<std::string, Counter> counters_;
};

} // namespace sakuin::api

namespace sakuin::api {
namespace {

std::int64_t floor_divide(std::int64_t numerator,
                          std::int64_t denominator) noexcept {
  const auto quotient = numerator / denominator;
  const auto remainder = numerator % denominator;
  return quotient - (remainder < 0 ? 1 : 0);
}

} // namespace

core::Result<std::unique_ptr<FixedWindowRequestGovernor>>
FixedWindowRequestGovernor::create(FixedWindowRequestPolicy policy) {
  if (policy.maximum_requests == 0 || policy.period <= core::Duration::zero())
    return std::unexpected(
        core::Error{core::ErrorCode::InvalidArgument,
                    "API request limit and window must both be positive"});
  return std::unique_ptr<FixedWindowRequestGovernor>{
      new FixedWindowRequestGovernor{policy}};
}

core::Result<RequestAdmission>
FixedWindowRequestGovernor::admit(std::string_view key_id,
                                  core::Timestamp now) {
  if (key_id.empty())
    return std::unexpected(core::Error{core::ErrorCode::InvalidArgument,
                                       "API rate limit requires a key id"});
  const auto elapsed =
      std::chrono::duration_cast<core::Duration>(now.time_since_epoch())
          .count();
  const auto period = policy_.period.count();
  const auto window = floor_divide(elapsed, period);

  std::lock_guard lock{mutex_};
  auto &counter = counters_[std::string{key_id}];
  if (!counter.initialized || counter.window != window)
    counter = {.window = window, .used = 0, .initialized = true};
  if (counter.used >= policy_.maximum_requests) {
    const auto elapsed_in_window = elapsed - window * period;
    return RequestAdmission{.allowed = false,
                            .remaining = 0,
                            .retry_after = policy_.period -
                                           core::Duration{elapsed_in_window}};
  }
  ++counter.used;
  return RequestAdmission{.allowed = true,
                          .remaining = policy_.maximum_requests - counter.used};
}

} // namespace sakuin::api

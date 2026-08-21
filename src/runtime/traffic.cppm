export module sakuin.runtime.traffic;

import std;

import sakuin.core.time;

export namespace sakuin::runtime {

using TrafficClassId = std::uint16_t;

enum class TrafficDirection : std::uint8_t { Inbound, Outbound };

struct TrafficRequest {
  TrafficDirection direction{TrafficDirection::Outbound};
  TrafficClassId traffic_class{};
  std::uint64_t bytes{};
};

struct TrafficDecision {
  bool allowed{true};
  bool request_too_large{};
  std::uint64_t remaining_bytes{std::numeric_limits<std::uint64_t>::max()};
  std::optional<core::Duration> retry_after;
};

class TrafficGovernor {
public:
  virtual ~TrafficGovernor() = default;
  virtual TrafficDecision admit(TrafficRequest request,
                                core::Timestamp now) = 0;
};

class UnlimitedTrafficGovernor final : public TrafficGovernor {
public:
  TrafficDecision admit(TrafficRequest, core::Timestamp) override {
    return {};
  }
};

} // namespace sakuin::runtime

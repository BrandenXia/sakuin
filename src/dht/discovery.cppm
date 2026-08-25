export module sakuin.dht.discovery;

import std;

import sakuin.core.bytes;
import sakuin.core.result;
import sakuin.core.time;
import sakuin.dht.krpc;
import sakuin.dht.node;
import sakuin.runtime.datagram;

export namespace sakuin::dht {

struct RoutingDiscoveryOptions {
  core::Duration interval{std::chrono::seconds{5}};
  std::size_t maximum_in_flight{2};
};

struct RoutingDiscoveryStep {
  std::vector<DatagramSend> sends;
  std::size_t in_flight{};
  std::size_t queries_started{};
  std::optional<core::Timestamp> next_wakeup;
};

// Continuously refreshes rotating Kademlia distance buckets. Bootstrap gets a
// node into the network; this planner keeps asking learned contacts for nodes
// in other parts of the keyspace so a sparse table can grow without depending
// on unsolicited traffic.
class RoutingDiscoveryPlanner {
public:
  static core::Result<std::unique_ptr<RoutingDiscoveryPlanner>>
  create(DhtNode &node, RoutingDiscoveryOptions options = {});

  core::Result<RoutingDiscoveryStep> poll(core::Timestamp now);
  bool consume(const DhtActions &actions);
  bool consume_timeout(const QueryTimeout &timeout);
  bool delivery_failed(const DatagramSend &send);

  std::size_t in_flight() const noexcept { return outstanding_.size(); }

private:
  struct Outstanding {
    core::ByteBuffer transaction;
    runtime::DatagramEndpoint remote;
  };

  RoutingDiscoveryPlanner(DhtNode &node, RoutingDiscoveryOptions options)
      : node_(&node), options_(options) {}

  krpc::NodeId next_target() noexcept;
  std::vector<Outstanding>::iterator
  find(core::ByteView transaction,
       const runtime::DatagramEndpoint &remote) noexcept;

  DhtNode *node_;
  RoutingDiscoveryOptions options_;
  std::vector<Outstanding> outstanding_;
  std::optional<core::Timestamp> next_query_at_;
  std::uint64_t sequence_{};
};

} // namespace sakuin::dht

namespace sakuin::dht {
namespace {

std::uint64_t mix(std::uint64_t value) noexcept {
  value += 0x9e3779b97f4a7c15ULL;
  value = (value ^ (value >> 30U)) * 0xbf58476d1ce4e5b9ULL;
  value = (value ^ (value >> 27U)) * 0x94d049bb133111ebULL;
  return value ^ (value >> 31U);
}

} // namespace

core::Result<std::unique_ptr<RoutingDiscoveryPlanner>>
RoutingDiscoveryPlanner::create(DhtNode &node,
                                RoutingDiscoveryOptions options) {
  if (options.interval <= core::Duration::zero() ||
      options.maximum_in_flight == 0)
    return std::unexpected(core::Error{core::ErrorCode::InvalidArgument,
                                       "Invalid DHT discovery limits"});
  return std::unique_ptr<RoutingDiscoveryPlanner>{
      new RoutingDiscoveryPlanner{node, options}};
}

krpc::NodeId RoutingDiscoveryPlanner::next_target() noexcept {
  auto target = node_->routing_table().local_id();
  const auto bucket = static_cast<std::size_t>(sequence_ % 160U);
  const auto byte_index = target.bytes.size() - 1U - bucket / 8U;
  const auto bit_index = static_cast<unsigned>(bucket % 8U);
  target.bytes[byte_index] ^= static_cast<std::uint8_t>(1U << bit_index);

  auto entropy = mix(sequence_++);
  for (std::size_t index = byte_index + 1U; index < target.bytes.size();
       ++index) {
    if ((index - byte_index - 1U) % sizeof(entropy) == 0)
      entropy = mix(entropy);
    target.bytes[index] = static_cast<std::uint8_t>(
        entropy >> (((index - byte_index - 1U) % sizeof(entropy)) * 8U));
  }
  if (bit_index != 0) {
    const auto lower_mask = static_cast<std::uint8_t>((1U << bit_index) - 1U);
    target.bytes[byte_index] = static_cast<std::uint8_t>(
        (target.bytes[byte_index] & ~lower_mask) |
        (static_cast<std::uint8_t>(entropy) & lower_mask));
  }
  return target;
}

std::vector<RoutingDiscoveryPlanner::Outstanding>::iterator
RoutingDiscoveryPlanner::find(
    core::ByteView transaction,
    const runtime::DatagramEndpoint &remote) noexcept {
  return std::ranges::find_if(outstanding_, [&](const auto &entry) {
    return entry.remote == remote &&
           std::ranges::equal(entry.transaction, transaction);
  });
}

core::Result<RoutingDiscoveryStep>
RoutingDiscoveryPlanner::poll(core::Timestamp now) {
  RoutingDiscoveryStep step;
  if (!next_query_at_)
    next_query_at_ = now;
  if (now < *next_query_at_) {
    step.in_flight = outstanding_.size();
    step.next_wakeup = next_query_at_;
    return step;
  }
  if (outstanding_.size() >= options_.maximum_in_flight) {
    step.in_flight = outstanding_.size();
    return step;
  }

  const auto target = next_target();
  auto contacts = node_->routing_table().closest(target, 1);
  next_query_at_ = now + std::chrono::duration_cast<core::Timestamp::duration>(
                             options_.interval);
  step.next_wakeup = next_query_at_;
  if (contacts.empty()) {
    step.in_flight = outstanding_.size();
    return step;
  }

  auto query = node_->find_node(contacts.front().endpoint, target, now);
  if (!query)
    return std::unexpected(query.error());
  outstanding_.push_back(
      {.transaction = query->query_transaction, .remote = query->destination});
  step.sends.push_back(std::move(*query));
  step.queries_started = 1;
  step.in_flight = outstanding_.size();
  return step;
}

bool RoutingDiscoveryPlanner::consume(const DhtActions &actions) {
  if (!actions.query_completion ||
      actions.query_completion->kind != krpc::QueryKind::FindNode)
    return false;
  const auto found = find(actions.query_completion->transaction,
                          actions.query_completion->remote);
  if (found == outstanding_.end())
    return false;
  outstanding_.erase(found);
  return true;
}

bool RoutingDiscoveryPlanner::consume_timeout(const QueryTimeout &timeout) {
  if (timeout.kind != krpc::QueryKind::FindNode)
    return false;
  const auto found = find(timeout.transaction, timeout.remote);
  if (found == outstanding_.end())
    return false;
  outstanding_.erase(found);
  return true;
}

bool RoutingDiscoveryPlanner::delivery_failed(const DatagramSend &send) {
  if (send.query_transaction.empty())
    return false;
  const auto found = find(send.query_transaction, send.destination);
  if (found == outstanding_.end())
    return false;
  node_->cancel_query(send.query_transaction, send.destination);
  outstanding_.erase(found);
  return true;
}

} // namespace sakuin::dht

import std;

import sakuin.core;
import sakuin.dht;
import sakuin.runtime;

namespace {

sakuin::dht::krpc::NodeId id(std::uint8_t suffix) {
  sakuin::dht::krpc::NodeId result;
  result.bytes.front() = 0x80;
  result.bytes.back() = suffix;
  return result;
}

sakuin::runtime::DatagramEndpoint endpoint(std::uint8_t suffix) {
  auto address = sakuin::runtime::IpAddress::loopback_v4();
  address.bytes.back() = static_cast<std::uint8_t>(suffix + 1);
  return {.address = address,
          .port = static_cast<std::uint16_t>(6'000 + suffix)};
}

sakuin::core::Timestamp seconds(std::int64_t value) {
  return sakuin::core::Timestamp{std::chrono::seconds{value}};
}

} // namespace

int main() {
  using namespace sakuin;

  dht::AnnounceTokenSecret secret;
  auto tokens = dht::RotatingAnnounceTokenProvider::create(secret);
  dht::DhtNode node{id(0),
                    *tokens,
                    {.query_timeout = std::chrono::seconds{2},
                     .address_family = runtime::AddressFamily::IPv4}};
  auto invalid = dht::RoutingDiscoveryPlanner::create(
      node, {.interval = core::Duration::zero(), .maximum_in_flight = 1});
  auto planner = dht::RoutingDiscoveryPlanner::create(
      node, {.interval = std::chrono::seconds{5}, .maximum_in_flight = 1});
  if (invalid || !planner)
    return 1;

  auto empty = (*planner)->poll(seconds(10));
  if (!empty || !empty->sends.empty() || empty->in_flight != 0 ||
      empty->next_wakeup != seconds(15))
    return 2;

  const auto remote = endpoint(1);
  node.routing_table().observe(
      {.id = id(1), .endpoint = remote, .last_seen = seconds(10)});
  auto started = (*planner)->poll(seconds(15));
  if (!started || started->sends.size() != 1 || started->queries_started != 1 ||
      started->in_flight != 1 || started->next_wakeup != seconds(20))
    return 3;
  auto decoded = dht::krpc::decode(started->sends.front().payload);
  const auto *query =
      decoded ? std::get_if<dht::krpc::Query>(&*decoded) : nullptr;
  if (!query || query->kind != dht::krpc::QueryKind::FindNode ||
      !query->target || !started->sends.front().query_transaction)
    return 4;

  auto saturated = (*planner)->poll(seconds(20));
  if (!saturated || !saturated->sends.empty() || saturated->in_flight != 1 ||
      saturated->next_wakeup)
    return 5;

  auto response = dht::krpc::encode(
      dht::krpc::Response{.transaction = query->transaction, .sender = id(1)});
  auto completion =
      response
          ? node.handle({.source = remote, .payload = std::move(*response)},
                        seconds(16))
          : core::Result<dht::DhtActions>{std::unexpected(response.error())};
  if (!completion || !(*planner)->consume(*completion) ||
      (*planner)->in_flight() != 0 || node.outstanding_queries() != 0)
    return 6;

  auto next = (*planner)->poll(seconds(20));
  if (!next || next->sends.size() != 1 || next->in_flight != 1)
    return 7;
  auto expired = node.expire_queries(seconds(22));
  if (expired.size() != 1 || !(*planner)->consume_timeout(expired.front()) ||
      (*planner)->in_flight() != 0)
    return 8;

  auto third = (*planner)->poll(seconds(25));
  if (!third || third->sends.size() != 1)
    return 9;
  if (!(*planner)->delivery_failed(third->sends.front()) ||
      (*planner)->in_flight() != 0 || node.outstanding_queries() != 0)
    return 10;
  return 0;
}

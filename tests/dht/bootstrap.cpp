import std;

import sakuin.core;
import sakuin.dht;
import sakuin.runtime;

namespace {

sakuin::dht::krpc::NodeId node(std::uint8_t seed) {
  sakuin::dht::krpc::NodeId result;
  std::ranges::iota(result.bytes, seed);
  return result;
}

sakuin::runtime::DatagramEndpoint endpoint(std::uint8_t host,
                                           std::uint16_t port) {
  auto address = sakuin::runtime::IpAddress::loopback_v4();
  address.bytes[3] = host;
  return {.address = address, .port = port};
}

sakuin::core::Timestamp seconds(std::int64_t value) {
  return sakuin::core::Timestamp{
      std::chrono::duration_cast<sakuin::core::Timestamp::duration>(
          std::chrono::seconds{value})};
}

} // namespace

int main() {
  using namespace sakuin;

  dht::AnnounceTokenSecret secret;
  auto tokens = dht::RotatingAnnounceTokenProvider::create(secret);
  dht::DhtNode node_engine{node(100), *tokens};
  const std::array routers{endpoint(2, 6881)};
  auto planner =
      dht::BootstrapPlanner::create(node_engine, routers,
                                    {.maximum_in_flight = 1,
                                     .maximum_attempts = 3,
                                     .retry_delay = std::chrono::seconds{1}});
  if (!planner || dht::BootstrapPlanner::create(node_engine, {}, {}))
    return 1;

  auto first = (*planner)->poll(seconds(0));
  if (!first || first->sends.size() != 1 || first->known_candidates != 1 ||
      first->complete || node_engine.outstanding_queries() != 1)
    return 2;
  auto query = dht::krpc::decode(first->sends.front().payload);
  const auto *typed_query =
      query ? std::get_if<dht::krpc::Query>(&*query) : nullptr;
  if (!typed_query || typed_query->kind != dht::krpc::QueryKind::FindNode ||
      typed_query->target != node_engine.routing_table().local_id())
    return 3;

  const dht::NodeContact discovered{
      .id = node(20), .endpoint = endpoint(3, 6881), .last_seen = seconds(1)};
  const std::array contacts{discovered};
  auto compact =
      dht::encode_compact_nodes(contacts, runtime::AddressFamily::IPv4);
  auto response = dht::krpc::encode(dht::krpc::Response{
      .transaction = typed_query->transaction,
      .sender = node(2),
      .values = {{"nodes", dht::bencode::Value{*compact}}}});
  auto actions = node_engine.handle(
      {.source = routers.front(), .payload = std::move(*response)}, seconds(1));
  if (!actions || !actions->query_completion)
    return 4;
  (*planner)->consume(*actions, seconds(1));

  // Contacts learned after the first successful response belong to normal
  // routing discovery. They must not keep extending bootstrap's completion
  // frontier on a live DHT.
  const dht::NodeContact late_contact{
      .id = node(30), .endpoint = endpoint(4, 6881), .last_seen = seconds(1)};
  node_engine.routing_table().observe(late_contact);

  auto expansion = (*planner)->poll(seconds(1));
  if (!expansion || expansion->sends.size() != 1 ||
      expansion->sends.front().destination != discovered.endpoint ||
      expansion->known_candidates != 2)
    return 5;

  const auto timeouts = node_engine.expire_queries(seconds(16));
  if (timeouts.size() != 1 ||
      !(*planner)->consume_timeout(timeouts.front(), seconds(16)))
    return 6;
  auto waiting = (*planner)->poll(seconds(16));
  if (!waiting || !waiting->sends.empty() ||
      waiting->next_wakeup != seconds(17))
    return 11;
  auto retry = (*planner)->poll(seconds(17));
  if (!retry || retry->sends.size() != 1)
    return 7;
  (*planner)->delivery_failed(retry->sends.front(), seconds(17));
  if (node_engine.outstanding_queries() != 0)
    return 8;
  auto last = (*planner)->poll(seconds(18));
  if (!last || last->sends.size() != 1)
    return 9;
  (*planner)->delivery_failed(last->sends.front(), seconds(18));
  core::InfoHash unrelated_hash;
  unrelated_hash.bytes.fill(0x44);
  if (!node_engine.get_peers(endpoint(9, 6881), unrelated_hash, seconds(19)))
    return 12;
  auto complete = (*planner)->poll(seconds(19));
  if (!complete || !complete->complete || complete->exhausted ||
      !complete->sends.empty() || node_engine.outstanding_queries() != 1)
    return 10;

  dht::DhtNode unreachable_node{node(101), *tokens};
  auto unreachable =
      dht::BootstrapPlanner::create(unreachable_node, routers,
                                    {.maximum_in_flight = 1,
                                     .maximum_attempts = 1,
                                     .retry_delay = std::chrono::seconds{1}});
  if (!unreachable)
    return 13;
  auto unreachable_attempt = (*unreachable)->poll(seconds(20));
  if (!unreachable_attempt || unreachable_attempt->sends.size() != 1 ||
      !(*unreachable)
           ->delivery_failed(unreachable_attempt->sends.front(), seconds(20)))
    return 14;
  auto exhausted = (*unreachable)->poll(seconds(21));
  if (!exhausted || exhausted->complete || !exhausted->exhausted ||
      !exhausted->sends.empty())
    return 15;
  return 0;
}

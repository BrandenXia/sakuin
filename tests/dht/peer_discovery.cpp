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

sakuin::core::InfoHash hash(std::uint8_t suffix) {
  sakuin::core::InfoHash result;
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

const sakuin::dht::krpc::Query *query(const sakuin::dht::DatagramSend &send) {
  auto decoded = sakuin::dht::krpc::decode(send.payload);
  if (!decoded)
    return nullptr;
  static sakuin::dht::krpc::Message message;
  message = std::move(*decoded);
  return std::get_if<sakuin::dht::krpc::Query>(&message);
}

sakuin::core::Result<sakuin::dht::DhtActions>
respond(sakuin::dht::DhtNode &node, const sakuin::dht::DatagramSend &send,
        sakuin::dht::krpc::NodeId sender,
        sakuin::dht::bencode::Value::Dictionary values,
        sakuin::core::Timestamp now) {
  const auto *request = query(send);
  if (!request)
    return std::unexpected(sakuin::core::Error{
        sakuin::core::ErrorCode::Internal, "Unable to decode test query"});
  auto packet = sakuin::dht::krpc::encode(
      sakuin::dht::krpc::Response{.transaction = request->transaction,
                                  .sender = sender,
                                  .values = std::move(values)});
  if (!packet)
    return std::unexpected(packet.error());
  return node.handle(
      {.source = send.destination, .payload = std::move(*packet)}, now);
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
  auto invalid = dht::PeerDiscoveryPlanner::create(
      node, {.maximum_pending = 1,
             .maximum_in_flight = 1,
             .parallelism_per_hash = 2,
             .maximum_queries_per_hash = 1,
             .retry_delay = std::chrono::minutes{5}});
  auto planner = dht::PeerDiscoveryPlanner::create(
      node, {.maximum_pending = 2,
             .maximum_in_flight = 2,
             .parallelism_per_hash = 2,
             .maximum_queries_per_hash = 3,
             .retry_delay = std::chrono::minutes{5}});
  if (invalid || !planner)
    return 1;

  node.routing_table().observe(
      {.id = id(1), .endpoint = endpoint(1), .last_seen = seconds(1)});
  node.routing_table().observe(
      {.id = id(2), .endpoint = endpoint(2), .last_seen = seconds(1)});
  const auto wanted = hash(10);
  auto offered = (*planner)->offer(wanted, seconds(10));
  auto duplicate = (*planner)->offer(wanted, seconds(11));
  if (!offered.value_or(false) || duplicate.value_or(true) ||
      (*planner)->pending() != 1)
    return 2;

  auto first = (*planner)->poll(seconds(12));
  if (!first || first->sends.size() != 2 || first->queries_started != 2 ||
      first->in_flight != 2 ||
      std::ranges::any_of(first->sends, [](const auto &send) {
        const auto *request = query(send);
        return !request || request->kind != dht::krpc::QueryKind::GetPeers ||
               send.traffic_class != dht::traffic_class::metadata_discovery;
      }))
    return 3;

  dht::NodeContact closer{
      .id = id(3), .endpoint = endpoint(3), .last_seen = seconds(13)};
  const std::array closer_nodes{closer};
  auto compact_nodes =
      dht::encode_compact_nodes(closer_nodes, runtime::AddressFamily::IPv4);
  auto empty_response = respond(
      node, first->sends.front(), id(1),
      {{"nodes", dht::bencode::Value{std::move(*compact_nodes)}}}, seconds(13));
  if (!empty_response || !(*planner)->consume(*empty_response, seconds(13)))
    return 4;

  auto second = (*planner)->poll(seconds(13));
  if (!second || second->sends.size() != 1 || second->queries_started != 1 ||
      second->sends.front().destination != closer.endpoint ||
      second->in_flight != 2)
    return 5;

  const runtime::DatagramEndpoint peer = endpoint(40);
  const runtime::DatagramEndpoint alternate_peer = endpoint(41);
  auto compact_peer = dht::krpc::encode_compact_endpoint(peer);
  auto compact_alternate_peer =
      dht::krpc::encode_compact_endpoint(alternate_peer);
  auto peer_response =
      respond(node, second->sends.front(), id(3),
              {{"values",
                dht::bencode::Value{dht::bencode::Value::List{
                    dht::bencode::Value{std::move(*compact_peer)},
                    dht::bencode::Value{std::move(*compact_alternate_peer)}}}}},
              seconds(14));
  if (!peer_response || !(*planner)->consume(*peer_response, seconds(14)) ||
      (*planner)->pending() != 0 || node.outstanding_queries() != 0)
    return 6;

  auto completed = (*planner)->poll(seconds(14));
  if (!completed || completed->candidates.size() != 2 ||
      completed->peers_found != 2 ||
      completed->candidates.front().info_hash != wanted ||
      completed->candidates.front().peer.address != peer.address ||
      completed->candidates.front().peer.port != peer.port)
    return 7;
  if (!(*planner)->retry(completed->candidates[0]) ||
      !(*planner)->retry(completed->candidates[1]) ||
      (*planner)->retry(completed->candidates[0]) || (*planner)->pending() != 2)
    return 14;
  auto retried = (*planner)->poll(seconds(14));
  if (!retried || retried->candidates.size() != 2 || (*planner)->pending() != 0)
    return 15;
  if ((*planner)->offer(wanted, seconds(15)).value_or(true) ||
      !(*planner)->offer(wanted, seconds(315)).value_or(false))
    return 8;

  dht::DhtNode timeout_node{id(50),
                            *tokens,
                            {.query_timeout = std::chrono::seconds{2},
                             .address_family = runtime::AddressFamily::IPv4}};
  timeout_node.routing_table().observe(
      {.id = id(51), .endpoint = endpoint(51), .last_seen = seconds(1)});
  auto timeout_planner = dht::PeerDiscoveryPlanner::create(
      timeout_node, {.maximum_pending = 1,
                     .maximum_in_flight = 1,
                     .parallelism_per_hash = 1,
                     .maximum_queries_per_hash = 1,
                     .retry_delay = std::chrono::minutes{5}});
  if (!timeout_planner ||
      !(*timeout_planner)->offer(hash(20), seconds(20)).value_or(false))
    return 9;
  auto timeout_started = (*timeout_planner)->poll(seconds(20));
  auto expired = timeout_node.expire_queries(seconds(22));
  if (!timeout_started || timeout_started->sends.size() != 1 ||
      expired.size() != 1 ||
      !(*timeout_planner)->consume_timeout(expired.front(), seconds(22)))
    return 10;
  auto exhausted = (*timeout_planner)->poll(seconds(22));
  if (!exhausted || exhausted->exhausted != 1 ||
      (*timeout_planner)->pending() != 0)
    return 11;

  if (!(*timeout_planner)->offer(hash(21), seconds(400)).value_or(false))
    return 12;
  auto failed_send = (*timeout_planner)->poll(seconds(400));
  if (!failed_send || failed_send->sends.size() != 1 ||
      !(*timeout_planner)
           ->delivery_failed(failed_send->sends.front(), seconds(400)) ||
      timeout_node.outstanding_queries() != 0)
    return 13;
  return 0;
}

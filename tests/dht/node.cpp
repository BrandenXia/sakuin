import std;

import sakuin.core;
import sakuin.dht;
import sakuin.runtime;

namespace {

sakuin::core::ByteBuffer bytes(std::string_view text) {
  const auto view = std::as_bytes(std::span{text});
  return {view.begin(), view.end()};
}

sakuin::dht::krpc::NodeId node(std::uint8_t seed) {
  sakuin::dht::krpc::NodeId result;
  std::ranges::iota(result.bytes, seed);
  return result;
}

sakuin::core::InfoHash hash(std::uint8_t seed) {
  sakuin::core::InfoHash result;
  std::ranges::iota(result.bytes, seed);
  return result;
}

sakuin::runtime::DatagramEndpoint endpoint(std::uint8_t host,
                                           std::uint16_t port) {
  auto address = sakuin::runtime::IpAddress::loopback_v4();
  address.bytes[3] = host;
  return {.address = address, .port = port};
}

sakuin::core::Result<sakuin::dht::DhtActions>
handle(sakuin::dht::DhtNode &server,
       const sakuin::dht::krpc::Query &query,
       sakuin::runtime::DatagramEndpoint source,
       sakuin::core::Timestamp now) {
  auto packet = sakuin::dht::krpc::encode(query);
  if (!packet)
    return std::unexpected(packet.error());
  return server.handle(
      sakuin::runtime::Datagram{.source = source, .payload = std::move(*packet)},
      now);
}

const sakuin::dht::krpc::Message *
decode_send(const sakuin::dht::DhtActions &actions) {
  if (actions.sends.size() != 1)
    return nullptr;
  auto decoded = sakuin::dht::krpc::decode(actions.sends.front().payload);
  if (!decoded)
    return nullptr;
  static sakuin::dht::krpc::Message result;
  result = std::move(*decoded);
  return &result;
}

} // namespace

int main() {
  using namespace sakuin;

  dht::AnnounceTokenSecret secret;
  std::ranges::iota(secret.bytes, std::uint8_t{1});
  auto tokens = dht::RotatingAnnounceTokenProvider::create(
      secret, std::chrono::minutes{5});
  if (!tokens || dht::RotatingAnnounceTokenProvider::create(
                     secret, core::Duration::zero()))
    return 1;

  const auto source = endpoint(2, 6881);
  const auto other_source = endpoint(3, 6882);
  const auto at_four_minutes =
      core::Timestamp{std::chrono::duration_cast<core::Timestamp::duration>(
          std::chrono::minutes{4})};
  const auto at_five_minutes =
      core::Timestamp{std::chrono::duration_cast<core::Timestamp::duration>(
          std::chrono::minutes{5})};
  const auto at_ten_minutes =
      core::Timestamp{std::chrono::duration_cast<core::Timestamp::duration>(
          std::chrono::minutes{10})};
  auto early_token = tokens->issue(source.address, at_four_minutes);
  if (!early_token ||
      !tokens->validate(*early_token, source.address, at_five_minutes).value_or(
          false) ||
      tokens->validate(*early_token, source.address, at_ten_minutes).value_or(
          true) ||
      tokens->validate(*early_token, other_source.address, at_four_minutes)
          .value_or(true))
    return 2;

  dht::DhtNode server{node(100), *tokens};
  const dht::krpc::Query ping{.transaction = bytes("p1"),
                              .kind = dht::krpc::QueryKind::Ping,
                              .method = "ping",
                              .sender = node(1)};
  auto ping_actions = handle(server, ping, source, at_five_minutes);
  if (!ping_actions || ping_actions->observation ||
      server.routing_table().size() != 1)
    return 3;
  const auto *ping_message = decode_send(*ping_actions);
  const auto *ping_response =
      ping_message ? std::get_if<dht::krpc::Response>(ping_message) : nullptr;
  if (!ping_response || ping_response->transaction != ping.transaction ||
      ping_response->sender != server.routing_table().local_id() ||
      ping_response->observed_endpoint != source ||
      ping_actions->sends.front().destination != source)
    return 4;

  const dht::krpc::Query find{
      .transaction = bytes("f1"),
      .kind = dht::krpc::QueryKind::FindNode,
      .method = "find_node",
      .sender = node(2),
      .target = node(1)};
  auto find_actions = handle(server, find, other_source, at_five_minutes);
  const auto *find_message =
      find_actions ? decode_send(*find_actions) : nullptr;
  const auto *find_response =
      find_message ? std::get_if<dht::krpc::Response>(find_message) : nullptr;
  if (!find_response)
    return 5;
  const auto nodes_value = find_response->values.find("nodes");
  if (nodes_value == find_response->values.end() ||
      !nodes_value->second.string())
    return 6;
  auto compact_nodes = dht::decode_compact_nodes(
      *nodes_value->second.string(), runtime::AddressFamily::IPv4,
      at_five_minutes);
  if (!compact_nodes || compact_nodes->size() != 2 ||
      compact_nodes->front().id != ping.sender)
    return 7;

  const auto wanted_hash = hash(20);
  const dht::krpc::Query get_peers{
      .transaction = bytes("g1"),
      .kind = dht::krpc::QueryKind::GetPeers,
      .method = "get_peers",
      .sender = node(3),
      .info_hash = wanted_hash};
  auto get_actions = handle(server, get_peers, source, at_five_minutes);
  const auto *get_message = get_actions ? decode_send(*get_actions) : nullptr;
  const auto *get_response =
      get_message ? std::get_if<dht::krpc::Response>(get_message) : nullptr;
  if (!get_actions || !get_actions->observation || !get_response ||
      get_actions->observation->info_hash != wanted_hash ||
      get_actions->observation->observed_at != at_five_minutes)
    return 8;
  const auto token_value = get_response->values.find("token");
  if (token_value == get_response->values.end() ||
      !token_value->second.string())
    return 9;

  const dht::krpc::Query announce{
      .transaction = bytes("a1"),
      .kind = dht::krpc::QueryKind::AnnouncePeer,
      .method = "announce_peer",
      .sender = node(3),
      .info_hash = wanted_hash,
      .port = 6881,
      .token = *token_value->second.string()};
  auto announce_actions = handle(server, announce, source, at_five_minutes);
  const auto *announce_message =
      announce_actions ? decode_send(*announce_actions) : nullptr;
  if (!announce_actions || !announce_actions->observation ||
      !announce_message ||
      !std::get_if<dht::krpc::Response>(announce_message))
    return 10;

  auto bad_announce = announce;
  bad_announce.transaction = bytes("a2");
  bad_announce.token = bytes("wrong");
  auto rejected_actions =
      handle(server, bad_announce, source, at_five_minutes);
  const auto *rejected_message =
      rejected_actions ? decode_send(*rejected_actions) : nullptr;
  const auto *rejected_error =
      rejected_message ? std::get_if<dht::krpc::Error>(rejected_message)
                       : nullptr;
  if (!rejected_actions || !rejected_actions->observation || !rejected_error ||
      rejected_error->code != 203)
    return 11;

  const dht::krpc::Query unknown{.transaction = bytes("u1"),
                                 .kind = dht::krpc::QueryKind::Unknown,
                                 .method = "unknown_method",
                                 .sender = node(4)};
  auto unknown_actions = handle(server, unknown, source, at_five_minutes);
  const auto *unknown_message =
      unknown_actions ? decode_send(*unknown_actions) : nullptr;
  const auto *unknown_error =
      unknown_message ? std::get_if<dht::krpc::Error>(unknown_message) : nullptr;
  if (!unknown_error || unknown_error->code != 204)
    return 12;

  auto malformed = server.handle(
      runtime::Datagram{.source = source, .payload = bytes("not bencode")},
      at_five_minutes);
  if (malformed || malformed.error().code != core::ErrorCode::InvalidArgument)
    return 13;

  const auto remote = endpoint(9, 49001);
  auto outbound = server.find_node(remote, node(40), at_ten_minutes);
  auto outbound_message =
      outbound ? dht::krpc::decode(outbound->payload)
               : core::Result<dht::krpc::Message>{
                     std::unexpected(outbound.error())};
  const auto *outbound_query =
      outbound_message ? std::get_if<dht::krpc::Query>(&*outbound_message)
                       : nullptr;
  if (!outbound_query || outbound_query->sender != server.routing_table().local_id() ||
      outbound_query->kind != dht::krpc::QueryKind::FindNode ||
      server.outstanding_queries() != 1)
    return 14;

  dht::NodeContact discovered{.id = node(41),
                              .endpoint = endpoint(10, 49002),
                              .last_seen = at_ten_minutes};
  const std::array discovered_nodes{discovered};
  auto compact = dht::encode_compact_nodes(discovered_nodes,
                                            runtime::AddressFamily::IPv4);
  dht::bencode::Value::Dictionary response_values{
      {"nodes", dht::bencode::Value{*compact}}};
  auto response_packet = dht::krpc::encode(dht::krpc::Response{
      .transaction = outbound_query->transaction,
      .sender = node(9),
      .values = std::move(response_values),
      .observed_endpoint = endpoint(42, 50'000)});
  if (!response_packet)
    return 15;

  auto spoofed = server.handle(
      runtime::Datagram{.source = endpoint(8, 49001),
                        .payload = *response_packet},
      at_ten_minutes);
  if (!spoofed || spoofed->query_completion ||
      server.outstanding_queries() != 1)
    return 16;
  auto completed = server.handle(
      runtime::Datagram{.source = remote, .payload = std::move(*response_packet)},
      at_ten_minutes);
  if (!completed || !completed->query_completion ||
      completed->query_completion->kind != dht::krpc::QueryKind::FindNode ||
      completed->query_completion->remote != remote ||
      completed->query_completion->protocol_error ||
      !completed->observed_address ||
      completed->observed_address->reporter != remote ||
      completed->observed_address->observed != endpoint(42, 50'000) ||
      server.outstanding_queries() != 0 ||
      server.routing_table().closest(discovered.id, 1).front().id !=
          discovered.id)
    return 17;

  auto errored_query = server.get_peers(remote, hash(50), at_ten_minutes);
  auto encoded_error_query = dht::krpc::decode(errored_query->payload);
  const auto *error_query =
      std::get_if<dht::krpc::Query>(&*encoded_error_query);
  auto protocol_error_packet = dht::krpc::encode(dht::krpc::Error{
      .transaction = error_query->transaction,
      .code = 202,
      .message = "Server Error"});
  auto protocol_error = server.handle(
      runtime::Datagram{.source = remote,
                        .payload = std::move(*protocol_error_packet)},
      at_ten_minutes);
  if (!protocol_error || !protocol_error->query_completion ||
      !protocol_error->query_completion->protocol_error ||
      protocol_error->query_completion->protocol_error->code != 202)
    return 18;

  auto expiring = server.ping(remote, at_ten_minutes);
  if (!expiring ||
      !server.expire_queries(at_ten_minutes + std::chrono::seconds{14}).empty() ||
      server.expire_queries(at_ten_minutes + std::chrono::seconds{15}).size() !=
          1 ||
      server.outstanding_queries() != 0)
    return 19;

  auto canceled = server.ping(remote, at_ten_minutes);
  if (!canceled || !canceled->query_transaction ||
      !server.cancel_query(*canceled->query_transaction, remote) ||
      server.cancel_query(*canceled->query_transaction, remote) ||
      server.outstanding_queries() != 0)
    return 20;

  dht::DhtNode ipv4_only{
      node(70), *tokens,
      {.query_timeout = std::chrono::seconds{15},
       .address_family = runtime::AddressFamily::IPv4}};
  auto ipv6_remote = remote;
  ipv6_remote.address.family = runtime::AddressFamily::IPv6;
  if (ipv4_only.ping(ipv6_remote, at_ten_minutes) ||
      ipv4_only.handle(
          runtime::Datagram{.source = ipv6_remote, .payload = bytes("bad")},
          at_ten_minutes))
    return 21;
  return 0;
}

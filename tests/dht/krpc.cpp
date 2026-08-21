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

template <typename Message>
const Message *round_trip(const sakuin::dht::krpc::Message &input) {
  auto encoded = sakuin::dht::krpc::encode(input);
  if (!encoded)
    return nullptr;
  auto decoded = sakuin::dht::krpc::decode(*encoded);
  if (!decoded)
    return nullptr;
  static sakuin::dht::krpc::Message result;
  result = std::move(*decoded);
  return std::get_if<Message>(&result);
}

} // namespace

int main() {
  using namespace sakuin;

  const dht::krpc::Query ping{.transaction = bytes("aa"),
                              .kind = dht::krpc::QueryKind::Ping,
                              .method = "ping",
                              .sender = node(1)};
  const auto *decoded_ping = round_trip<dht::krpc::Query>(ping);
  if (!decoded_ping || decoded_ping->transaction != ping.transaction ||
      decoded_ping->kind != dht::krpc::QueryKind::Ping ||
      decoded_ping->sender != ping.sender)
    return 1;

  const dht::krpc::Query get_peers{
      .transaction = bytes("gp"),
      .kind = dht::krpc::QueryKind::GetPeers,
      .method = "get_peers",
      .sender = node(2),
      .info_hash = hash(20)};
  const auto *decoded_get_peers = round_trip<dht::krpc::Query>(get_peers);
  if (!decoded_get_peers ||
      decoded_get_peers->kind != dht::krpc::QueryKind::GetPeers ||
      decoded_get_peers->info_hash != get_peers.info_hash)
    return 2;

  const dht::krpc::Query announce{
      .transaction = bytes("ap"),
      .kind = dht::krpc::QueryKind::AnnouncePeer,
      .method = "announce_peer",
      .sender = node(3),
      .info_hash = hash(40),
      .port = 6881,
      .implied_port = true,
      .token = bytes("token")};
  const auto *decoded_announce = round_trip<dht::krpc::Query>(announce);
  if (!decoded_announce || !decoded_announce->implied_port ||
      decoded_announce->port != announce.port ||
      decoded_announce->token != announce.token)
    return 3;

  dht::bencode::Value::Dictionary response_values;
  response_values.emplace("nodes", dht::bencode::Value{bytes("")});
  const runtime::DatagramEndpoint observed{
      .address = runtime::IpAddress::loopback_v4(), .port = 49'999};
  const dht::krpc::Response response{.transaction = bytes("rr"),
                                     .sender = node(4),
                                     .values = response_values,
                                     .observed_endpoint = observed};
  const auto *decoded_response = round_trip<dht::krpc::Response>(response);
  if (!decoded_response || decoded_response->sender != response.sender ||
      !decoded_response->values.contains("nodes") ||
      decoded_response->observed_endpoint != observed)
    return 4;

  const dht::krpc::Error error{.transaction = bytes("ee"),
                                .code = 203,
                                .message = "Protocol Error"};
  const auto *decoded_error = round_trip<dht::krpc::Error>(error);
  if (!decoded_error || decoded_error->code != error.code ||
      decoded_error->message != error.message)
    return 5;

  const dht::krpc::Query incomplete{
      .transaction = bytes("x"),
      .kind = dht::krpc::QueryKind::FindNode,
      .method = "find_node",
      .sender = node(5)};
  if (dht::krpc::encode(incomplete))
    return 6;

  const auto malformed = bytes(
      "d1:ad2:id20:abcdefghijklmnopqrst9:info_hash3:bad"
      "e1:q9:get_peers1:t2:aa1:y1:qe");
  auto rejected = dht::krpc::decode(malformed);
  if (rejected || rejected.error().code != core::ErrorCode::InvalidArgument)
    return 7;

  const dht::bencode::Value canonical{
      dht::bencode::Value::Dictionary{{"z", dht::bencode::Value{std::int64_t{1}}},
                                      {"a", dht::bencode::Value{bytes("x")}}}};
  auto encoded = dht::bencode::encode(canonical);
  if (!encoded || *encoded != bytes("d1:a1:x1:zi1ee"))
    return 8;

  if (dht::bencode::parse(bytes("i01e")) ||
      dht::bencode::parse(bytes("i1ejunk")))
    return 9;
  auto compact = dht::krpc::encode_compact_endpoint(observed);
  if (!compact || dht::krpc::decode_compact_endpoint(*compact) != observed ||
      dht::krpc::decode_compact_endpoint(bytes("short")))
    return 10;
  return 0;
}

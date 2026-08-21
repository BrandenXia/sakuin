export module sakuin.dht.krpc;

import std;

import sakuin.core.bytes;
import sakuin.core.ids;
import sakuin.core.result;
import sakuin.dht.bencode;
import sakuin.runtime.datagram;

export namespace sakuin::dht::krpc {

struct NodeId {
  std::array<std::uint8_t, 20> bytes{};
  friend bool operator==(const NodeId &, const NodeId &) = default;
};

enum class QueryKind { Ping, FindNode, GetPeers, AnnouncePeer, Unknown };

struct Query {
  core::ByteBuffer transaction;
  QueryKind kind{QueryKind::Unknown};
  std::string method;
  NodeId sender;
  std::optional<NodeId> target;
  std::optional<core::InfoHash> info_hash;
  std::optional<std::uint16_t> port;
  bool implied_port{};
  core::ByteBuffer token;
};

struct Response {
  core::ByteBuffer transaction;
  NodeId sender;
  bencode::Value::Dictionary values;
  std::optional<runtime::DatagramEndpoint> observed_endpoint;
};

struct Error {
  core::ByteBuffer transaction;
  std::int64_t code{};
  std::string message;
  std::optional<runtime::DatagramEndpoint> observed_endpoint;
};

using Message = std::variant<Query, Response, Error>;

core::Result<Message> decode(core::ByteView packet);
core::Result<core::ByteBuffer> encode(const Message &message);

core::Result<runtime::DatagramEndpoint>
decode_compact_endpoint(core::ByteView encoded);
core::Result<core::ByteBuffer>
encode_compact_endpoint(runtime::DatagramEndpoint endpoint);

} // namespace sakuin::dht::krpc

namespace sakuin::dht::krpc {
namespace {

core::Error malformed(std::string message) {
  return {core::ErrorCode::InvalidArgument, std::move(message)};
}

const bencode::Value *find(const bencode::Value::Dictionary &dictionary,
                           std::string_view key) {
  const auto found = dictionary.find(key);
  return found == dictionary.end() ? nullptr : &found->second;
}

std::string text(const core::ByteBuffer &value) {
  return {reinterpret_cast<const char *>(value.data()), value.size()};
}

core::ByteBuffer bytes(std::string_view value) {
  const auto view = std::as_bytes(std::span{value});
  return {view.begin(), view.end()};
}

template <typename Id>
core::Result<Id> id(const bencode::Value *value, std::string_view name) {
  if (!value || !value->string() || value->string()->size() != 20)
    return std::unexpected(
        malformed(std::string{name} + " must be a 20-byte string"));
  Id result;
  std::ranges::transform(*value->string(), result.bytes.begin(),
                         [](std::byte byte) {
                           return std::to_integer<std::uint8_t>(byte);
                         });
  return result;
}

template <typename Id> core::ByteBuffer id_bytes(const Id &value) {
  core::ByteBuffer result;
  result.reserve(value.bytes.size());
  for (const auto byte : value.bytes)
    result.push_back(static_cast<std::byte>(byte));
  return result;
}

core::Result<core::ByteBuffer>
transaction(const bencode::Value::Dictionary &root) {
  const auto *value = find(root, "t");
  if (!value || !value->string() || value->string()->empty() ||
      value->string()->size() > 32)
    return std::unexpected(
        malformed("KRPC transaction must contain 1 to 32 bytes"));
  return *value->string();
}

core::Result<void> validate_transaction(core::ByteView value) {
  if (value.empty() || value.size() > 32)
    return std::unexpected(
        malformed("KRPC transaction must contain 1 to 32 bytes"));
  return {};
}

core::Result<void> validate_query(const Query &query) {
  if (auto valid = validate_transaction(query.transaction); !valid)
    return valid;
  if (query.method.empty())
    return std::unexpected(malformed("KRPC query method must not be empty"));

  const auto require_method = [&](std::string_view expected) {
    return query.method == expected
               ? core::Result<void>{}
               : core::Result<void>{std::unexpected(malformed(
                     "KRPC query kind does not match its method"))};
  };
  switch (query.kind) {
  case QueryKind::Ping:
    return require_method("ping");
  case QueryKind::FindNode:
    if (auto valid = require_method("find_node"); !valid)
      return valid;
    if (!query.target)
      return std::unexpected(malformed("find_node requires a target"));
    return {};
  case QueryKind::GetPeers:
    if (auto valid = require_method("get_peers"); !valid)
      return valid;
    if (!query.info_hash)
      return std::unexpected(malformed("get_peers requires an info_hash"));
    return {};
  case QueryKind::AnnouncePeer:
    if (auto valid = require_method("announce_peer"); !valid)
      return valid;
    if (!query.info_hash || !query.port || query.token.empty())
      return std::unexpected(
          malformed("announce_peer requires info_hash, port, and token"));
    return {};
  case QueryKind::Unknown:
    return {};
  }
  std::unreachable();
}

core::Result<Query> decode_query(const bencode::Value::Dictionary &root,
                                 core::ByteBuffer transaction_id) {
  const auto *method_value = find(root, "q");
  const auto *arguments_value = find(root, "a");
  if (!method_value || !method_value->string() ||
      method_value->string()->empty() || !arguments_value ||
      !arguments_value->dictionary())
    return std::unexpected(malformed("Invalid KRPC query envelope"));
  const auto &arguments = *arguments_value->dictionary();
  auto sender = id<NodeId>(find(arguments, "id"), "KRPC node id");
  if (!sender)
    return std::unexpected(sender.error());

  Query result{.transaction = std::move(transaction_id),
               .method = text(*method_value->string()),
               .sender = *sender};
  if (result.method == "ping") {
    result.kind = QueryKind::Ping;
  } else if (result.method == "find_node") {
    result.kind = QueryKind::FindNode;
    auto target = id<NodeId>(find(arguments, "target"), "find_node target");
    if (!target)
      return std::unexpected(target.error());
    result.target = *target;
  } else if (result.method == "get_peers" ||
             result.method == "announce_peer") {
    result.kind = result.method == "get_peers" ? QueryKind::GetPeers
                                                : QueryKind::AnnouncePeer;
    auto hash =
        id<core::InfoHash>(find(arguments, "info_hash"), "query info_hash");
    if (!hash)
      return std::unexpected(hash.error());
    result.info_hash = *hash;
    if (result.kind == QueryKind::AnnouncePeer) {
      const auto *port = find(arguments, "port");
      const auto *token = find(arguments, "token");
      if (!port || !port->integer() || *port->integer() < 1 ||
          *port->integer() > 65'535 || !token || !token->string() ||
          token->string()->empty())
        return std::unexpected(malformed("Invalid announce_peer arguments"));
      result.port = static_cast<std::uint16_t>(*port->integer());
      result.token = *token->string();
      if (const auto *implied = find(arguments, "implied_port")) {
        if (!implied->integer() || *implied->integer() < 0 ||
            *implied->integer() > 1)
          return std::unexpected(malformed("Invalid implied_port value"));
        result.implied_port = *implied->integer() == 1;
      }
    }
  }
  return result;
}

bencode::Value::Dictionary envelope(core::ByteBuffer transaction_id,
                                     std::string_view type) {
  return {{"t", bencode::Value{std::move(transaction_id)}},
          {"y", bencode::Value{bytes(type)}}};
}

core::Result<std::optional<runtime::DatagramEndpoint>>
observed_endpoint(const bencode::Value::Dictionary &root) {
  const auto *value = find(root, "ip");
  if (!value)
    return std::optional<runtime::DatagramEndpoint>{};
  if (!value->string())
    return std::unexpected(malformed("KRPC ip field must be a byte string"));
  auto endpoint = decode_compact_endpoint(*value->string());
  if (!endpoint)
    return std::unexpected(endpoint.error());
  return std::optional{*endpoint};
}

} // namespace

core::Result<runtime::DatagramEndpoint>
decode_compact_endpoint(core::ByteView encoded) {
  if (encoded.size() != 6 && encoded.size() != 18)
    return std::unexpected(
        malformed("Compact endpoint must contain 6 or 18 bytes"));
  const auto address_size = encoded.size() - 2;
  runtime::DatagramEndpoint result;
  result.address.family = address_size == 4 ? runtime::AddressFamily::IPv4
                                            : runtime::AddressFamily::IPv6;
  for (std::size_t index = 0; index < address_size; ++index)
    result.address.bytes[index] =
        std::to_integer<std::uint8_t>(encoded[index]);
  result.port = static_cast<std::uint16_t>(
      (std::to_integer<std::uint16_t>(encoded[address_size]) << 8) |
      std::to_integer<std::uint16_t>(encoded[address_size + 1]));
  if (result.port == 0)
    return std::unexpected(malformed("Compact endpoint port must be nonzero"));
  return result;
}

core::Result<core::ByteBuffer>
encode_compact_endpoint(runtime::DatagramEndpoint endpoint) {
  if (endpoint.port == 0)
    return std::unexpected(malformed("Compact endpoint port must be nonzero"));
  const std::size_t address_size =
      endpoint.address.family == runtime::AddressFamily::IPv4 ? 4 : 16;
  core::ByteBuffer result;
  result.reserve(address_size + 2);
  for (std::size_t index = 0; index < address_size; ++index)
    result.push_back(static_cast<std::byte>(endpoint.address.bytes[index]));
  result.push_back(static_cast<std::byte>(endpoint.port >> 8));
  result.push_back(static_cast<std::byte>(endpoint.port & 0xff));
  return result;
}

core::Result<Message> decode(core::ByteView packet) {
  auto parsed = bencode::parse(packet);
  if (!parsed)
    return std::unexpected(parsed.error());
  const auto *root = parsed->dictionary();
  if (!root)
    return std::unexpected(malformed("KRPC message must be a dictionary"));
  auto transaction_id = transaction(*root);
  if (!transaction_id)
    return std::unexpected(transaction_id.error());
  const auto *type = find(*root, "y");
  if (!type || !type->string())
    return std::unexpected(malformed("KRPC message has no type"));
  const auto type_text = text(*type->string());
  auto observed = observed_endpoint(*root);
  if (!observed)
    return std::unexpected(observed.error());
  if (type_text == "q") {
    auto query = decode_query(*root, std::move(*transaction_id));
    if (!query)
      return std::unexpected(query.error());
    return Message{std::move(*query)};
  }
  if (type_text == "r") {
    const auto *values = find(*root, "r");
    if (!values || !values->dictionary())
      return std::unexpected(malformed("KRPC response has no value dictionary"));
    auto sender = id<NodeId>(find(*values->dictionary(), "id"),
                             "KRPC response node id");
    if (!sender)
      return std::unexpected(sender.error());
    return Message{Response{.transaction = std::move(*transaction_id),
                            .sender = *sender,
                            .values = *values->dictionary(),
                            .observed_endpoint = *observed}};
  }
  if (type_text == "e") {
    const auto *values = find(*root, "e");
    if (!values || !values->list() || values->list()->size() != 2 ||
        !(*values->list())[0].integer() || !(*values->list())[1].string())
      return std::unexpected(malformed("Invalid KRPC error payload"));
    return Message{Error{.transaction = std::move(*transaction_id),
                         .code = *(*values->list())[0].integer(),
                         .message = text(*(*values->list())[1].string()),
                         .observed_endpoint = *observed}};
  }
  return std::unexpected(malformed("Unknown KRPC message type"));
}

core::Result<core::ByteBuffer> encode(const Message &message) {
  bencode::Value::Dictionary root;
  if (const auto *query = std::get_if<Query>(&message)) {
    if (auto valid = validate_query(*query); !valid)
      return std::unexpected(valid.error());
    root = envelope(query->transaction, "q");
    root.emplace("q", bencode::Value{bytes(query->method)});
    bencode::Value::Dictionary arguments{
        {"id", bencode::Value{id_bytes(query->sender)}}};
    if (query->target)
      arguments.emplace("target", bencode::Value{id_bytes(*query->target)});
    if (query->info_hash)
      arguments.emplace("info_hash",
                        bencode::Value{id_bytes(*query->info_hash)});
    if (query->port)
      arguments.emplace("port",
                        bencode::Value{static_cast<std::int64_t>(*query->port)});
    if (query->kind == QueryKind::AnnouncePeer) {
      arguments.emplace("token", bencode::Value{query->token});
      if (query->implied_port)
        arguments.emplace("implied_port", bencode::Value{std::int64_t{1}});
    }
    root.emplace("a", bencode::Value{std::move(arguments)});
  } else if (const auto *response = std::get_if<Response>(&message)) {
    if (auto valid = validate_transaction(response->transaction); !valid)
      return std::unexpected(valid.error());
    root = envelope(response->transaction, "r");
    auto values = response->values;
    values["id"] = bencode::Value{id_bytes(response->sender)};
    root.emplace("r", bencode::Value{std::move(values)});
    if (response->observed_endpoint) {
      auto compact = encode_compact_endpoint(*response->observed_endpoint);
      if (!compact)
        return std::unexpected(compact.error());
      root.emplace("ip", bencode::Value{std::move(*compact)});
    }
  } else {
    const auto &error = std::get<Error>(message);
    if (auto valid = validate_transaction(error.transaction); !valid)
      return std::unexpected(valid.error());
    root = envelope(error.transaction, "e");
    root.emplace(
        "e", bencode::Value{bencode::Value::List{
                 bencode::Value{error.code}, bencode::Value{bytes(error.message)}}});
    if (error.observed_endpoint) {
      auto compact = encode_compact_endpoint(*error.observed_endpoint);
      if (!compact)
        return std::unexpected(compact.error());
      root.emplace("ip", bencode::Value{std::move(*compact)});
    }
  }
  return bencode::encode(bencode::Value{std::move(root)});
}

} // namespace sakuin::dht::krpc

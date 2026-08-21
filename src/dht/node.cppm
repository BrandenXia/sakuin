export module sakuin.dht.node;

import std;

import sakuin.core.bytes;
import sakuin.core.ids;
import sakuin.core.result;
import sakuin.core.time;
import sakuin.dht.bencode;
import sakuin.dht.identity;
import sakuin.dht.krpc;
import sakuin.dht.metadata_candidate;
import sakuin.dht.routing;
import sakuin.dht.token;
import sakuin.model.observation;
import sakuin.runtime.datagram;
import sakuin.runtime.stream;
import sakuin.runtime.traffic;

export namespace sakuin::dht {

namespace traffic_class {
inline constexpr runtime::TrafficClassId inbound = 1;
inline constexpr runtime::TrafficClassId protocol_response = 2;
inline constexpr runtime::TrafficClassId routing_maintenance = 3;
inline constexpr runtime::TrafficClassId discovery = 4;
} // namespace traffic_class

struct DatagramSend {
  runtime::DatagramEndpoint destination;
  core::ByteBuffer payload;
  runtime::TrafficClassId traffic_class{traffic_class::protocol_response};
  std::optional<core::ByteBuffer> query_transaction;
};

struct QueryCompletion {
  core::ByteBuffer transaction;
  krpc::QueryKind kind{krpc::QueryKind::Unknown};
  runtime::DatagramEndpoint remote;
  std::optional<krpc::Error> protocol_error;
};

struct QueryTimeout {
  core::ByteBuffer transaction;
  krpc::QueryKind kind{krpc::QueryKind::Unknown};
  runtime::DatagramEndpoint remote;
};

struct DhtNodeOptions {
  core::Duration query_timeout{std::chrono::seconds{15}};
  std::optional<runtime::AddressFamily> address_family;
};

struct DhtActions {
  std::vector<DatagramSend> sends;
  std::optional<model::ObservationRecord> observation;
  std::optional<PeerMetadataCandidate> metadata_candidate;
  std::vector<NodeContact> probes_required;
  std::optional<QueryCompletion> query_completion;
  std::optional<ObservedAddressReport> observed_address;
};

class DhtNode {
public:
  DhtNode(krpc::NodeId local_id, AnnounceTokenProvider &tokens,
          DhtNodeOptions options = {})
      : routing_(local_id), tokens_(&tokens), options_(options) {}

  DhtNode(const DhtNode &) = delete;
  DhtNode &operator=(const DhtNode &) = delete;

  // Pure protocol/runtime boundary: callers decide where sends, persistence,
  // and liveness probes execute. No Asio completion token or awaitable leaks
  // into node behavior, and a future stdexec orchestration layer can consume
  // these actions directly.
  core::Result<DhtActions> handle(runtime::Datagram datagram,
                                  core::Timestamp received_at);

  core::Result<DatagramSend> ping(runtime::DatagramEndpoint remote,
                                  core::Timestamp sent_at);
  core::Result<DatagramSend> find_node(runtime::DatagramEndpoint remote,
                                       krpc::NodeId target,
                                       core::Timestamp sent_at);
  core::Result<DatagramSend> get_peers(runtime::DatagramEndpoint remote,
                                       core::InfoHash info_hash,
                                       core::Timestamp sent_at);

  std::vector<QueryTimeout> expire_queries(core::Timestamp now);
  bool cancel_query(core::ByteView transaction,
                    const runtime::DatagramEndpoint &remote);
  std::size_t outstanding_queries() const noexcept;

  RoutingTable &routing_table() noexcept { return routing_; }
  const RoutingTable &routing_table() const noexcept { return routing_; }

private:
  std::vector<NodeContact> closest_for(const krpc::NodeId &target,
                                       runtime::AddressFamily family) const;
  core::Result<DatagramSend>
  response(runtime::DatagramEndpoint destination, core::ByteBuffer transaction,
           bencode::Value::Dictionary values = {}) const;
  core::Result<DatagramSend> error(runtime::DatagramEndpoint destination,
                                   core::ByteBuffer transaction,
                                   std::int64_t code,
                                   std::string message) const;
  core::Result<DatagramSend> begin_query(runtime::DatagramEndpoint remote,
                                         krpc::Query query,
                                         core::Timestamp sent_at);

  struct PendingQuery {
    core::ByteBuffer transaction;
    krpc::QueryKind kind{krpc::QueryKind::Unknown};
    runtime::DatagramEndpoint remote;
    core::Timestamp deadline;
  };

  std::optional<PendingQuery>
  take_pending(core::ByteView transaction,
               const runtime::DatagramEndpoint &remote);

  RoutingTable routing_;
  AnnounceTokenProvider *tokens_;
  DhtNodeOptions options_;
  std::atomic<std::uint64_t> next_transaction_{};
  mutable std::mutex pending_mutex_;
  std::unordered_map<std::string, PendingQuery> pending_;
};

} // namespace sakuin::dht

namespace sakuin::dht {
namespace {

std::string transaction_key(core::ByteView transaction) {
  return {reinterpret_cast<const char *>(transaction.data()),
          transaction.size()};
}

krpc::NodeId node_id(const core::InfoHash &hash) {
  krpc::NodeId result;
  result.bytes = hash.bytes;
  return result;
}

} // namespace

core::Result<DatagramSend>
DhtNode::begin_query(runtime::DatagramEndpoint remote, krpc::Query query,
                     core::Timestamp sent_at) {
  if (options_.address_family &&
      remote.address.family != *options_.address_family)
    return std::unexpected(core::Error{
        core::ErrorCode::InvalidArgument,
        "DHT query endpoint does not match this node's address family"});
  if (options_.query_timeout <= core::Duration::zero())
    return std::unexpected(core::Error{core::ErrorCode::InvalidArgument,
                                       "DHT query timeout must be positive"});
  const auto sequence =
      next_transaction_.fetch_add(1, std::memory_order_relaxed);
  query.transaction.resize(sizeof(sequence));
  for (std::size_t index = 0; index < sizeof(sequence); ++index) {
    const auto shift =
        static_cast<unsigned>((sizeof(sequence) - index - 1) * 8);
    query.transaction[index] =
        static_cast<std::byte>((sequence >> shift) & 0xff);
  }
  query.sender = routing_.local_id();
  auto packet = krpc::encode(query);
  if (!packet)
    return std::unexpected(packet.error());

  PendingQuery pending{
      .transaction = query.transaction,
      .kind = query.kind,
      .remote = remote,
      .deadline =
          sent_at + std::chrono::duration_cast<core::Timestamp::duration>(
                        options_.query_timeout)};
  {
    std::scoped_lock lock{pending_mutex_};
    const auto [_, inserted] = pending_.emplace(
        transaction_key(query.transaction), std::move(pending));
    if (!inserted)
      return std::unexpected(core::Error{
          core::ErrorCode::Conflict, "DHT transaction id is already in use"});
  }
  const auto traffic = query.kind == krpc::QueryKind::Ping
                           ? traffic_class::routing_maintenance
                           : traffic_class::discovery;
  return DatagramSend{.destination = remote,
                      .payload = std::move(*packet),
                      .traffic_class = traffic,
                      .query_transaction = query.transaction};
}

core::Result<DatagramSend> DhtNode::ping(runtime::DatagramEndpoint remote,
                                         core::Timestamp sent_at) {
  return begin_query(
      remote, krpc::Query{.kind = krpc::QueryKind::Ping, .method = "ping"},
      sent_at);
}

core::Result<DatagramSend> DhtNode::find_node(runtime::DatagramEndpoint remote,
                                              krpc::NodeId target,
                                              core::Timestamp sent_at) {
  return begin_query(remote,
                     krpc::Query{.kind = krpc::QueryKind::FindNode,
                                 .method = "find_node",
                                 .target = target},
                     sent_at);
}

core::Result<DatagramSend> DhtNode::get_peers(runtime::DatagramEndpoint remote,
                                              core::InfoHash info_hash,
                                              core::Timestamp sent_at) {
  return begin_query(remote,
                     krpc::Query{.kind = krpc::QueryKind::GetPeers,
                                 .method = "get_peers",
                                 .info_hash = info_hash},
                     sent_at);
}

std::optional<DhtNode::PendingQuery>
DhtNode::take_pending(core::ByteView transaction,
                      const runtime::DatagramEndpoint &remote) {
  std::scoped_lock lock{pending_mutex_};
  const auto found = pending_.find(transaction_key(transaction));
  if (found == pending_.end() || found->second.remote != remote)
    return std::nullopt;
  auto result = std::move(found->second);
  pending_.erase(found);
  return result;
}

std::vector<QueryTimeout> DhtNode::expire_queries(core::Timestamp now) {
  std::vector<QueryTimeout> result;
  std::scoped_lock lock{pending_mutex_};
  for (auto current = pending_.begin(); current != pending_.end();) {
    if (current->second.deadline > now) {
      ++current;
      continue;
    }
    result.push_back(QueryTimeout{.transaction = current->second.transaction,
                                  .kind = current->second.kind,
                                  .remote = current->second.remote});
    current = pending_.erase(current);
  }
  return result;
}

bool DhtNode::cancel_query(core::ByteView transaction,
                           const runtime::DatagramEndpoint &remote) {
  return take_pending(transaction, remote).has_value();
}

std::size_t DhtNode::outstanding_queries() const noexcept {
  std::scoped_lock lock{pending_mutex_};
  return pending_.size();
}

std::vector<NodeContact>
DhtNode::closest_for(const krpc::NodeId &target,
                     runtime::AddressFamily family) const {
  auto contacts = routing_.closest(target, routing_.size());
  std::erase_if(contacts, [family](const auto &contact) {
    return contact.endpoint.address.family != family;
  });
  if (contacts.size() > k_bucket_size)
    contacts.resize(k_bucket_size);
  return contacts;
}

core::Result<DatagramSend>
DhtNode::response(runtime::DatagramEndpoint destination,
                  core::ByteBuffer transaction,
                  bencode::Value::Dictionary values) const {
  auto packet =
      krpc::encode(krpc::Response{.transaction = std::move(transaction),
                                  .sender = routing_.local_id(),
                                  .values = std::move(values),
                                  .observed_endpoint = destination});
  if (!packet)
    return std::unexpected(packet.error());
  return DatagramSend{.destination = destination,
                      .payload = std::move(*packet)};
}

core::Result<DatagramSend> DhtNode::error(runtime::DatagramEndpoint destination,
                                          core::ByteBuffer transaction,
                                          std::int64_t code,
                                          std::string message) const {
  auto packet = krpc::encode(krpc::Error{.transaction = std::move(transaction),
                                         .code = code,
                                         .message = std::move(message),
                                         .observed_endpoint = destination});
  if (!packet)
    return std::unexpected(packet.error());
  return DatagramSend{.destination = destination,
                      .payload = std::move(*packet)};
}

core::Result<DhtActions> DhtNode::handle(runtime::Datagram datagram,
                                         core::Timestamp received_at) {
  if (options_.address_family &&
      datagram.source.address.family != *options_.address_family)
    return std::unexpected(
        core::Error{core::ErrorCode::InvalidArgument,
                    "DHT datagram does not match this node's address family"});
  auto decoded = krpc::decode(datagram.payload);
  if (!decoded)
    return std::unexpected(decoded.error());

  DhtActions actions;
  const auto *query = std::get_if<krpc::Query>(&*decoded);
  if (const auto *response_message = std::get_if<krpc::Response>(&*decoded)) {
    auto pending = take_pending(response_message->transaction, datagram.source);
    if (!pending)
      return actions;
    if (response_message->observed_endpoint)
      actions.observed_address = ObservedAddressReport{
          .reporter = datagram.source,
          .observed = *response_message->observed_endpoint};
    auto update = routing_.observe(NodeContact{.id = response_message->sender,
                                               .endpoint = datagram.source,
                                               .last_seen = received_at});
    if (update.least_recently_seen)
      actions.probes_required.push_back(*update.least_recently_seen);

    for (const auto [name, family] :
         {std::pair{std::string_view{"nodes"}, runtime::AddressFamily::IPv4},
          std::pair{std::string_view{"nodes6"},
                    runtime::AddressFamily::IPv6}}) {
      const auto found = response_message->values.find(name);
      if (found == response_message->values.end())
        continue;
      if (!found->second.string())
        return std::unexpected(core::Error{
            core::ErrorCode::InvalidArgument,
            std::string{name} + " KRPC response value must be a byte string"});
      auto contacts =
          decode_compact_nodes(*found->second.string(), family, received_at);
      if (!contacts)
        return std::unexpected(contacts.error());
      for (auto &contact : *contacts) {
        auto contact_update = routing_.observe(std::move(contact));
        if (contact_update.least_recently_seen)
          actions.probes_required.push_back(
              *contact_update.least_recently_seen);
      }
    }
    actions.query_completion =
        QueryCompletion{.transaction = std::move(pending->transaction),
                        .kind = pending->kind,
                        .remote = pending->remote};
    return actions;
  }
  if (const auto *error_message = std::get_if<krpc::Error>(&*decoded)) {
    auto pending = take_pending(error_message->transaction, datagram.source);
    if (!pending)
      return actions;
    if (error_message->observed_endpoint)
      actions.observed_address =
          ObservedAddressReport{.reporter = datagram.source,
                                .observed = *error_message->observed_endpoint};
    actions.query_completion =
        QueryCompletion{.transaction = std::move(pending->transaction),
                        .kind = pending->kind,
                        .remote = pending->remote,
                        .protocol_error = *error_message};
    return actions;
  }

  auto routing_update =
      routing_.observe(NodeContact{.id = query->sender,
                                   .endpoint = datagram.source,
                                   .last_seen = received_at});
  if (routing_update.least_recently_seen)
    actions.probes_required.push_back(*routing_update.least_recently_seen);
  if (query->info_hash)
    actions.observation = model::ObservationRecord{
        .info_hash = *query->info_hash, .observed_at = received_at};

  if (query->kind == krpc::QueryKind::Ping) {
    auto send = response(datagram.source, query->transaction);
    if (!send)
      return std::unexpected(send.error());
    actions.sends.push_back(std::move(*send));
    return actions;
  }

  if (query->kind == krpc::QueryKind::FindNode ||
      query->kind == krpc::QueryKind::GetPeers) {
    const auto target =
        query->target ? *query->target : node_id(*query->info_hash);
    auto contacts = closest_for(target, datagram.source.address.family);
    auto compact =
        encode_compact_nodes(contacts, datagram.source.address.family);
    if (!compact)
      return std::unexpected(compact.error());
    const auto nodes_key =
        datagram.source.address.family == runtime::AddressFamily::IPv4
            ? "nodes"
            : "nodes6";
    bencode::Value::Dictionary values{
        {nodes_key, bencode::Value{std::move(*compact)}}};
    if (query->kind == krpc::QueryKind::GetPeers) {
      auto token = tokens_->issue(datagram.source.address, received_at);
      if (!token)
        return std::unexpected(token.error());
      values.emplace("token", bencode::Value{std::move(*token)});
    }
    auto send =
        response(datagram.source, query->transaction, std::move(values));
    if (!send)
      return std::unexpected(send.error());
    actions.sends.push_back(std::move(*send));
    return actions;
  }

  if (query->kind == krpc::QueryKind::AnnouncePeer) {
    auto valid =
        tokens_->validate(query->token, datagram.source.address, received_at);
    if (!valid)
      return std::unexpected(valid.error());
    auto send = *valid ? response(datagram.source, query->transaction)
                       : error(datagram.source, query->transaction, 203,
                               "Invalid announce token");
    if (!send)
      return std::unexpected(send.error());
    if (*valid)
      actions.metadata_candidate = PeerMetadataCandidate{
          .info_hash = *query->info_hash,
          .peer = {.address = datagram.source.address,
                   .port = query->implied_port ? datagram.source.port
                                               : *query->port},
          .observed_at = received_at};
    actions.sends.push_back(std::move(*send));
    return actions;
  }

  auto send = error(datagram.source, query->transaction, 204, "Method Unknown");
  if (!send)
    return std::unexpected(send.error());
  actions.sends.push_back(std::move(*send));
  return actions;
}

} // namespace sakuin::dht

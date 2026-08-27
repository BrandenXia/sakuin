import std;

import sakuin.core;
import sakuin.dht;
import sakuin.runtime;
import sakuin.runtime.asio;

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

class EventCollector final : public sakuin::dht::DhtRuntimeEvents {
public:
  void on_actions(sakuin::dht::DhtActions value) override {
    {
      std::scoped_lock lock{mutex_};
      actions_ = std::move(value);
    }
    condition_.notify_all();
  }

  void on_error(sakuin::core::Error value) override {
    {
      std::scoped_lock lock{mutex_};
      error_ = std::move(value);
    }
    condition_.notify_all();
  }

  bool wait() {
    std::unique_lock lock{mutex_};
    return condition_.wait_for(
               lock, std::chrono::seconds{3},
               [&] { return actions_.has_value() || error_.has_value(); }) &&
           actions_.has_value() && !error_;
  }

  std::optional<sakuin::dht::DhtActions> actions() {
    std::scoped_lock lock{mutex_};
    return actions_;
  }

  void reset() {
    std::scoped_lock lock{mutex_};
    actions_.reset();
    error_.reset();
  }

private:
  std::mutex mutex_;
  std::condition_variable condition_;
  std::optional<sakuin::dht::DhtActions> actions_;
  std::optional<sakuin::core::Error> error_;
};

class DatagramCollector final : public sakuin::runtime::DatagramReceiver {
public:
  void on_datagram(sakuin::runtime::Datagram value) override {
    {
      std::scoped_lock lock{mutex_};
      datagram_ = std::move(value);
    }
    condition_.notify_all();
  }

  void on_transport_error(sakuin::core::Error value) override {
    {
      std::scoped_lock lock{mutex_};
      error_ = std::move(value);
    }
    condition_.notify_all();
  }

  std::optional<sakuin::runtime::Datagram> wait() {
    std::unique_lock lock{mutex_};
    condition_.wait_for(lock, std::chrono::seconds{3}, [&] {
      return datagram_.has_value() || error_.has_value();
    });
    return datagram_;
  }

private:
  std::mutex mutex_;
  std::condition_variable condition_;
  std::optional<sakuin::runtime::Datagram> datagram_;
  std::optional<sakuin::core::Error> error_;
};

class CountingGovernor final : public sakuin::runtime::TrafficGovernor {
public:
  sakuin::runtime::TrafficDecision
  admit(sakuin::runtime::TrafficRequest request,
        sakuin::core::Timestamp) override {
    if (request.direction == sakuin::runtime::TrafficDirection::Inbound) {
      ++inbound;
      inbound_class = request.traffic_class;
    } else {
      ++outbound;
      outbound_class = request.traffic_class;
    }
    return {};
  }

  std::atomic<int> inbound{};
  std::atomic<int> outbound{};
  std::atomic<sakuin::runtime::TrafficClassId> inbound_class{};
  std::atomic<sakuin::runtime::TrafficClassId> outbound_class{};
};

} // namespace

int main() {
  using namespace sakuin;

  dht::AnnounceTokenSecret secret;
  std::ranges::iota(secret.bytes, std::uint8_t{1});
  auto tokens = dht::RotatingAnnounceTokenProvider::create(secret);
  if (!tokens)
    return 1;
  dht::DhtNode node_engine{node(100), *tokens};
  EventCollector server_events;
  DatagramCollector client_events;
  CountingGovernor traffic;
  const auto fixed_time = core::Timestamp{core::Timestamp::duration{123}};

  runtime::DatagramTransportOptions options{
      .bind_to = {.address = runtime::IpAddress::loopback_v4(), .port = 0}};
  auto server = runtime::AsioDatagramTransport::create(options);
  auto client = runtime::AsioDatagramTransport::create(options);
  if (!server || !client)
    return 2;
  dht::DhtRuntimeDriver driver{node_engine, **server, server_events, traffic,
                               [fixed_time] { return fixed_time; }};
  if (!driver.start() || !(*client)->start(client_events))
    return 3;

  const auto wanted = hash(20);
  const dht::krpc::Query query{.transaction = bytes("gp"),
                               .kind = dht::krpc::QueryKind::GetPeers,
                               .method = "get_peers",
                               .sender = node(1),
                               .info_hash = wanted};
  auto packet = dht::krpc::encode(query);
  if (!packet ||
      !(*client)->send((*server)->local_endpoint(), std::move(*packet)))
    return 4;
  auto response_datagram = client_events.wait();
  if (!response_datagram || !server_events.wait())
    return 5;

  auto response = dht::krpc::decode(response_datagram->payload);
  const auto *typed_response =
      response ? std::get_if<dht::krpc::Response>(&*response) : nullptr;
  auto actions = server_events.actions();
  if (!typed_response || typed_response->transaction != query.transaction ||
      !typed_response->values.contains("token") || !actions ||
      !actions->observation || actions->observation->info_hash != wanted ||
      actions->observation->observed_at != fixed_time ||
      !actions->sends.empty() || traffic.inbound != 1 ||
      traffic.outbound != 1 ||
      traffic.inbound_class != dht::traffic_class::inbound ||
      traffic.outbound_class != dht::traffic_class::protocol_response)
    return 6;

  server_events.reset();
  if (!(*client)->send((*server)->local_endpoint(),
                       bytes("d1:ri0e1:t2:aa1:y1:re")) ||
      !server_events.wait())
    return 8;
  auto malformed_actions = server_events.actions();
  if (!malformed_actions || !malformed_actions->inbound_message ||
      malformed_actions->inbound_message->type !=
          dht::InboundMessageType::ProtocolError ||
      traffic.inbound != 2 || traffic.outbound != 1)
    return 9;

  (*client)->stop();
  driver.stop();
  if (driver.running())
    return 7;
  return 0;
}

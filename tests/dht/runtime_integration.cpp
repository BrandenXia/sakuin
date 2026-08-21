import std;

import sakuin.config;
import sakuin.core;
import sakuin.dht;
import sakuin.integration.dht_runtime;
import sakuin.integration.metadata_config;
import sakuin.model.observation;
import sakuin.model.torrent;
import sakuin.runtime;

namespace {

class ObservationSink final : public sakuin::dht::ObservationSink {
public:
  sakuin::core::Result<void>
  observe(const sakuin::model::ObservationRecord &observation) override {
    if (fail_once) {
      fail_once = false;
      return std::unexpected(
          sakuin::core::Error{sakuin::core::ErrorCode::StorageUnavailable,
                              "Synthetic observation storage outage"});
    }
    records.push_back(observation);
    return {};
  }

  bool fail_once{true};
  std::vector<sakuin::model::ObservationRecord> records;
};

class PassiveTransport final : public sakuin::runtime::StreamTransport {
public:
  explicit PassiveTransport(sakuin::runtime::StreamEndpoint remote)
      : remote_(remote) {}

  sakuin::core::Result<void> start(sakuin::runtime::StreamReceiver &) override {
    started = true;
    return {};
  }
  sakuin::core::Result<void> send(sakuin::core::ByteBuffer) override {
    return {};
  }
  sakuin::runtime::StreamEndpoint remote_endpoint() const noexcept override {
    return remote_;
  }
  void stop() noexcept override { stopped = true; }

  bool started{};
  bool stopped{};

private:
  sakuin::runtime::StreamEndpoint remote_;
};

class TransportFactory final : public sakuin::runtime::StreamTransportFactory {
public:
  sakuin::core::Result<std::shared_ptr<sakuin::runtime::StreamTransport>>
  create(sakuin::runtime::StreamTransportOptions options) override {
    auto transport = std::make_shared<PassiveTransport>(options.remote);
    last = transport;
    return std::shared_ptr<sakuin::runtime::StreamTransport>{
        std::move(transport)};
  }

  std::shared_ptr<PassiveTransport> last;
};

class MetadataSink final : public sakuin::dht::MetadataFetchObserver {
public:
  sakuin::core::Result<void>
  on_metadata_fetched(sakuin::model::TorrentRecord) override {
    return {};
  }
  void on_metadata_fetch_failed(sakuin::core::Error error) override {
    errors.push_back(std::move(error));
  }
  std::vector<sakuin::core::Error> errors;
};

sakuin::core::InfoHash hash(std::uint8_t seed) {
  sakuin::core::InfoHash result;
  std::ranges::iota(result.bytes, seed);
  return result;
}

} // namespace

int main() {
  using namespace sakuin;

  auto configured = config::defaults().network.dht.metadata;
  configured.enabled = false;
  configured.maximum_in_flight = 7;
  configured.maximum_queued = 99;
  configured.maximum_attempts_per_peer = 5;
  configured.maximum_metadata_bytes = 2U * 1024U * 1024U;
  configured.maximum_outstanding_requests = 6;
  configured.maximum_queued_write_bytes = 12'345;
  configured.storage_conflict_attempts = 4;
  const auto mapped = integration::metadata_runtime_config(configured);
  if (mapped.enabled || mapped.controller.queue.maximum_in_flight != 7 ||
      mapped.controller.queue.maximum_queued != 99 ||
      mapped.controller.queue.maximum_attempts_per_peer != 5 ||
      mapped.controller.fetch.exchange.maximum_metadata_bytes !=
          2U * 1024U * 1024U ||
      mapped.controller.fetch.metainfo.maximum_metadata_bytes !=
          2U * 1024U * 1024U ||
      mapped.controller.fetch.exchange.maximum_outstanding_requests != 6 ||
      mapped.controller.transport.maximum_queued_write_bytes != 12'345 ||
      mapped.storage_conflict_attempts != 4)
    return 1;

  TransportFactory factory;
  MetadataSink metadata_sink;
  dht::PeerId peer_id{};
  std::ranges::iota(peer_id, std::uint8_t{1});
  auto controller = dht::MetadataAcquisitionController::create(
      peer_id, factory, metadata_sink, mapped.controller);
  if (!controller)
    return 2;

  ObservationSink observations;
  auto pump = integration::DhtRuntimeActionPump::create(observations,
                                                        controller->get());
  auto invalid_pump = integration::DhtRuntimeActionPump::create(
      observations, nullptr, {.maximum_pending_actions = 0});
  if (!pump || invalid_pump ||
      invalid_pump.error().code != core::ErrorCode::InvalidArgument)
    return 3;
  auto bounded_pump = integration::DhtRuntimeActionPump::create(
      observations, nullptr,
      {.maximum_pending_actions = 1, .maximum_pending_errors = 1});
  if (!bounded_pump)
    return 4;
  (*bounded_pump)->on_actions({});
  (*bounded_pump)->on_actions({});
  (*bounded_pump)->on_error({core::ErrorCode::IoError, "First bounded error"});
  (*bounded_pump)->on_error({core::ErrorCode::IoError, "Second bounded error"});
  auto bounded = (*bounded_pump)->poll({});
  if (bounded.errors.size() != 3 || (*bounded_pump)->pending() != 0)
    return 5;
  runtime::IpAddress peer_address = runtime::IpAddress::loopback_v4();
  peer_address.bytes[3] = 9;
  const auto wanted = hash(10);
  const auto observed_at = core::Timestamp{std::chrono::seconds{50}};
  dht::DhtActions actions{
      .observation = model::ObservationRecord{.info_hash = wanted,
                                              .observed_at = observed_at},
      .metadata_candidate =
          dht::PeerMetadataCandidate{
              .info_hash = wanted,
              .peer = {.address = peer_address, .port = 51'413},
              .observed_at = observed_at},
      .observed_address = dht::ObservedAddressReport{
          .reporter = {.address = peer_address, .port = 6'881},
          .observed = {.address = runtime::IpAddress::loopback_v4(),
                       .port = 6'881}}};
  std::thread producer{[&] {
    (*pump)->on_actions(std::move(actions));
    (*pump)->on_error(core::Error{core::ErrorCode::QuotaExceeded,
                                  "Synthetic runtime quota report"});
  }};
  producer.join();
  if ((*pump)->pending() != 1)
    return 6;

  const auto now = core::Timestamp{std::chrono::seconds{100}};
  auto first = (*pump)->poll(now);
  if (first.observations_stored != 0 ||
      first.metadata_candidates_accepted != 1 || first.actions.size() != 1 ||
      !first.actions.front().observed_address || first.errors.size() != 2 ||
      controller->get()->in_flight() != 1 || !factory.last ||
      !factory.last->started || (*pump)->pending() != 1)
    return 7;

  auto second = (*pump)->poll(now);
  if (second.observations_stored != 1 ||
      second.metadata_candidates_accepted != 0 || !second.actions.empty() ||
      !second.errors.empty() || observations.records.size() != 1 ||
      observations.records.front().info_hash != wanted ||
      (*pump)->pending() != 0)
    return 8;

  controller->get()->stop();
  if (!factory.last->stopped)
    return 9;
  return 0;
}

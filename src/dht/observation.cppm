export module sakuin.dht.observation;

import std;

import sakuin.core.bytes;
import sakuin.core.ids;
import sakuin.core.result;
import sakuin.core.time;
import sakuin.dht.krpc;
import sakuin.model.observation;

export namespace sakuin::dht {

class ObservationSink {
public:
  virtual ~ObservationSink() = default;
  virtual core::Result<void>
  observe(const model::ObservationRecord &observation) = 0;
};

core::Result<std::optional<model::ObservationRecord>>
extract_observation(core::ByteView krpc_packet, core::Timestamp observed_at);

class ObservationIngestor {
public:
  explicit ObservationIngestor(ObservationSink &sink) : sink_(&sink) {}

  core::Result<bool> ingest(core::ByteView krpc_packet,
                            core::Timestamp observed_at);

private:
  ObservationSink *sink_;
};

} // namespace sakuin::dht

namespace sakuin::dht {
namespace {

} // namespace

core::Result<std::optional<model::ObservationRecord>>
extract_observation(core::ByteView packet, core::Timestamp observed_at) {
  auto message = krpc::decode(packet);
  if (!message)
    return std::unexpected(message.error());
  const auto *query = std::get_if<krpc::Query>(&*message);
  if (!query || (query->kind != krpc::QueryKind::GetPeers &&
                 query->kind != krpc::QueryKind::AnnouncePeer))
    return std::optional<model::ObservationRecord>{};
  return std::optional<model::ObservationRecord>{
      model::ObservationRecord{.info_hash = *query->info_hash,
                               .observed_at = observed_at}};
}

core::Result<bool> ObservationIngestor::ingest(core::ByteView packet,
                                                core::Timestamp observed_at) {
  auto observation = extract_observation(packet, observed_at);
  if (!observation)
    return std::unexpected(observation.error());
  if (!*observation)
    return false;
  if (auto emitted = sink_->observe(**observation); !emitted)
    return std::unexpected(emitted.error());
  return true;
}

} // namespace sakuin::dht

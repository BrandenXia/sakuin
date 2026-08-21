import std;

import sakuin.core;
import sakuin.dht;
import sakuin.model.torrent;
import sakuin.runtime;

namespace {

void append_u32(sakuin::core::ByteBuffer &output, std::uint32_t value) {
  output.push_back(static_cast<std::byte>(value >> 24));
  output.push_back(static_cast<std::byte>(value >> 16));
  output.push_back(static_cast<std::byte>(value >> 8));
  output.push_back(static_cast<std::byte>(value));
}

sakuin::core::ByteBuffer handshake(const sakuin::core::InfoHash &hash) {
  constexpr std::string_view protocol{"BitTorrent protocol"};
  sakuin::core::ByteBuffer result{std::byte{19}};
  const auto protocol_bytes = std::as_bytes(std::span{protocol});
  result.insert(result.end(), protocol_bytes.begin(), protocol_bytes.end());
  std::array<std::byte, 8> reserved{};
  reserved[5] = std::byte{0x10};
  result.insert(result.end(), reserved.begin(), reserved.end());
  for (const auto byte : hash.bytes)
    result.push_back(static_cast<std::byte>(byte));
  result.insert(result.end(), 20, std::byte{0x77});
  return result;
}

sakuin::core::ByteBuffer
frame(std::uint8_t extension,
      sakuin::dht::bencode::Value::Dictionary dictionary,
      sakuin::core::ByteView suffix = {}) {
  auto encoded = sakuin::dht::bencode::encode(
      sakuin::dht::bencode::Value{std::move(dictionary)}, 64U * 1024U);
  sakuin::core::ByteBuffer result;
  append_u32(result,
             static_cast<std::uint32_t>(encoded->size() + suffix.size() + 2));
  result.push_back(std::byte{20});
  result.push_back(static_cast<std::byte>(extension));
  result.insert(result.end(), encoded->begin(), encoded->end());
  result.insert(result.end(), suffix.begin(), suffix.end());
  return result;
}

std::pair<sakuin::core::InfoHash, sakuin::core::ByteBuffer> metadata() {
  using namespace sakuin;
  dht::bencode::Value::Dictionary dictionary;
  dictionary.emplace("length", dht::bencode::Value{std::int64_t{3}});
  constexpr std::string_view name{"run"};
  const auto name_bytes = std::as_bytes(std::span{name});
  dictionary.emplace("name", dht::bencode::Value{core::ByteBuffer{
                                 name_bytes.begin(), name_bytes.end()}});
  dictionary.emplace("piece length", dht::bencode::Value{std::int64_t{16'384}});
  dictionary.emplace("pieces", dht::bencode::Value{core::ByteBuffer(20)});
  auto encoded = dht::bencode::encode(
      dht::bencode::Value{std::move(dictionary)}, 64U * 1024U);
  const auto digest = core::sha1(*encoded);
  return {core::InfoHash{.bytes = digest.bytes}, std::move(*encoded)};
}

sakuin::core::ByteBuffer extension_handshake(std::size_t size) {
  using namespace sakuin::dht;
  bencode::Value::Dictionary extensions;
  extensions.emplace("ut_metadata", bencode::Value{std::int64_t{3}});
  bencode::Value::Dictionary dictionary;
  dictionary.emplace("m", bencode::Value{std::move(extensions)});
  dictionary.emplace("metadata_size",
                     bencode::Value{static_cast<std::int64_t>(size)});
  return frame(0, std::move(dictionary));
}

sakuin::core::ByteBuffer data(std::size_t size,
                              sakuin::core::ByteView payload) {
  using namespace sakuin::dht;
  bencode::Value::Dictionary dictionary;
  dictionary.emplace("msg_type", bencode::Value{std::int64_t{1}});
  dictionary.emplace("piece", bencode::Value{std::int64_t{0}});
  dictionary.emplace("total_size",
                     bencode::Value{static_cast<std::int64_t>(size)});
  return frame(1, std::move(dictionary), payload);
}

class AutomaticTransport final : public sakuin::runtime::StreamTransport {
public:
  AutomaticTransport(sakuin::runtime::StreamEndpoint remote,
                     sakuin::core::InfoHash hash,
                     sakuin::core::ByteBuffer metadata)
      : remote_(remote), hash_(hash), metadata_(std::move(metadata)) {}

  sakuin::core::Result<void>
  start(sakuin::runtime::StreamReceiver &receiver) override {
    receiver_ = &receiver;
    running_ = true;
    receiver_->on_stream_connected();
    return {};
  }

  sakuin::core::Result<void> send(sakuin::core::ByteBuffer bytes) override {
    if (!running_)
      return std::unexpected(sakuin::core::Error{
          sakuin::core::ErrorCode::Conflict, "Automatic transport stopped"});
    if (state_ == 0) {
      if (bytes.size() != 68)
        return std::unexpected(sakuin::core::Error{
            sakuin::core::ErrorCode::InvalidArgument, "Expected handshake"});
      ++state_;
      receiver_->on_stream_data(handshake(hash_));
    } else if (state_ == 1) {
      ++state_;
      receiver_->on_stream_data(extension_handshake(metadata_.size()));
    } else if (state_ == 2) {
      ++state_;
      receiver_->on_stream_data(data(metadata_.size(), metadata_));
    }
    return {};
  }

  sakuin::runtime::StreamEndpoint remote_endpoint() const noexcept override {
    return remote_;
  }
  void stop() noexcept override { running_ = false; }

private:
  sakuin::runtime::StreamEndpoint remote_;
  sakuin::core::InfoHash hash_;
  sakuin::core::ByteBuffer metadata_;
  sakuin::runtime::StreamReceiver *receiver_{};
  int state_{};
  bool running_{};
};

class Factory final : public sakuin::runtime::StreamTransportFactory {
public:
  Factory(sakuin::core::InfoHash hash, sakuin::core::ByteBuffer metadata)
      : hash_(hash), metadata_(std::move(metadata)) {}

  sakuin::core::Result<std::shared_ptr<sakuin::runtime::StreamTransport>>
  create(sakuin::runtime::StreamTransportOptions options) override {
    last_remote = options.remote;
    return std::shared_ptr<sakuin::runtime::StreamTransport>{
        std::make_shared<AutomaticTransport>(options.remote, hash_, metadata_)};
  }

  std::optional<sakuin::runtime::StreamEndpoint> last_remote;

private:
  sakuin::core::InfoHash hash_;
  sakuin::core::ByteBuffer metadata_;
};

class Sink final : public sakuin::dht::MetadataFetchObserver {
public:
  sakuin::core::Result<void>
  on_metadata_fetched(sakuin::model::TorrentRecord value) override {
    if (fail_once) {
      fail_once = false;
      return std::unexpected(
          sakuin::core::Error{sakuin::core::ErrorCode::StorageUnavailable,
                              "Synthetic storage outage"});
    }
    record = std::move(value);
    return {};
  }

  void on_metadata_fetch_failed(sakuin::core::Error error) override {
    errors.push_back(std::move(error));
  }

  bool fail_once{true};
  std::optional<sakuin::model::TorrentRecord> record;
  std::vector<sakuin::core::Error> errors;
};

} // namespace

int main() {
  using namespace sakuin;
  auto [hash, info] = metadata();
  Factory factory{hash, info};
  Sink sink;
  std::size_t wakeups{};
  dht::PeerId peer_id{};
  std::ranges::iota(peer_id, std::uint8_t{1});
  auto controller = dht::MetadataAcquisitionController::create(
      peer_id, factory, sink,
      {.storage_retry_delay = std::chrono::seconds{2},
       .wake_owner = [&] { ++wakeups; }});
  if (!controller)
    return 1;
  auto address = runtime::IpAddress::loopback_v4();
  address.bytes[3] = 8;
  const dht::PeerMetadataCandidate candidate{
      .info_hash = hash,
      .peer = {.address = address, .port = 51'413},
      .observed_at = core::Timestamp{std::chrono::seconds{50}}};
  if (!(*controller)->offer(candidate).value_or(false))
    return 2;
  const auto now = core::Timestamp{std::chrono::seconds{100}};
  if (!(*controller)->poll(now) || (*controller)->in_flight() != 1 ||
      factory.last_remote != candidate.peer || wakeups == 0)
    return 3;

  auto storage_failure = (*controller)->poll(now);
  if (storage_failure ||
      storage_failure.error().code != core::ErrorCode::StorageUnavailable ||
      (*controller)->in_flight() != 0 ||
      (*controller)->pending_storage() != 1 || sink.errors.size() != 1 ||
      (*controller)->next_wakeup() != now + std::chrono::seconds{2})
    return 4;
  if (!(*controller)->poll(now) || (*controller)->pending_storage() != 1 ||
      !(*controller)->poll(now + std::chrono::seconds{2}) ||
      (*controller)->pending_storage() != 0 || (*controller)->next_wakeup() ||
      !sink.record || sink.record->info_hash != hash ||
      sink.record->name != "run")
    return 5;

  (*controller)->stop();
  auto stopped = (*controller)->offer(candidate);
  if (stopped || stopped.error().code != core::ErrorCode::Conflict)
    return 6;
  return 0;
}

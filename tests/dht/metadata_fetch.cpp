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

sakuin::core::ByteBuffer peer_handshake(const sakuin::core::InfoHash &hash) {
  constexpr std::string_view protocol{"BitTorrent protocol"};
  sakuin::core::ByteBuffer result;
  result.push_back(std::byte{19});
  const auto protocol_bytes = std::as_bytes(std::span{protocol});
  result.insert(result.end(), protocol_bytes.begin(), protocol_bytes.end());
  std::array<std::byte, 8> reserved{};
  reserved[5] = std::byte{0x10};
  result.insert(result.end(), reserved.begin(), reserved.end());
  for (const auto byte : hash.bytes)
    result.push_back(static_cast<std::byte>(byte));
  result.insert(result.end(), 20, std::byte{0x44});
  return result;
}

sakuin::core::ByteBuffer
extended_frame(std::uint8_t extension,
               sakuin::dht::bencode::Value::Dictionary dictionary,
               sakuin::core::ByteView suffix = {}) {
  auto header = sakuin::dht::bencode::encode(
      sakuin::dht::bencode::Value{std::move(dictionary)}, 64U * 1024U);
  if (!header)
    return {};
  sakuin::core::ByteBuffer result;
  append_u32(result,
             static_cast<std::uint32_t>(header->size() + suffix.size() + 2));
  result.push_back(std::byte{20});
  result.push_back(static_cast<std::byte>(extension));
  result.insert(result.end(), header->begin(), header->end());
  result.insert(result.end(), suffix.begin(), suffix.end());
  return result;
}

std::pair<sakuin::core::InfoHash, sakuin::core::ByteBuffer> metadata() {
  using namespace sakuin;
  dht::bencode::Value::Dictionary dictionary;
  dictionary.emplace("length", dht::bencode::Value{std::int64_t{9}});
  const auto name = std::string_view{"fetch.bin"};
  const auto name_bytes = std::as_bytes(std::span{name});
  dictionary.emplace("name", dht::bencode::Value{core::ByteBuffer{
                                 name_bytes.begin(), name_bytes.end()}});
  dictionary.emplace("piece length", dht::bencode::Value{std::int64_t{16'384}});
  dictionary.emplace("pieces",
                     dht::bencode::Value{core::ByteBuffer(20, std::byte{1})});
  auto encoded = dht::bencode::encode(
      dht::bencode::Value{std::move(dictionary)}, 64U * 1024U);
  if (!encoded)
    return {};
  const auto digest = core::sha1(*encoded);
  return {core::InfoHash{.bytes = digest.bytes}, std::move(*encoded)};
}

sakuin::core::ByteBuffer extension_handshake(std::size_t metadata_size) {
  using namespace sakuin::dht;
  bencode::Value::Dictionary extensions;
  extensions.emplace("ut_metadata", bencode::Value{std::int64_t{3}});
  bencode::Value::Dictionary dictionary;
  dictionary.emplace("m", bencode::Value{std::move(extensions)});
  dictionary.emplace("metadata_size",
                     bencode::Value{static_cast<std::int64_t>(metadata_size)});
  return extended_frame(0, std::move(dictionary));
}

sakuin::core::ByteBuffer metadata_data(std::size_t total,
                                       sakuin::core::ByteView payload) {
  using namespace sakuin::dht;
  bencode::Value::Dictionary dictionary;
  dictionary.emplace("msg_type", bencode::Value{std::int64_t{1}});
  dictionary.emplace("piece", bencode::Value{std::int64_t{0}});
  dictionary.emplace("total_size",
                     bencode::Value{static_cast<std::int64_t>(total)});
  return extended_frame(1, std::move(dictionary), payload);
}

class FakeTransport final : public sakuin::runtime::StreamTransport {
public:
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
          sakuin::core::ErrorCode::Conflict, "Fake transport is stopped"});
    sends.push_back(std::move(bytes));
    return {};
  }

  sakuin::runtime::StreamEndpoint remote_endpoint() const noexcept override {
    return {};
  }

  void stop() noexcept override {
    running_ = false;
    stopped = true;
  }

  void deliver(sakuin::core::ByteBuffer bytes) {
    receiver_->on_stream_data(std::move(bytes));
  }

  void close(std::optional<sakuin::core::Error> error = std::nullopt) {
    running_ = false;
    receiver_->on_stream_closed(std::move(error));
  }

  std::vector<sakuin::core::ByteBuffer> sends;
  bool stopped{};

private:
  sakuin::runtime::StreamReceiver *receiver_{};
  bool running_{};
};

class Observer final : public sakuin::dht::MetadataFetchObserver {
public:
  sakuin::core::Result<void>
  on_metadata_fetched(sakuin::model::TorrentRecord value) override {
    record = std::move(value);
    return {};
  }

  void on_metadata_fetch_failed(sakuin::core::Error value) override {
    error = std::move(value);
  }

  std::optional<sakuin::model::TorrentRecord> record;
  std::optional<sakuin::core::Error> error;
};

} // namespace

int main() {
  using namespace sakuin;
  auto [hash, info] = metadata();
  dht::PeerId peer_id{};
  std::ranges::iota(peer_id, std::uint8_t{1});
  const auto observed = core::Timestamp{std::chrono::seconds{123}};

  FakeTransport transport;
  Observer observer;
  auto session = dht::MetadataFetchSession::create(hash, peer_id, observed,
                                                   transport, observer);
  if (!session || !(*session)->start() || transport.sends.size() != 1)
    return 1;
  transport.deliver(peer_handshake(hash));
  if (transport.sends.size() != 2)
    return 2;
  transport.deliver(extension_handshake(info.size()));
  if (transport.sends.size() != 3)
    return 3;
  transport.deliver(metadata_data(info.size(), info));
  if (!(*session)->complete() || !transport.stopped || observer.error ||
      !observer.record || observer.record->info_hash != hash ||
      observer.record->name != "fetch.bin" ||
      observer.record->total_size != 9 ||
      observer.record->first_seen != observed)
    return 4;

  FakeTransport early_close_transport;
  Observer early_close_observer;
  auto early_close = dht::MetadataFetchSession::create(
      hash, peer_id, observed, early_close_transport, early_close_observer);
  if (!early_close || !(*early_close)->start())
    return 5;
  early_close_transport.close();
  if ((*early_close)->complete() || !early_close_observer.error ||
      early_close_observer.error->code != core::ErrorCode::IoError)
    return 6;

  return 0;
}

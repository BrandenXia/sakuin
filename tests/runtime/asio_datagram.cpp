import std;

import sakuin.core;
import sakuin.dht;
import sakuin.integration.dht_storage;
import sakuin.runtime;
import sakuin.runtime.asio;
import sakuin.storage;
import sakuin.storage.dataset.observations;

namespace {

struct TemporaryDirectory {
  std::filesystem::path path;
  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path, ignored);
  }
};

sakuin::core::ByteBuffer packet(std::uint8_t hash) {
  const std::string_view prefix =
      "d1:ad2:id20:abcdefghijklmnopqrst9:info_hash20:";
  const std::string_view suffix =
      "e1:q9:get_peers1:t2:aa1:y1:qe";
  const auto first = std::as_bytes(std::span{prefix});
  const auto last = std::as_bytes(std::span{suffix});
  sakuin::core::ByteBuffer result{first.begin(), first.end()};
  result.insert(result.end(), 20, static_cast<std::byte>(hash));
  result.insert(result.end(), last.begin(), last.end());
  return result;
}

class IgnoringReceiver final : public sakuin::runtime::DatagramReceiver {
public:
  void on_datagram(sakuin::runtime::Datagram) override {}
  void on_transport_error(sakuin::core::Error error) override {
    last_error = std::move(error);
  }
  std::optional<sakuin::core::Error> last_error;
};

class IngestingReceiver final : public sakuin::runtime::DatagramReceiver {
public:
  explicit IngestingReceiver(sakuin::dht::ObservationIngestor &ingestor)
      : ingestor_(&ingestor) {}

  void on_datagram(sakuin::runtime::Datagram datagram) override {
    auto result = ingestor_->ingest(datagram.payload,
                                    sakuin::core::Timestamp{
                                        sakuin::core::Timestamp::duration{123}});
    {
      std::lock_guard lock{mutex};
      completed = true;
      if (!result || !*result)
        error = result ? sakuin::core::Error{
                             sakuin::core::ErrorCode::Internal,
                             "Datagram did not produce an observation"}
                       : result.error();
    }
    condition.notify_one();
  }

  void on_transport_error(sakuin::core::Error transport_error) override {
    {
      std::lock_guard lock{mutex};
      completed = true;
      error = std::move(transport_error);
    }
    condition.notify_one();
  }

  bool wait() {
    std::unique_lock lock{mutex};
    return condition.wait_for(lock, std::chrono::seconds{3},
                              [&] { return completed; }) &&
           !error;
  }

private:
  sakuin::dht::ObservationIngestor *ingestor_;
  std::mutex mutex;
  std::condition_variable condition;
  bool completed{};
  std::optional<sakuin::core::Error> error;
};

} // namespace

int main() {
  using namespace sakuin;
  const auto nonce = std::to_string(
      std::chrono::steady_clock::now().time_since_epoch().count());
  TemporaryDirectory directory{std::filesystem::temp_directory_path() /
                               ("sakuin-asio-udp-" + nonce)};
  storage::LocalBlobStore blobs{directory.path / "blobs"};
  auto catalog =
      storage::LocalManifestCatalog::open(directory.path / "catalog", blobs);
  if (!catalog)
    return 1;
  storage::ObservationDataset dataset{blobs, **catalog};
  integration::BufferedObservationSink sink{dataset, 1};
  dht::ObservationIngestor ingestor{sink};
  IngestingReceiver server_receiver{ingestor};
  IgnoringReceiver client_receiver;

  runtime::DatagramTransportOptions options{
      .bind_to = {.address = runtime::IpAddress::loopback_v4(), .port = 0}};
  auto server = runtime::AsioDatagramTransport::create(options);
  auto client = runtime::AsioDatagramTransport::create(options);
  if (!server) {
    std::cerr << server.error().message << '\n';
    return 2;
  }
  if (!client) {
    std::cerr << client.error().message << '\n';
    return 3;
  }
  if (!(*server)->start(server_receiver))
    return 4;
  if (!(*client)->start(client_receiver))
    return 5;
  if (!(*client)->send((*server)->local_endpoint(), packet(0x5a)) ||
      !server_receiver.wait())
    return 6;
  (*client)->stop();
  (*server)->stop();

  auto snapshot = dataset.snapshot();
  auto stream = (*snapshot)->scan({});
  auto observed = (*stream)->next();
  if (!observed || !*observed)
    return 7;
  if ((*observed)->info_hash.bytes[0] != 0x5a)
    return 8;
  if ((*observed)->observed_at !=
      core::Timestamp{core::Timestamp::duration{123}})
    return 9;
  auto end = (*stream)->next();
  if (!end || end->has_value())
    return 10;
  if (client_receiver.last_error) {
    std::cerr << client_receiver.last_error->message << '\n';
    return 11;
  }
  return 0;
}

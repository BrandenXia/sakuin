import std;

import sakuin.config;
import sakuin.core;
import sakuin.dht;
import sakuin.integration.metadata_config;
import sakuin.integration.metadata_storage;
import sakuin.model.torrent;
import sakuin.runtime;
import sakuin.storage;
import sakuin.storage.dataset.torrents;

namespace {

struct TemporaryDirectory {
  std::filesystem::path path;
  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path, ignored);
  }
};

sakuin::core::InfoHash hash(std::uint8_t value) {
  sakuin::core::InfoHash result;
  result.bytes.fill(value);
  return result;
}

class TransportFactory final : public sakuin::runtime::StreamTransportFactory {
public:
  sakuin::core::Result<std::shared_ptr<sakuin::runtime::StreamTransport>>
  create(sakuin::runtime::StreamTransportOptions) override {
    return std::unexpected(sakuin::core::Error{
        sakuin::core::ErrorCode::IoError,
        "Storage composition test does not open a transport"});
  }
};

} // namespace

int main() {
  using namespace sakuin;
  const auto nonce = std::to_string(
      std::chrono::steady_clock::now().time_since_epoch().count());
  TemporaryDirectory directory{std::filesystem::temp_directory_path() /
                               ("sakuin-metadata-store-" + nonce)};
  storage::LocalBlobStore blobs{directory.path / "blobs"};
  auto catalog =
      storage::LocalManifestCatalog::open(directory.path / "catalog", blobs);
  if (!catalog)
    return 1;
  storage::TorrentDataset dataset{blobs, **catalog};

  const auto info_hash = hash(0x33);
  model::TorrentRecord placeholder{
      .info_hash = info_hash,
      .first_seen = core::Timestamp{core::Timestamp::duration{10}},
      .last_seen = core::Timestamp{core::Timestamp::duration{90}},
      .name = std::nullopt,
      .total_size = 0,
      .files = {}};
  auto write = dataset.begin_write();
  if (!write || !(*write)->append(placeholder) || !(*write)->commit())
    return 2;

  std::atomic<std::uint64_t> notified_generation{};
  integration::TorrentMetadataSink sink{
      dataset, 3,
      [&](std::uint64_t generation) { notified_generation = generation; }};
  model::TorrentRecord metadata{
      .info_hash = info_hash,
      .first_seen = core::Timestamp{core::Timestamp::duration{40}},
      .last_seen = core::Timestamp{core::Timestamp::duration{40}},
      .name = "archive",
      .total_size = 12,
      .files = {{.path = "a.bin", .size = 5},
                {.path = "dir/b.bin", .size = 7}}};
  if (auto stored = sink.on_metadata_fetched(std::move(metadata)); !stored)
    return 3;
  if (sink.last_generation() != 2 || sink.last_error() ||
      notified_generation != 2)
    return 4;

  auto snapshot = dataset.keyed_snapshot();
  auto record = (*snapshot)->get(info_hash);
  if (!record || !*record || (*record)->name != "archive" ||
      (*record)->total_size != 12 || (*record)->files.size() != 2 ||
      (*record)->first_seen != placeholder.first_seen ||
      (*record)->last_seen != placeholder.last_seen)
    return 5;

  model::TorrentRecord invalid = **record;
  invalid.files.clear();
  auto rejected = sink.on_metadata_fetched(std::move(invalid));
  if (rejected || rejected.error().code != core::ErrorCode::InvalidArgument ||
      !sink.last_error() ||
      sink.last_error()->code != core::ErrorCode::InvalidArgument)
    return 6;

  TransportFactory factory;
  dht::PeerId peer_id{};
  std::ranges::iota(peer_id, std::uint8_t{1});
  auto metadata_config = config::defaults().network.dht.metadata;
  metadata_config.enabled = false;
  auto disabled = integration::TorrentMetadataAcquisition::create(
      peer_id, factory, dataset, metadata_config);
  if (!disabled || (*disabled)->enabled() || (*disabled)->controller())
    return 7;

  metadata_config.enabled = true;
  metadata_config.maximum_in_flight = 2;
  auto enabled = integration::TorrentMetadataAcquisition::create(
      peer_id, factory, dataset, metadata_config);
  if (!enabled || !(*enabled)->enabled() || !(*enabled)->controller())
    return 8;
  (*enabled)->stop();

  return 0;
}

import std;

import sakuin.core;
import sakuin.integration.metadata_storage;
import sakuin.model.torrent;
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

  integration::TorrentMetadataSink sink{dataset};
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
  if (sink.last_generation() != 2 || sink.last_error())
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

  return 0;
}

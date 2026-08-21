import std;

import sakuin.core;
import sakuin.index;
import sakuin.model.observation;
import sakuin.storage;
import sakuin.storage.dataset.observations;
import sakuin.storage.dataset.torrents;

namespace {

struct TemporaryDirectory {
  std::filesystem::path path;
  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path, ignored);
  }
};

sakuin::model::ObservationRecord observation(std::uint8_t hash,
                                             std::int64_t time) {
  sakuin::core::InfoHash info_hash;
  info_hash.bytes.fill(hash);
  return {.info_hash = info_hash,
          .observed_at = sakuin::core::Timestamp{
              sakuin::core::Timestamp::duration{time}}};
}

} // namespace

int main() {
  using namespace sakuin;
  const auto nonce = std::to_string(
      std::chrono::steady_clock::now().time_since_epoch().count());
  TemporaryDirectory directory{std::filesystem::temp_directory_path() /
                               ("sakuin-materialize-" + nonce)};
  storage::LocalBlobStore blobs{directory.path / "blobs"};
  auto observation_catalog = storage::LocalManifestCatalog::open(
      directory.path / "observations", blobs);
  auto torrent_catalog =
      storage::LocalManifestCatalog::open(directory.path / "torrents", blobs);
  if (!observation_catalog || !torrent_catalog)
    return 1;
  storage::ObservationDataset observations{blobs, **observation_catalog};
  storage::TorrentDataset torrents{blobs, **torrent_catalog};

  auto write = observations.begin_write();
  const std::array records{observation(2, 30), observation(1, 20),
                           observation(2, 10), observation(1, 40)};
  if (!write || !(*write)->append(records) || !(*write)->commit())
    return 2;
  auto source = observations.snapshot();
  auto rebuilt = index::ObservationMaterializer::rebuild(**source, torrents);
  if (!rebuilt || rebuilt->observations_read != 4 ||
      rebuilt->torrents_written != 2 || rebuilt->generation != 1)
    return 3;

  auto snapshot = torrents.keyed_snapshot();
  for (const auto [hash_value, first, last] :
       {std::tuple{std::uint8_t{1}, std::int64_t{20}, std::int64_t{40}},
        std::tuple{std::uint8_t{2}, std::int64_t{10}, std::int64_t{30}}}) {
    core::InfoHash hash;
    hash.bytes.fill(hash_value);
    auto torrent = (*snapshot)->get(hash);
    if (!torrent || !*torrent || (*torrent)->first_seen != core::Timestamp{
                                                 core::Timestamp::duration{first}} ||
        (*torrent)->last_seen !=
            core::Timestamp{core::Timestamp::duration{last}} ||
        (*torrent)->name || (*torrent)->total_size != 0 ||
        !(*torrent)->files.empty())
      return 4;
  }

  auto rejected = index::ObservationMaterializer::rebuild(**source, torrents);
  if (rejected || rejected.error().code != core::ErrorCode::Conflict)
    return 5;
  return 0;
}

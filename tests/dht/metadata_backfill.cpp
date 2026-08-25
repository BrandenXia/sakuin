import std;

import sakuin.core;
import sakuin.dht;
import sakuin.integration.metadata_backfill;
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

sakuin::model::TorrentRecord placeholder(std::uint8_t value,
                                         std::int64_t last_seen) {
  return {.info_hash = hash(value),
          .first_seen = sakuin::core::Timestamp{std::chrono::seconds{1}},
          .last_seen = sakuin::core::Timestamp{std::chrono::seconds{last_seen}},
          .name = std::nullopt};
}

sakuin::model::TorrentRecord metadata(std::uint8_t value) {
  auto result = placeholder(value, 20);
  result.name = "already-present";
  result.total_size = 42;
  result.files = {{.path = "file.bin", .size = 42}};
  return result;
}

} // namespace

int main() {
  using namespace sakuin;

  const auto nonce = std::to_string(
      std::chrono::steady_clock::now().time_since_epoch().count());
  TemporaryDirectory directory{std::filesystem::temp_directory_path() /
                               ("sakuin-metadata-backfill-" + nonce)};
  storage::LocalBlobStore blobs{directory.path / "blobs"};
  auto catalog =
      storage::LocalManifestCatalog::open(directory.path / "catalog", blobs);
  if (!catalog)
    return 1;
  storage::TorrentDataset torrents{blobs, **catalog};
  auto write = torrents.begin_write();
  const std::array initial{placeholder(1, 10), metadata(2), placeholder(3, 30)};
  if (!write || !(*write)->append(initial) || !(*write)->commit())
    return 2;

  dht::AnnounceTokenSecret secret;
  auto tokens = dht::RotatingAnnounceTokenProvider::create(secret);
  dht::DhtNode node{{}, *tokens};
  auto discovery = dht::PeerDiscoveryPlanner::create(
      node, {.maximum_pending = 1,
             .maximum_in_flight = 1,
             .parallelism_per_hash = 1,
             .maximum_queries_per_hash = 1,
             .retry_delay = std::chrono::minutes{5}});
  if (!discovery)
    return 3;
  auto invalid = integration::MetadataDiscoveryBackfill::create(
      torrents, **discovery, {.maximum_records_per_poll = 0});
  auto backfill = integration::MetadataDiscoveryBackfill::create(
      torrents, **discovery,
      {.maximum_records_per_poll = 5,
       .refresh_interval = std::chrono::minutes{1},
       .full_rescan_interval = std::chrono::minutes{2},
       .backpressure_retry_delay = std::chrono::seconds{1}});
  if (invalid || !backfill)
    return 4;

  const auto now = core::Timestamp{std::chrono::seconds{100}};
  auto first = (*backfill)->poll(now);
  if (!first || first->records_scanned != 3 || first->targets_offered != 1 ||
      first->records_with_metadata != 1 || !first->full_rebuild ||
      !first->scan_in_progress ||
      first->next_wakeup != now + std::chrono::seconds{1} ||
      (*discovery)->pending() != 1)
    return 5;

  auto exhausted = (*discovery)->poll(now);
  if (!exhausted || exhausted->exhausted != 1 || (*discovery)->pending() != 0)
    return 6;
  auto resumed = (*backfill)->poll(now + std::chrono::seconds{1});
  if (!resumed || resumed->records_scanned != 0 ||
      resumed->targets_offered != 1 || resumed->scan_in_progress ||
      resumed->source_generation != 1 ||
      resumed->next_wakeup != now + std::chrono::seconds{61} ||
      (*discovery)->pending() != 1)
    return 7;
  auto waiting = (*backfill)->poll(now + std::chrono::seconds{2});
  if (!waiting || waiting->records_scanned != 0 ||
      waiting->targets_offered != 0 ||
      waiting->next_wakeup != now + std::chrono::seconds{61})
    return 8;

  if (!(*discovery)->poll(now + std::chrono::seconds{2}))
    return 9;
  auto delta = torrents.begin_write();
  if (!delta || !(*delta)->append(placeholder(4, 40)) || !(*delta)->commit())
    return 10;
  auto incremental = (*backfill)->poll(now + std::chrono::seconds{61});
  if (!incremental || incremental->records_scanned != 1 ||
      incremental->targets_offered != 1 || incremental->full_rebuild ||
      incremental->source_generation != 2 || (*discovery)->pending() != 1)
    return 11;
  if (!(*discovery)->poll(now + std::chrono::seconds{62}))
    return 12;
  auto retry_sweep = (*backfill)->poll(now + std::chrono::seconds{362});
  if (!retry_sweep || retry_sweep->records_scanned != 2 ||
      retry_sweep->targets_offered != 1 || !retry_sweep->full_rebuild ||
      !retry_sweep->scan_in_progress || (*discovery)->pending() != 1)
    return 13;
  return 0;
}

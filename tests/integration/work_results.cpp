import std;

import sakuin.core;
import sakuin.dht.observation;
import sakuin.integration.work_results;
import sakuin.integration.remote_metadata;
import sakuin.model.observation;
import sakuin.model.torrent;
import sakuin.scheduler;
import sakuin.storage;
import sakuin.storage.admin.row_v1;
import sakuin.storage.codec.model_records;
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

sakuin::core::ByteBuffer bytes(std::string_view text) {
  const auto view = std::as_bytes(std::span{text});
  return {view.begin(), view.end()};
}

sakuin::core::InfoHash hash(std::uint8_t seed) {
  sakuin::core::InfoHash result;
  for (std::size_t index = 0; index < result.bytes.size(); ++index)
    result.bytes[index] = static_cast<std::uint8_t>(seed + index);
  return result;
}

} // namespace

int main() {
  using namespace sakuin;
  const auto nonce = std::to_string(
      std::chrono::steady_clock::now().time_since_epoch().count());
  TemporaryDirectory directory{std::filesystem::temp_directory_path() /
                               ("sakuin-work-results-" + nonce)};
  storage::LocalBlobStore blobs{directory.path / "objects"};
  const auto catalog_path = directory.path / "manifests";
  const auto batch = integration::make_work_result_batch(
      scheduler::WorkResultKind::TorrentMetadataBatch, bytes("metadata"));

  {
    auto catalog = storage::LocalManifestCatalog::open(catalog_path, blobs);
    if (!catalog)
      return 1;
    integration::CanonicalWorkResultInbox inbox{
        blobs,
        **catalog,
        {.compression = storage::CompressionCodec::None,
         .target_block_size = 64}};
    auto published = inbox.publish_result("worker-1", batch);
    auto duplicate = inbox.publish_result("worker-1", batch);
    auto results = inbox.scan();
    if (!published || !*published || !duplicate || *duplicate || !results ||
        results->size() != 1 || results->front() != batch)
      return 2;

    auto invalid = batch;
    invalid.payload.push_back(std::byte{0});
    auto rejected = inbox.publish_result("worker-1", std::move(invalid));
    if (rejected || rejected.error().code != core::ErrorCode::ChecksumMismatch)
      return 3;
  }

  auto reopened_catalog =
      storage::LocalManifestCatalog::open(catalog_path, blobs);
  if (!reopened_catalog)
    return 4;
  integration::CanonicalWorkResultInbox reopened{blobs, **reopened_catalog};
  auto duplicate = reopened.publish_result("worker-1", batch);
  auto results = reopened.scan();
  if (!duplicate || *duplicate || !results || results->size() != 1 ||
      results->front() != batch ||
      (*reopened_catalog)->current_id().generation != 1)
    return 5;

  const std::array observations{
      model::ObservationRecord{.info_hash = hash(1),
                               .observed_at =
                                   core::Timestamp{std::chrono::seconds{10}}},
      model::ObservationRecord{.info_hash = hash(2),
                               .observed_at =
                                   core::Timestamp{std::chrono::seconds{20}}}};
  auto encoded = integration::encode_observation_result_batch(observations);
  if (!encoded)
    return 6;
  auto decoded = integration::decode_observation_result_batch(*encoded);
  if (!decoded || decoded->size() != observations.size() ||
      (*decoded)[0].info_hash != observations[0].info_hash ||
      (*decoded)[0].observed_at != observations[0].observed_at ||
      (*decoded)[1].info_hash != observations[1].info_hash ||
      (*decoded)[1].observed_at != observations[1].observed_at)
    return 7;

  auto observation_catalog = storage::LocalManifestCatalog::open(
      directory.path / "observation-manifests", blobs);
  if (!observation_catalog)
    return 8;
  storage::ObservationDataset observation_dataset{blobs, **observation_catalog};
  integration::CanonicalObservationResultPublisher observation_results{
      blobs,
      **observation_catalog,
      {.compression = storage::CompressionCodec::None,
       .target_block_size = 64}};
  auto torrent_catalog = storage::LocalManifestCatalog::open(
      directory.path / "torrent-manifests", blobs);
  if (!torrent_catalog)
    return 9;
  storage::TorrentDataset torrents{blobs, **torrent_catalog};
  integration::CanonicalTorrentMetadataResultPublisher metadata_results{
      torrents};
  integration::CanonicalWorkResultRouter router{observation_results,
                                                metadata_results, reopened};
  integration::RemoteObservationSink sink{router, "worker-1", 2};
  if (!sink.observe(observations[0]) || sink.pending() != 1 ||
      !sink.observe(observations[1]) || sink.pending() != 0)
    return 10;
  auto observation_snapshot = observation_dataset.snapshot();
  if (!observation_snapshot)
    return 11;
  auto observation_stream =
      (*observation_snapshot)->scan(storage::AllObservations{});
  std::vector<model::ObservationRecord> stored;
  if (!observation_stream)
    return 12;
  while (true) {
    auto next = (*observation_stream)->next();
    if (!next)
      return 13;
    if (!*next)
      break;
    stored.push_back(std::move(**next));
  }
  if (stored.size() != observations.size() ||
      stored[0].info_hash != observations[0].info_hash ||
      stored[1].info_hash != observations[1].info_hash)
    return 14;

  auto observation_payload =
      integration::encode_observation_result_batch(observations);
  if (!observation_payload)
    return 15;
  auto observation_batch = integration::make_work_result_batch(
      scheduler::WorkResultKind::ObservationBatch,
      std::move(*observation_payload));
  auto observation_duplicate =
      router.publish_result("worker-1", std::move(observation_batch));
  if (!observation_duplicate || *observation_duplicate ||
      (*observation_catalog)->current_id().generation != 1)
    return 16;

  results = reopened.scan();
  if (!results || results->size() != 2 || results->front() != batch ||
      (*reopened_catalog)->current_id().generation != 2)
    return 17;

  const std::array later_observations{model::ObservationRecord{
      .info_hash = hash(3),
      .observed_at = core::Timestamp{std::chrono::seconds{30}}}};
  auto later_payload =
      integration::encode_observation_result_batch(later_observations);
  if (!later_payload)
    return 18;
  auto later_batch = integration::make_work_result_batch(
      scheduler::WorkResultKind::ObservationBatch, std::move(*later_payload));
  if (auto published =
          router.publish_result("worker-1", std::move(later_batch));
      !published || !*published)
    return 19;
  auto compacted = storage::RowV1DatasetMaintenance::compact(
      blobs, **observation_catalog,
      {.minimum_segment_count = 2,
       .target_block_size = 64,
       .compression = storage::CompressionCodec::None});
  if (!compacted || compacted->segments_removed != 2 ||
      compacted->segments_created != 1)
    return 20;
  auto replay_payload =
      integration::encode_observation_result_batch(observations);
  if (!replay_payload)
    return 21;
  auto replay_batch = integration::make_work_result_batch(
      scheduler::WorkResultKind::ObservationBatch, std::move(*replay_payload));
  const auto compacted_generation =
      (*observation_catalog)->current_id().generation;
  integration::CanonicalWorkResultInbox restarted_receipts{blobs,
                                                           **reopened_catalog};
  integration::CanonicalWorkResultRouter restarted_router{
      observation_results, metadata_results, restarted_receipts};
  auto late_replay =
      restarted_router.publish_result("worker-1", std::move(replay_batch));
  if (!late_replay || *late_replay ||
      (*observation_catalog)->current_id().generation != compacted_generation)
    return 22;

  const model::TorrentRecord metadata{
      .info_hash = hash(9),
      .first_seen = core::Timestamp{std::chrono::seconds{30}},
      .last_seen = core::Timestamp{std::chrono::seconds{30}},
      .name = "remote metadata",
      .total_size = 12,
      .files = {{.path = "a.bin", .size = 5},
                {.path = "dir/b.bin", .size = 7}}};
  auto metadata_payload = integration::encode_torrent_metadata_result_batch(
      std::span{&metadata, std::size_t{1}});
  auto decoded_metadata =
      metadata_payload
          ? integration::decode_torrent_metadata_result_batch(*metadata_payload)
          : core::Result<std::vector<model::TorrentRecord>>{
                std::unexpected(metadata_payload.error())};
  if (!decoded_metadata || decoded_metadata->size() != 1 ||
      decoded_metadata->front() != metadata)
    return 23;

  integration::RemoteTorrentMetadataSink remote_metadata{restarted_router,
                                                         "worker-1", 4'096};
  if (!remote_metadata.on_metadata_fetched(metadata) ||
      remote_metadata.last_error())
    return 24;
  auto torrent_snapshot = torrents.keyed_snapshot();
  auto stored_metadata =
      torrent_snapshot ? (*torrent_snapshot)->get(metadata.info_hash)
                       : core::Result<std::optional<model::TorrentRecord>>{
                             std::unexpected(torrent_snapshot.error())};
  if (!stored_metadata || !*stored_metadata || **stored_metadata != metadata ||
      (*torrent_catalog)->current_id().generation != 1)
    return 25;
  if (!remote_metadata.on_metadata_fetched(metadata) ||
      (*torrent_catalog)->current_id().generation != 1 ||
      (*reopened_catalog)->current_id().generation != 4)
    return 26;

  auto tiny_sink =
      integration::RemoteTorrentMetadataSink{restarted_router, "worker-1", 8};
  auto oversized = tiny_sink.on_metadata_fetched(metadata);
  if (oversized || oversized.error().code != core::ErrorCode::QuotaExceeded)
    return 27;

  results = reopened.scan();
  if (!results || results->size() != 4 ||
      results->back().kind != scheduler::WorkResultKind::TorrentMetadataBatch)
    return 28;
  return 0;
}

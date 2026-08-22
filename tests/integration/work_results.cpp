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

bool has_receipt(
    std::span<const sakuin::integration::WorkResultReceipt> receipts,
    const sakuin::scheduler::WorkResultBatch &batch) {
  return std::ranges::any_of(receipts, [&](const auto &receipt) {
    return receipt.id == batch.id && receipt.kind == batch.kind;
  });
}

sakuin::core::ByteBuffer
legacy_receipt(const sakuin::scheduler::WorkResultBatch &batch) {
  sakuin::core::ByteBuffer encoded;
  encoded.reserve(21 + batch.payload.size());
  for (const auto byte : batch.id.bytes)
    encoded.push_back(static_cast<std::byte>(byte));
  encoded.push_back(static_cast<std::byte>(batch.kind));
  auto size = static_cast<std::uint32_t>(batch.payload.size());
  for (unsigned shift = 0; shift < 32; shift += 8)
    encoded.push_back(static_cast<std::byte>(size >> shift));
  encoded.insert(encoded.end(), batch.payload.begin(), batch.payload.end());
  return encoded;
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

  // Schema-v1 receipts stored the full payload. A current inbox must validate
  // and compact them into the schema-v2 identity-only representation.
  auto legacy_catalog = storage::LocalManifestCatalog::open(
      directory.path / "legacy-manifests", blobs);
  if (!legacy_catalog)
    return 33;
  auto legacy_writer = storage::RowV1SegmentWriter::create(
      blobs, {.schema_id = integration::WorkResultReceiptSchema,
              .schema_version = {1},
              .encoding = storage::SegmentEncoding::RowV1,
              .compression = storage::CompressionCodec::None,
              .target_block_size = 64});
  if (!legacy_writer || !(*legacy_writer)->append(legacy_receipt(batch)))
    return 34;
  auto legacy_segment = (*legacy_writer)->finalize();
  if (!legacy_segment ||
      !(*legacy_catalog)
           ->publish((*legacy_catalog)->current_id(), {*legacy_segment}))
    return 35;
  integration::CanonicalWorkResultInbox legacy_inbox{blobs, **legacy_catalog};
  auto legacy_scan = legacy_inbox.scan_receipts();
  auto migrated =
      legacy_inbox.compact({.minimum_segment_count = 64,
                            .target_block_size = 64,
                            .compression = storage::CompressionCodec::None});
  auto migrated_pin = (*legacy_catalog)->pin_current();
  if (!legacy_scan || legacy_scan->size() != 1 ||
      !has_receipt(*legacy_scan, batch) || !migrated ||
      migrated->segments_removed != 1 || !migrated_pin ||
      (*migrated_pin)->manifest().segments.size() != 1 ||
      (*migrated_pin)->manifest().segments.front().schema_version.value != 2)
    return 36;

  auto corrupt_catalog = storage::LocalManifestCatalog::open(
      directory.path / "corrupt-manifests", blobs);
  if (!corrupt_catalog)
    return 37;
  auto corrupt_writer = storage::RowV1SegmentWriter::create(
      blobs, {.schema_id = integration::WorkResultReceiptSchema,
              .schema_version = {1},
              .encoding = storage::SegmentEncoding::RowV1,
              .compression = storage::CompressionCodec::None,
              .target_block_size = 64});
  auto corrupt_encoded = legacy_receipt(batch);
  corrupt_encoded.back() ^= std::byte{1};
  if (!corrupt_writer || !(*corrupt_writer)->append(corrupt_encoded))
    return 38;
  auto corrupt_segment = (*corrupt_writer)->finalize();
  if (!corrupt_segment ||
      !(*corrupt_catalog)
           ->publish((*corrupt_catalog)->current_id(), {*corrupt_segment}))
    return 39;
  integration::CanonicalWorkResultInbox corrupt_inbox{blobs, **corrupt_catalog};
  auto corrupt_scan = corrupt_inbox.scan_receipts();
  if (corrupt_scan ||
      corrupt_scan.error().code != core::ErrorCode::ChecksumMismatch)
    return 40;

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
    auto results = inbox.scan_receipts();
    if (!published || !*published || !duplicate || *duplicate || !results ||
        results->size() != 1 || !has_receipt(*results, batch))
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
  auto results = reopened.scan_receipts();
  if (!duplicate || *duplicate || !results || results->size() != 1 ||
      !has_receipt(*results, batch) ||
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

  results = reopened.scan_receipts();
  if (!results || results->size() != 2 || !has_receipt(*results, batch) ||
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

  results = reopened.scan_receipts();
  if (!results || results->size() != 4 ||
      !std::ranges::any_of(*results, [](const auto &receipt) {
        return receipt.kind == scheduler::WorkResultKind::TorrentMetadataBatch;
      }))
    return 28;

  auto receipt_compaction = restarted_receipts.compact(
      {.minimum_segment_count = 2,
       .target_block_size = 64,
       .compression = storage::CompressionCodec::None});
  if (!receipt_compaction || receipt_compaction->segments_removed != 4 ||
      receipt_compaction->segments_created != 1 ||
      receipt_compaction->bytes_after >= receipt_compaction->bytes_before)
    return 29;
  auto receipt_pin = (*reopened_catalog)->pin_current();
  if (!receipt_pin || (*receipt_pin)->manifest().segments.size() != 1 ||
      (*receipt_pin)->manifest().segments.front().schema_version.value != 2 ||
      (*receipt_pin)->manifest().segments.front().record_count != 4)
    return 30;
  receipt_pin->reset();

  integration::CanonicalWorkResultInbox compacted_receipts{blobs,
                                                           **reopened_catalog};
  auto replay_after_receipt_compaction =
      compacted_receipts.publish_result("worker-1", batch);
  auto compacted_scan = compacted_receipts.scan_receipts();
  if (!replay_after_receipt_compaction || *replay_after_receipt_compaction ||
      !compacted_scan || compacted_scan->size() != 4 ||
      !has_receipt(*compacted_scan, batch))
    return 31;
  auto receipt_gc = (*reopened_catalog)->garbage_collect();
  if (!receipt_gc || receipt_gc->objects_deleted < 4)
    return 32;
  return 0;
}

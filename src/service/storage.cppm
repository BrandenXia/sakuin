module;

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>

export module sakuin.service.storage;

import std;

import sakuin.config.model;
import sakuin.core.result;
import sakuin.core.time;
import sakuin.dht.observation;
import sakuin.integration.dht_storage;
import sakuin.integration.work_results;
import sakuin.scheduler.work;
import sakuin.storage.admin;
import sakuin.storage.admin.compaction;
import sakuin.storage.admin.retention;
import sakuin.storage.admin.row_v1;
import sakuin.storage.blob.local;
import sakuin.storage.blob.s3;
import sakuin.storage.blob.store;
import sakuin.storage.catalog.manifest;
import sakuin.storage.catalog.local;
import sakuin.storage.dataset.observations;
import sakuin.storage.dataset.torrents;
import sakuin.storage.format.block;
import sakuin.storage.format.segment;

export namespace sakuin::service {

enum class LocalDataset { Observations, Torrents, WorkResults };

// Owns the canonical local storage graph. Both DHT address-family runtimes may
// share observations(); the buffered sink serializes publication to its
// final observation segment.
class LocalCanonicalStorage final {
public:
  static core::Result<std::unique_ptr<LocalCanonicalStorage>>
  open(const config::StorageConfig &configuration);

  ~LocalCanonicalStorage();

  LocalCanonicalStorage(const LocalCanonicalStorage &) = delete;
  LocalCanonicalStorage &operator=(const LocalCanonicalStorage &) = delete;

  core::Result<void> flush();

  core::Result<storage::CompactionResult> compact(LocalDataset dataset);
  core::Result<storage::RetentionResult>
  retain_observations(core::Timestamp now);
  core::Result<storage::VerifyResult> verify(LocalDataset dataset);
  core::Result<storage::GcResult> garbage_collect(LocalDataset dataset);

  dht::ObservationSink &observations() noexcept { return *observation_sink_; }
  storage::ObservationDataset &observation_dataset() noexcept {
    return *observations_;
  }
  storage::TorrentDataset &torrents() noexcept { return *torrents_; }
  scheduler::WorkResultPublisher &work_results() noexcept {
    return *work_result_router_;
  }
  void set_work_result_torrent_callback(
      std::function<void(std::uint64_t)> callback) {
    torrent_metadata_results_->set_on_torrent_committed(std::move(callback));
  }
  std::size_t observation_batch_size() const noexcept {
    return observation_batch_size_;
  }
  const std::filesystem::path &root() const noexcept { return root_; }

private:
  LocalCanonicalStorage(std::filesystem::path root,
                        std::unique_ptr<storage::BlobStore> blobs);

  std::filesystem::path root_;
  std::unique_ptr<storage::BlobStore> blobs_;
  std::unique_ptr<storage::LocalManifestCatalog> observation_catalog_;
  std::unique_ptr<storage::LocalManifestCatalog> torrent_catalog_;
  std::unique_ptr<storage::LocalManifestCatalog> work_result_catalog_;
  std::unique_ptr<storage::ObservationDataset> observations_;
  std::unique_ptr<storage::TorrentDataset> torrents_;
  std::unique_ptr<integration::CanonicalWorkResultInbox> work_results_;
  std::unique_ptr<integration::CanonicalObservationResultPublisher>
      observation_results_;
  std::unique_ptr<integration::CanonicalTorrentMetadataResultPublisher>
      torrent_metadata_results_;
  std::unique_ptr<integration::CanonicalWorkResultRouter> work_result_router_;
  std::unique_ptr<integration::BufferedObservationSink> observation_sink_;
  std::size_t observation_batch_size_{};
  storage::CompactionPolicy compaction_policy_;
  config::StorageConfig::RetentionConfig retention_configuration_;
  int lock_file_{-1};
};

} // namespace sakuin::service

namespace sakuin::service {
namespace {

constexpr std::uint64_t ObservationRowBytes = 32;

storage::CompressionCodec
storage_compression(config::CompressionCodec compression) {
  switch (compression) {
  case config::CompressionCodec::None:
    return storage::CompressionCodec::None;
  case config::CompressionCodec::Zstd:
    return storage::CompressionCodec::Zstd;
  }
  std::unreachable();
}

core::Result<storage::SegmentHeader>
segment_header(const config::StorageConfig &configuration) {
  if (configuration.block_target_bytes == 0 ||
      configuration.block_target_bytes >
          std::numeric_limits<std::uint32_t>::max())
    return std::unexpected(core::Error{
        core::ErrorCode::InvalidArgument,
        "Local storage block target must fit the RowV1 32-bit block format"});
  return storage::SegmentHeader{
      .compression = storage_compression(configuration.compression),
      .target_block_size =
          static_cast<std::uint32_t>(configuration.block_target_bytes)};
}

std::size_t calculated_observation_batch_size(std::uint64_t target_bytes) {
  const auto records =
      std::max<std::uint64_t>(1, target_bytes / ObservationRowBytes);
  return static_cast<std::size_t>(std::min<std::uint64_t>(
      records, std::numeric_limits<std::size_t>::max()));
}

} // namespace

LocalCanonicalStorage::LocalCanonicalStorage(
    std::filesystem::path root, std::unique_ptr<storage::BlobStore> blobs)
    : root_(std::move(root)), blobs_(std::move(blobs)) {}

core::Result<std::unique_ptr<LocalCanonicalStorage>>
LocalCanonicalStorage::open(const config::StorageConfig &configuration) {
  if (configuration.local_root.empty() ||
      configuration.segment_target_bytes == 0 ||
      configuration.compaction_warm_block_target_bytes == 0 ||
      configuration.compaction_warm_block_target_bytes >
          std::numeric_limits<std::uint32_t>::max() ||
      (configuration.retention.enabled &&
       (configuration.retention.observation_cold_age <=
            core::Duration::zero() ||
        configuration.retention.observation_max_age <=
            configuration.retention.observation_cold_age ||
        configuration.retention.cold_block_target_bytes == 0 ||
        configuration.retention.cold_block_target_bytes >
            std::numeric_limits<std::uint32_t>::max())))
    return std::unexpected(core::Error{
        core::ErrorCode::InvalidArgument,
        "Canonical storage requires valid local state paths and block/segment "
        "targets"});
  auto header = segment_header(configuration);
  if (!header)
    return std::unexpected(header.error());

  std::unique_ptr<storage::BlobStore> blobs;
  if (configuration.backend == config::StorageBackend::Local) {
    blobs = std::make_unique<storage::LocalBlobStore>(configuration.local_root /
                                                      "objects");
  } else {
    auto remote = storage::S3BlobStore::open_from_environment(
        {.endpoint = configuration.s3.endpoint,
         .bucket = configuration.s3.bucket,
         .region = configuration.s3.region,
         .prefix = configuration.s3.prefix,
         .staging_directory =
             configuration.local_root / "operational" / "s3-staging",
         .connect_timeout =
             std::chrono::duration_cast<std::chrono::milliseconds>(
                 configuration.s3.connect_timeout),
         .request_timeout =
             std::chrono::duration_cast<std::chrono::milliseconds>(
                 configuration.s3.request_timeout),
         .maximum_attempts = configuration.s3.maximum_attempts,
         .retry_delay = std::chrono::duration_cast<std::chrono::milliseconds>(
             configuration.s3.retry_delay),
         .verify_tls = configuration.s3.verify_tls});
    if (!remote)
      return std::unexpected(remote.error());
    blobs = std::move(*remote);
  }
  auto result = std::unique_ptr<LocalCanonicalStorage>{
      new LocalCanonicalStorage{configuration.local_root, std::move(blobs)}};
  std::error_code directory_error;
  const auto operational = configuration.local_root / "operational";
  std::filesystem::create_directories(operational, directory_error);
  if (directory_error)
    return std::unexpected(
        core::Error{core::ErrorCode::IoError,
                    "Could not create local storage operational directory: " +
                        directory_error.message()});
  const auto lock_path = operational / "storage.lock";
  result->lock_file_ =
      ::open(lock_path.c_str(), O_CREAT | O_RDWR | O_CLOEXEC, 0600);
  if (result->lock_file_ < 0)
    return std::unexpected(core::Error{core::ErrorCode::IoError,
                                       "Could not open local storage lock: " +
                                           std::string{std::strerror(errno)}});
  if (::flock(result->lock_file_, LOCK_EX | LOCK_NB) != 0)
    return std::unexpected(core::Error{
        errno == EWOULDBLOCK ? core::ErrorCode::Conflict
                             : core::ErrorCode::IoError,
        errno == EWOULDBLOCK
            ? "Local storage is already open by another Sakuin process"
            : "Could not lock local storage: " +
                  std::string{std::strerror(errno)}});
  auto observation_catalog = storage::LocalManifestCatalog::open(
      configuration.local_root / "manifests" / "observations", *result->blobs_);
  if (!observation_catalog)
    return std::unexpected(observation_catalog.error());
  result->observation_catalog_ = std::move(*observation_catalog);

  auto torrent_catalog = storage::LocalManifestCatalog::open(
      configuration.local_root / "manifests" / "torrents", *result->blobs_);
  if (!torrent_catalog)
    return std::unexpected(torrent_catalog.error());
  result->torrent_catalog_ = std::move(*torrent_catalog);

  auto work_result_catalog = storage::LocalManifestCatalog::open(
      configuration.local_root / "manifests" / "work-results", *result->blobs_);
  if (!work_result_catalog)
    return std::unexpected(work_result_catalog.error());
  result->work_result_catalog_ = std::move(*work_result_catalog);

  result->observations_ = std::make_unique<storage::ObservationDataset>(
      *result->blobs_, *result->observation_catalog_, *header);
  result->torrents_ = std::make_unique<storage::TorrentDataset>(
      *result->blobs_, *result->torrent_catalog_, *header);
  result->work_results_ =
      std::make_unique<integration::CanonicalWorkResultInbox>(
          *result->blobs_, *result->work_result_catalog_, *header);
  result->observation_results_ =
      std::make_unique<integration::CanonicalObservationResultPublisher>(
          *result->blobs_, *result->observation_catalog_, *header);
  result->torrent_metadata_results_ =
      std::make_unique<integration::CanonicalTorrentMetadataResultPublisher>(
          *result->torrents_);
  result->work_result_router_ =
      std::make_unique<integration::CanonicalWorkResultRouter>(
          *result->observation_results_, *result->torrent_metadata_results_,
          *result->work_results_);
  result->observation_batch_size_ =
      calculated_observation_batch_size(configuration.segment_target_bytes);
  result->compaction_policy_ = {
      .minimum_segment_count = configuration.compaction_minimum_segments,
      .maximum_warm_segment_count =
          configuration.compaction_maximum_warm_segments,
      .target_block_size = header->target_block_size,
      .warm_target_block_size = static_cast<std::uint32_t>(
          configuration.compaction_warm_block_target_bytes),
      .compression = header->compression,
      .compression_level = configuration.compression_level};
  result->retention_configuration_ = configuration.retention;
  result->observation_sink_ =
      std::make_unique<integration::BufferedObservationSink>(
          *result->observations_, result->observation_batch_size_);
  return result;
}

LocalCanonicalStorage::~LocalCanonicalStorage() {
  if (observation_sink_)
    static_cast<void>(observation_sink_->flush());
  if (lock_file_ >= 0)
    ::close(lock_file_);
}

core::Result<void> LocalCanonicalStorage::flush() {
  return observation_sink_->flush();
}

core::Result<storage::CompactionResult>
LocalCanonicalStorage::compact(LocalDataset dataset) {
  if (dataset == LocalDataset::Torrents)
    return torrents_->compact(compaction_policy_);
  if (dataset == LocalDataset::WorkResults)
    return work_results_->compact(compaction_policy_);
  return storage::RowV1DatasetMaintenance::compact(
      *blobs_, *observation_catalog_, compaction_policy_);
}

core::Result<storage::RetentionResult>
LocalCanonicalStorage::retain_observations(core::Timestamp now) {
  if (!retention_configuration_.enabled)
    return storage::RetentionResult{
        .source_generation = observation_catalog_->current_id().generation};
  const auto cold_age = std::chrono::duration_cast<core::Timestamp::duration>(
      retention_configuration_.observation_cold_age);
  const auto maximum_age =
      std::chrono::duration_cast<core::Timestamp::duration>(
          retention_configuration_.observation_max_age);
  return storage::RowV1DatasetMaintenance::retain_unkeyed(
      *blobs_, *observation_catalog_,
      storage::RetentionPolicy{
          .cold_before = now - cold_age,
          .expire_before = now - maximum_age,
          .cold_target_block_size = static_cast<std::uint32_t>(
              retention_configuration_.cold_block_target_bytes),
          .cold_compression =
              storage_compression(retention_configuration_.cold_compression),
          .cold_compression_level =
              retention_configuration_.cold_compression_level});
}

core::Result<storage::VerifyResult>
LocalCanonicalStorage::verify(LocalDataset dataset) {
  switch (dataset) {
  case LocalDataset::Observations:
    return storage::RowV1DatasetMaintenance::verify(*blobs_,
                                                    *observation_catalog_);
  case LocalDataset::Torrents:
    return storage::RowV1DatasetMaintenance::verify(*blobs_, *torrent_catalog_);
  case LocalDataset::WorkResults:
    return storage::RowV1DatasetMaintenance::verify(*blobs_,
                                                    *work_result_catalog_);
  }
  std::unreachable();
}

core::Result<storage::GcResult>
LocalCanonicalStorage::garbage_collect(LocalDataset dataset) {
  switch (dataset) {
  case LocalDataset::Observations:
    return storage::RowV1DatasetMaintenance::garbage_collect(
        *observation_catalog_);
  case LocalDataset::Torrents:
    return storage::RowV1DatasetMaintenance::garbage_collect(*torrent_catalog_);
  case LocalDataset::WorkResults:
    return storage::RowV1DatasetMaintenance::garbage_collect(
        *work_result_catalog_);
  }
  std::unreachable();
}

} // namespace sakuin::service

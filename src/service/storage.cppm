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
import sakuin.dht.observation;
import sakuin.integration.dht_storage;
import sakuin.storage.admin;
import sakuin.storage.admin.compaction;
import sakuin.storage.admin.row_v1;
import sakuin.storage.blob.local;
import sakuin.storage.catalog.manifest;
import sakuin.storage.catalog.local;
import sakuin.storage.dataset.observations;
import sakuin.storage.dataset.torrents;
import sakuin.storage.format.block;
import sakuin.storage.format.segment;

export namespace sakuin::service {

enum class LocalDataset { Observations, Torrents };

// Owns the canonical local storage graph. Both DHT address-family runtimes may
// share observations(); the buffered sink serializes publication to its
// manifest. Call flush() after stopping producers to durably publish a partial
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
  core::Result<storage::VerifyResult> verify(LocalDataset dataset);
  core::Result<storage::GcResult> garbage_collect(LocalDataset dataset);

  dht::ObservationSink &observations() noexcept { return *observation_sink_; }
  storage::ObservationDataset &observation_dataset() noexcept {
    return *observations_;
  }
  storage::TorrentDataset &torrents() noexcept { return *torrents_; }
  std::size_t observation_batch_size() const noexcept {
    return observation_batch_size_;
  }
  const std::filesystem::path &root() const noexcept { return root_; }

private:
  explicit LocalCanonicalStorage(std::filesystem::path root);

  std::filesystem::path root_;
  storage::LocalBlobStore blobs_;
  std::unique_ptr<storage::LocalManifestCatalog> observation_catalog_;
  std::unique_ptr<storage::LocalManifestCatalog> torrent_catalog_;
  std::unique_ptr<storage::ObservationDataset> observations_;
  std::unique_ptr<storage::TorrentDataset> torrents_;
  std::unique_ptr<integration::BufferedObservationSink> observation_sink_;
  std::size_t observation_batch_size_{};
  storage::CompactionPolicy compaction_policy_;
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

LocalCanonicalStorage::LocalCanonicalStorage(std::filesystem::path root)
    : root_(std::move(root)), blobs_(root_ / "objects") {}

core::Result<std::unique_ptr<LocalCanonicalStorage>>
LocalCanonicalStorage::open(const config::StorageConfig &configuration) {
  if (configuration.backend != config::StorageBackend::Local)
    return std::unexpected(core::Error{
        core::ErrorCode::InvalidArgument,
        "Local canonical storage requires the local storage backend"});
  if (configuration.local_root.empty() ||
      configuration.segment_target_bytes == 0 ||
      configuration.compaction_warm_block_target_bytes == 0 ||
      configuration.compaction_warm_block_target_bytes >
          std::numeric_limits<std::uint32_t>::max())
    return std::unexpected(core::Error{
        core::ErrorCode::InvalidArgument,
        "Local canonical storage requires valid paths and block/segment "
        "targets"});
  auto header = segment_header(configuration);
  if (!header)
    return std::unexpected(header.error());

  auto result = std::unique_ptr<LocalCanonicalStorage>{
      new LocalCanonicalStorage{configuration.local_root}};
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
      configuration.local_root / "manifests" / "observations", result->blobs_);
  if (!observation_catalog)
    return std::unexpected(observation_catalog.error());
  result->observation_catalog_ = std::move(*observation_catalog);

  auto torrent_catalog = storage::LocalManifestCatalog::open(
      configuration.local_root / "manifests" / "torrents", result->blobs_);
  if (!torrent_catalog)
    return std::unexpected(torrent_catalog.error());
  result->torrent_catalog_ = std::move(*torrent_catalog);

  result->observations_ = std::make_unique<storage::ObservationDataset>(
      result->blobs_, *result->observation_catalog_, *header);
  result->torrents_ = std::make_unique<storage::TorrentDataset>(
      result->blobs_, *result->torrent_catalog_, *header);
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
  return storage::RowV1DatasetMaintenance::compact(
      blobs_, *observation_catalog_, compaction_policy_);
}

core::Result<storage::VerifyResult>
LocalCanonicalStorage::verify(LocalDataset dataset) {
  auto &catalog = dataset == LocalDataset::Observations ? *observation_catalog_
                                                        : *torrent_catalog_;
  return storage::RowV1DatasetMaintenance::verify(blobs_, catalog);
}

core::Result<storage::GcResult>
LocalCanonicalStorage::garbage_collect(LocalDataset dataset) {
  auto &catalog = dataset == LocalDataset::Observations ? *observation_catalog_
                                                        : *torrent_catalog_;
  return storage::RowV1DatasetMaintenance::garbage_collect(catalog);
}

} // namespace sakuin::service

export module sakuin.service.storage;

import std;

import sakuin.config.model;
import sakuin.core.result;
import sakuin.dht.observation;
import sakuin.integration.dht_storage;
import sakuin.storage.blob.local;
import sakuin.storage.catalog.local;
import sakuin.storage.dataset.observations;
import sakuin.storage.dataset.torrents;
import sakuin.storage.format.block;
import sakuin.storage.format.segment;

export namespace sakuin::service {

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

  dht::ObservationSink &observations() noexcept { return *observation_sink_; }
  storage::ObservationDataset &observation_dataset() noexcept {
    return *observations_;
  }
  storage::TorrentDataset &torrents() noexcept { return *torrents_; }
  std::size_t observation_batch_size() const noexcept {
    return observation_batch_size_;
  }

private:
  explicit LocalCanonicalStorage(std::filesystem::path root);

  storage::LocalBlobStore blobs_;
  std::unique_ptr<storage::LocalManifestCatalog> observation_catalog_;
  std::unique_ptr<storage::LocalManifestCatalog> torrent_catalog_;
  std::unique_ptr<storage::ObservationDataset> observations_;
  std::unique_ptr<storage::TorrentDataset> torrents_;
  std::unique_ptr<integration::BufferedObservationSink> observation_sink_;
  std::size_t observation_batch_size_{};
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
    : blobs_(std::move(root) / "objects") {}

core::Result<std::unique_ptr<LocalCanonicalStorage>>
LocalCanonicalStorage::open(const config::StorageConfig &configuration) {
  if (configuration.backend != config::StorageBackend::Local)
    return std::unexpected(core::Error{
        core::ErrorCode::InvalidArgument,
        "Local canonical storage requires the local storage backend"});
  if (configuration.local_root.empty() ||
      configuration.segment_target_bytes == 0)
    return std::unexpected(core::Error{
        core::ErrorCode::InvalidArgument,
        "Local canonical storage requires a root and segment target"});
  auto header = segment_header(configuration);
  if (!header)
    return std::unexpected(header.error());

  auto result = std::unique_ptr<LocalCanonicalStorage>{
      new LocalCanonicalStorage{configuration.local_root}};
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
  result->observation_sink_ =
      std::make_unique<integration::BufferedObservationSink>(
          *result->observations_, result->observation_batch_size_);
  return result;
}

LocalCanonicalStorage::~LocalCanonicalStorage() {
  if (observation_sink_)
    static_cast<void>(observation_sink_->flush());
}

core::Result<void> LocalCanonicalStorage::flush() {
  return observation_sink_->flush();
}

} // namespace sakuin::service

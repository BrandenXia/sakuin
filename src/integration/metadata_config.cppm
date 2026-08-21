export module sakuin.integration.metadata_config;

import std;

import sakuin.config.model;
import sakuin.core.result;
import sakuin.dht.metadata;
import sakuin.dht.metadata_controller;
import sakuin.integration.metadata_storage;
import sakuin.runtime.stream;
import sakuin.storage.dataset.torrents;

export namespace sakuin::integration {

struct MetadataRuntimeConfig {
  bool enabled{true};
  dht::MetadataControllerOptions controller;
  std::size_t storage_conflict_attempts{3};
};

MetadataRuntimeConfig
metadata_runtime_config(const config::MetadataAcquisitionConfig &config);

// Owns the storage observer and controller as one lifetime-safe composition.
// Disabled TOML configuration produces a valid pipeline with no controller.
class TorrentMetadataAcquisition final {
public:
  static core::Result<std::unique_ptr<TorrentMetadataAcquisition>>
  create(dht::PeerId peer_id, runtime::StreamTransportFactory &factory,
         storage::TorrentDataset &dataset,
         const config::MetadataAcquisitionConfig &config);
  static core::Result<std::unique_ptr<TorrentMetadataAcquisition>>
  create(dht::PeerId peer_id, runtime::StreamTransportFactory &factory,
         storage::TorrentDataset &dataset,
         const config::MetadataAcquisitionConfig &config,
         std::function<void()> wake_owner,
         std::function<void(std::uint64_t)> on_torrent_committed = {});

  dht::MetadataAcquisitionController *controller() noexcept {
    return controller_.get();
  }
  const dht::MetadataAcquisitionController *controller() const noexcept {
    return controller_.get();
  }
  TorrentMetadataSink &sink() noexcept { return sink_; }
  bool enabled() const noexcept { return controller_ != nullptr; }
  void stop() noexcept;

private:
  TorrentMetadataAcquisition(
      storage::TorrentDataset &dataset, std::size_t storage_conflict_attempts,
      std::function<void(std::uint64_t)> on_torrent_committed)
      : sink_(dataset, storage_conflict_attempts,
              std::move(on_torrent_committed)) {}

  // Controller is destroyed first and therefore cannot callback into a
  // destroyed sink during teardown.
  TorrentMetadataSink sink_;
  std::unique_ptr<dht::MetadataAcquisitionController> controller_;
};

} // namespace sakuin::integration

namespace sakuin::integration {

MetadataRuntimeConfig
metadata_runtime_config(const config::MetadataAcquisitionConfig &config) {
  MetadataRuntimeConfig result{.enabled = config.enabled,
                               .storage_conflict_attempts =
                                   config.storage_conflict_attempts};
  result.controller.queue.maximum_in_flight = config.maximum_in_flight;
  result.controller.queue.maximum_queued = config.maximum_queued;
  result.controller.queue.maximum_attempts_per_peer =
      config.maximum_attempts_per_peer;
  result.controller.queue.initial_retry_delay = config.initial_retry_delay;
  result.controller.queue.maximum_retry_delay = config.maximum_retry_delay;
  result.controller.transport.connect_timeout = config.connect_timeout;
  result.controller.transport.idle_timeout = config.idle_timeout;
  result.controller.transport.maximum_queued_write_bytes =
      config.maximum_queued_write_bytes;
  result.controller.fetch.exchange.maximum_metadata_bytes =
      config.maximum_metadata_bytes;
  result.controller.fetch.exchange.maximum_outstanding_requests =
      config.maximum_outstanding_requests;
  result.controller.fetch.metainfo.maximum_metadata_bytes =
      config.maximum_metadata_bytes;
  result.controller.storage_retry_delay = config.storage_retry_delay;
  return result;
}

core::Result<std::unique_ptr<TorrentMetadataAcquisition>>
TorrentMetadataAcquisition::create(
    dht::PeerId peer_id, runtime::StreamTransportFactory &factory,
    storage::TorrentDataset &dataset,
    const config::MetadataAcquisitionConfig &config) {
  return create(peer_id, factory, dataset, config, {});
}

core::Result<std::unique_ptr<TorrentMetadataAcquisition>>
TorrentMetadataAcquisition::create(
    dht::PeerId peer_id, runtime::StreamTransportFactory &factory,
    storage::TorrentDataset &dataset,
    const config::MetadataAcquisitionConfig &config,
    std::function<void()> wake_owner,
    std::function<void(std::uint64_t)> on_torrent_committed) {
  if (config.storage_conflict_attempts == 0)
    return std::unexpected(
        core::Error{core::ErrorCode::InvalidArgument,
                    "Metadata storage conflict-attempt limit must be nonzero"});
  auto result = std::unique_ptr<TorrentMetadataAcquisition>{
      new TorrentMetadataAcquisition{dataset, config.storage_conflict_attempts,
                                     std::move(on_torrent_committed)}};
  auto runtime = metadata_runtime_config(config);
  if (!runtime.enabled)
    return result;
  runtime.controller.wake_owner = std::move(wake_owner);
  auto controller = dht::MetadataAcquisitionController::create(
      peer_id, factory, result->sink_, runtime.controller);
  if (!controller)
    return std::unexpected(controller.error());
  result->controller_ = std::move(*controller);
  return result;
}

void TorrentMetadataAcquisition::stop() noexcept {
  if (controller_)
    controller_->stop();
}

} // namespace sakuin::integration

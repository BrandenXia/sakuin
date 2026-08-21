export module sakuin.integration.metadata_storage;

import std;

import sakuin.core.result;
import sakuin.dht.metadata_fetch;
import sakuin.model.torrent;
import sakuin.storage.dataset.torrents;
import sakuin.storage.dataset.write_session;

export namespace sakuin::integration {

// Persists a fetched record before acknowledging session completion. Catalog
// conflicts are retried from a newly pinned generation; other failures are
// retained for orchestration/diagnostics and propagated to the fetch session.
class TorrentMetadataSink final : public dht::MetadataFetchObserver {
public:
  explicit TorrentMetadataSink(
      storage::TorrentDataset &dataset,
      std::size_t maximum_conflict_attempts = 3,
      std::function<void(std::uint64_t)> on_torrent_committed = {});

  core::Result<void> on_metadata_fetched(model::TorrentRecord record) override;
  void on_metadata_fetch_failed(core::Error error) override;

  std::optional<std::uint64_t> last_generation() const;
  std::optional<core::Error> last_error() const;

private:
  storage::TorrentDataset *dataset_;
  std::size_t maximum_conflict_attempts_;
  std::function<void(std::uint64_t)> on_torrent_committed_;
  mutable std::mutex mutex_;
  std::optional<std::uint64_t> last_generation_;
  std::optional<core::Error> last_error_;
};

} // namespace sakuin::integration

namespace sakuin::integration {

TorrentMetadataSink::TorrentMetadataSink(
    storage::TorrentDataset &dataset, std::size_t maximum_conflict_attempts,
    std::function<void(std::uint64_t)> on_torrent_committed)
    : dataset_(&dataset), maximum_conflict_attempts_(maximum_conflict_attempts),
      on_torrent_committed_(std::move(on_torrent_committed)) {}

core::Result<void>
TorrentMetadataSink::on_metadata_fetched(model::TorrentRecord record) {
  if (maximum_conflict_attempts_ == 0)
    return std::unexpected(
        core::Error{core::ErrorCode::InvalidArgument,
                    "Metadata storage conflict-attempt limit must be nonzero"});

  for (std::size_t attempt = 0; attempt < maximum_conflict_attempts_;
       ++attempt) {
    auto committed = dataset_->enrich(record);
    if (committed) {
      {
        std::lock_guard lock{mutex_};
        last_generation_ = committed->generation;
        last_error_.reset();
      }
      if (on_torrent_committed_) {
        try {
          on_torrent_committed_(committed->generation);
        } catch (...) {
          // Persistence succeeded. A derived-index wakeup must not turn that
          // durable success into a metadata-fetch failure.
        }
      }
      return {};
    }
    if (committed.error().code != core::ErrorCode::Conflict ||
        attempt + 1 == maximum_conflict_attempts_) {
      std::lock_guard lock{mutex_};
      last_error_ = committed.error();
      return std::unexpected(committed.error());
    }
  }
  return std::unexpected(core::Error{core::ErrorCode::Internal,
                                     "Metadata storage retry loop failed"});
}

void TorrentMetadataSink::on_metadata_fetch_failed(core::Error error) {
  std::lock_guard lock{mutex_};
  last_error_ = std::move(error);
}

std::optional<std::uint64_t> TorrentMetadataSink::last_generation() const {
  std::lock_guard lock{mutex_};
  return last_generation_;
}

std::optional<core::Error> TorrentMetadataSink::last_error() const {
  std::lock_guard lock{mutex_};
  return last_error_;
}

} // namespace sakuin::integration

export module sakuin.integration.metadata_backfill;

import std;

import sakuin.core.result;
import sakuin.core.time;
import sakuin.dht.peer_discovery;
import sakuin.model.torrent;
import sakuin.storage.dataset.stream;
import sakuin.storage.dataset.torrents;

export namespace sakuin::integration {

struct MetadataDiscoveryBackfillOptions {
  std::size_t maximum_records_per_poll{256};
  core::Duration refresh_interval{std::chrono::minutes{1}};
  core::Duration full_rescan_interval{std::chrono::minutes{30}};
  core::Duration backpressure_retry_delay{std::chrono::seconds{1}};
};

struct MetadataDiscoveryBackfillStep {
  std::size_t records_scanned{};
  std::size_t targets_offered{};
  std::size_t records_with_metadata{};
  std::uint64_t source_generation{};
  bool full_rebuild{};
  bool scan_in_progress{};
  std::optional<core::Timestamp> next_wakeup;
};

// Incrementally maps canonical torrent placeholders back into the active DHT
// peer-discovery planner. A change cursor avoids rescanning an unchanged
// catalog, while compaction automatically falls back to a complete keyed scan.
class MetadataDiscoveryBackfill {
public:
  static core::Result<std::unique_ptr<MetadataDiscoveryBackfill>>
  create(storage::TorrentDataset &torrents,
         dht::PeerDiscoveryPlanner &discovery,
         MetadataDiscoveryBackfillOptions options = {});

  core::Result<MetadataDiscoveryBackfillStep> poll(core::Timestamp now);

private:
  struct ActiveScan {
    storage::TorrentChangeCursor cursor;
    std::unique_ptr<storage::RecordStream<model::TorrentRecord>> records;
    bool full_rebuild{};
  };

  MetadataDiscoveryBackfill(storage::TorrentDataset &torrents,
                            dht::PeerDiscoveryPlanner &discovery,
                            MetadataDiscoveryBackfillOptions options)
      : torrents_(&torrents), discovery_(&discovery), options_(options) {}

  storage::TorrentDataset *torrents_;
  dht::PeerDiscoveryPlanner *discovery_;
  MetadataDiscoveryBackfillOptions options_;
  storage::TorrentChangeCursor cursor_;
  std::optional<ActiveScan> scan_;
  std::optional<model::TorrentRecord> pending_;
  std::optional<core::Timestamp> next_scan_at_;
  std::optional<core::Timestamp> next_full_scan_at_;
};

} // namespace sakuin::integration

namespace sakuin::integration {

core::Result<std::unique_ptr<MetadataDiscoveryBackfill>>
MetadataDiscoveryBackfill::create(storage::TorrentDataset &torrents,
                                  dht::PeerDiscoveryPlanner &discovery,
                                  MetadataDiscoveryBackfillOptions options) {
  if (options.maximum_records_per_poll == 0 ||
      options.refresh_interval <= core::Duration::zero() ||
      options.full_rescan_interval <= core::Duration::zero() ||
      options.backpressure_retry_delay <= core::Duration::zero())
    return std::unexpected(
        core::Error{core::ErrorCode::InvalidArgument,
                    "Invalid metadata-discovery backfill limits"});
  return std::unique_ptr<MetadataDiscoveryBackfill>{
      new MetadataDiscoveryBackfill{torrents, discovery, options}};
}

core::Result<MetadataDiscoveryBackfillStep>
MetadataDiscoveryBackfill::poll(core::Timestamp now) {
  MetadataDiscoveryBackfillStep step;
  if (!scan_) {
    if (next_scan_at_ && now < *next_scan_at_) {
      step.source_generation = cursor_.source_generation;
      step.next_wakeup = next_scan_at_;
      return step;
    }
    const bool full_rescan_due =
        next_full_scan_at_ && now >= *next_full_scan_at_;
    auto changes = torrents_->changes_since(
        full_rescan_due ? storage::TorrentChangeCursor{} : cursor_);
    if (!changes)
      return std::unexpected(changes.error());
    scan_ = ActiveScan{.cursor = changes->cursor,
                       .records = std::move(changes->records),
                       .full_rebuild = changes->full_rebuild_required};
    next_scan_at_.reset();
  }

  step.source_generation = scan_->cursor.source_generation;
  step.full_rebuild = scan_->full_rebuild;
  for (std::size_t count = 0; count < options_.maximum_records_per_poll;
       ++count) {
    if (!pending_) {
      auto next = scan_->records->next();
      if (!next)
        return std::unexpected(next.error());
      if (!*next) {
        cursor_ = scan_->cursor;
        if (scan_->full_rebuild)
          next_full_scan_at_ =
              now + std::chrono::duration_cast<core::Timestamp::duration>(
                        options_.full_rescan_interval);
        scan_.reset();
        next_scan_at_ =
            now + std::chrono::duration_cast<core::Timestamp::duration>(
                      options_.refresh_interval);
        if (next_full_scan_at_ && *next_full_scan_at_ < *next_scan_at_)
          next_scan_at_ = next_full_scan_at_;
        step.next_wakeup = next_scan_at_;
        return step;
      }
      pending_ = std::move(**next);
      ++step.records_scanned;
    }

    if (pending_->name) {
      ++step.records_with_metadata;
      pending_.reset();
      continue;
    }
    auto offered = discovery_->offer(pending_->info_hash, now);
    if (!offered) {
      if (offered.error().code != core::ErrorCode::QuotaExceeded)
        return std::unexpected(offered.error());
      step.scan_in_progress = true;
      step.next_wakeup =
          now + std::chrono::duration_cast<core::Timestamp::duration>(
                    options_.backpressure_retry_delay);
      return step;
    }
    if (*offered)
      ++step.targets_offered;
    pending_.reset();
  }

  step.scan_in_progress = true;
  step.next_wakeup = now;
  return step;
}

} // namespace sakuin::integration

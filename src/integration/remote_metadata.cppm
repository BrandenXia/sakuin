export module sakuin.integration.remote_metadata;

import std;

import sakuin.core.result;
import sakuin.dht.metadata_fetch;
import sakuin.integration.work_results;
import sakuin.model.torrent;
import sakuin.scheduler.work;

export namespace sakuin::integration {

// Remote metadata completion sink. Records cross only the bounded logical
// result protocol; workers never receive a BlobStore, manifest, or dataset.
class RemoteTorrentMetadataSink final : public dht::MetadataFetchObserver {
public:
  RemoteTorrentMetadataSink(scheduler::WorkResultPublisher &publisher,
                            std::string worker,
                            std::size_t maximum_payload_bytes)
      : publisher_(&publisher), worker_(std::move(worker)),
        maximum_payload_bytes_(maximum_payload_bytes) {}

  core::Result<void> on_metadata_fetched(model::TorrentRecord record) override;
  void on_metadata_fetch_failed(core::Error error) override;
  std::optional<core::Error> last_error() const;

private:
  scheduler::WorkResultPublisher *publisher_;
  std::string worker_;
  std::size_t maximum_payload_bytes_{};
  mutable std::mutex mutex_;
  std::optional<core::Error> last_error_;
};

} // namespace sakuin::integration

namespace sakuin::integration {

core::Result<void>
RemoteTorrentMetadataSink::on_metadata_fetched(model::TorrentRecord record) {
  if (worker_.empty() || maximum_payload_bytes_ == 0)
    return std::unexpected(core::Error{core::ErrorCode::InvalidArgument,
                                       "Remote metadata sink is invalid"});
  auto payload =
      encode_torrent_metadata_result_batch(std::span{&record, std::size_t{1}});
  if (!payload)
    return std::unexpected(payload.error());
  if (payload->size() > maximum_payload_bytes_)
    return std::unexpected(core::Error{
        core::ErrorCode::QuotaExceeded,
        "Encoded torrent metadata exceeds the distributed payload limit"});
  auto batch = make_work_result_batch(
      scheduler::WorkResultKind::TorrentMetadataBatch, std::move(*payload));
  auto published = publisher_->publish_result(worker_, std::move(batch));
  if (!published) {
    std::lock_guard lock{mutex_};
    last_error_ = published.error();
    return std::unexpected(published.error());
  }
  std::lock_guard lock{mutex_};
  last_error_.reset();
  return {};
}

void RemoteTorrentMetadataSink::on_metadata_fetch_failed(core::Error error) {
  std::lock_guard lock{mutex_};
  last_error_ = std::move(error);
}

std::optional<core::Error> RemoteTorrentMetadataSink::last_error() const {
  std::lock_guard lock{mutex_};
  return last_error_;
}

} // namespace sakuin::integration

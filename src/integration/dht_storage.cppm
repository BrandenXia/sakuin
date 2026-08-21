export module sakuin.integration.dht_storage;

import std;

import sakuin.core.result;
import sakuin.dht.observation;
import sakuin.model.observation;
import sakuin.storage.dataset.observations;

export namespace sakuin::integration {

class BufferedObservationSink final : public dht::ObservationSink {
public:
  BufferedObservationSink(storage::ObservationDataset &dataset,
                          std::size_t batch_size);

  core::Result<void>
  observe(const model::ObservationRecord &observation) override;

  core::Result<void> flush();
  std::size_t pending() const noexcept { return pending_.size(); }

private:
  storage::ObservationDataset *dataset_;
  std::size_t batch_size_;
  std::vector<model::ObservationRecord> pending_;
};

} // namespace sakuin::integration

namespace sakuin::integration {

BufferedObservationSink::BufferedObservationSink(
    storage::ObservationDataset &dataset, std::size_t batch_size)
    : dataset_(&dataset), batch_size_(batch_size) {
  if (batch_size_ > 0)
    pending_.reserve(batch_size_);
}

core::Result<void> BufferedObservationSink::observe(
    const model::ObservationRecord &observation) {
  if (batch_size_ == 0)
    return std::unexpected(core::Error{core::ErrorCode::InvalidArgument,
                                       "Observation batch size must be nonzero"});
  pending_.push_back(observation);
  if (pending_.size() >= batch_size_)
    return flush();
  return {};
}

core::Result<void> BufferedObservationSink::flush() {
  if (pending_.empty())
    return {};
  auto write = dataset_->begin_write();
  if (!write)
    return std::unexpected(write.error());
  if (auto appended = (*write)->append(pending_); !appended)
    return std::unexpected(appended.error());
  if (auto committed = (*write)->commit(); !committed)
    return std::unexpected(committed.error());
  pending_.clear();
  return {};
}

} // namespace sakuin::integration

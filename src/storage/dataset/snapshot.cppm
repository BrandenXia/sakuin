export module sakuin.storage.dataset.snapshot;

import std;

import sakuin.core.result;
import sakuin.core.time;

import sakuin.storage.catalog.manifest;
import sakuin.storage.dataset.stream;

export namespace sakuin::storage {

template <typename Record, typename Query> class Snapshot {
public:
  virtual ~Snapshot() = default;

  SnapshotId id() const noexcept { return pin_->manifest().id; }

  virtual core::Timestamp created_at() const noexcept = 0;

  virtual core::Result<std::unique_ptr<RecordStream<Record>>>
  scan(const Query &query) const = 0;

protected:
  explicit Snapshot(std::shared_ptr<const ManifestPin> pin)
      : pin_(std::move(pin)) {}

  const Manifest &manifest() const noexcept { return pin_->manifest(); }

private:
  // This ownership is the GC barrier for the snapshot's repeatable-read view.
  std::shared_ptr<const ManifestPin> pin_;
};

template <typename Record, typename Key, typename Query>
class KeyedSnapshot : public Snapshot<Record, Query> {
public:
  using Snapshot<Record, Query>::Snapshot;

  virtual core::Result<std::optional<Record>> get(const Key &key) const = 0;
};

} // namespace sakuin::storage

export module sakuin.storage.dataset.dataset;

import std;

import sakuin.core.result;

import sakuin.storage.dataset.snapshot;
import sakuin.storage.dataset.write_session;

export namespace sakuin::storage {

template <typename Record, typename Query> class Dataset {
public:
  virtual ~Dataset() = default;

  // Captures a consistent point-in-time view.
  virtual core::Result<std::shared_ptr<const Snapshot<Record, Query>>>
  snapshot() const = 0;

  // Starts a new append/commit transaction.
  virtual core::Result<std::unique_ptr<WriteSession<Record>>> begin_write() = 0;
};

template <typename Record, typename Key, typename Query>
class KeyedDataset : public Dataset<Record, Query> {
public:
  virtual core::Result<
      std::shared_ptr<const KeyedSnapshot<Record, Key, Query>>>
  keyed_snapshot() const = 0;
};

} // namespace sakuin::storage

export module sakuin.storage.blob.store;

import std;

import sakuin.core.ids;
import sakuin.core.result;
import sakuin.storage.blob.reader;
import sakuin.storage.blob.writer;

export namespace sakuin::storage {

class BlobStore {
public:
  virtual ~BlobStore() = default;

  virtual core::Result<std::unique_ptr<BlobWriter>> create() = 0;

  virtual core::Result<std::unique_ptr<BlobReader>> open(core::ObjectId id) = 0;

  virtual core::Result<bool> exists(core::ObjectId id) = 0;

  virtual core::Result<void> remove(core::ObjectId id) = 0;
};

} // namespace sakuin::storage

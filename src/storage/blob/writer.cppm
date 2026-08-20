export module sakuin.storage.blob.writer;

import sakuin.core.bytes;
import sakuin.core.ids;
import sakuin.core.result;

export namespace sakuin::storage {

class BlobWriter {
public:
  virtual ~BlobWriter() = default;

  BlobWriter(const BlobWriter &) = delete;
  BlobWriter &operator=(const BlobWriter &) = delete;

  virtual core::Result<void> write(core::ByteView data) = 0;

  // The writer becomes invalid after successful finalize().
  virtual core::Result<core::ObjectId> finalize() = 0;

  // Discards the partially written object.
  virtual void abort() noexcept = 0;

protected:
  BlobWriter() = default;
};

} // namespace sakuin::storage

export module sakuin.storage.blob.reader;

import std;

import sakuin.core.bytes;
import sakuin.core.result;

export namespace sakuin::storage {

class BlobReader {
public:
  virtual ~BlobReader() = default;

  // Returns the number of bytes actually read.
  // 0 means EOF.
  virtual core::Result<std::size_t> read(core::MutableByteView buffer) = 0;

  virtual std::uint64_t size() const noexcept = 0;
};

} // namespace sakuin::storage

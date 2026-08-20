export module sakuin.storage.format.index;

import std;

import sakuin.core.bytes;
import sakuin.core.result;
import sakuin.storage.format.location;

export namespace sakuin::storage {

// Physical keys and index encodings remain format-specific. Implementations may
// sample keys at block boundaries or more frequently.
class SparseSegmentIndex {
public:
  virtual ~SparseSegmentIndex() = default;
  virtual core::Result<std::optional<RecordLocation>>
  locate(core::ByteView encoded_key) const = 0;
};

// A negative result is authoritative; a positive result requires index lookup.
class BloomFilter {
public:
  virtual ~BloomFilter() = default;
  virtual bool may_contain(core::ByteView encoded_key) const noexcept = 0;
};

} // namespace sakuin::storage

export module sakuin.storage.codec.record;

import std;

import sakuin.core.bytes;
import sakuin.core.result;

export namespace sakuin::storage {

struct CodecVersion {
  std::uint32_t value{1};

  friend bool operator==(const CodecVersion &, const CodecVersion &) = default;
};

template <typename Record> class RecordCodec {
public:
  virtual ~RecordCodec() = default;

  virtual CodecVersion version() const noexcept = 0;

  // Replaces output with the encoded record bytes.
  virtual core::Result<void> encode(const Record &record,
                                    core::ByteBuffer &output) const = 0;

  virtual core::Result<Record> decode(core::ByteView input) const = 0;
};

} // namespace sakuin::storage

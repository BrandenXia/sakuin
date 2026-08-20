export module sakuin.storage.codec.record;

import sakuin.core.bytes;
import sakuin.core.result;

export namespace sakuin::storage {

template <typename Record> class RecordCodec {
public:
  virtual ~RecordCodec() = default;

  virtual core::Result<void> encode(const Record &record,
                                    core::ByteBuffer &output) const = 0;

  virtual core::Result<Record> decode(core::ByteView input) const = 0;
};

} // namespace sakuin::storage

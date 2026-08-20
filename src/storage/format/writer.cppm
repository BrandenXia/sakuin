export module sakuin.storage.format.writer;

import sakuin.core.bytes;
import sakuin.core.result;

import sakuin.storage.format.segment;

export namespace sakuin::storage {

class SegmentWriter {
public:
  virtual ~SegmentWriter() = default;

  virtual core::Result<void> append(core::ByteView encoded_record) = 0;

  virtual core::Result<SegmentDescriptor> finalize() = 0;

  virtual void abort() noexcept = 0;
};

} // namespace sakuin::storage

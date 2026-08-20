export module sakuin.storage.format.reader;

import std;

import sakuin.core.bytes;
import sakuin.core.ids;
import sakuin.core.result;

import sakuin.storage.format.location;
import sakuin.storage.format.segment;

export namespace sakuin::storage {

class SegmentReader {
public:
  virtual ~SegmentReader() = default;

  // The key is in the segment's physical key encoding. No logical record or
  // model type crosses this boundary.
  virtual core::Result<std::optional<RecordLocation>>
  locate(core::ByteView encoded_key) const = 0;

  virtual core::Result<core::ByteBuffer>
  read(RecordLocation location) const = 0;
};

} // namespace sakuin::storage

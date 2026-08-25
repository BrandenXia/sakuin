export module sakuin.storage.dataset.stream;

import std;

import sakuin.core.result;

export namespace sakuin::storage {

template <typename Record> class RecordStream {
public:
  virtual ~RecordStream() = default;
  virtual core::Result<std::optional<Record>> next() = 0;
};

} // namespace sakuin::storage

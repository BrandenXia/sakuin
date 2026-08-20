export module sakuin.storage.format.location;

import std;

export namespace sakuin::storage {

struct RecordLocation {
  std::uint32_t block;
  std::uint32_t offset;
  std::uint32_t size;

  friend bool operator==(const RecordLocation &,
                         const RecordLocation &) = default;
};

} // namespace sakuin::storage

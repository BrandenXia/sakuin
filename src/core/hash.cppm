export module sakuin.core.hash;

import std;

import sakuin.core.bytes;

export namespace sakuin::core {

struct Hash256 {
  std::array<std::uint8_t, 32> bytes;

  friend bool operator==(const Hash256 &, const Hash256 &) = default;
};

Hash256 sha256(ByteView data);

} // namespace sakuin::core

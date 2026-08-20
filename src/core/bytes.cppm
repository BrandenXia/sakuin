export module sakuin.core.bytes;

import std;

export namespace sakuin::core {

using Byte = std::byte;

using ByteView = std::span<const Byte>;

using MutableByteView = std::span<Byte>;

using ByteBuffer = std::vector<Byte>;

} // namespace sakuin::core

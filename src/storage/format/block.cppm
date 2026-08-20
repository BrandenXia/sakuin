export module sakuin.storage.format.block;

import std;

export namespace sakuin::storage {

enum class CompressionCodec : std::uint8_t { None, Zstd };

// Blocks are independently compressed and are the unit of segment I/O.
struct BlockHeader {
  std::uint32_t record_count{};
  std::uint32_t uncompressed_size{};
  std::uint32_t compressed_size{};
  CompressionCodec compression{CompressionCodec::None};
  std::uint32_t checksum{};

  friend bool operator==(const BlockHeader &, const BlockHeader &) = default;
};

} // namespace sakuin::storage

module;

#include <zstd.h>

export module sakuin.storage.format.row_v1;

import std;

import sakuin.core.bytes;
import sakuin.core.hash;
import sakuin.core.ids;
import sakuin.core.result;
import sakuin.storage.blob.reader;
import sakuin.storage.blob.store;
import sakuin.storage.blob.writer;
import sakuin.storage.format.block;
import sakuin.storage.format.location;
import sakuin.storage.format.reader;
import sakuin.storage.format.segment;
import sakuin.storage.format.writer;

export namespace sakuin::storage {

struct RowV1WriterOptions {
  int compression_level{1};
};

class RowV1SegmentWriter final : public SegmentWriter {
public:
  static core::Result<std::unique_ptr<RowV1SegmentWriter>>
  create(BlobStore &store, SegmentHeader header = {},
         RowV1WriterOptions options = {});

  ~RowV1SegmentWriter() override;
  core::Result<void> append(core::ByteView encoded_record) override;
  core::Result<void> append_keyed(core::ByteView encoded_key,
                                  core::ByteView encoded_record);
  core::Result<SegmentDescriptor> finalize() override;
  void abort() noexcept override;

private:
  struct Impl;
  explicit RowV1SegmentWriter(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> impl_;
};

class RowV1SegmentReader final : public SegmentReader {
public:
  static core::Result<std::unique_ptr<RowV1SegmentReader>>
  open(BlobStore &store, core::ObjectId object);

  ~RowV1SegmentReader() override;
  core::Result<std::optional<RecordLocation>>
  locate(core::ByteView encoded_key) const override;
  core::Result<core::ByteBuffer> read(RecordLocation location) const override;
  std::uint64_t record_count() const noexcept override;
  core::Result<RecordLocation> location(std::uint64_t ordinal) const override;
  // Fully decompresses and validates every block. Opening a reader validates
  // framing and metadata; publishing callers use this stronger check before
  // making a segment reachable.
  core::Result<void> verify() const;

  const SegmentHeader &header() const noexcept;
  const SegmentFooter &footer() const noexcept;

private:
  struct Impl;
  explicit RowV1SegmentReader(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> impl_;
};

} // namespace sakuin::storage

namespace sakuin::storage {

struct RowV1BloomData {
  std::uint16_t hash_count{};
  std::uint64_t bit_count{};
  std::uint64_t record_count{};
  core::ByteBuffer bits;
};

struct BlockInfo {
  std::uint64_t payload_offset{};
  std::uint32_t record_count{};
  std::uint32_t uncompressed_size{};
  std::uint32_t compressed_size{};
  CompressionCodec compression{};
  std::uint32_t checksum{};
  std::uint64_t first_ordinal{};
};

struct SparseIndexEntry {
  core::ByteBuffer key;
  std::uint32_t block{};
};

namespace {

constexpr std::size_t LegacyHeaderSize = 24;
constexpr std::size_t HeaderSize = 28;
constexpr std::size_t HeaderPrefixSize = 12;
constexpr std::size_t BlockHeaderSize = 24;
constexpr std::size_t FooterSize = 56;
constexpr std::array<std::byte, 8> HeaderMagic{
    std::byte{'S'}, std::byte{'A'}, std::byte{'K'}, std::byte{'R'},
    std::byte{'O'}, std::byte{'W'}, std::byte{'1'}, std::byte{0}};
constexpr std::array<std::byte, 4> BlockMagic{std::byte{'B'}, std::byte{'L'},
                                              std::byte{'K'}, std::byte{'1'}};
constexpr std::array<std::byte, 8> FooterMagic{
    std::byte{'S'}, std::byte{'A'}, std::byte{'K'}, std::byte{'F'},
    std::byte{'T'}, std::byte{'R'}, std::byte{'1'}, std::byte{0}};
constexpr std::array<std::byte, 8> SparseIndexMagic{
    std::byte{'S'}, std::byte{'A'}, std::byte{'K'}, std::byte{'I'},
    std::byte{'D'}, std::byte{'X'}, std::byte{'1'}, std::byte{0}};
constexpr std::array<std::byte, 8> BloomFilterMagic{
    std::byte{'S'}, std::byte{'A'}, std::byte{'K'}, std::byte{'B'},
    std::byte{'L'}, std::byte{'M'}, std::byte{'1'}, std::byte{0}};

core::Error corrupt(std::string message) {
  return {core::ErrorCode::CorruptSegment, std::move(message)};
}

template <std::unsigned_integral Integer>
void append_integer(core::ByteBuffer &output, Integer value) {
  for (std::size_t index = 0; index < sizeof(Integer); ++index) {
    output.push_back(static_cast<std::byte>(value & 0xffU));
    if constexpr (sizeof(Integer) > 1)
      value >>= 8U;
  }
}

template <std::unsigned_integral Integer>
core::Result<Integer> read_integer(core::ByteView input,
                                   std::size_t &position) {
  if (position > input.size() || input.size() - position < sizeof(Integer))
    return std::unexpected(corrupt("Truncated RowV1 structure"));
  Integer value{};
  for (std::size_t index = 0; index < sizeof(Integer); ++index)
    value |= static_cast<Integer>(
                 std::to_integer<std::uint8_t>(input[position + index]))
             << (index * 8U);
  position += sizeof(Integer);
  return value;
}

std::uint32_t crc32c(core::ByteView input) {
  std::uint32_t crc = 0xffffffffU;
  for (const auto byte : input) {
    crc ^= std::to_integer<std::uint8_t>(byte);
    for (int bit = 0; bit < 8; ++bit)
      crc = (crc >> 1U) ^ (0x82f63b78U & (0U - (crc & 1U)));
  }
  return ~crc;
}

core::ByteBuffer encode_header(const SegmentHeader &header) {
  core::ByteBuffer output{HeaderMagic.begin(), HeaderMagic.end()};
  append_integer(output, header.format_version.major);
  append_integer(output, header.format_version.minor);
  append_integer(output, header.schema_id.value);
  append_integer(output, header.schema_version.value);
  append_integer(output, static_cast<std::uint8_t>(header.encoding));
  append_integer(output, static_cast<std::uint8_t>(header.tier));
  append_integer(output, static_cast<std::uint8_t>(header.compression));
  append_integer(output, std::uint8_t{});
  append_integer(output, header.target_block_size);
  return output;
}

core::Result<SegmentHeader> decode_header(core::ByteView input) {
  if ((input.size() != LegacyHeaderSize && input.size() != HeaderSize) ||
      !std::equal(HeaderMagic.begin(), HeaderMagic.end(), input.begin()))
    return std::unexpected(corrupt("Invalid RowV1 segment header"));
  std::size_t position = HeaderMagic.size();
  auto major = read_integer<std::uint16_t>(input, position);
  auto minor = read_integer<std::uint16_t>(input, position);
  core::Result<std::uint32_t> schema_id = std::uint32_t{};
  if (minor && *minor >= 1)
    schema_id = read_integer<std::uint32_t>(input, position);
  auto schema = read_integer<std::uint32_t>(input, position);
  auto encoding = read_integer<std::uint8_t>(input, position);
  auto tier = read_integer<std::uint8_t>(input, position);
  auto compression = read_integer<std::uint8_t>(input, position);
  auto reserved = read_integer<std::uint8_t>(input, position);
  auto block_size = read_integer<std::uint32_t>(input, position);
  if (!major || !minor || !schema_id || !schema || !encoding || !tier ||
      !compression || !reserved || !block_size)
    return std::unexpected(corrupt("Truncated RowV1 segment header"));
  if (*major != 1 || *minor > 3 ||
      (*minor == 0 && input.size() != LegacyHeaderSize) ||
      (*minor >= 1 && input.size() != HeaderSize) ||
      *encoding != static_cast<std::uint8_t>(SegmentEncoding::RowV1))
    return std::unexpected(core::Error{core::ErrorCode::UnsupportedFormat,
                                       "Unsupported segment format"});
  if (*reserved != 0 || *block_size == 0 ||
      *tier > static_cast<std::uint8_t>(SegmentTier::Cold) ||
      *compression > static_cast<std::uint8_t>(CompressionCodec::Zstd))
    return std::unexpected(corrupt("Invalid RowV1 header enum value"));
  return SegmentHeader{.format_version = {*major, *minor},
                       .schema_id = {*schema_id},
                       .schema_version = {*schema},
                       .encoding = static_cast<SegmentEncoding>(*encoding),
                       .tier = static_cast<SegmentTier>(*tier),
                       .compression =
                           static_cast<CompressionCodec>(*compression),
                       .target_block_size = *block_size};
}

core::Result<void> write_bytes(BlobWriter &writer, core::ByteView bytes) {
  return writer.write(bytes);
}

core::Result<void> read_exact(BlobReader &reader, std::uint64_t offset,
                              core::MutableByteView output) {
  std::size_t position{};
  while (position < output.size()) {
    auto count = reader.read_at(offset + position, output.subspan(position));
    if (!count)
      return std::unexpected(count.error());
    if (*count == 0)
      return std::unexpected(corrupt("Unexpected end of segment"));
    position += *count;
  }
  return {};
}

core::SegmentId segment_id(core::ObjectId object) {
  core::SegmentId id;
  std::copy_n(object.bytes.begin(), id.bytes.size(), id.bytes.begin());
  return id;
}

std::strong_ordering compare_bytes(core::ByteView left,
                                   core::ByteView right) noexcept {
  return std::lexicographical_compare_three_way(left.begin(), left.end(),
                                                right.begin(), right.end());
}

std::uint64_t hash_word(const core::Hash256 &hash, std::size_t offset) {
  std::uint64_t result{};
  for (std::size_t index = 0; index < sizeof(result); ++index)
    result |= static_cast<std::uint64_t>(hash.bytes[offset + index])
              << (index * 8U);
  return result;
}

bool bloom_may_contain(const RowV1BloomData &bloom,
                       core::ByteView key) noexcept {
  if (bloom.bit_count == 0 || bloom.hash_count == 0)
    return false;
  const auto digest = core::sha256(key);
  const auto first = hash_word(digest, 0);
  const auto second = hash_word(digest, 8) | 1U;
  for (std::uint16_t index = 0; index < bloom.hash_count; ++index) {
    const auto bit = (first + index * second) % bloom.bit_count;
    if ((std::to_integer<std::uint8_t>(bloom.bits[bit / 8]) &
         (1U << (bit % 8))) == 0)
      return false;
  }
  return true;
}

core::ByteBuffer
encode_sparse_index(std::span<const SparseIndexEntry> entries) {
  core::ByteBuffer output{SparseIndexMagic.begin(), SparseIndexMagic.end()};
  append_integer(output, std::uint16_t{1});
  append_integer(output, std::uint16_t{});
  append_integer(output, static_cast<std::uint32_t>(entries.size()));
  for (const auto &entry : entries) {
    append_integer(output, static_cast<std::uint32_t>(entry.key.size()));
    output.insert(output.end(), entry.key.begin(), entry.key.end());
    append_integer(output, entry.block);
  }
  append_integer(output, crc32c(output));
  return output;
}

core::Result<std::vector<SparseIndexEntry>>
decode_sparse_index(core::ByteView input) {
  if (input.size() < SparseIndexMagic.size() + 12 ||
      !std::equal(SparseIndexMagic.begin(), SparseIndexMagic.end(),
                  input.begin()))
    return std::unexpected(corrupt("Invalid RowV1 sparse index"));
  const auto checksum_offset = input.size() - sizeof(std::uint32_t);
  std::size_t checksum_position = checksum_offset;
  auto checksum = read_integer<std::uint32_t>(input, checksum_position);
  if (!checksum || crc32c(input.first(checksum_offset)) != *checksum)
    return std::unexpected(core::Error{core::ErrorCode::ChecksumMismatch,
                                       "RowV1 sparse-index checksum mismatch"});
  std::size_t position = SparseIndexMagic.size();
  auto version = read_integer<std::uint16_t>(input, position);
  auto reserved = read_integer<std::uint16_t>(input, position);
  auto count = read_integer<std::uint32_t>(input, position);
  if (!version || !reserved || !count || *version != 1 || *reserved != 0)
    return std::unexpected(corrupt("Invalid RowV1 sparse-index header"));
  constexpr std::size_t MinimumEntrySize = sizeof(std::uint32_t) * 2 + 1;
  if (*count > (checksum_offset - position) / MinimumEntrySize)
    return std::unexpected(
        corrupt("RowV1 sparse-index count exceeds its encoded size"));
  std::vector<SparseIndexEntry> result;
  result.reserve(*count);
  for (std::uint32_t index = 0; index < *count; ++index) {
    auto size = read_integer<std::uint32_t>(input, position);
    if (!size || *size == 0 || position > checksum_offset ||
        *size > checksum_offset - position)
      return std::unexpected(corrupt("Invalid RowV1 sparse-index key"));
    core::ByteBuffer key(input.begin() + position,
                         input.begin() + position + *size);
    position += *size;
    auto block = read_integer<std::uint32_t>(input, position);
    if (!block)
      return std::unexpected(block.error());
    if (!result.empty() &&
        compare_bytes(result.back().key, key) != std::strong_ordering::less)
      return std::unexpected(corrupt("RowV1 sparse-index keys are not sorted"));
    result.push_back({.key = std::move(key), .block = *block});
  }
  if (position != checksum_offset)
    return std::unexpected(corrupt("RowV1 sparse index has trailing bytes"));
  return result;
}

core::Result<core::ByteBuffer>
encode_bloom_filter(std::span<const core::Hash256> digests) {
  constexpr std::uint16_t hash_count = 7;
  if (digests.size() > (std::numeric_limits<std::uint64_t>::max() - 63) / 10)
    return std::unexpected(core::Error{core::ErrorCode::InvalidArgument,
                                       "RowV1 Bloom filter is too large"});
  const auto requested = std::max<std::uint64_t>(
      64, static_cast<std::uint64_t>(digests.size()) * 10U);
  const auto bit_count = (requested + 63U) / 64U * 64U;
  if (bit_count / 8U > std::numeric_limits<std::size_t>::max())
    return std::unexpected(core::Error{core::ErrorCode::InvalidArgument,
                                       "RowV1 Bloom filter is too large"});
  core::ByteBuffer bits(static_cast<std::size_t>(bit_count / 8U));
  for (const auto &digest : digests) {
    const auto first = hash_word(digest, 0);
    const auto second = hash_word(digest, 8) | 1U;
    for (std::uint16_t index = 0; index < hash_count; ++index) {
      const auto bit = (first + index * second) % bit_count;
      bits[bit / 8] |= static_cast<std::byte>(1U << (bit % 8));
    }
  }
  core::ByteBuffer output{BloomFilterMagic.begin(), BloomFilterMagic.end()};
  append_integer(output, std::uint16_t{1});
  append_integer(output, hash_count);
  append_integer(output, bit_count);
  append_integer(output, static_cast<std::uint64_t>(digests.size()));
  output.insert(output.end(), bits.begin(), bits.end());
  append_integer(output, crc32c(output));
  return output;
}

core::Result<RowV1BloomData> decode_bloom_filter(core::ByteView input) {
  if (input.size() < BloomFilterMagic.size() + 24 ||
      !std::equal(BloomFilterMagic.begin(), BloomFilterMagic.end(),
                  input.begin()))
    return std::unexpected(corrupt("Invalid RowV1 Bloom filter"));
  const auto checksum_offset = input.size() - sizeof(std::uint32_t);
  std::size_t checksum_position = checksum_offset;
  auto checksum = read_integer<std::uint32_t>(input, checksum_position);
  if (!checksum || crc32c(input.first(checksum_offset)) != *checksum)
    return std::unexpected(core::Error{core::ErrorCode::ChecksumMismatch,
                                       "RowV1 Bloom-filter checksum mismatch"});
  std::size_t position = BloomFilterMagic.size();
  auto version = read_integer<std::uint16_t>(input, position);
  auto hashes = read_integer<std::uint16_t>(input, position);
  auto bits = read_integer<std::uint64_t>(input, position);
  auto records = read_integer<std::uint64_t>(input, position);
  if (!version || !hashes || !bits || !records || *version != 1 ||
      *hashes == 0 || *hashes > 16 || *bits == 0 || *bits % 8 != 0 ||
      *bits / 8 != checksum_offset - position)
    return std::unexpected(corrupt("Invalid RowV1 Bloom-filter metadata"));
  core::ByteBuffer encoded(input.begin() + position,
                           input.begin() + checksum_offset);
  return RowV1BloomData{.hash_count = *hashes,
                        .bit_count = *bits,
                        .record_count = *records,
                        .bits = std::move(encoded)};
}

core::Result<BlockInfo> decode_block_header(core::ByteView input,
                                            std::uint64_t payload_offset,
                                            std::uint64_t first_ordinal) {
  if (input.size() != BlockHeaderSize ||
      !std::equal(BlockMagic.begin(), BlockMagic.end(), input.begin()))
    return std::unexpected(corrupt("Invalid RowV1 block header"));
  std::size_t position = BlockMagic.size();
  auto records = read_integer<std::uint32_t>(input, position);
  auto plain = read_integer<std::uint32_t>(input, position);
  auto compressed = read_integer<std::uint32_t>(input, position);
  auto codec = read_integer<std::uint8_t>(input, position);
  auto reserved = read_integer<std::uint8_t>(input, position);
  auto reserved_2 = read_integer<std::uint8_t>(input, position);
  auto reserved_3 = read_integer<std::uint8_t>(input, position);
  auto checksum = read_integer<std::uint32_t>(input, position);
  if (!records || !plain || !compressed || !codec || !reserved || !reserved_2 ||
      !reserved_3 || !checksum || *records == 0 || *plain == 0 ||
      *compressed == 0 || *reserved != 0 || *reserved_2 != 0 ||
      *reserved_3 != 0 ||
      *codec > static_cast<std::uint8_t>(CompressionCodec::Zstd))
    return std::unexpected(corrupt("Invalid RowV1 block metadata"));
  return BlockInfo{.payload_offset = payload_offset,
                   .record_count = *records,
                   .uncompressed_size = *plain,
                   .compressed_size = *compressed,
                   .compression = static_cast<CompressionCodec>(*codec),
                   .checksum = *checksum,
                   .first_ordinal = first_ordinal};
}

core::ByteBuffer encode_footer(const SegmentFooter &footer) {
  core::ByteBuffer output{FooterMagic.begin(), FooterMagic.end()};
  append_integer(output, footer.sparse_index_offset);
  append_integer(output, footer.sparse_index_size);
  append_integer(output, footer.bloom_filter_offset);
  append_integer(output, footer.bloom_filter_size);
  append_integer(output, footer.block_count);
  append_integer(output, footer.record_count);
  append_integer(output, crc32c(output));
  return output;
}

core::Result<SegmentFooter> decode_footer(core::ByteView input) {
  if (input.size() != FooterSize ||
      !std::equal(FooterMagic.begin(), FooterMagic.end(), input.begin()))
    return std::unexpected(corrupt("Invalid RowV1 segment footer"));
  std::size_t position = FooterMagic.size();
  SegmentFooter footer;
  auto index_offset = read_integer<std::uint64_t>(input, position);
  auto index_size = read_integer<std::uint64_t>(input, position);
  auto bloom_offset = read_integer<std::uint64_t>(input, position);
  auto bloom_size = read_integer<std::uint64_t>(input, position);
  auto blocks = read_integer<std::uint32_t>(input, position);
  auto records = read_integer<std::uint64_t>(input, position);
  auto checksum = read_integer<std::uint32_t>(input, position);
  if (!index_offset || !index_size || !bloom_offset || !bloom_size || !blocks ||
      !records || !checksum)
    return std::unexpected(corrupt("Truncated RowV1 footer"));
  if (crc32c(input.first(FooterSize - sizeof(std::uint32_t))) != *checksum)
    return std::unexpected(core::Error{core::ErrorCode::ChecksumMismatch,
                                       "RowV1 footer checksum mismatch"});
  footer.sparse_index_offset = *index_offset;
  footer.sparse_index_size = *index_size;
  footer.bloom_filter_offset = *bloom_offset;
  footer.bloom_filter_size = *bloom_size;
  footer.block_count = *blocks;
  footer.record_count = *records;
  footer.checksum = *checksum;
  return footer;
}

} // namespace

struct RowV1SegmentWriter::Impl {
  BlobStore *store{};
  std::unique_ptr<BlobWriter> blob;
  SegmentHeader header;
  RowV1WriterOptions options;
  core::ByteBuffer block;
  std::uint32_t block_records{};
  std::uint32_t block_count{};
  std::uint64_t record_count{};
  std::uint64_t logical_size{};
  std::uint64_t physical_size{};
  std::optional<core::ByteBuffer> block_first_key;
  std::optional<core::ByteBuffer> last_key;
  std::vector<SparseIndexEntry> sparse_index;
  std::vector<core::Hash256> bloom_digests;
  bool active{true};

  core::Result<void> flush_block() {
    if (block_records == 0)
      return {};
    if (block_count == std::numeric_limits<std::uint32_t>::max())
      return std::unexpected(core::Error{core::ErrorCode::InvalidArgument,
                                         "RowV1 block-count limit exceeded"});
    if (header.tier == SegmentTier::Warm && !block_first_key)
      return std::unexpected(
          corrupt("Indexed RowV1 block is missing its first physical key"));
    core::ByteBuffer payload;
    if (header.compression == CompressionCodec::None) {
      payload = block;
    } else {
      payload.resize(ZSTD_compressBound(block.size()));
      const auto size =
          ZSTD_compress(payload.data(), payload.size(), block.data(),
                        block.size(), options.compression_level);
      if (ZSTD_isError(size))
        return std::unexpected(
            core::Error{core::ErrorCode::Internal, ZSTD_getErrorName(size)});
      payload.resize(size);
    }
    if (block.size() > std::numeric_limits<std::uint32_t>::max() ||
        payload.size() > std::numeric_limits<std::uint32_t>::max())
      return std::unexpected(core::Error{core::ErrorCode::InvalidArgument,
                                         "RowV1 block exceeds size limit"});

    core::ByteBuffer encoded{BlockMagic.begin(), BlockMagic.end()};
    append_integer(encoded, block_records);
    append_integer(encoded, static_cast<std::uint32_t>(block.size()));
    append_integer(encoded, static_cast<std::uint32_t>(payload.size()));
    append_integer(encoded, static_cast<std::uint8_t>(header.compression));
    append_integer(encoded, std::uint8_t{});
    append_integer(encoded, std::uint8_t{});
    append_integer(encoded, std::uint8_t{});
    append_integer(encoded, crc32c(block));
    if (auto result = write_bytes(*blob, encoded); !result)
      return result;
    if (auto result = write_bytes(*blob, payload); !result)
      return result;
    physical_size += encoded.size() + payload.size();
    if (header.tier == SegmentTier::Warm) {
      sparse_index.push_back(
          {.key = std::move(*block_first_key), .block = block_count});
      block_first_key.reset();
    }
    ++block_count;
    block_records = 0;
    block.clear();
    return {};
  }
};

RowV1SegmentWriter::RowV1SegmentWriter(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}
RowV1SegmentWriter::~RowV1SegmentWriter() { abort(); }

core::Result<std::unique_ptr<RowV1SegmentWriter>>
RowV1SegmentWriter::create(BlobStore &store, SegmentHeader header,
                           RowV1WriterOptions options) {
  const bool hot = header.tier == SegmentTier::Hot &&
                   header.format_version == StorageFormatVersion{1, 1};
  const bool warm = header.tier == SegmentTier::Warm &&
                    header.format_version == StorageFormatVersion{1, 2};
  const bool cold = header.tier == SegmentTier::Cold &&
                    header.format_version == StorageFormatVersion{1, 3};
  if (header.encoding != SegmentEncoding::RowV1 || (!hot && !warm && !cold) ||
      header.target_block_size == 0 ||
      options.compression_level < ZSTD_minCLevel() ||
      options.compression_level > ZSTD_maxCLevel())
    return std::unexpected(core::Error{
        core::ErrorCode::InvalidArgument,
        "RowV1 writer requires HOT format 1.1, WARM format 1.2, or COLD "
        "format 1.3 and valid compression settings"});
  auto blob = store.create();
  if (!blob)
    return std::unexpected(blob.error());
  auto encoded = encode_header(header);
  if (auto written = (*blob)->write(encoded); !written) {
    (*blob)->abort();
    return std::unexpected(written.error());
  }
  auto impl = std::make_unique<Impl>();
  impl->store = &store;
  impl->blob = std::move(*blob);
  impl->header = header;
  impl->options = options;
  impl->physical_size = encoded.size();
  impl->block.reserve(header.target_block_size);
  return std::unique_ptr<RowV1SegmentWriter>{
      new RowV1SegmentWriter{std::move(impl)}};
}

core::Result<void> RowV1SegmentWriter::append(core::ByteView record) {
  if (!impl_ || !impl_->active)
    return std::unexpected(core::Error{core::ErrorCode::InvalidArgument,
                                       "Segment writer is no longer active"});
  if (record.size() > std::numeric_limits<std::uint32_t>::max())
    return std::unexpected(core::Error{core::ErrorCode::InvalidArgument,
                                       "Encoded record exceeds RowV1 limit"});
  if (impl_->record_count == std::numeric_limits<std::uint64_t>::max() ||
      record.size() >
          std::numeric_limits<std::uint64_t>::max() - impl_->logical_size)
    return std::unexpected(core::Error{core::ErrorCode::InvalidArgument,
                                       "RowV1 segment statistics overflow"});
  if (impl_->header.tier != SegmentTier::Hot &&
      impl_->header.tier != SegmentTier::Cold)
    return std::unexpected(
        core::Error{core::ErrorCode::InvalidArgument,
                    "WARM RowV1 records must be appended with a physical key"});
  const auto encoded_size = sizeof(std::uint32_t) + record.size();
  if (impl_->block_records > 0 &&
      impl_->block.size() + encoded_size > impl_->header.target_block_size) {
    if (auto flushed = impl_->flush_block(); !flushed)
      return flushed;
  }
  append_integer(impl_->block, static_cast<std::uint32_t>(record.size()));
  impl_->block.insert(impl_->block.end(), record.begin(), record.end());
  ++impl_->block_records;
  ++impl_->record_count;
  impl_->logical_size += record.size();
  return {};
}

core::Result<void> RowV1SegmentWriter::append_keyed(core::ByteView key,
                                                    core::ByteView record) {
  if (!impl_ || !impl_->active)
    return std::unexpected(core::Error{core::ErrorCode::InvalidArgument,
                                       "Segment writer is no longer active"});
  if (impl_->header.tier != SegmentTier::Warm || key.empty() ||
      key.size() > std::numeric_limits<std::uint32_t>::max() ||
      record.size() > std::numeric_limits<std::uint32_t>::max())
    return std::unexpected(core::Error{
        core::ErrorCode::InvalidArgument,
        "Keyed RowV1 append requires a WARM writer and bounded key/record"});
  if (impl_->record_count == std::numeric_limits<std::uint64_t>::max() ||
      record.size() >
          std::numeric_limits<std::uint64_t>::max() - impl_->logical_size)
    return std::unexpected(core::Error{core::ErrorCode::InvalidArgument,
                                       "RowV1 segment statistics overflow"});
  if (impl_->last_key &&
      compare_bytes(*impl_->last_key, key) != std::strong_ordering::less)
    return std::unexpected(
        core::Error{core::ErrorCode::InvalidArgument,
                    "WARM RowV1 physical keys must be strictly increasing"});
  const auto encoded_size =
      sizeof(std::uint32_t) * 2 + key.size() + record.size();
  if (impl_->block_records > 0 &&
      impl_->block.size() + encoded_size > impl_->header.target_block_size) {
    if (auto flushed = impl_->flush_block(); !flushed)
      return flushed;
  }
  if (impl_->block_records == 0)
    impl_->block_first_key = core::ByteBuffer{key.begin(), key.end()};
  append_integer(impl_->block, static_cast<std::uint32_t>(key.size()));
  append_integer(impl_->block, static_cast<std::uint32_t>(record.size()));
  impl_->block.insert(impl_->block.end(), key.begin(), key.end());
  impl_->block.insert(impl_->block.end(), record.begin(), record.end());
  impl_->last_key = core::ByteBuffer{key.begin(), key.end()};
  impl_->bloom_digests.push_back(core::sha256(key));
  ++impl_->block_records;
  ++impl_->record_count;
  impl_->logical_size += record.size();
  return {};
}

core::Result<SegmentDescriptor> RowV1SegmentWriter::finalize() {
  if (!impl_ || !impl_->active)
    return std::unexpected(core::Error{core::ErrorCode::InvalidArgument,
                                       "Segment writer is no longer active"});
  if (auto flushed = impl_->flush_block(); !flushed)
    return std::unexpected(flushed.error());
  SegmentFooter footer{.block_count = impl_->block_count,
                       .record_count = impl_->record_count};
  if (impl_->header.tier == SegmentTier::Warm) {
    auto sparse = encode_sparse_index(impl_->sparse_index);
    footer.sparse_index_offset = impl_->physical_size;
    footer.sparse_index_size = sparse.size();
    if (auto written = impl_->blob->write(sparse); !written)
      return std::unexpected(written.error());
    impl_->physical_size += sparse.size();

    auto bloom = encode_bloom_filter(impl_->bloom_digests);
    if (!bloom)
      return std::unexpected(bloom.error());
    footer.bloom_filter_offset = impl_->physical_size;
    footer.bloom_filter_size = bloom->size();
    if (auto written = impl_->blob->write(*bloom); !written)
      return std::unexpected(written.error());
    impl_->physical_size += bloom->size();
  }
  auto encoded_footer = encode_footer(footer);
  if (auto written = impl_->blob->write(encoded_footer); !written)
    return std::unexpected(written.error());
  impl_->physical_size += encoded_footer.size();
  auto object = impl_->blob->finalize();
  if (!object)
    return std::unexpected(object.error());
  impl_->active = false;
  return SegmentDescriptor{.id = segment_id(*object),
                           .object = *object,
                           .tier = impl_->header.tier,
                           .record_count = impl_->record_count,
                           .logical_size = impl_->logical_size,
                           .physical_size = impl_->physical_size,
                           .min_timestamp = std::nullopt,
                           .max_timestamp = std::nullopt,
                           .format_version = impl_->header.format_version,
                           .schema_id = impl_->header.schema_id,
                           .schema_version = impl_->header.schema_version,
                           .encoding = impl_->header.encoding,
                           .compression = impl_->header.compression};
}

void RowV1SegmentWriter::abort() noexcept {
  if (impl_ && impl_->active) {
    impl_->active = false;
    impl_->blob->abort();
  }
}

struct RowV1SegmentReader::Impl {
  std::unique_ptr<BlobReader> blob;
  SegmentHeader header;
  SegmentFooter footer;
  std::vector<BlockInfo> blocks;
  mutable std::mutex cache_mutex;
  mutable std::optional<std::uint32_t> cached_block;
  mutable core::ByteBuffer cached_data;
  mutable std::vector<RecordLocation> cached_locations;
  mutable std::vector<core::ByteBuffer> cached_keys;
  std::vector<SparseIndexEntry> sparse_index;
  RowV1BloomData bloom;

  // cache_mutex must be held by the caller so loading and consuming cached
  // locations are one atomic operation across concurrent readers.
  core::Result<void> load_block_locked(std::uint32_t index) const {
    if (cached_block == index)
      return {};
    if (index >= blocks.size())
      return std::unexpected(
          corrupt("Record location references invalid block"));
    const auto &info = blocks[index];
    core::ByteBuffer payload(info.compressed_size);
    if (auto read = read_exact(*blob, info.payload_offset, payload); !read)
      return read;
    core::ByteBuffer plain(info.uncompressed_size);
    if (info.compression == CompressionCodec::None) {
      if (payload.size() != plain.size())
        return std::unexpected(corrupt("Invalid uncompressed block size"));
      plain = std::move(payload);
    } else {
      const auto size = ZSTD_decompress(plain.data(), plain.size(),
                                        payload.data(), payload.size());
      if (ZSTD_isError(size) || size != plain.size())
        return std::unexpected(corrupt("Could not decompress RowV1 block"));
    }
    if (crc32c(plain) != info.checksum)
      return std::unexpected(core::Error{core::ErrorCode::ChecksumMismatch,
                                         "RowV1 block checksum mismatch"});
    std::vector<RecordLocation> locations;
    locations.reserve(info.record_count);
    std::vector<core::ByteBuffer> keys;
    if (header.tier == SegmentTier::Warm)
      keys.reserve(info.record_count);
    std::size_t position{};
    for (std::uint32_t record = 0; record < info.record_count; ++record) {
      std::uint32_t key_size{};
      if (header.tier == SegmentTier::Warm) {
        auto encoded_key_size = read_integer<std::uint32_t>(plain, position);
        if (!encoded_key_size || *encoded_key_size == 0)
          return std::unexpected(corrupt("Invalid RowV1 physical key"));
        key_size = *encoded_key_size;
      }
      auto size = read_integer<std::uint32_t>(plain, position);
      if (!size || key_size > plain.size() - position ||
          *size > plain.size() - position - key_size)
        return std::unexpected(corrupt("Invalid RowV1 record boundary"));
      if (header.tier == SegmentTier::Warm) {
        core::ByteBuffer key(plain.begin() + position,
                             plain.begin() + position + key_size);
        if (!keys.empty() &&
            compare_bytes(keys.back(), key) != std::strong_ordering::less)
          return std::unexpected(
              corrupt("RowV1 block physical keys are not sorted"));
        keys.push_back(std::move(key));
        position += key_size;
      }
      locations.push_back({.block = index,
                           .offset = static_cast<std::uint32_t>(position),
                           .size = *size});
      position += *size;
    }
    if (position != plain.size())
      return std::unexpected(corrupt("RowV1 block has trailing bytes"));
    cached_data = std::move(plain);
    cached_locations = std::move(locations);
    cached_keys = std::move(keys);
    cached_block = index;
    return {};
  }
};

RowV1SegmentReader::RowV1SegmentReader(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}
RowV1SegmentReader::~RowV1SegmentReader() = default;

core::Result<std::unique_ptr<RowV1SegmentReader>>
RowV1SegmentReader::open(BlobStore &store, core::ObjectId object) {
  auto blob = store.open(object);
  if (!blob)
    return std::unexpected(blob.error());
  if ((*blob)->size() < LegacyHeaderSize + FooterSize)
    return std::unexpected(corrupt("RowV1 segment is too small"));
  core::ByteBuffer prefix(HeaderPrefixSize);
  if (auto read = read_exact(**blob, 0, prefix); !read)
    return std::unexpected(read.error());
  if (!std::equal(HeaderMagic.begin(), HeaderMagic.end(), prefix.begin()))
    return std::unexpected(corrupt("Invalid RowV1 segment header"));
  std::size_t prefix_position = HeaderMagic.size();
  auto major = read_integer<std::uint16_t>(prefix, prefix_position);
  auto minor = read_integer<std::uint16_t>(prefix, prefix_position);
  if (!major || !minor || *major != 1 || *minor > 3)
    return std::unexpected(core::Error{core::ErrorCode::UnsupportedFormat,
                                       "Unsupported segment format"});
  const auto header_size = *minor == 0 ? LegacyHeaderSize : HeaderSize;
  core::ByteBuffer header_bytes(header_size);
  if (auto read = read_exact(**blob, 0, header_bytes); !read)
    return std::unexpected(read.error());
  auto header = decode_header(header_bytes);
  if (!header)
    return std::unexpected(header.error());
  core::ByteBuffer footer_bytes(FooterSize);
  const auto footer_offset = (*blob)->size() - FooterSize;
  if (auto read = read_exact(**blob, footer_offset, footer_bytes); !read)
    return std::unexpected(read.error());
  auto footer = decode_footer(footer_bytes);
  if (!footer)
    return std::unexpected(footer.error());
  std::uint64_t data_end = footer_offset;
  std::vector<SparseIndexEntry> sparse_index;
  RowV1BloomData bloom;
  if (header->tier == SegmentTier::Hot) {
    if (header->format_version != StorageFormatVersion{1, 0} &&
        header->format_version != StorageFormatVersion{1, 1})
      return std::unexpected(core::Error{core::ErrorCode::UnsupportedFormat,
                                         "Unsupported HOT RowV1 format"});
    if (footer->sparse_index_offset != 0 || footer->sparse_index_size != 0 ||
        footer->bloom_filter_offset != 0 || footer->bloom_filter_size != 0)
      return std::unexpected(corrupt("HOT RowV1 segment contains an index"));
  } else if (header->tier == SegmentTier::Warm) {
    if (header->format_version != StorageFormatVersion{1, 2})
      return std::unexpected(core::Error{core::ErrorCode::UnsupportedFormat,
                                         "Unsupported WARM RowV1 format"});
    if (footer->sparse_index_size == 0 || footer->bloom_filter_size == 0 ||
        footer->sparse_index_offset < header_size ||
        footer->sparse_index_offset > footer_offset ||
        footer->sparse_index_size >
            footer_offset - footer->sparse_index_offset ||
        footer->bloom_filter_offset !=
            footer->sparse_index_offset + footer->sparse_index_size ||
        footer->bloom_filter_offset > footer_offset ||
        footer->bloom_filter_size >
            footer_offset - footer->bloom_filter_offset ||
        footer->bloom_filter_offset + footer->bloom_filter_size !=
            footer_offset ||
        footer->sparse_index_size > std::numeric_limits<std::size_t>::max() ||
        footer->bloom_filter_size > std::numeric_limits<std::size_t>::max())
      return std::unexpected(corrupt("Invalid WARM RowV1 index regions"));
    data_end = footer->sparse_index_offset;
    core::ByteBuffer index_bytes(
        static_cast<std::size_t>(footer->sparse_index_size));
    if (auto read =
            read_exact(**blob, footer->sparse_index_offset, index_bytes);
        !read)
      return std::unexpected(read.error());
    auto decoded_index = decode_sparse_index(index_bytes);
    if (!decoded_index)
      return std::unexpected(decoded_index.error());
    sparse_index = std::move(*decoded_index);

    core::ByteBuffer bloom_bytes(
        static_cast<std::size_t>(footer->bloom_filter_size));
    if (auto read =
            read_exact(**blob, footer->bloom_filter_offset, bloom_bytes);
        !read)
      return std::unexpected(read.error());
    auto decoded_bloom = decode_bloom_filter(bloom_bytes);
    if (!decoded_bloom)
      return std::unexpected(decoded_bloom.error());
    if (decoded_bloom->record_count != footer->record_count)
      return std::unexpected(
          corrupt("RowV1 Bloom-filter record count does not match footer"));
    bloom = std::move(*decoded_bloom);
  } else {
    if (header->format_version != StorageFormatVersion{1, 3})
      return std::unexpected(core::Error{core::ErrorCode::UnsupportedFormat,
                                         "Unsupported COLD RowV1 format"});
    if (footer->sparse_index_offset != 0 || footer->sparse_index_size != 0 ||
        footer->bloom_filter_offset != 0 || footer->bloom_filter_size != 0)
      return std::unexpected(corrupt("COLD RowV1 segment contains an index"));
  }

  std::vector<BlockInfo> blocks;
  std::uint64_t offset = header_size;
  std::uint64_t ordinal{};
  while (offset < data_end) {
    if (data_end - offset < BlockHeaderSize)
      return std::unexpected(corrupt("Truncated RowV1 block header"));
    core::ByteBuffer bytes(BlockHeaderSize);
    if (auto read = read_exact(**blob, offset, bytes); !read)
      return std::unexpected(read.error());
    auto block = decode_block_header(bytes, offset + BlockHeaderSize, ordinal);
    if (!block)
      return std::unexpected(block.error());
    if (block->compression != header->compression)
      return std::unexpected(
          corrupt("RowV1 block compression does not match its header"));
    if (block->payload_offset > data_end ||
        block->compressed_size > data_end - block->payload_offset)
      return std::unexpected(corrupt("RowV1 block extends beyond data region"));
    ordinal += block->record_count;
    offset = block->payload_offset + block->compressed_size;
    blocks.push_back(*block);
  }
  if (offset != data_end || blocks.size() != footer->block_count ||
      ordinal != footer->record_count)
    return std::unexpected(
        corrupt("RowV1 footer statistics do not match blocks"));
  if (header->tier == SegmentTier::Warm) {
    if (sparse_index.size() != blocks.size())
      return std::unexpected(
          corrupt("RowV1 sparse-index block count does not match footer"));
    for (std::size_t index = 0; index < sparse_index.size(); ++index) {
      if (sparse_index[index].block != index)
        return std::unexpected(
            corrupt("RowV1 sparse index references an invalid block"));
    }
  }

  auto impl = std::make_unique<Impl>();
  impl->blob = std::move(*blob);
  impl->header = *header;
  impl->footer = *footer;
  impl->blocks = std::move(blocks);
  impl->sparse_index = std::move(sparse_index);
  impl->bloom = std::move(bloom);
  return std::unique_ptr<RowV1SegmentReader>{
      new RowV1SegmentReader{std::move(impl)}};
}

core::Result<std::optional<RecordLocation>>
RowV1SegmentReader::locate(core::ByteView key) const {
  if (impl_->header.tier != SegmentTier::Warm || key.empty() ||
      impl_->sparse_index.empty() || !bloom_may_contain(impl_->bloom, key))
    return std::optional<RecordLocation>{};
  const auto upper = std::upper_bound(
      impl_->sparse_index.begin(), impl_->sparse_index.end(), key,
      [](core::ByteView value, const SparseIndexEntry &entry) {
        return compare_bytes(value, entry.key) == std::strong_ordering::less;
      });
  if (upper == impl_->sparse_index.begin())
    return std::optional<RecordLocation>{};
  const auto block = std::prev(upper)->block;
  std::lock_guard lock{impl_->cache_mutex};
  if (auto loaded = impl_->load_block_locked(block); !loaded)
    return std::unexpected(loaded.error());
  const auto found = std::lower_bound(
      impl_->cached_keys.begin(), impl_->cached_keys.end(), key,
      [](const core::ByteBuffer &candidate, core::ByteView value) {
        return compare_bytes(candidate, value) == std::strong_ordering::less;
      });
  if (found == impl_->cached_keys.end() ||
      compare_bytes(*found, key) != std::strong_ordering::equal)
    return std::optional<RecordLocation>{};
  return std::optional<RecordLocation>{
      impl_
          ->cached_locations[std::distance(impl_->cached_keys.begin(), found)]};
}

core::Result<core::ByteBuffer>
RowV1SegmentReader::read(RecordLocation location) const {
  std::lock_guard lock{impl_->cache_mutex};
  if (auto loaded = impl_->load_block_locked(location.block); !loaded)
    return std::unexpected(loaded.error());
  const auto found = std::find(impl_->cached_locations.begin(),
                               impl_->cached_locations.end(), location);
  if (found == impl_->cached_locations.end())
    return std::unexpected(corrupt("Invalid RowV1 record location"));
  return core::ByteBuffer{impl_->cached_data.begin() + location.offset,
                          impl_->cached_data.begin() + location.offset +
                              location.size};
}

std::uint64_t RowV1SegmentReader::record_count() const noexcept {
  return impl_->footer.record_count;
}

core::Result<RecordLocation>
RowV1SegmentReader::location(std::uint64_t ordinal) const {
  if (ordinal >= impl_->footer.record_count)
    return std::unexpected(core::Error{core::ErrorCode::InvalidArgument,
                                       "Record ordinal is out of range"});
  const auto iterator =
      std::upper_bound(impl_->blocks.begin(), impl_->blocks.end(), ordinal,
                       [](std::uint64_t value, const BlockInfo &block) {
                         return value < block.first_ordinal;
                       });
  const auto block_index = static_cast<std::uint32_t>(
      std::distance(impl_->blocks.begin(), iterator) - 1);
  std::lock_guard lock{impl_->cache_mutex};
  if (auto loaded = impl_->load_block_locked(block_index); !loaded)
    return std::unexpected(loaded.error());
  return impl_
      ->cached_locations[ordinal - impl_->blocks[block_index].first_ordinal];
}

core::Result<void> RowV1SegmentReader::verify() const {
  std::optional<core::ByteBuffer> previous_last_key;
  std::lock_guard lock{impl_->cache_mutex};
  for (std::uint32_t index = 0; index < impl_->blocks.size(); ++index) {
    if (auto loaded = impl_->load_block_locked(index); !loaded)
      return loaded;
    if (impl_->header.tier == SegmentTier::Warm) {
      if (impl_->cached_keys.empty() ||
          compare_bytes(impl_->cached_keys.front(),
                        impl_->sparse_index[index].key) !=
              std::strong_ordering::equal)
        return std::unexpected(
            corrupt("RowV1 sparse-index key does not match its block"));
      if (previous_last_key &&
          compare_bytes(*previous_last_key, impl_->cached_keys.front()) !=
              std::strong_ordering::less)
        return std::unexpected(
            corrupt("RowV1 physical keys are not globally sorted"));
      for (const auto &key : impl_->cached_keys) {
        if (!bloom_may_contain(impl_->bloom, key))
          return std::unexpected(
              corrupt("RowV1 Bloom filter omits a stored physical key"));
      }
      previous_last_key = impl_->cached_keys.back();
    }
  }
  return {};
}

const SegmentHeader &RowV1SegmentReader::header() const noexcept {
  return impl_->header;
}
const SegmentFooter &RowV1SegmentReader::footer() const noexcept {
  return impl_->footer;
}

} // namespace sakuin::storage

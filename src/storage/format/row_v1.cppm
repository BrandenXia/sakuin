module;

#include <zstd.h>

export module sakuin.storage.format.row_v1;

import std;

import sakuin.core.bytes;
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

class RowV1SegmentWriter final : public SegmentWriter {
public:
  static core::Result<std::unique_ptr<RowV1SegmentWriter>>
  create(BlobStore &store, SegmentHeader header = {});

  ~RowV1SegmentWriter() override;
  core::Result<void> append(core::ByteView encoded_record) override;
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
namespace {

constexpr std::size_t LegacyHeaderSize = 24;
constexpr std::size_t HeaderSize = 28;
constexpr std::size_t HeaderPrefixSize = 12;
constexpr std::size_t BlockHeaderSize = 24;
constexpr std::size_t FooterSize = 56;
constexpr std::array<std::byte, 8> HeaderMagic{
    std::byte{'S'}, std::byte{'A'}, std::byte{'K'}, std::byte{'R'},
    std::byte{'O'}, std::byte{'W'}, std::byte{'1'}, std::byte{0}};
constexpr std::array<std::byte, 4> BlockMagic{
    std::byte{'B'}, std::byte{'L'}, std::byte{'K'}, std::byte{'1'}};
constexpr std::array<std::byte, 8> FooterMagic{
    std::byte{'S'}, std::byte{'A'}, std::byte{'K'}, std::byte{'F'},
    std::byte{'T'}, std::byte{'R'}, std::byte{'1'}, std::byte{0}};

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
core::Result<Integer> read_integer(core::ByteView input, std::size_t &position) {
  if (input.size() - position < sizeof(Integer))
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
  if (!major || !minor || !schema_id || !schema || !encoding || !tier || !compression ||
      !reserved || !block_size)
    return std::unexpected(corrupt("Truncated RowV1 segment header"));
  if (*major != 1 || *minor > 1 ||
      (*minor == 0 && input.size() != LegacyHeaderSize) ||
      (*minor == 1 && input.size() != HeaderSize) ||
      *encoding != static_cast<std::uint8_t>(SegmentEncoding::RowV1))
    return std::unexpected(core::Error{core::ErrorCode::UnsupportedFormat,
                                       "Unsupported segment format"});
  if (*tier > static_cast<std::uint8_t>(SegmentTier::Cold) ||
      *compression > static_cast<std::uint8_t>(CompressionCodec::Zstd))
    return std::unexpected(corrupt("Invalid RowV1 header enum value"));
  return SegmentHeader{.format_version = {*major, *minor},
                       .schema_id = {*schema_id},
                       .schema_version = {*schema},
                       .encoding = static_cast<SegmentEncoding>(*encoding),
                       .tier = static_cast<SegmentTier>(*tier),
                       .compression = static_cast<CompressionCodec>(*compression),
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

struct BlockInfo {
  std::uint64_t payload_offset{};
  std::uint32_t record_count{};
  std::uint32_t uncompressed_size{};
  std::uint32_t compressed_size{};
  CompressionCodec compression{};
  std::uint32_t checksum{};
  std::uint64_t first_ordinal{};
};

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
  position += 3;
  auto checksum = read_integer<std::uint32_t>(input, position);
  if (!records || !plain || !compressed || !codec || !checksum || *records == 0 ||
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
  core::ByteBuffer block;
  std::uint32_t block_records{};
  std::uint32_t block_count{};
  std::uint64_t record_count{};
  std::uint64_t logical_size{};
  std::uint64_t physical_size{};
  bool active{true};

  core::Result<void> flush_block() {
    if (block_records == 0)
      return {};
    core::ByteBuffer payload;
    if (header.compression == CompressionCodec::None) {
      payload = block;
    } else {
      payload.resize(ZSTD_compressBound(block.size()));
      const auto size = ZSTD_compress(payload.data(), payload.size(), block.data(),
                                      block.size(), 1);
      if (ZSTD_isError(size))
        return std::unexpected(core::Error{core::ErrorCode::Internal,
                                           ZSTD_getErrorName(size)});
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
RowV1SegmentWriter::create(BlobStore &store, SegmentHeader header) {
  if (header.encoding != SegmentEncoding::RowV1 ||
      header.tier != SegmentTier::Hot || header.target_block_size == 0 ||
      header.format_version != StorageFormatVersion{1, 1})
    return std::unexpected(core::Error{
        core::ErrorCode::InvalidArgument,
        "RowV1 writer requires HOT tier, format 1.1, and nonzero block size"});
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

core::Result<SegmentDescriptor> RowV1SegmentWriter::finalize() {
  if (!impl_ || !impl_->active)
    return std::unexpected(core::Error{core::ErrorCode::InvalidArgument,
                                       "Segment writer is no longer active"});
  if (auto flushed = impl_->flush_block(); !flushed)
    return std::unexpected(flushed.error());
  SegmentFooter footer{.block_count = impl_->block_count,
                       .record_count = impl_->record_count};
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

  core::Result<void> load_block(std::uint32_t index) const {
    std::lock_guard lock{cache_mutex};
    if (cached_block == index)
      return {};
    if (index >= blocks.size())
      return std::unexpected(corrupt("Record location references invalid block"));
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
      const auto size = ZSTD_decompress(plain.data(), plain.size(), payload.data(),
                                        payload.size());
      if (ZSTD_isError(size) || size != plain.size())
        return std::unexpected(corrupt("Could not decompress RowV1 block"));
    }
    if (crc32c(plain) != info.checksum)
      return std::unexpected(core::Error{core::ErrorCode::ChecksumMismatch,
                                         "RowV1 block checksum mismatch"});
    std::vector<RecordLocation> locations;
    locations.reserve(info.record_count);
    std::size_t position{};
    for (std::uint32_t record = 0; record < info.record_count; ++record) {
      auto size = read_integer<std::uint32_t>(plain, position);
      if (!size || plain.size() - position < *size)
        return std::unexpected(corrupt("Invalid RowV1 record boundary"));
      locations.push_back({.block = index,
                           .offset = static_cast<std::uint32_t>(position),
                           .size = *size});
      position += *size;
    }
    if (position != plain.size())
      return std::unexpected(corrupt("RowV1 block has trailing bytes"));
    cached_data = std::move(plain);
    cached_locations = std::move(locations);
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
  if (!major || !minor || *major != 1 || *minor > 1)
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
  if (footer->sparse_index_size != 0 || footer->bloom_filter_size != 0)
    return std::unexpected(core::Error{core::ErrorCode::UnsupportedFormat,
                                       "Indexed RowV1 segments are not supported yet"});

  std::vector<BlockInfo> blocks;
  std::uint64_t offset = header_size;
  std::uint64_t ordinal{};
  while (offset < footer_offset) {
    if (footer_offset - offset < BlockHeaderSize)
      return std::unexpected(corrupt("Truncated RowV1 block header"));
    core::ByteBuffer bytes(BlockHeaderSize);
    if (auto read = read_exact(**blob, offset, bytes); !read)
      return std::unexpected(read.error());
    auto block = decode_block_header(bytes, offset + BlockHeaderSize, ordinal);
    if (!block)
      return std::unexpected(block.error());
    if (block->compressed_size > footer_offset - block->payload_offset)
      return std::unexpected(corrupt("RowV1 block extends beyond footer"));
    ordinal += block->record_count;
    offset = block->payload_offset + block->compressed_size;
    blocks.push_back(*block);
  }
  if (offset != footer_offset || blocks.size() != footer->block_count ||
      ordinal != footer->record_count)
    return std::unexpected(corrupt("RowV1 footer statistics do not match blocks"));

  auto impl = std::make_unique<Impl>();
  impl->blob = std::move(*blob);
  impl->header = *header;
  impl->footer = *footer;
  impl->blocks = std::move(blocks);
  return std::unique_ptr<RowV1SegmentReader>{
      new RowV1SegmentReader{std::move(impl)}};
}

core::Result<std::optional<RecordLocation>>
RowV1SegmentReader::locate(core::ByteView) const {
  return std::optional<RecordLocation>{};
}

core::Result<core::ByteBuffer>
RowV1SegmentReader::read(RecordLocation location) const {
  if (auto loaded = impl_->load_block(location.block); !loaded)
    return std::unexpected(loaded.error());
  std::lock_guard lock{impl_->cache_mutex};
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
  const auto iterator = std::upper_bound(
      impl_->blocks.begin(), impl_->blocks.end(), ordinal,
      [](std::uint64_t value, const BlockInfo &block) {
        return value < block.first_ordinal;
      });
  const auto block_index = static_cast<std::uint32_t>(
      std::distance(impl_->blocks.begin(), iterator) - 1);
  if (auto loaded = impl_->load_block(block_index); !loaded)
    return std::unexpected(loaded.error());
  std::lock_guard lock{impl_->cache_mutex};
  return impl_->cached_locations[ordinal - impl_->blocks[block_index].first_ordinal];
}

core::Result<void> RowV1SegmentReader::verify() const {
  for (std::uint32_t index = 0; index < impl_->blocks.size(); ++index) {
    if (auto loaded = impl_->load_block(index); !loaded)
      return loaded;
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

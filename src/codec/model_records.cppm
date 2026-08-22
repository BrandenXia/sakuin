export module sakuin.storage.codec.model_records;

import std;

import sakuin.core.bytes;
import sakuin.core.ids;
import sakuin.core.result;
import sakuin.core.time;
import sakuin.model.observation;
import sakuin.model.torrent;
import sakuin.storage.codec.record;
import sakuin.storage.format.segment;

export namespace sakuin::storage {

inline constexpr SchemaId ObservationRecordSchema{1};
inline constexpr SchemaId TorrentRecordSchema{2};

struct TorrentRecordDecodeLimits {
  std::size_t maximum_record_bytes{64U * 1024U * 1024U};
  std::size_t maximum_files{1'000'000};
  std::size_t maximum_name_bytes{4U * 1024U * 1024U};
  std::size_t maximum_path_bytes{4U * 1024U * 1024U};
};

class ObservationRecordCodec final
    : public RecordCodec<model::ObservationRecord> {
public:
  CodecVersion version() const noexcept override { return {1}; }

  core::Result<void> encode(const model::ObservationRecord &record,
                            core::ByteBuffer &output) const override;
  core::Result<model::ObservationRecord>
  decode(core::ByteView input) const override;
};

class TorrentRecordCodec final : public RecordCodec<model::TorrentRecord> {
public:
  CodecVersion version() const noexcept override { return {1}; }

  core::Result<void> encode(const model::TorrentRecord &record,
                            core::ByteBuffer &output) const override;
  core::Result<model::TorrentRecord>
  decode(core::ByteView input) const override;
  core::Result<model::TorrentRecord>
  decode(core::ByteView input, TorrentRecordDecodeLimits limits) const;
};

} // namespace sakuin::storage

namespace sakuin::storage {
namespace {

core::Error invalid_record(std::string message) {
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

void append_timestamp(core::ByteBuffer &output, core::Timestamp value) {
  const auto ticks = std::chrono::duration_cast<std::chrono::nanoseconds>(
                         value.time_since_epoch())
                         .count();
  append_integer(output, std::bit_cast<std::uint64_t>(ticks));
}

core::Result<void> append_string(core::ByteBuffer &output,
                                 const std::string &value) {
  if (value.size() > std::numeric_limits<std::uint32_t>::max())
    return std::unexpected(core::Error{core::ErrorCode::InvalidArgument,
                                       "String exceeds RowV1 length limit"});
  append_integer(output, static_cast<std::uint32_t>(value.size()));
  const auto bytes = std::as_bytes(std::span{value});
  output.insert(output.end(), bytes.begin(), bytes.end());
  return {};
}

class Decoder {
public:
  explicit Decoder(core::ByteView input) : input_(input) {}

  template <std::unsigned_integral Integer> core::Result<Integer> integer() {
    if (remaining() < sizeof(Integer))
      return std::unexpected(invalid_record("Encoded record is truncated"));
    Integer value{};
    for (std::size_t index = 0; index < sizeof(Integer); ++index) {
      value |= static_cast<Integer>(
                   std::to_integer<std::uint8_t>(input_[position_ + index]))
               << (index * 8U);
    }
    position_ += sizeof(Integer);
    return value;
  }

  core::Result<core::Timestamp> timestamp() {
    auto encoded = integer<std::uint64_t>();
    if (!encoded)
      return std::unexpected(encoded.error());
    const auto ticks = std::bit_cast<std::int64_t>(*encoded);
    return core::Timestamp{
        std::chrono::duration_cast<core::Timestamp::duration>(
            std::chrono::nanoseconds{ticks})};
  }

  core::Result<std::string>
  string(std::size_t maximum_bytes = std::numeric_limits<std::size_t>::max()) {
    auto length = integer<std::uint32_t>();
    if (!length)
      return std::unexpected(length.error());
    if (*length > maximum_bytes)
      return std::unexpected(invalid_record("Encoded string exceeds limit"));
    if (remaining() < *length)
      return std::unexpected(invalid_record("Encoded string is truncated"));
    const auto *begin =
        reinterpret_cast<const char *>(input_.data() + position_);
    std::string value{begin, begin + *length};
    position_ += *length;
    return value;
  }

  template <std::size_t Size>
  core::Result<std::array<std::uint8_t, Size>> byte_array() {
    if (remaining() < Size)
      return std::unexpected(invalid_record("Encoded byte array is truncated"));
    std::array<std::uint8_t, Size> value{};
    for (std::size_t index = 0; index < Size; ++index)
      value[index] = std::to_integer<std::uint8_t>(input_[position_ + index]);
    position_ += Size;
    return value;
  }

  bool finished() const noexcept { return position_ == input_.size(); }

private:
  std::size_t remaining() const noexcept { return input_.size() - position_; }

  core::ByteView input_;
  std::size_t position_{};
};

void append_info_hash(core::ByteBuffer &output, const core::InfoHash &hash) {
  for (const auto byte : hash.bytes)
    output.push_back(static_cast<std::byte>(byte));
}

core::Result<core::InfoHash> decode_info_hash(Decoder &decoder) {
  auto bytes = decoder.byte_array<20>();
  if (!bytes)
    return std::unexpected(bytes.error());
  return core::InfoHash{.bytes = *bytes};
}

} // namespace

core::Result<void>
ObservationRecordCodec::encode(const model::ObservationRecord &record,
                               core::ByteBuffer &output) const {
  output.clear();
  output.reserve(28);
  append_info_hash(output, record.info_hash);
  append_timestamp(output, record.observed_at);
  return {};
}

core::Result<model::ObservationRecord>
ObservationRecordCodec::decode(core::ByteView input) const {
  Decoder decoder{input};
  auto info_hash = decode_info_hash(decoder);
  if (!info_hash)
    return std::unexpected(info_hash.error());
  auto observed_at = decoder.timestamp();
  if (!observed_at)
    return std::unexpected(observed_at.error());
  if (!decoder.finished())
    return std::unexpected(
        invalid_record("Encoded observation has trailing bytes"));
  return model::ObservationRecord{.info_hash = *info_hash,
                                  .observed_at = *observed_at};
}

core::Result<void>
TorrentRecordCodec::encode(const model::TorrentRecord &record,
                           core::ByteBuffer &output) const {
  if (record.files.size() > std::numeric_limits<std::uint32_t>::max())
    return std::unexpected(core::Error{core::ErrorCode::InvalidArgument,
                                       "File count exceeds RowV1 limit"});

  output.clear();
  append_info_hash(output, record.info_hash);
  append_timestamp(output, record.first_seen);
  append_timestamp(output, record.last_seen);
  output.push_back(record.name ? std::byte{1} : std::byte{0});
  if (record.name) {
    if (auto encoded = append_string(output, *record.name); !encoded)
      return encoded;
  }
  append_integer(output, record.total_size);
  append_integer(output, static_cast<std::uint32_t>(record.files.size()));
  for (const auto &file : record.files) {
    if (auto encoded = append_string(output, file.path); !encoded)
      return encoded;
    append_integer(output, file.size);
  }
  return {};
}

core::Result<model::TorrentRecord>
TorrentRecordCodec::decode(core::ByteView input) const {
  return decode(input, {});
}

core::Result<model::TorrentRecord>
TorrentRecordCodec::decode(core::ByteView input,
                           TorrentRecordDecodeLimits limits) const {
  if (limits.maximum_record_bytes == 0 || limits.maximum_files == 0 ||
      limits.maximum_name_bytes == 0 || limits.maximum_path_bytes == 0 ||
      input.size() > limits.maximum_record_bytes)
    return std::unexpected(
        invalid_record("Encoded torrent record exceeds decode limits"));
  Decoder decoder{input};
  auto info_hash = decode_info_hash(decoder);
  auto first_seen = decoder.timestamp();
  auto last_seen = decoder.timestamp();
  auto has_name = decoder.integer<std::uint8_t>();
  if (!info_hash || !first_seen || !last_seen || !has_name)
    return std::unexpected(
        invalid_record("Encoded torrent header is truncated"));
  if (*has_name > 1)
    return std::unexpected(invalid_record("Invalid optional-name marker"));

  std::optional<std::string> name;
  if (*has_name == 1) {
    auto decoded_name = decoder.string(limits.maximum_name_bytes);
    if (!decoded_name)
      return std::unexpected(decoded_name.error());
    name = std::move(*decoded_name);
  }

  auto total_size = decoder.integer<std::uint64_t>();
  auto file_count = decoder.integer<std::uint32_t>();
  if (!total_size || !file_count)
    return std::unexpected(invalid_record("Encoded torrent body is truncated"));
  if (*file_count > limits.maximum_files)
    return std::unexpected(invalid_record("Encoded file count exceeds limit"));

  std::vector<model::FileRecord> files;
  files.reserve(*file_count);
  for (std::uint32_t index = 0; index < *file_count; ++index) {
    auto path = decoder.string(limits.maximum_path_bytes);
    auto size = decoder.integer<std::uint64_t>();
    if (!path || !size)
      return std::unexpected(invalid_record("Encoded file entry is truncated"));
    files.push_back({.path = std::move(*path), .size = *size});
  }
  if (!decoder.finished())
    return std::unexpected(
        invalid_record("Encoded torrent has trailing bytes"));

  return model::TorrentRecord{
      .info_hash = *info_hash,
      .first_seen = *first_seen,
      .last_seen = *last_seen,
      .name = std::move(name),
      .total_size = *total_size,
      .files = std::move(files),
  };
}

} // namespace sakuin::storage

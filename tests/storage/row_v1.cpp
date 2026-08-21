import std;

import sakuin.core;
import sakuin.storage;

namespace {

struct TemporaryDirectory {
  std::filesystem::path path;
  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path, ignored);
  }
};

std::string object_name(const sakuin::core::ObjectId &id) {
  static constexpr char digits[] = "0123456789abcdef";
  std::string result(id.bytes.size() * 2, '0');
  for (std::size_t index = 0; index < id.bytes.size(); ++index) {
    result[index * 2] = digits[id.bytes[index] >> 4];
    result[index * 2 + 1] = digits[id.bytes[index] & 0x0f];
  }
  return result;
}

sakuin::core::ByteBuffer bytes(std::string_view value) {
  sakuin::core::ByteBuffer result;
  const auto view = std::as_bytes(std::span{value});
  result.assign(view.begin(), view.end());
  return result;
}

} // namespace

int main() {
  using namespace sakuin;

  const auto nonce = std::to_string(
      std::chrono::steady_clock::now().time_since_epoch().count());
  TemporaryDirectory directory{
      std::filesystem::temp_directory_path() / ("sakuin-row-test-" + nonce)};
  storage::LocalBlobStore store{directory.path};

  storage::SegmentHeader header;
  header.target_block_size = 18;
  auto writer = storage::RowV1SegmentWriter::create(store, header);
  if (!writer)
    return 1;

  const std::array records{bytes("first"), bytes("second-record"), bytes("third")};
  for (const auto &record : records) {
    if (!(*writer)->append(record))
      return 2;
  }
  auto descriptor = (*writer)->finalize();
  if (!descriptor || descriptor->record_count != records.size() ||
      descriptor->compression != storage::CompressionCodec::Zstd)
    return 3;

  auto reader = storage::RowV1SegmentReader::open(store, descriptor->object);
  if (!reader || (*reader)->record_count() != records.size() ||
      (*reader)->footer().block_count < 2 || !(*reader)->verify())
    return 4;
  for (std::uint64_t ordinal = 0; ordinal < records.size(); ++ordinal) {
    auto location = (*reader)->location(ordinal);
    if (!location)
      return 5;
    auto record = (*reader)->read(*location);
    if (!record || *record != records[ordinal])
      return 6;
  }
  if ((*reader)->location(records.size()) ||
      (*reader)->locate(bytes("key")).value().has_value())
    return 7;

  storage::SegmentHeader plain_header;
  plain_header.compression = storage::CompressionCodec::None;
  auto plain_writer = storage::RowV1SegmentWriter::create(store, plain_header);
  if (!plain_writer || !(*plain_writer)->append(records[0]))
    return 8;
  auto plain = (*plain_writer)->finalize();
  if (!plain)
    return 9;

  const auto name = object_name(plain->object);
  const auto path = directory.path / "objects" / name.substr(0, 2) / name;
  {
    std::fstream file{path, std::ios::binary | std::ios::in | std::ios::out};
    if (!file)
      return 10;
    constexpr std::streamoff first_payload = 28 + 24;
    file.seekg(first_payload);
    char value{};
    file.read(&value, 1);
    value ^= 0x01;
    file.seekp(first_payload);
    file.write(&value, 1);
  }
  auto corrupted_reader = storage::RowV1SegmentReader::open(store, plain->object);
  if (!corrupted_reader)
    return 11;
  auto verification = (*corrupted_reader)->verify();
  if (verification ||
      verification.error().code != core::ErrorCode::ChecksumMismatch)
    return 12;
  auto location = (*corrupted_reader)->location(0);
  if (location || location.error().code != core::ErrorCode::ChecksumMismatch)
    return 13;

  return 0;
}

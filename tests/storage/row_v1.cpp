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
  TemporaryDirectory directory{std::filesystem::temp_directory_path() /
                               ("sakuin-row-test-" + nonce)};
  storage::LocalBlobStore store{directory.path};

  storage::SegmentHeader header;
  header.target_block_size = 18;
  auto writer = storage::RowV1SegmentWriter::create(store, header);
  if (!writer)
    return 1;

  const std::array records{bytes("first"), bytes("second-record"),
                           bytes("third")};
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
  auto corrupted_reader =
      storage::RowV1SegmentReader::open(store, plain->object);
  if (!corrupted_reader)
    return 11;
  auto verification = (*corrupted_reader)->verify();
  if (verification ||
      verification.error().code != core::ErrorCode::ChecksumMismatch)
    return 12;
  auto location = (*corrupted_reader)->location(0);
  if (location || location.error().code != core::ErrorCode::ChecksumMismatch)
    return 13;

  storage::SegmentHeader warm_header{
      .format_version = {1, 2},
      .tier = storage::SegmentTier::Warm,
      .compression = storage::CompressionCodec::Zstd,
      .target_block_size = 24,
  };
  auto warm_writer = storage::RowV1SegmentWriter::create(
      store, warm_header, storage::RowV1WriterOptions{.compression_level = 5});
  if (!warm_writer || (*warm_writer)->append(records[0]))
    return 20;
  const std::array keys{bytes("alpha"), bytes("bravo"), bytes("charlie")};
  for (std::size_t index = 0; index < keys.size(); ++index) {
    if (!(*warm_writer)->append_keyed(keys[index], records[index]))
      return 21;
    if (index == 0 && (*warm_writer)->append_keyed(keys[index], records[index]))
      return 22;
  }
  auto warm = (*warm_writer)->finalize();
  if (!warm || warm->tier != storage::SegmentTier::Warm ||
      warm->format_version != storage::StorageFormatVersion{1, 2})
    return 23;
  auto warm_reader = storage::RowV1SegmentReader::open(store, warm->object);
  if (!warm_reader || (*warm_reader)->footer().sparse_index_size == 0 ||
      (*warm_reader)->footer().bloom_filter_size == 0 ||
      (*warm_reader)->footer().block_count < 2 || !(*warm_reader)->verify())
    return 24;
  for (std::size_t index = 0; index < keys.size(); ++index) {
    auto found = (*warm_reader)->locate(keys[index]);
    if (!found || !*found)
      return 25;
    auto record = (*warm_reader)->read(**found);
    if (!record || *record != records[index])
      return 26;
    auto ordinal = (*warm_reader)->location(index);
    if (!ordinal || *ordinal != **found)
      return 27;
  }
  if ((*warm_reader)->locate(bytes("missing")).value().has_value() ||
      (*warm_reader)->locate(bytes("aardvark")).value().has_value())
    return 28;
  std::atomic<bool> concurrent_ok{true};
  {
    std::vector<std::jthread> readers;
    for (std::size_t worker = 0; worker < 8; ++worker) {
      readers.emplace_back([&, worker] {
        for (std::size_t iteration = 0; iteration < 1'000; ++iteration) {
          const auto index = (worker + iteration) % keys.size();
          auto found = (*warm_reader)->locate(keys[index]);
          if (!found || !*found) {
            concurrent_ok = false;
            return;
          }
          auto record = (*warm_reader)->read(**found);
          if (!record || *record != records[index]) {
            concurrent_ok = false;
            return;
          }
        }
      });
    }
  }
  if (!concurrent_ok)
    return 29;

  const auto warm_name = object_name(warm->object);
  const auto warm_path =
      directory.path / "objects" / warm_name.substr(0, 2) / warm_name;
  {
    std::fstream file{warm_path,
                      std::ios::binary | std::ios::in | std::ios::out};
    if (!file)
      return 30;
    file.seekg((*warm_reader)->footer().sparse_index_offset + 8);
    char value{};
    file.read(&value, 1);
    value ^= 0x01;
    file.seekp((*warm_reader)->footer().sparse_index_offset + 8);
    file.write(&value, 1);
  }
  auto corrupt_index = storage::RowV1SegmentReader::open(store, warm->object);
  if (corrupt_index ||
      corrupt_index.error().code != core::ErrorCode::ChecksumMismatch)
    return 31;
  {
    std::fstream file{warm_path,
                      std::ios::binary | std::ios::in | std::ios::out};
    if (!file)
      return 32;
    for (const auto offset : {
             (*warm_reader)->footer().sparse_index_offset + 8,
             (*warm_reader)->footer().bloom_filter_offset + 8,
         }) {
      file.seekg(offset);
      char value{};
      file.read(&value, 1);
      value ^= 0x01;
      file.seekp(offset);
      file.write(&value, 1);
    }
  }
  auto corrupt_bloom = storage::RowV1SegmentReader::open(store, warm->object);
  if (corrupt_bloom ||
      corrupt_bloom.error().code != core::ErrorCode::ChecksumMismatch)
    return 33;

  return 0;
}

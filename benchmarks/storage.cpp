import std;

import sakuin.core;
import sakuin.model.observation;
import sakuin.storage;
import sakuin.storage.dataset.observations;

namespace {

struct TemporaryDirectory {
  std::filesystem::path path;
  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path, ignored);
  }
};

std::uint64_t mix(std::uint64_t value) {
  value += 0x9e3779b97f4a7c15ULL;
  value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ULL;
  value = (value ^ (value >> 27)) * 0x94d049bb133111ebULL;
  return value ^ (value >> 31);
}

sakuin::model::ObservationRecord observation(std::uint64_t ordinal) {
  sakuin::core::InfoHash hash;
  auto state = ordinal;
  for (std::size_t offset = 0; offset < hash.bytes.size(); offset += 8) {
    state = mix(state);
    for (std::size_t byte = 0; byte < 8 && offset + byte < hash.bytes.size();
         ++byte)
      hash.bytes[offset + byte] =
          static_cast<std::uint8_t>(state >> (byte * 8));
  }
  return {.info_hash = hash,
          .observed_at = sakuin::core::Timestamp{
              sakuin::core::Timestamp::duration{ordinal}}};
}

double seconds_since(std::chrono::steady_clock::time_point start) {
  return std::chrono::duration<double>(std::chrono::steady_clock::now() - start)
      .count();
}

} // namespace

int main(int argc, char **argv) {
  using namespace sakuin;

  std::uint64_t record_count = 100'000;
  if (argc > 1) {
    const std::string_view input{argv[1]};
    const auto [end, error] =
        std::from_chars(input.data(), input.data() + input.size(), record_count);
    if (error != std::errc{} || end != input.data() + input.size() ||
        record_count == 0 || record_count > 10'000'000) {
      std::cerr << "record count must be between 1 and 10000000\n";
      return 1;
    }
  }

  const auto nonce = std::to_string(
      std::chrono::steady_clock::now().time_since_epoch().count());
  TemporaryDirectory directory{std::filesystem::temp_directory_path() /
                               ("sakuin-storage-benchmark-" + nonce)};
  storage::LocalBlobStore blobs{directory.path / "blobs"};
  auto catalog =
      storage::LocalManifestCatalog::open(directory.path / "catalog", blobs);
  if (!catalog) {
    std::cerr << catalog.error().message << '\n';
    return 2;
  }
  storage::ObservationDataset dataset{blobs, **catalog};

  constexpr std::size_t segment_count = 8;
  const auto append_start = std::chrono::steady_clock::now();
  for (std::size_t segment = 0; segment < segment_count; ++segment) {
    const auto begin = record_count * segment / segment_count;
    const auto end = record_count * (segment + 1) / segment_count;
    std::vector<model::ObservationRecord> records;
    records.reserve(end - begin);
    for (auto ordinal = begin; ordinal < end; ++ordinal)
      records.push_back(observation(ordinal));
    auto write = dataset.begin_write();
    if (!write || !(*write)->append(records) || !(*write)->commit()) {
      const auto message = !write ? write.error().message : "dataset write failed";
      std::cerr << message << '\n';
      return 3;
    }
  }
  const auto append_seconds = seconds_since(append_start);

  auto manifest = (*catalog)->pin_current();
  if (!manifest)
    return 4;
  const auto bytes_before = std::transform_reduce(
      (*manifest)->manifest().segments.begin(),
      (*manifest)->manifest().segments.end(), std::uint64_t{}, std::plus{},
      [](const storage::SegmentDescriptor &segment) {
        return segment.physical_size;
      });
  manifest->reset();

  const auto scan_start = std::chrono::steady_clock::now();
  auto snapshot = dataset.snapshot();
  auto stream = snapshot ? (*snapshot)->scan({})
                         : core::Result<std::unique_ptr<storage::RecordStream<
                               model::ObservationRecord>>>{
                               std::unexpected(snapshot.error())};
  std::uint64_t scanned{};
  if (!stream)
    return 5;
  while (true) {
    auto next = (*stream)->next();
    if (!next)
      return 6;
    if (!*next)
      break;
    ++scanned;
  }
  const auto scan_seconds = seconds_since(scan_start);
  snapshot->reset();

  const auto compact_start = std::chrono::steady_clock::now();
  auto compacted =
      storage::RowV1DatasetMaintenance::compact(blobs, **catalog);
  if (!compacted)
    return 7;
  const auto compact_seconds = seconds_since(compact_start);

  const auto verify_start = std::chrono::steady_clock::now();
  auto verified = storage::RowV1DatasetMaintenance::verify(blobs, **catalog);
  if (!verified)
    return 8;
  const auto verify_seconds = seconds_since(verify_start);

  std::cout << std::fixed << std::setprecision(3)
            << "records=" << record_count << '\n'
            << "segments_before=" << segment_count << '\n'
            << "physical_bytes_before=" << bytes_before << '\n'
            << "physical_bytes_after=" << compacted->bytes_after << '\n'
            << "append_seconds=" << append_seconds << '\n'
            << "append_records_per_second=" << record_count / append_seconds
            << '\n'
            << "scan_seconds=" << scan_seconds << '\n'
            << "scan_records_per_second=" << scanned / scan_seconds << '\n'
            << "compaction_seconds=" << compact_seconds << '\n'
            << "verify_seconds=" << verify_seconds << '\n';
  return scanned == record_count && verified->records_checked == record_count
             ? 0
             : 9;
}

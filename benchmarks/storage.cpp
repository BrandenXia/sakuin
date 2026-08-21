import std;

import sakuin.core;
import sakuin.model.observation;
import sakuin.model.torrent;
import sakuin.storage;
import sakuin.storage.dataset.observations;
import sakuin.storage.dataset.torrents;

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

sakuin::core::InfoHash info_hash(std::uint64_t ordinal) {
  sakuin::core::InfoHash hash;
  auto state = ordinal;
  for (std::size_t offset = 0; offset < hash.bytes.size(); offset += 8) {
    state = mix(state);
    for (std::size_t byte = 0; byte < 8 && offset + byte < hash.bytes.size();
         ++byte)
      hash.bytes[offset + byte] =
          static_cast<std::uint8_t>(state >> (byte * 8));
  }
  return hash;
}

sakuin::model::ObservationRecord observation(std::uint64_t ordinal) {
  return {.info_hash = info_hash(ordinal),
          .observed_at = sakuin::core::Timestamp{
              sakuin::core::Timestamp::duration{ordinal}}};
}

sakuin::model::TorrentRecord torrent(std::uint64_t ordinal) {
  const auto name = "torrent-" + std::to_string(ordinal);
  const auto size = 1'000U + ordinal % 1'000'000U;
  return {
      .info_hash = info_hash(ordinal),
      .first_seen =
          sakuin::core::Timestamp{sakuin::core::Timestamp::duration{ordinal}},
      .last_seen = sakuin::core::Timestamp{sakuin::core::Timestamp::duration{
          ordinal + 100}},
      .name = name,
      .total_size = size,
      .files = {{name + "/payload.bin", size}},
  };
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
    const auto [end, error] = std::from_chars(
        input.data(), input.data() + input.size(), record_count);
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
      const auto message =
          !write ? write.error().message : "dataset write failed";
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
  auto stream = snapshot
                    ? (*snapshot)->scan({})
                    : core::Result<std::unique_ptr<
                          storage::RecordStream<model::ObservationRecord>>>{
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
  auto compacted = storage::RowV1DatasetMaintenance::compact(blobs, **catalog);
  if (!compacted)
    return 7;
  const auto compact_seconds = seconds_since(compact_start);

  const auto verify_start = std::chrono::steady_clock::now();
  auto verified = storage::RowV1DatasetMaintenance::verify(blobs, **catalog);
  if (!verified)
    return 8;
  const auto verify_seconds = seconds_since(verify_start);

  auto torrent_catalog = storage::LocalManifestCatalog::open(
      directory.path / "torrent-catalog", blobs);
  if (!torrent_catalog)
    return 9;
  storage::TorrentDataset torrents{blobs, **torrent_catalog};
  for (std::size_t segment = 0; segment < segment_count; ++segment) {
    const auto begin = record_count * segment / segment_count;
    const auto end = record_count * (segment + 1) / segment_count;
    auto write = torrents.begin_write();
    if (!write)
      return 10;
    for (auto ordinal = begin; ordinal < end; ++ordinal) {
      if (auto appended = (*write)->append(torrent(ordinal)); !appended)
        return 11;
    }
    if (!(*write)->commit())
      return 12;
  }
  const auto torrent_compact_start = std::chrono::steady_clock::now();
  auto torrent_compacted = torrents.compact();
  if (!torrent_compacted)
    return 13;
  const auto torrent_compact_seconds = seconds_since(torrent_compact_start);
  auto warm_manifest = (*torrent_catalog)->pin_current();
  if (!warm_manifest || (*warm_manifest)->manifest().segments.size() != 1)
    return 14;
  const auto warm_segment = (*warm_manifest)->manifest().segments.front();
  const auto warm_ratio =
      warm_segment.logical_size == 0
          ? 0.0
          : static_cast<double>(warm_segment.physical_size) /
                static_cast<double>(warm_segment.logical_size);
  warm_manifest->reset();

  auto warm_snapshot = torrents.keyed_snapshot();
  if (!warm_snapshot)
    return 15;
  const auto lookup_count = std::min<std::uint64_t>(record_count, 10'000);
  const auto lookup_start = std::chrono::steady_clock::now();
  for (std::uint64_t sample = 0; sample < lookup_count; ++sample) {
    const auto ordinal = mix(sample) % record_count;
    auto found = (*warm_snapshot)->get(info_hash(ordinal));
    if (!found || !*found || (*found)->info_hash != info_hash(ordinal))
      return 16;
  }
  const auto lookup_seconds = seconds_since(lookup_start);
  const auto warm_scan_start = std::chrono::steady_clock::now();
  auto warm_stream = (*warm_snapshot)->scan({});
  if (!warm_stream)
    return 17;
  std::uint64_t warm_scanned{};
  while (true) {
    auto next = (*warm_stream)->next();
    if (!next)
      return 18;
    if (!*next)
      break;
    ++warm_scanned;
  }
  const auto warm_scan_seconds = seconds_since(warm_scan_start);

  std::cout << std::fixed << std::setprecision(3) << "records=" << record_count
            << '\n'
            << "segments_before=" << segment_count << '\n'
            << "physical_bytes_before=" << bytes_before << '\n'
            << "physical_bytes_after=" << compacted->bytes_after << '\n'
            << "append_seconds=" << append_seconds << '\n'
            << "append_records_per_second=" << record_count / append_seconds
            << '\n'
            << "scan_seconds=" << scan_seconds << '\n'
            << "scan_records_per_second=" << scanned / scan_seconds << '\n'
            << "compaction_seconds=" << compact_seconds << '\n'
            << "verify_seconds=" << verify_seconds << '\n'
            << "warm_torrent_physical_bytes=" << warm_segment.physical_size
            << '\n'
            << "warm_torrent_logical_bytes=" << warm_segment.logical_size
            << '\n'
            << "warm_torrent_physical_per_logical=" << warm_ratio << '\n'
            << "warm_torrent_compaction_seconds=" << torrent_compact_seconds
            << '\n'
            << "warm_lookup_seconds=" << lookup_seconds << '\n'
            << "warm_lookups_per_second=" << lookup_count / lookup_seconds
            << '\n'
            << "warm_scan_seconds=" << warm_scan_seconds << '\n'
            << "warm_scan_records_per_second="
            << warm_scanned / warm_scan_seconds << '\n';
  return scanned == record_count && verified->records_checked == record_count &&
                 warm_scanned == record_count
             ? 0
             : 19;
}

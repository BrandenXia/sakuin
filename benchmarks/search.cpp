import std;

import sakuin.core;
import sakuin.model.torrent;
import sakuin.search;

namespace {

std::uint64_t parse_count(char *input, std::uint64_t maximum,
                          std::string_view label) {
  const std::string_view value{input};
  std::uint64_t result{};
  const auto [end, error] =
      std::from_chars(value.data(), value.data() + value.size(), result);
  if (error != std::errc{} || end != value.data() + value.size() ||
      result == 0 || result > maximum) {
    std::cerr << label << " must be between 1 and " << maximum << '\n';
    return 0;
  }
  return result;
}

std::uint64_t mix(std::uint64_t value) {
  value += 0x9e3779b97f4a7c15ULL;
  value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ULL;
  value = (value ^ (value >> 27)) * 0x94d049bb133111ebULL;
  return value ^ (value >> 31);
}

sakuin::model::TorrentRecord torrent(std::uint64_t ordinal,
                                     std::uint64_t target) {
  sakuin::core::InfoHash hash;
  auto state = ordinal;
  for (std::size_t offset = 0; offset < hash.bytes.size(); offset += 8) {
    state = mix(state);
    for (std::size_t byte = 0; byte < 8 && offset + byte < hash.bytes.size();
         ++byte)
      hash.bytes[offset + byte] =
          static_cast<std::uint8_t>(state >> (byte * 8));
  }

  std::vector<sakuin::model::FileRecord> files;
  files.reserve(64);
  std::uint64_t total_size{};
  for (std::size_t file = 0; file < 64; ++file) {
    const auto size = 64U * 1024U * 1024U + mix(ordinal + file) % 1'000'000U;
    total_size += size;
    files.push_back({.path = "collection-" + std::to_string(ordinal) +
                             "/season-" + std::to_string(file % 20) +
                             "/episode-" + std::to_string(file) + "-payload-" +
                             std::to_string(mix(ordinal * 64 + file)) + ".mkv",
                     .size = size});
  }
  return {
      .info_hash = hash,
      .first_seen =
          sakuin::core::Timestamp{sakuin::core::Timestamp::duration{ordinal}},
      .last_seen = sakuin::core::Timestamp{sakuin::core::Timestamp::duration{
          ordinal + 100}},
      .name = ordinal == target ? "Needle Target " + std::to_string(ordinal)
                                : "Archive " + std::to_string(ordinal),
      .total_size = total_size,
      .files = std::move(files)};
}

double seconds_since(std::chrono::steady_clock::time_point start) {
  return std::chrono::duration<double>(std::chrono::steady_clock::now() - start)
      .count();
}

bool run_queries(const sakuin::search::InMemorySearchIndex &index,
                 std::string_view label, std::string text,
                 std::uint64_t query_count, std::uint64_t expected_matches,
                 std::uint64_t &checksum) {
  const auto start = std::chrono::steady_clock::now();
  for (std::uint64_t query = 0; query < query_count; ++query) {
    auto result = index.search({.text = text, .limit = 20});
    if (!result || result->total_matches != expected_matches) {
      std::cerr << label << " query returned an unexpected result\n";
      return false;
    }
    checksum += result->total_matches + result->hits.size();
  }
  const auto seconds = seconds_since(start);
  std::cout << label << "_queries_per_second=" << query_count / seconds << '\n'
            << label << "_mean_ms=" << seconds * 1000 / query_count << '\n';
  return true;
}

} // namespace

int main(int argc, char **argv) {
  std::uint64_t record_count = 5'000;
  std::uint64_t query_count = 100;
  if (argc > 1 && !(record_count = parse_count(argv[1], 1'000'000, "records")))
    return 1;
  if (argc > 2 && !(query_count = parse_count(argv[2], 100'000, "queries")))
    return 1;

  sakuin::search::InMemorySearchIndex index({.enabled = false});
  auto rebuild = index.begin_rebuild(1);
  if (!rebuild)
    return 2;
  const auto rebuild_start = std::chrono::steady_clock::now();
  for (std::uint64_t ordinal = 0; ordinal < record_count; ++ordinal) {
    const auto record = torrent(ordinal, record_count / 2);
    if (!(*rebuild)->append(record))
      return 3;
  }
  if (!(*rebuild)->commit())
    return 4;
  const auto rebuild_seconds = seconds_since(rebuild_start);
  const auto statistics = index.classification_stats();

  std::cout << std::fixed << std::setprecision(3) << "records=" << record_count
            << '\n'
            << "files_per_record=64\n"
            << "projection_mib="
            << static_cast<double>(statistics.estimated_memory_bytes) /
                   (1024U * 1024U)
            << '\n'
            << "projection_bytes_per_record="
            << static_cast<double>(statistics.estimated_memory_bytes) /
                   record_count
            << '\n'
            << "rebuild_seconds=" << rebuild_seconds << '\n';

  std::uint64_t checksum{};
  if (!run_queries(index, "absent", "definitely-absent-token", query_count, 0,
                   checksum) ||
      !run_queries(index, "sparse", "needle", query_count, 1, checksum) ||
      !run_queries(index, "common", "episode", query_count, record_count,
                   checksum))
    return 5;
  std::cout << "checksum=" << checksum << '\n';
  return 0;
}

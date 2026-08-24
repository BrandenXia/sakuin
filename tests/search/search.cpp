import std;

import sakuin.classification;
import sakuin.core;
import sakuin.model.torrent;
import sakuin.search;
import sakuin.search.rebuild;
import sakuin.storage;
import sakuin.storage.admin.row_v1;
import sakuin.storage.dataset.torrents;

namespace {

struct TemporaryDirectory {
  std::filesystem::path path;
  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path, ignored);
  }
};

sakuin::model::TorrentRecord torrent(std::uint8_t id, std::string name,
                                     std::uint64_t size,
                                     std::vector<std::string> paths,
                                     std::int64_t seen) {
  sakuin::core::InfoHash hash;
  hash.bytes.fill(id);
  std::vector<sakuin::model::FileRecord> files;
  for (auto &path : paths)
    files.push_back({.path = std::move(path), .size = size / paths.size()});
  return {
      .info_hash = hash,
      .first_seen =
          sakuin::core::Timestamp{sakuin::core::Timestamp::duration{seen}},
      .last_seen =
          sakuin::core::Timestamp{sakuin::core::Timestamp::duration{seen + 10}},
      .name = std::move(name),
      .total_size = size,
      .files = std::move(files)};
}

} // namespace

int main() {
  using namespace sakuin;
  const auto nonce = std::to_string(
      std::chrono::steady_clock::now().time_since_epoch().count());
  TemporaryDirectory directory{std::filesystem::temp_directory_path() /
                               ("sakuin-search-" + nonce)};
  storage::LocalBlobStore blobs{directory.path / "blobs"};
  auto catalog =
      storage::LocalManifestCatalog::open(directory.path / "torrents", blobs);
  if (!catalog)
    return 1;
  storage::TorrentDataset torrents{blobs, **catalog};
  auto write = torrents.begin_write();
  const std::array records{
      torrent(1, "Linux Distribution", 4'000, {"linux.iso"}, 10),
      torrent(2, "Nature Collection", 8'000, {"forest.mp4", "ocean.mp4"}, 20),
      torrent(3, "Linux Documentation", 500, {"manual.pdf", "examples.txt"},
              30)};
  if (!write || !(*write)->append(records) || !(*write)->commit())
    return 2;
  auto snapshot = torrents.snapshot();
  search::InMemorySearchIndex index;
  auto rebuilt = search::rebuild(**snapshot, index);
  if (!rebuilt || rebuilt->source_generation != 1 ||
      rebuilt->records_indexed != 3)
    return 3;

  auto linux = index.search({.text = "linux", .limit = 10});
  if (!linux) {
    std::cerr << linux.error().message << '\n';
    return 4;
  }
  if (linux->total_matches != 2 || linux->hits.size() != 2 ||
      linux->source_generation != 1 ||
      linux->hits.front().name != "Linux Distribution") {
    std::cerr << "matches=" << linux->total_matches
              << " hits=" << linux->hits.size()
              << " generation=" << linux->source_generation;
    if (!linux->hits.empty() && linux->hits.front().name)
      std::cerr << " first=" << *linux->hits.front().name;
    std::cerr << '\n';
    return 4;
  }
  auto filtered = index.search({.text = "mp4",
                                .minimum_size = 7'000,
                                .maximum_size = 9'000,
                                .limit = 10});
  if (!filtered || filtered->hits.size() != 1 ||
      filtered->hits.front().file_count != 2)
    return 5;
  auto file_filtered = index.search(
      {.minimum_file_count = 2, .maximum_file_count = 2, .limit = 10});
  if (!file_filtered || file_filtered->total_matches != 2)
    return 15;
  auto paged = index.search({.text = "", .offset = 1, .limit = 1});
  if (!paged || paged->total_matches != 3 || paged->hits.size() != 1)
    return 6;
  auto time_filtered = index.search(
      {.first_seen_at_or_after = core::Timestamp{core::Timestamp::duration{20}},
       .last_seen_at_or_before = core::Timestamp{core::Timestamp::duration{30}},
       .limit = 10});
  if (!time_filtered || time_filtered->total_matches != 1 ||
      time_filtered->hits.front().name != "Nature Collection")
    return 14;
  if (index.search({.limit = 0}) ||
      index.search({.minimum_size = 10, .maximum_size = 1}) ||
      index.search({.minimum_file_count = 3, .maximum_file_count = 1}) ||
      index.search({.first_seen_at_or_after =
                        core::Timestamp{core::Timestamp::duration{30}},
                    .last_seen_at_or_before =
                        core::Timestamp{core::Timestamp::duration{20}}}))
    return 7;

  search::InMemorySearchIndex incremental_index;
  auto synchronized = search::synchronize(torrents, incremental_index);
  if (!synchronized || !synchronized->full_rebuild ||
      synchronized->records_indexed != 3)
    return 8;
  const auto cursor = synchronized->cursor;
  auto update = torrents.begin_write();
  const auto replacement =
      torrent(1, "Linux Kernel Distribution", 4'500,
              {"linux-kernel.iso", "release-notes.txt"}, 10);
  if (!update || !(*update)->append(replacement) || !(*update)->commit())
    return 9;
  synchronized = search::synchronize(torrents, incremental_index, cursor);
  if (!synchronized || synchronized->full_rebuild ||
      synchronized->records_indexed != 1 ||
      synchronized->source_generation != 2)
    return 10;
  auto kernel = incremental_index.search({.text = "kernel", .limit = 10});
  if (!kernel || kernel->total_matches != 1 ||
      kernel->hits.front().name != "Linux Kernel Distribution" ||
      kernel->hits.front().total_size != 4'500)
    return 11;

  const auto pre_compaction_cursor = synchronized->cursor;
  auto compacted = storage::RowV1DatasetMaintenance::compact(
      blobs, **catalog,
      {.minimum_segment_count = 2,
       .target_block_size = storage::DefaultTargetBlockSize,
       .compression = storage::CompressionCodec::None});
  if (!compacted || compacted->segments_removed != 2)
    return 12;
  synchronized =
      search::synchronize(torrents, incremental_index, pre_compaction_cursor);
  if (!synchronized || !synchronized->full_rebuild ||
      synchronized->records_indexed != 3)
    return 13;

  search::InMemorySearchIndex classified_index;
  auto classified_rebuild = classified_index.begin_rebuild(1);
  if (!classified_rebuild)
    return 16;
  const auto safe_movie = torrent(4, "Example Movie 2024 1080p", 1'000'000,
                                  {"Example.Movie.2024.1080p.mkv"}, 40);
  const auto adult_movie =
      torrent(5, "Explicit Porn Movie 2024 1080p", 1'000'000,
              {"Explicit.Porn.Movie.2024.1080p.mkv"}, 50);
  if (!(*classified_rebuild)->append(safe_movie) ||
      !(*classified_rebuild)->append(adult_movie) ||
      !(*classified_rebuild)->commit())
    return 17;
  auto included = classified_index.search({.limit = 10});
  auto excluded = classified_index.search(
      {.adult_content = search::AdultContentMode::Exclude, .limit = 10});
  auto only = classified_index.search(
      {.adult_content = search::AdultContentMode::Only, .limit = 10});
  auto movie_page = classified_index.search(
      {.categories = {classification::MediaCategory::Movie},
       .adult_content = search::AdultContentMode::Include,
       .offset = 1,
       .limit = 1});
  if (!included || included->total_matches != 2 || !excluded ||
      excluded->total_matches != 1 || !only || only->total_matches != 1 ||
      !movie_page || movie_page->total_matches != 2 ||
      movie_page->hits.size() != 1 ||
      !std::ranges::contains(movie_page->hits.front().categories,
                             classification::MediaCategory::Movie))
    return 18;
  return 0;
}

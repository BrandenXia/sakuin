import std;

import sakuin.core;
import sakuin.model.torrent;
import sakuin.search;
import sakuin.search.rebuild;
import sakuin.storage;
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
  return {.info_hash = hash,
          .first_seen = sakuin::core::Timestamp{
              sakuin::core::Timestamp::duration{seen}},
          .last_seen = sakuin::core::Timestamp{
              sakuin::core::Timestamp::duration{seen + 10}},
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
      torrent(2, "Nature Collection", 8'000,
              {"forest.mp4", "ocean.mp4"}, 20),
      torrent(3, "Linux Documentation", 500,
              {"manual.pdf", "examples.txt"}, 30)};
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
  auto filtered = index.search(
      {.text = "mp4", .minimum_size = 7'000, .maximum_size = 9'000, .limit = 10});
  if (!filtered || filtered->hits.size() != 1 ||
      filtered->hits.front().file_count != 2)
    return 5;
  auto paged = index.search({.text = "", .offset = 1, .limit = 1});
  if (!paged || paged->total_matches != 3 || paged->hits.size() != 1)
    return 6;
  if (index.search({.limit = 0}) ||
      index.search({.minimum_size = 10, .maximum_size = 1}))
    return 7;
  return 0;
}

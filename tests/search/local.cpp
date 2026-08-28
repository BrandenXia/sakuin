import std;

import sakuin.classification;
import sakuin.core;
import sakuin.model.torrent;
import sakuin.search.checkpoint;
import sakuin.search.local;
import sakuin.storage.dataset.torrents;

namespace {

struct TemporaryDirectory {
  TemporaryDirectory() {
    path = std::filesystem::temp_directory_path() /
           ("sakuin-local-search-" +
            std::to_string(
                std::chrono::steady_clock::now().time_since_epoch().count()));
  }
  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path, ignored);
  }
  std::filesystem::path path;
};

sakuin::model::TorrentRecord record(std::uint8_t marker, std::string name,
                                    std::uint64_t size) {
  sakuin::model::TorrentRecord result;
  result.info_hash.bytes.fill(marker);
  result.first_seen =
      sakuin::core::Timestamp{sakuin::core::Timestamp::duration{marker}};
  result.last_seen =
      sakuin::core::Timestamp{sakuin::core::Timestamp::duration{marker + 1}};
  result.name = std::move(name);
  result.total_size = size;
  result.files.push_back({.path = "payload.bin", .size = size});
  return result;
}

} // namespace

int main() {
  using namespace sakuin;
  TemporaryDirectory temporary;
  const auto index_path = temporary.path / "index.v1";
  {
    auto index = search::LocalSearchIndex::open(index_path);
    if (!index || (*index)->source_generation() != 0)
      return 1;
    auto rebuild = (*index)->begin_rebuild(7);
    auto anime_movie = record(3, "Example Anime Movie 2024", 300);
    anime_movie.files.front().path = "Example.Anime.Movie.2024.mkv";
    const std::array records{record(1, "Linux Archive", 100),
                             record(2, "Nature Archive", 200), anime_movie};
    if (!rebuild || !(*rebuild)->append(records[0]) ||
        !(*rebuild)->append(records[1]) || !(*rebuild)->append(records[2]) ||
        !(*rebuild)->commit())
      return 2;
  }
  {
    auto index = search::LocalSearchIndex::open(index_path);
    if (!index || (*index)->source_generation() != 7)
      return 3;
    auto found = (*index)->search({.text = "linux", .limit = 10});
    auto classified = (*index)->search(
        {.classification_state =
             classification::ClassificationState::Classified,
         .content_kind = classification::ContentKind::Movie,
         .minimum_kind_confidence = classification::Confidence::High,
         .labels = {classification::ContentLabel::Anime},
         .minimum_label_confidence = classification::Confidence::High,
         .limit = 10});
    const auto stats = (*index)->classification_stats();
    auto substring = (*index)->search({.text = "inux", .limit = 10});
    if (!found || found->total_matches != 1 || !substring ||
        substring->total_matches != 1 || !classified ||
        classified->total_matches != 1 ||
        classified->hits.front().name != "Example Anime Movie 2024" ||
        stats.total_records != 3)
      return 4;
    auto update = (*index)->begin_update(8);
    const auto replacement = record(1, "Kernel Archive", 300);
    if (!update || !(*update)->upsert(replacement) || !(*update)->commit())
      return 5;
  }
  {
    auto index = search::LocalSearchIndex::open(index_path);
    if (!index || (*index)->source_generation() != 8)
      return 6;
    auto found = (*index)->search({.text = "kernel", .limit = 10});
    auto stale = (*index)->search({.text = "linux", .limit = 10});
    if (!found || found->total_matches != 1 ||
        found->hits.front().total_size != 300 || !stale ||
        stale->total_matches != 0)
      return 7;
  }
  const auto updates_path =
      std::filesystem::path{index_path.string() + ".updates"};
  if (!std::filesystem::exists(updates_path))
    return 11;
  {
    auto index = search::LocalSearchIndex::open(index_path);
    if (!index)
      return 12;
    auto rebuild = (*index)->begin_rebuild(9);
    const auto replacement = record(4, "Fresh Base", 400);
    auto placeholder = record(5, std::string(1024U * 1024U, 'x'), 0);
    placeholder.name.reset();
    placeholder.files.clear();
    if (!rebuild || !(*rebuild)->append(placeholder) ||
        !(*rebuild)->append(replacement) || !(*rebuild)->commit() ||
        std::filesystem::exists(updates_path))
      return 13;
    const auto all = (*index)->search({.limit = 10});
    if (!all || all->total_matches != 1 ||
        all->hits.front().name != "Fresh Base")
      return 14;
  }

  storage::TorrentChangeCursor cursor{
      .initialized = true, .source_generation = 8, .segment_count = 3};
  std::ranges::iota(cursor.segment_prefix_digest.bytes, std::uint8_t{1});
  search::LocalSearchCursorStore cursors{temporary.path / "cursor.v1"};
  if (!cursors.save(cursor))
    return 8;
  auto loaded = cursors.load();
  if (!loaded || *loaded != cursor)
    return 9;

  {
    std::fstream corrupt{index_path,
                         std::ios::binary | std::ios::in | std::ios::out};
    corrupt.seekp(4);
    const char changed = 'X';
    corrupt.write(&changed, 1);
  }
  auto corrupt = search::LocalSearchIndex::open(index_path);
  if (corrupt || corrupt.error().code != core::ErrorCode::UnsupportedFormat)
    return 10;
  return 0;
}

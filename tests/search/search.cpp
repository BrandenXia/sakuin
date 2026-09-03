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
  auto substring = index.search({.text = "inu", .limit = 10});
  auto short_query = index.search({.text = "li", .limit = 10});
  auto multiple_terms = index.search({.text = "linux iso", .limit = 10});
  auto by_hash = index.search({.text = "010101", .limit = 10});
  auto absent = index.search({.text = "definitely-absent", .limit = 10});
  if (!substring || substring->total_matches != 2 || !short_query ||
      short_query->total_matches != 2 || !multiple_terms ||
      multiple_terms->total_matches != 1 ||
      multiple_terms->hits.front().name != "Linux Distribution" || !by_hash ||
      by_hash->total_matches != 1 || !absent || absent->total_matches != 0)
    return 29;

  search::InMemorySearchIndex exact_verification_index;
  auto exact_verification_rebuild = exact_verification_index.begin_rebuild(1);
  const auto split_trigrams = torrent(30, "abc", 1'000, {"bcd"}, 1);
  if (!exact_verification_rebuild ||
      !(*exact_verification_rebuild)->append(split_trigrams) ||
      !(*exact_verification_rebuild)->commit())
    return 30;
  auto split_match =
      exact_verification_index.search({.text = "abcd", .limit = 10});
  if (!split_match || split_match->total_matches != 0)
    return 31;

  search::SearchClassificationOptions operator_search_options;
  operator_search_options.classifier.operator_rules.push_back(
      {.id = "workbench-tools",
       .kind = classification::ContentKind::Application,
       .tokens = {"workbench", "tool"},
       .weight = 100});
  search::InMemorySearchIndex operator_index(
      std::move(operator_search_options));
  auto operator_rebuild = operator_index.begin_rebuild(1);
  const auto operator_record =
      torrent(31, "Workbench Tool Archive", 100'000'000, {"payload.bin"}, 1);
  if (!operator_rebuild || !(*operator_rebuild)->append(operator_record) ||
      !(*operator_rebuild)->commit())
    return 32;
  auto operator_result =
      operator_index.search({.text = "workbench", .limit = 10});
  if (!operator_result || operator_result->total_matches != 1 ||
      operator_result->hits.front().classification.kind !=
          classification::ContentKind::Application ||
      !std::ranges::contains(
          operator_result->hits.front().classification.evidence,
          classification::EvidenceCode::OperatorRule,
          &classification::Evidence::code))
    return 33;
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
  const auto ambiguous_movie =
      torrent(6, "Software Bundle", 1'000'000, {"walkthrough.mkv"}, 60);
  if (!(*classified_rebuild)->append(safe_movie) ||
      !(*classified_rebuild)->append(adult_movie) ||
      !(*classified_rebuild)->append(ambiguous_movie) ||
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
  if (!included || included->total_matches != 3 || !excluded ||
      excluded->total_matches != 2 || !only || only->total_matches != 1 ||
      !movie_page || movie_page->total_matches != 2 ||
      movie_page->hits.size() != 1 ||
      !std::ranges::contains(movie_page->hits.front().categories,
                             classification::MediaCategory::Movie))
    return 18;
  auto ambiguous_page = classified_index.search(
      {.classification_state = classification::ClassificationState::Ambiguous,
       .limit = 10});
  auto confident_movies = classified_index.search(
      {.content_kind = classification::ContentKind::Movie,
       .minimum_kind_confidence = classification::Confidence::High,
       .limit = 10});
  auto adult_labeled = classified_index.search(
      {.labels = {classification::ContentLabel::Adult},
       .minimum_label_confidence = classification::Confidence::High,
       .limit = 10});
  auto adult_labeled_hidden = classified_index.search(
      {.labels = {classification::ContentLabel::Adult},
       .minimum_label_confidence = classification::Confidence::High,
       .adult_content = search::AdultContentMode::Exclude,
       .limit = 10});
  auto adult_anime_labeled =
      classified_index.search({.labels = {classification::ContentLabel::Adult,
                                          classification::ContentLabel::Anime},
                               .limit = 10});
  if (!ambiguous_page || ambiguous_page->total_matches != 1 ||
      ambiguous_page->hits.front().name != "Software Bundle" ||
      !confident_movies || confident_movies->total_matches != 2 ||
      !adult_labeled || adult_labeled->total_matches != 1 ||
      adult_labeled->hits.front().name != "Explicit Porn Movie 2024 1080p" ||
      !adult_labeled_hidden || adult_labeled_hidden->total_matches != 0 ||
      !adult_anime_labeled || adult_anime_labeled->total_matches != 0)
    return 24;
  const auto classification_stats = classified_index.classification_stats();
  if (!classification_stats.enabled ||
      classification_stats.total_records != 3 ||
      classification_stats.adult_labeled != 1 ||
      classification_stats.category_count(
          classification::MediaCategory::Movie) != 2 ||
      classification_stats.category_count(
          classification::MediaCategory::MovieHd) != 2 ||
      classification_stats.category_count(
          classification::MediaCategory::Adult) != 1)
    return 19;

  search::InMemorySearchIndex disabled_index({.enabled = false});
  auto disabled_rebuild = disabled_index.begin_rebuild(1);
  if (!disabled_rebuild || !(*disabled_rebuild)->append(adult_movie) ||
      !(*disabled_rebuild)->commit())
    return 20;
  const auto disabled_stats = disabled_index.classification_stats();
  if (disabled_stats.enabled || disabled_stats.total_records != 1 ||
      disabled_stats.learned_enabled || disabled_stats.adult_labeled != 0 ||
      disabled_stats.state_count(
          classification::ClassificationState::Unknown) != 1 ||
      disabled_stats.category_count(classification::MediaCategory::Other) != 1)
    return 21;

  search::InMemorySearchIndex learned_index(
      {.learned = {.minimum_examples_per_kind = 3,
                   .minimum_feature_occurrences = 2,
                   .minimum_probability = 0.65,
                   .minimum_margin = 0.20}});
  auto learned_rebuild = learned_index.begin_rebuild(1);
  const std::array learned_records{
      torrent(20, "Workbench Tool Suite", 120'000'000, {"workbench.rpm"}, 1),
      torrent(21, "Workbench Tool Package", 130'000'000, {"workbench.exe"}, 2),
      torrent(22, "Workbench Tool Release", 140'000'000, {"workbench.apk"}, 3),
      torrent(23, "Cinema Feature 2021", 2'000'000'000, {"cinema-2021.mkv"}, 4),
      torrent(24, "Cinema Feature 2022", 2'100'000'000, {"cinema-2022.mp4"}, 5),
      torrent(25, "Cinema Feature 2023", 2'200'000'000, {"cinema-2023.webm"},
              6),
      torrent(26, "Workbench Tool Archive", 125'000'000, {"payload.bin"}, 7)};
  if (!learned_rebuild)
    return 25;
  for (const auto &learned_record : learned_records)
    if (!(*learned_rebuild)->append(learned_record))
      return 26;
  if (!(*learned_rebuild)->commit())
    return 27;
  auto learned_result = learned_index.search({.text = "archive", .limit = 10});
  const auto learned_stats = learned_index.classification_stats();
  if (!learned_result || learned_result->total_matches != 1 ||
      learned_result->hits.front().classification.kind !=
          classification::ContentKind::Application ||
      learned_result->hits.front().classification.kind_confidence !=
          classification::Confidence::Medium ||
      !std::ranges::contains(
          learned_result->hits.front().classification.evidence,
          classification::EvidenceCode::LearnedContentModel,
          &classification::Evidence::code) ||
      !learned_stats.learned_enabled || !learned_stats.learned_ready ||
      learned_stats.learned_training_records != 6 ||
      learned_stats.learned_classified_records != 1 ||
      learned_stats.learned_eligible_kinds != 2 ||
      learned_stats.learned_vocabulary_size == 0)
    return 28;

  search::InMemorySearchIndex completeness_index;
  auto completeness_rebuild = completeness_index.begin_rebuild(1);
  auto placeholder = torrent(7, "placeholder", 0, {"placeholder"}, 70);
  placeholder.name.reset();
  placeholder.files.clear();
  const auto zero_byte = torrent(8, "Empty File", 0, {"empty.txt"}, 80);
  if (!completeness_rebuild || !(*completeness_rebuild)->append(placeholder) ||
      !(*completeness_rebuild)->append(zero_byte) ||
      !(*completeness_rebuild)->commit())
    return 22;
  auto complete = completeness_index.search({.limit = 10});
  std::string placeholder_hash;
  for (std::size_t index = 0; index < core::InfoHash{}.bytes.size(); ++index)
    placeholder_hash += "07";
  auto placeholder_by_hash = completeness_index.search(
      {.text = std::move(placeholder_hash), .limit = 10});
  if (!complete || complete->total_matches != 1 ||
      complete->hits.front().name != "Empty File" ||
      complete->hits.front().total_size != 0 ||
      complete->hits.front().file_count != 1 || !placeholder_by_hash ||
      placeholder_by_hash->total_matches != 0)
    return 23;

  search::InMemorySearchIndex compact_index({.enabled = false});
  auto compact_rebuild = compact_index.begin_rebuild(1);
  if (!compact_rebuild)
    return 34;
  constexpr std::size_t compact_records = 10'000;
  for (std::size_t ordinal = 0; ordinal < compact_records; ++ordinal) {
    std::vector<std::string> paths;
    for (std::size_t file = 0; file < 8; ++file)
      paths.push_back("collection/season-01/episode-" +
                      std::to_string(ordinal) + "-file-" +
                      std::to_string(file) + ".mkv");
    auto value = torrent(static_cast<std::uint8_t>(ordinal),
                         "Compact Search Record " + std::to_string(ordinal),
                         8'000'000, std::move(paths), ordinal);
    for (std::size_t byte = 0; byte < sizeof(ordinal); ++byte)
      value.info_hash.bytes[byte] =
          static_cast<std::uint8_t>(ordinal >> (byte * 8U));
    if (!(*compact_rebuild)->append(value))
      return 35;
  }
  if (!(*compact_rebuild)->commit())
    return 36;
  const auto compact_stats = compact_index.classification_stats();
  auto compact_match =
      compact_index.search({.text = "episode-9999-file-7", .limit = 10});
  auto compact_first_page = compact_index.search({.limit = 10});
  auto compact_deep_page =
      compact_index.search({.offset = compact_records - 10, .limit = 10});
  auto compact_past_end =
      compact_index.search({.offset = compact_records + 1, .limit = 10});
  if (compact_stats.total_records != compact_records ||
      compact_stats.estimated_memory_bytes == 0 ||
      compact_stats.estimated_memory_bytes > 16U * 1024U * 1024U ||
      !compact_match || compact_match->total_matches != 1)
    return 37;
  if (!compact_first_page ||
      compact_first_page->total_matches != compact_records ||
      compact_first_page->hits.size() != 10 ||
      compact_first_page->hits.front().name != "Compact Search Record 9999" ||
      compact_first_page->hits.back().name != "Compact Search Record 9990" ||
      !compact_deep_page ||
      compact_deep_page->total_matches != compact_records ||
      compact_deep_page->hits.size() != 10 ||
      compact_deep_page->hits.front().name != "Compact Search Record 9" ||
      compact_deep_page->hits.back().name != "Compact Search Record 0" ||
      !compact_past_end || compact_past_end->total_matches != compact_records ||
      !compact_past_end->hits.empty())
    return 41;

  search::InMemorySearchIndex placeholder_index({.enabled = false});
  auto placeholder_rebuild = placeholder_index.begin_rebuild(1);
  if (!placeholder_rebuild)
    return 38;
  for (std::size_t ordinal = 0; ordinal < 50'000; ++ordinal) {
    auto value = torrent(static_cast<std::uint8_t>(ordinal), "placeholder", 0,
                         {"placeholder"}, ordinal);
    value.name.reset();
    value.files.clear();
    for (std::size_t byte = 0; byte < sizeof(ordinal); ++byte)
      value.info_hash.bytes[byte] =
          static_cast<std::uint8_t>(ordinal >> (byte * 8U));
    if (!(*placeholder_rebuild)->append(value))
      return 39;
  }
  if (!(*placeholder_rebuild)->commit() ||
      placeholder_index.classification_stats().total_records != 0 ||
      placeholder_index.classification_stats().estimated_memory_bytes != 0)
    return 40;
  return 0;
}

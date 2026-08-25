import std;

import sakuin.classification;
import sakuin.core;
import sakuin.model.torrent;

namespace {

using sakuin::classification::Classification;
using sakuin::classification::ClassificationState;
using sakuin::classification::ClassifierOptions;
using sakuin::classification::Confidence;
using sakuin::classification::ContentKind;
using sakuin::classification::ContentLabel;
using sakuin::classification::MediaCategory;
using sakuin::classification::VideoResolution;

sakuin::model::TorrentRecord
record(std::string name, std::vector<sakuin::model::FileRecord> files) {
  sakuin::core::InfoHash hash{};
  hash.bytes[0] = 1;
  std::uint64_t total{};
  for (const auto &file : files)
    total += file.size;
  return {.info_hash = hash,
          .first_seen = {},
          .last_seen = {},
          .name = std::move(name),
          .total_size = total,
          .files = std::move(files)};
}

bool label(const Classification &result, ContentLabel expected,
           Confidence confidence) {
  return sakuin::classification::label_confidence(result, expected) ==
         confidence;
}

} // namespace

int main() {
  using sakuin::classification::classify;
  using sakuin::model::FileRecord;

  const auto awaiting = classify(record("", {}));
  if (awaiting.state != ClassificationState::AwaitingMetadata ||
      awaiting.kind != ContentKind::Unknown)
    return 1;

  const auto movie = classify(
      record("Example.Movie.2024.2160p.WEB-DL",
             {FileRecord{"Example.Movie.2024.2160p.mkv", 8'000'000'000},
              FileRecord{"Example.Movie.2024.en.srt", 80'000}}));
  if (movie.state != ClassificationState::Classified ||
      movie.kind != ContentKind::Movie ||
      movie.kind_confidence != Confidence::High ||
      movie.resolution != VideoResolution::Uhd2160)
    return 2;
  const auto movie_categories = sakuin::classification::media_categories(movie);
  if (!std::ranges::contains(movie_categories, MediaCategory::Movie) ||
      !std::ranges::contains(movie_categories, MediaCategory::MovieUhd))
    return 16;

  const auto series = classify(
      record("Example.Show.S02E07.1080p",
             {FileRecord{"Example.Show.S02E07.1080p.mkv", 2'000'000'000}}));
  if (series.kind != ContentKind::Series ||
      series.kind_confidence != Confidence::High ||
      series.resolution != VideoResolution::Hd1080)
    return 3;

  std::vector<FileRecord> album_files;
  for (int track = 1; track <= 10; ++track)
    album_files.push_back(
        {"Album/" + std::to_string(track) + " - Track.flac", 30'000'000});
  album_files.push_back({"Album/cover.jpg", 2'000'000});
  const auto album = classify(record("Artist Album FLAC", album_files));
  std::ranges::reverse(album_files);
  const auto reversed_album =
      classify(record("Artist Album FLAC", album_files));
  if (album.kind != ContentKind::Music ||
      album.kind_confidence != Confidence::High || album != reversed_album)
    return 4;

  const auto audiobook =
      classify(record("Example Novel Unabridged Audiobook",
                      {FileRecord{"Chapter 01.mp3", 80'000'000},
                       FileRecord{"Chapter 02.mp3", 75'000'000}}));
  if (audiobook.kind != ContentKind::Audiobook ||
      audiobook.kind_confidence != Confidence::High)
    return 5;

  const auto ebook = classify(
      record("Example Ebook", {FileRecord{"Example.epub", 4'000'000}}));
  if (ebook.kind != ContentKind::Ebook ||
      ebook.kind_confidence != Confidence::High)
    return 6;

  const auto game = classify(
      record("Example Game", {FileRecord{"Example.xci", 12'000'000'000}}));
  if (game.kind != ContentKind::Game ||
      game.kind_confidence != Confidence::High)
    return 7;

  const auto application = classify(
      record("Example Software", {FileRecord{"example.rpm", 120'000'000}}));
  if (application.kind != ContentKind::Application ||
      application.kind_confidence != Confidence::High)
    return 8;

  const auto mixed = classify(
      record("Mixed collection", {FileRecord{"Feature.mkv", 500'000'000},
                                  FileRecord{"Soundtrack.flac", 500'000'000}}));
  if (mixed.kind != ContentKind::Mixed ||
      mixed.state != ClassificationState::Classified)
    return 9;

  const auto adult =
      classify(record("Explicit Porn Movie 2024 1080p",
                      {FileRecord{"Feature.mkv", 1'000'000'000}}));
  if (!label(adult, ContentLabel::Adult, Confidence::High))
    return 10;
  if (!std::ranges::contains(sakuin::classification::media_categories(adult),
                             MediaCategory::Adult))
    return 17;
  const auto adult_disabled =
      classify(record("Explicit Porn Movie 2024 1080p",
                      {FileRecord{"Feature.mkv", 1'000'000'000}}),
               ClassifierOptions{.adult_detection_enabled = false});
  if (sakuin::classification::label_confidence(adult_disabled,
                                               ContentLabel::Adult))
    return 11;

  const auto contextual_adult = classify(
      record("XXX Movie 2024", {FileRecord{"Feature.mkv", 1'000'000'000}}));
  if (!label(contextual_adult, ContentLabel::Adult, Confidence::Medium))
    return 12;
  if (std::ranges::contains(
          sakuin::classification::media_categories(contextual_adult),
          MediaCategory::Adult))
    return 18;

  const auto anime =
      classify(record("Example Anime S01E01 1080p",
                      {FileRecord{"Example.Anime.S01E01.mkv", 1'000'000'000}}));
  if (anime.kind != ContentKind::Series ||
      !label(anime, ContentLabel::Anime, Confidence::High))
    return 13;

  const auto bounded = classify(
      record("Example Movie 2024",
             {FileRecord{std::string(100, 'a') + ".mkv", 1'000'000'000}}),
      ClassifierOptions{.maximum_path_bytes = 8});
  if (!bounded.input_truncated)
    return 14;

  const auto unknown =
      classify(record("Unclassified data", {FileRecord{"payload.bin", 12}}));
  if (unknown.state != ClassificationState::Unknown ||
      unknown.kind != ContentKind::Unknown)
    return 15;

  return 0;
}

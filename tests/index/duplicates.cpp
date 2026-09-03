import std;

import sakuin.core;
import sakuin.index;
import sakuin.model.torrent;
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

sakuin::core::InfoHash info_hash(std::uint8_t value) {
  sakuin::core::InfoHash result;
  result.bytes.fill(value);
  return result;
}

sakuin::model::TorrentRecord
record(std::uint8_t hash, std::optional<std::string> name,
       std::uint64_t total_size, std::vector<sakuin::model::FileRecord> files) {
  return {.info_hash = info_hash(hash),
          .first_seen = {},
          .last_seen = {},
          .name = std::move(name),
          .total_size = total_size,
          .files = std::move(files)};
}

} // namespace

int main() {
  using namespace sakuin;
  const auto nonce = std::to_string(
      std::chrono::steady_clock::now().time_since_epoch().count());
  TemporaryDirectory directory{std::filesystem::temp_directory_path() /
                               ("sakuin-duplicates-" + nonce)};
  storage::LocalBlobStore blobs{directory.path / "blobs"};
  auto catalog =
      storage::LocalManifestCatalog::open(directory.path / "torrents", blobs);
  if (!catalog)
    return 1;
  storage::TorrentDataset torrents{blobs, **catalog};

  const std::array records{record(1, "Ubuntu Image", 100,
                                  {{.path = "docs/readme.txt", .size = 10},
                                   {.path = "payload.bin", .size = 90}}),
                           record(2, "Different Name", 100,
                                  {{.path = "payload.bin", .size = 90},
                                   {.path = "docs/readme.txt", .size = 10}}),
                           record(3, "  UBUNTU   image ", 100,
                                  {{.path = "DOCS\\README.TXT", .size = 10},
                                   {.path = "PAYLOAD.BIN", .size = 90}}),
                           record(4, "Ubuntu Image", 101,
                                  {{.path = "docs/readme.txt", .size = 10},
                                   {.path = "payload.bin", .size = 91}}),
                           record(5, std::nullopt, 0, {}),
                           record(6, "Renamed release", 100,
                                  {{.path = "notes/guide.TXT", .size = 10},
                                   {.path = "data/content.BIN", .size = 90}})};
  auto write = torrents.begin_write();
  if (!write || !(*write)->append(records) || !(*write)->commit())
    return 2;

  auto snapshot = torrents.keyed_snapshot();
  auto duplicates = index::DuplicateIndex::rebuild(**snapshot);
  if (!duplicates || duplicates->get()->stats().source_generation != 1 ||
      duplicates->get()->stats().records_read != 6 ||
      duplicates->get()->stats().records_indexed != 5 ||
      duplicates->get()->stats().fingerprints != 20 ||
      duplicates->get()->stats().groups != 4)
    return 3;

  auto exact =
      (*duplicates)
          ->groups(index::DuplicateFingerprintAlgorithm::ExactFileLayoutV1);
  auto normalized =
      (*duplicates)
          ->groups(index::DuplicateFingerprintAlgorithm::NormalizedMetadataV1);
  auto payload =
      (*duplicates)
          ->groups(index::DuplicateFingerprintAlgorithm::PayloadLayoutV1);
  auto release =
      (*duplicates)
          ->groups(index::DuplicateFingerprintAlgorithm::ReleaseIdentityV1);
  if (!exact || exact->size() != 1 || exact->front().torrents.size() != 2 ||
      exact->front().torrents[0] != info_hash(1) ||
      exact->front().torrents[1] != info_hash(2) || !normalized ||
      normalized->size() != 1 || normalized->front().torrents.size() != 2 ||
      normalized->front().torrents[0] != info_hash(1) ||
      normalized->front().torrents[1] != info_hash(3) || !payload ||
      payload->size() != 1 || payload->front().torrents.size() != 4 ||
      payload->front().torrents[0] != info_hash(1) ||
      payload->front().torrents[1] != info_hash(2) ||
      payload->front().torrents[2] != info_hash(3) ||
      payload->front().torrents[3] != info_hash(6) || !release ||
      release->size() != 1 || release->front().torrents.size() != 3 ||
      release->front().torrents[0] != info_hash(1) ||
      release->front().torrents[1] != info_hash(3) ||
      release->front().torrents[2] != info_hash(4))
    return 4;

  if ((*duplicates)->matches(info_hash(1)).size() != 4 ||
      (*duplicates)->matches(info_hash(2)).size() != 2 ||
      (*duplicates)->matches(info_hash(3)).size() != 3 ||
      (*duplicates)->matches(info_hash(4)).size() != 1 ||
      !(*duplicates)->matches(info_hash(5)).empty() ||
      (*duplicates)->matches(info_hash(6)).size() != 1)
    return 5;
  if ((*duplicates)
          ->groups(index::DuplicateFingerprintAlgorithm::ExactFileLayoutV1, 1))
    return 6;

  auto first = index::duplicate_fingerprint(
      records[0], index::DuplicateFingerprintAlgorithm::ExactFileLayoutV1);
  auto reordered = index::duplicate_fingerprint(
      records[1], index::DuplicateFingerprintAlgorithm::ExactFileLayoutV1);
  if (!first || !*first || !reordered || !*reordered || **first != **reordered)
    return 7;
  auto renamed = index::duplicate_fingerprint(
      records[5], index::DuplicateFingerprintAlgorithm::PayloadLayoutV1);
  auto payload_first = index::duplicate_fingerprint(
      records[0], index::DuplicateFingerprintAlgorithm::PayloadLayoutV1);
  const auto small = record(7, "small", 42, {{.path = "one.bin", .size = 42}});
  auto small_payload = index::duplicate_fingerprint(
      small, index::DuplicateFingerprintAlgorithm::PayloadLayoutV1);
  if (!renamed || !*renamed || !payload_first || !*payload_first ||
      **renamed != **payload_first || !small_payload || *small_payload)
    return 8;
  const auto release_first =
      record(8, "Example Show S01E02 1080p WEB-DL x265-GROUPA", 1'000'000'000,
             {{.path = "first.mkv", .size = 1'000'000'000}});
  const auto release_second =
      record(9, "example.show.s01e02.2160p.bluray.hevc-GROUPB", 4'000'000'000,
             {{.path = "second.mkv", .size = 4'000'000'000}});
  auto fuzzy_first = index::duplicate_fingerprint(
      release_first, index::DuplicateFingerprintAlgorithm::ReleaseIdentityV1);
  auto fuzzy_second = index::duplicate_fingerprint(
      release_second, index::DuplicateFingerprintAlgorithm::ReleaseIdentityV1);
  auto weak = index::duplicate_fingerprint(
      record(10, "Example", 1'000'000,
             {{.path = "example.bin", .size = 1'000'000}}),
      index::DuplicateFingerprintAlgorithm::ReleaseIdentityV1);
  if (!fuzzy_first || !*fuzzy_first || !fuzzy_second || !*fuzzy_second ||
      **fuzzy_first != **fuzzy_second || !weak || *weak)
    return 9;
  auto unicode_first = index::duplicate_fingerprint(
      record(11, "示例 剧集 S01E02 1080p", 1'000'000,
             {{.path = "first.mkv", .size = 1'000'000}}),
      index::DuplicateFingerprintAlgorithm::ReleaseIdentityV1);
  auto unicode_second = index::duplicate_fingerprint(
      record(12, "示例.剧集.S01E02.2160p.HEVC", 4'000'000,
             {{.path = "second.mkv", .size = 4'000'000}}),
      index::DuplicateFingerprintAlgorithm::ReleaseIdentityV1);
  auto different_episode = index::duplicate_fingerprint(
      record(13, "示例 剧集 S01E03 1080p", 1'000'000,
             {{.path = "third.mkv", .size = 1'000'000}}),
      index::DuplicateFingerprintAlgorithm::ReleaseIdentityV1);
  if (!unicode_first || !*unicode_first || !unicode_second ||
      !*unicode_second || **unicode_first != **unicode_second ||
      !different_episode || !*different_episode ||
      **unicode_first == **different_episode)
    return 10;
  return 0;
}

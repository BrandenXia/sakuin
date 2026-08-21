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
                           record(5, std::nullopt, 0, {})};
  auto write = torrents.begin_write();
  if (!write || !(*write)->append(records) || !(*write)->commit())
    return 2;

  auto snapshot = torrents.keyed_snapshot();
  auto duplicates = index::DuplicateIndex::rebuild(**snapshot);
  if (!duplicates || duplicates->get()->stats().source_generation != 1 ||
      duplicates->get()->stats().records_read != 5 ||
      duplicates->get()->stats().records_indexed != 4 ||
      duplicates->get()->stats().fingerprints != 8 ||
      duplicates->get()->stats().groups != 2)
    return 3;

  auto exact =
      (*duplicates)
          ->groups(index::DuplicateFingerprintAlgorithm::ExactFileLayoutV1);
  auto normalized =
      (*duplicates)
          ->groups(index::DuplicateFingerprintAlgorithm::NormalizedMetadataV1);
  if (!exact || exact->size() != 1 || exact->front().torrents.size() != 2 ||
      exact->front().torrents[0] != info_hash(1) ||
      exact->front().torrents[1] != info_hash(2) || !normalized ||
      normalized->size() != 1 || normalized->front().torrents.size() != 2 ||
      normalized->front().torrents[0] != info_hash(1) ||
      normalized->front().torrents[1] != info_hash(3))
    return 4;

  if ((*duplicates)->matches(info_hash(1)).size() != 2 ||
      (*duplicates)->matches(info_hash(2)).size() != 1 ||
      (*duplicates)->matches(info_hash(3)).size() != 1 ||
      !(*duplicates)->matches(info_hash(4)).empty() ||
      !(*duplicates)->matches(info_hash(5)).empty())
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
  return 0;
}

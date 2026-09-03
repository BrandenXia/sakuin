import std;

import sakuin.core;
import sakuin.index.local;
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

sakuin::model::TorrentRecord record(std::uint8_t hash, std::string name,
                                    std::string path) {
  return {.info_hash = info_hash(hash),
          .first_seen = {},
          .last_seen = {},
          .name = std::move(name),
          .total_size = 42,
          .files = {{.path = std::move(path), .size = 42}}};
}

sakuin::model::TorrentRecord payload_record(std::uint8_t hash, std::string name,
                                            std::string prefix) {
  return {.info_hash = info_hash(hash),
          .first_seen = {},
          .last_seen = {},
          .name = std::move(name),
          .total_size = 100,
          .files = {{.path = prefix + "/document.txt", .size = 10},
                    {.path = prefix + "/archive.bin", .size = 90}}};
}

} // namespace

int main() {
  using namespace sakuin;
  const auto nonce = std::to_string(
      std::chrono::steady_clock::now().time_since_epoch().count());
  TemporaryDirectory directory{std::filesystem::temp_directory_path() /
                               ("sakuin-local-duplicates-" + nonce)};
  storage::LocalBlobStore blobs{directory.path / "blobs"};
  auto catalog =
      storage::LocalManifestCatalog::open(directory.path / "torrents", blobs);
  if (!catalog)
    return 1;
  storage::TorrentDataset torrents{blobs, **catalog};
  const std::array records{
      record(1, "Example", "payload.bin"),
      record(2, "Other", "payload.bin"),
      record(3, " example ", "PAYLOAD.BIN"),
      record(4, "Unique", "different.bin"),
      payload_record(6, "Example Show S01E02 1080p WEB-DL", "first"),
      payload_record(7, "example.show.s01e02.2160p.bluray", "renamed")};
  auto write = torrents.begin_write();
  if (!write || !(*write)->append(records) || !(*write)->commit())
    return 2;
  auto snapshot = torrents.keyed_snapshot();
  if (!snapshot)
    return 3;

  const auto path = directory.path / "derived" / "duplicates.bin";
  auto rebuilt = index::LocalDuplicateIndex::rebuild(path, **snapshot);
  if (!rebuilt || (*rebuilt)->stats().source_generation != 1 ||
      (*rebuilt)->stats().records_indexed != 6 ||
      (*rebuilt)->stats().fingerprints != 16 || (*rebuilt)->stats().groups != 4)
    return 4;
  auto exact = (*rebuilt)->groups(
      index::DuplicateFingerprintAlgorithm::ExactFileLayoutV1);
  if (!exact || exact->size() != 1 || exact->front().torrents.size() != 2)
    return 5;
  auto payload =
      (*rebuilt)->groups(index::DuplicateFingerprintAlgorithm::PayloadLayoutV1);
  if (!payload || payload->size() != 1 || payload->front().torrents.size() != 2)
    return 5;
  auto release = (*rebuilt)->groups(
      index::DuplicateFingerprintAlgorithm::ReleaseIdentityV1);
  if (!release || release->size() != 1 || release->front().torrents.size() != 2)
    return 5;

  rebuilt->reset();
  auto reopened = index::LocalDuplicateIndex::open(path);
  if (!reopened || (*reopened)->stats().source_generation != 1 ||
      (*reopened)->matches(info_hash(1)).size() != 2 ||
      (*reopened)->matches(info_hash(4)).size() != 0 ||
      (*reopened)->matches(info_hash(6)).size() != 2)
    return 6;

  {
    std::fstream corrupt{path, std::ios::binary | std::ios::in | std::ios::out};
    corrupt.seekg(-1, std::ios::end);
    char value{};
    corrupt.read(&value, 1);
    value ^= 0x5a;
    corrupt.seekp(-1, std::ios::end);
    corrupt.write(&value, 1);
  }
  auto rejected = index::LocalDuplicateIndex::open(path);
  if (rejected || rejected.error().code != core::ErrorCode::ChecksumMismatch)
    return 7;

  auto repaired = index::LocalDuplicateIndex::rebuild(path, **snapshot);
  if (!repaired || (*repaired)->matches(info_hash(1)).size() != 2)
    return 8;
  auto initial_sync = (*repaired)->synchronize(torrents);
  if (!initial_sync || !initial_sync->full_rebuild ||
      initial_sync->records_processed != 6 ||
      !(*repaired)->cursor().initialized)
    return 9;
  const std::array update{record(5, "Unique", "different.bin")};
  auto update_write = torrents.begin_write();
  if (!update_write || !(*update_write)->append(update) ||
      !(*update_write)->commit())
    return 10;
  auto incremental = (*repaired)->synchronize(torrents);
  if (!incremental || incremental->full_rebuild ||
      incremental->source_generation != 2 ||
      incremental->records_processed != 1 ||
      (*repaired)->matches(info_hash(4)).size() != 2)
    return 11;
  repaired->reset();
  auto current = index::LocalDuplicateIndex::open(path);
  if (!current || (*current)->cursor().source_generation != 2 ||
      (*current)->matches(info_hash(5)).size() != 2)
    return 12;
  if (!torrents.compact({.minimum_segment_count = 2}))
    return 13;
  auto after_compaction = (*current)->synchronize(torrents);
  if (!after_compaction || !after_compaction->full_rebuild ||
      after_compaction->source_generation != 3 ||
      after_compaction->records_processed != 7)
    return 14;
  auto empty = index::LocalDuplicateIndex::open(directory.path / "missing.bin");
  if (!empty || (*empty)->stats().source_generation != 0 ||
      !(*empty)->matches(info_hash(1)).empty())
    return 15;
  return 0;
}

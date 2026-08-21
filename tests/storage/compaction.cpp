import std;

import sakuin.core;
import sakuin.model.observation;
import sakuin.storage;
import sakuin.storage.dataset.observations;

namespace {

struct TemporaryDirectory {
  std::filesystem::path path;
  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path, ignored);
  }
};

sakuin::model::ObservationRecord observation(std::uint8_t value) {
  sakuin::core::InfoHash hash;
  hash.bytes.fill(value);
  return {hash, sakuin::core::Timestamp{
                    sakuin::core::Timestamp::duration{value}}};
}

} // namespace

int main() {
  using namespace sakuin;
  const auto nonce = std::to_string(
      std::chrono::steady_clock::now().time_since_epoch().count());
  TemporaryDirectory directory{std::filesystem::temp_directory_path() /
                               ("sakuin-compaction-" + nonce)};
  storage::LocalBlobStore blobs{directory.path / "blobs"};
  auto catalog =
      storage::LocalManifestCatalog::open(directory.path / "catalog", blobs);
  if (!catalog)
    return 1;
  storage::ObservationDataset dataset{blobs, **catalog};

  for (const auto records : {std::vector{observation(1), observation(2)},
                             std::vector{observation(3)}}) {
    auto write = dataset.begin_write();
    if (!write || !(*write)->append(records) || !(*write)->commit())
      return 2;
  }
  auto old_snapshot = dataset.snapshot();
  auto old_manifest = (*catalog)->pin_current();
  if (!old_snapshot || !old_manifest ||
      (*old_manifest)->manifest().segments.size() != 2)
    return 3;
  const auto old_objects = std::array{
      (*old_manifest)->manifest().segments[0].object,
      (*old_manifest)->manifest().segments[1].object};

  auto compacted = storage::RowV1DatasetMaintenance::compact(blobs, **catalog);
  if (!compacted || compacted->segments_created != 1 ||
      compacted->segments_removed != 2 ||
      (*catalog)->current_id().generation != 3)
    return 4;
  auto current_manifest = (*catalog)->pin_current();
  if (!current_manifest || (*current_manifest)->manifest().segments.size() != 1)
    return 5;

  auto verified = storage::RowV1DatasetMaintenance::verify(blobs, **catalog);
  if (!verified || verified->segments_checked != 1 ||
      verified->records_checked != 3)
    return 6;

  for (const auto &object : old_objects) {
    auto exists = blobs.exists(object);
    if (!exists || !*exists)
      return 7;
  }

  for (auto snapshot : {old_snapshot, dataset.snapshot()}) {
    if (!snapshot)
      return 8;
    auto stream = (*snapshot)->scan({});
    std::uint8_t expected = 1;
    while (true) {
      auto next = (*stream)->next();
      if (!next)
        return 9;
      if (!*next)
        break;
      if ((*next)->info_hash.bytes[0] != expected++)
        return 10;
    }
    if (expected != 4)
      return 11;
  }

  auto no_op = storage::RowV1DatasetMaintenance::compact(blobs, **catalog);
  if (!no_op || no_op->segments_created != 0 ||
      (*catalog)->current_id().generation != 3)
    return 12;

  auto pinned_gc =
      storage::RowV1DatasetMaintenance::garbage_collect(**catalog);
  if (!pinned_gc || pinned_gc->objects_deleted != 0)
    return 13;
  for (const auto &object : old_objects) {
    auto exists = blobs.exists(object);
    if (!exists || !*exists)
      return 14;
  }

  old_snapshot->reset();
  old_manifest->reset();
  auto reclaimed =
      storage::RowV1DatasetMaintenance::garbage_collect(**catalog);
  if (!reclaimed || reclaimed->objects_deleted != 2 ||
      reclaimed->bytes_reclaimed == 0)
    return 15;
  for (const auto &object : old_objects) {
    auto exists = blobs.exists(object);
    if (!exists || *exists)
      return 16;
  }
  if (std::filesystem::exists(directory.path / "catalog" / "manifests" /
                              "1.manifest") ||
      std::filesystem::exists(directory.path / "catalog" / "manifests" /
                              "2.manifest") ||
      !std::filesystem::exists(directory.path / "catalog" / "manifests" /
                               "3.manifest"))
    return 17;

  auto reopened =
      storage::LocalManifestCatalog::open(directory.path / "catalog", blobs);
  if (!reopened)
    return 18;
  auto restart_verification =
      storage::RowV1DatasetMaintenance::verify(blobs, **reopened);
  if (!restart_verification || restart_verification->segments_checked != 1 ||
      restart_verification->records_checked != 3)
    return 19;
  return 0;
}

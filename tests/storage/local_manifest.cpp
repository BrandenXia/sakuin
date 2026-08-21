import std;

import sakuin.core;
import sakuin.storage;

namespace {

struct TemporaryDirectory {
  std::filesystem::path path;
  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path, ignored);
  }
};

} // namespace

int main() {
  using namespace sakuin;
  const auto nonce = std::to_string(
      std::chrono::steady_clock::now().time_since_epoch().count());
  TemporaryDirectory directory{
      std::filesystem::temp_directory_path() / ("sakuin-manifest-test-" + nonce)};
  storage::LocalBlobStore blobs{directory.path / "blobs"};

  auto segment_writer = storage::RowV1SegmentWriter::create(blobs);
  const core::ByteBuffer record{std::byte{1}, std::byte{2}, std::byte{3}};
  if (!segment_writer || !(*segment_writer)->append(record))
    return 1;
  auto segment = (*segment_writer)->finalize();
  if (!segment)
    return 2;

  auto catalog = storage::LocalManifestCatalog::open(directory.path / "catalog", blobs);
  if (!catalog || (*catalog)->current_id().generation != 0)
    return 3;
  auto generation_zero = (*catalog)->pin_current();
  if (!generation_zero || (*generation_zero)->manifest().id.generation != 0)
    return 4;

  auto generation_one = (*catalog)->publish({0}, {*segment});
  if (!generation_one || generation_one->generation != 1 ||
      !(*catalog)->is_pinned({0}))
    return 5;
  auto pinned_one = (*catalog)->pin_current();
  if (!pinned_one || (*pinned_one)->manifest().segments.size() != 1)
    return 6;

  auto generation_two = (*catalog)->publish({1}, {});
  if (!generation_two || generation_two->generation != 2 ||
      !(*catalog)->is_pinned({1}) ||
      (*pinned_one)->manifest().segments.front().object != segment->object)
    return 7;

  generation_zero->reset();
  if ((*catalog)->is_pinned({0}))
    return 8;

  auto reopened = storage::LocalManifestCatalog::open(directory.path / "catalog", blobs);
  if (!reopened || (*reopened)->current_id().generation != 2)
    return 9;
  auto current = (*reopened)->pin_current();
  if (!current || !(*current)->manifest().segments.empty())
    return 10;

  auto missing = *segment;
  missing.object = {};
  auto rejected = (*reopened)->publish({2}, {missing});
  if (rejected || rejected.error().code != core::ErrorCode::InvalidManifest)
    return 11;

  return 0;
}

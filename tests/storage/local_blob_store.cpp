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

int fail(int code) { return code; }

} // namespace

int main() {
  using namespace sakuin;

  const auto nonce = std::to_string(
      std::chrono::steady_clock::now().time_since_epoch().count());
  TemporaryDirectory directory{
      std::filesystem::temp_directory_path() / ("sakuin-blob-test-" + nonce)};
  storage::LocalBlobStore store{directory.path};

  auto created = store.create();
  if (!created)
    return fail(1);

  const core::ByteBuffer payload{std::byte{0x10}, std::byte{0x20},
                                 std::byte{0x30}, std::byte{0x40}};
  if (auto written = (*created)->write(payload); !written)
    return fail(2);
  auto finalized = (*created)->finalize();
  if (!finalized)
    return fail(3);

  auto found = store.exists(*finalized);
  if (!found || !*found)
    return fail(4);

  auto opened = store.open(*finalized);
  if (!opened || (*opened)->size() != payload.size())
    return fail(5);
  core::ByteBuffer output(payload.size());
  auto count = (*opened)->read(output);
  if (!count || *count != payload.size() || output != payload)
    return fail(6);

  auto duplicate = store.create();
  if (!duplicate || !(*duplicate)->write(payload))
    return fail(7);
  auto duplicate_id = (*duplicate)->finalize();
  if (!duplicate_id || *duplicate_id != *finalized)
    return fail(8);

  auto aborted = store.create();
  if (!aborted || !(*aborted)->write(payload))
    return fail(9);
  (*aborted)->abort();

  if (auto removed = store.remove(*finalized); !removed)
    return fail(10);
  found = store.exists(*finalized);
  if (!found || *found)
    return fail(11);
  opened = store.open(*finalized);
  if (opened || opened.error().code != core::ErrorCode::NotFound)
    return fail(12);

  return 0;
}

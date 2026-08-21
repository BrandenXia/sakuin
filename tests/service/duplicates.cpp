import std;

import sakuin.config.model;
import sakuin.core;
import sakuin.index.duplicates;
import sakuin.model.torrent;
import sakuin.service.duplicates;
import sakuin.service.storage;

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

sakuin::model::TorrentRecord record(std::uint8_t hash, std::string name) {
  return {.info_hash = info_hash(hash),
          .first_seen = {},
          .last_seen = {},
          .name = std::move(name),
          .total_size = 100,
          .files = {{.path = "payload.bin", .size = 100}}};
}

class Observer final : public sakuin::service::DuplicateIndexObserver {
public:
  void on_duplicate_index_synchronized(
      sakuin::index::DuplicateSynchronizationResult result) override {
    std::lock_guard lock{mutex};
    last = result;
  }
  void on_duplicate_index_error(sakuin::core::Error error) override {
    std::lock_guard lock{mutex};
    last_error = std::move(error);
  }

  std::mutex mutex;
  std::optional<sakuin::index::DuplicateSynchronizationResult> last;
  std::optional<sakuin::core::Error> last_error;
};

} // namespace

int main() {
  using namespace sakuin;
  const auto nonce = std::to_string(
      std::chrono::steady_clock::now().time_since_epoch().count());
  TemporaryDirectory directory{std::filesystem::temp_directory_path() /
                               ("sakuin-duplicate-service-" + nonce)};
  config::StorageConfig storage_config;
  storage_config.local_root = directory.path;
  auto storage = service::LocalCanonicalStorage::open(storage_config);
  if (!storage)
    return 1;
  const std::array records{record(1, "First"), record(2, "Second")};
  auto write = (*storage)->torrents().begin_write();
  if (!write || !(*write)->append(records) || !(*write)->commit())
    return 2;

  Observer observer;
  config::DuplicateIndexConfig configuration;
  configuration.interval = std::chrono::hours{1};
  auto coordinator = service::DuplicateIndexCoordinator::create(
      **storage, configuration, &observer);
  if (!coordinator)
    return 3;
  auto synchronized = (*coordinator)->run_once();
  auto groups =
      (*coordinator)
          ->groups(index::DuplicateFingerprintAlgorithm::ExactFileLayoutV1);
  if (!synchronized || !synchronized->full_rebuild ||
      synchronized->source_generation != 1 || !groups || groups->size() != 1 ||
      groups->front().torrents.size() != 2)
    return 4;
  auto unchanged = (*coordinator)->run_once();
  if (!unchanged || unchanged->full_rebuild ||
      unchanged->records_processed != 0)
    return 5;
  if (!(*coordinator)->start() || !(*coordinator)->running() ||
      (*coordinator)->start())
    return 6;
  (*coordinator)->stop();
  if ((*coordinator)->running())
    return 7;
  return 0;
}

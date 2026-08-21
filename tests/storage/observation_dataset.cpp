import std;
import sakuin.core;
import sakuin.model.observation;
import sakuin.storage;
import sakuin.storage.dataset.observations;

namespace {
struct Temp { std::filesystem::path path; ~Temp(){std::error_code e; std::filesystem::remove_all(path,e);} };
sakuin::model::ObservationRecord record(std::uint8_t value, std::int64_t ticks) {
  sakuin::core::InfoHash hash; hash.bytes.fill(value);
  return {hash, sakuin::core::Timestamp{sakuin::core::Timestamp::duration{ticks}}};
}
}

int main() {
  using namespace sakuin;
  Temp temp{std::filesystem::temp_directory_path() /
            ("sakuin-observations-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()))};
  storage::LocalBlobStore blobs{temp.path / "blobs"};
  auto catalog = storage::LocalManifestCatalog::open(temp.path / "catalog", blobs);
  if (!catalog) return 1;
  storage::ObservationDataset dataset{blobs, **catalog};

  auto old_snapshot = dataset.snapshot();
  auto first_write = dataset.begin_write();
  const std::array first_records{record(1, 10), record(2, 20)};
  if (!old_snapshot || !first_write || !(*first_write)->append(first_records)) return 2;
  auto first_commit = (*first_write)->commit();
  if (!first_commit || first_commit->generation != 1) return 3;

  auto old_stream = (*old_snapshot)->scan({});
  if (!old_stream || (*old_stream)->next()->has_value()) return 4;
  auto first_snapshot = dataset.snapshot();
  auto first_stream = (*first_snapshot)->scan({});
  if (!first_stream) return 5;
  for (const auto &expected : first_records) {
    auto next = (*first_stream)->next();
    if (!next || !*next || (*next)->info_hash != expected.info_hash ||
        (*next)->observed_at != expected.observed_at) return 6;
  }
  if ((*first_stream)->next()->has_value()) return 7;

  auto stale = dataset.begin_write();
  auto winner = dataset.begin_write();
  if (!stale || !winner || !(*stale)->append(record(3,30)) ||
      !(*winner)->append(record(4,40))) return 8;
  auto winner_commit = (*winner)->commit();
  if (!winner_commit || winner_commit->generation != 2) return 9;
  auto stale_commit = (*stale)->commit();
  if (stale_commit || stale_commit.error().code != core::ErrorCode::Conflict) return 10;

  auto pinned_stream = (*first_snapshot)->scan({});
  first_snapshot->reset();
  if (!(*catalog)->is_pinned({1})) return 11;
  pinned_stream->reset();

  auto empty = dataset.begin_write();
  auto empty_commit = (*empty)->commit();
  if (!empty_commit || empty_commit->generation != 2 ||
      (*catalog)->current_id().generation != 2) return 12;

  auto reopened = storage::LocalManifestCatalog::open(temp.path / "catalog", blobs);
  if (!reopened) return 13;
  storage::ObservationDataset restarted{blobs, **reopened};
  auto snapshot = restarted.snapshot();
  auto stream = (*snapshot)->scan({});
  std::size_t count{};
  while (true) {
    auto next = (*stream)->next();
    if (!next) return 14;
    if (!*next) break;
    ++count;
  }
  if (count != 3) return 15;
  return 0;
}

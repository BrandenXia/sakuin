import std;

import sakuin.config;
import sakuin.core;
import sakuin.model.observation;
import sakuin.service.storage;
import sakuin.storage.dataset.observations;

namespace {

struct TemporaryDirectory {
  TemporaryDirectory() {
    path = std::filesystem::temp_directory_path() /
           ("sakuin-service-storage-" +
            std::to_string(
                std::chrono::steady_clock::now().time_since_epoch().count()));
  }
  ~TemporaryDirectory() { std::filesystem::remove_all(path); }
  std::filesystem::path path;
};

sakuin::model::ObservationRecord observation(std::uint8_t marker) {
  sakuin::model::ObservationRecord result;
  result.info_hash.bytes.front() = marker;
  result.observed_at =
      sakuin::core::Timestamp{std::chrono::seconds{marker + 1}};
  return result;
}

sakuin::core::Result<std::size_t>
observation_count(sakuin::storage::ObservationDataset &dataset) {
  auto snapshot = dataset.snapshot();
  if (!snapshot)
    return std::unexpected(snapshot.error());
  auto stream = (*snapshot)->scan({});
  if (!stream)
    return std::unexpected(stream.error());
  std::size_t count{};
  while (true) {
    auto next = (*stream)->next();
    if (!next)
      return std::unexpected(next.error());
    if (!*next)
      return count;
    ++count;
  }
}

} // namespace

int main() {
  using namespace sakuin;

  TemporaryDirectory temporary;
  auto configuration = config::defaults().storage;
  configuration.local_root = temporary.path;
  configuration.block_target_bytes = 64;
  configuration.segment_target_bytes = 64;
  configuration.compression = config::CompressionCodec::None;

  auto local = service::LocalCanonicalStorage::open(configuration);
  if (!local || (*local)->observation_batch_size() != 2)
    return 1;

  constexpr std::size_t ThreadCount = 4;
  constexpr std::size_t RecordsPerThread = 8;
  std::atomic<bool> failed{};
  std::vector<std::jthread> writers;
  for (std::size_t thread = 0; thread < ThreadCount; ++thread)
    writers.emplace_back([&, thread] {
      for (std::size_t record = 0; record < RecordsPerThread; ++record)
        if (auto written = (*local)->observations().observe(observation(
                static_cast<std::uint8_t>(thread * RecordsPerThread + record)));
            !written)
          failed = true;
    });
  writers.clear();
  if (failed || !(*local)->flush())
    return 2;
  auto count = observation_count((*local)->observation_dataset());
  if (!count || *count != ThreadCount * RecordsPerThread)
    return 3;

  auto locked = service::LocalCanonicalStorage::open(configuration);
  if (locked || locked.error().code != core::ErrorCode::Conflict)
    return 4;
  auto compacted = (*local)->compact(service::LocalDataset::Observations);
  auto verified = (*local)->verify(service::LocalDataset::Observations);
  auto collected =
      (*local)->garbage_collect(service::LocalDataset::Observations);
  if (!compacted || compacted->segments_removed < 2 || !verified ||
      verified->records_checked != ThreadCount * RecordsPerThread || !collected)
    return 5;

  (*local).reset();
  auto reopened = service::LocalCanonicalStorage::open(configuration);
  if (!reopened)
    return 6;
  count = observation_count((*reopened)->observation_dataset());
  if (!count || *count != ThreadCount * RecordsPerThread)
    return 7;

  configuration.block_target_bytes =
      static_cast<std::uint64_t>(std::numeric_limits<std::uint32_t>::max()) + 1;
  auto invalid = service::LocalCanonicalStorage::open(configuration);
  if (invalid || invalid.error().code != core::ErrorCode::InvalidArgument)
    return 8;
  return 0;
}

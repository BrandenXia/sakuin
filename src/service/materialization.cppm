module;

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>

export module sakuin.service.materialization;

import std;

import sakuin.config.model;
import sakuin.core.result;
import sakuin.index.materialize;
import sakuin.service.storage;

export namespace sakuin::service {

class MaterializationObserver {
public:
  virtual ~MaterializationObserver() = default;
  virtual void on_materialization_completed(
      index::IncrementalMaterializationResult result) = 0;
  virtual void on_materialization_error(core::Error error) = 0;
};

class LocalMaterializationCheckpointStore final {
public:
  explicit LocalMaterializationCheckpointStore(std::filesystem::path path)
      : path_(std::move(path)) {}

  core::Result<index::MaterializationCheckpoint> load() const;
  core::Result<void>
  save(const index::MaterializationCheckpoint &checkpoint) const;
  const std::filesystem::path &path() const noexcept { return path_; }

private:
  std::filesystem::path path_;
};

// Periodically publishes buffered observations, folds them into the durable
// torrent view, then atomically advances the checkpoint. The jthread is an
// implementation detail behind this Sakuin-owned lifecycle boundary.
class TorrentMaterializationCoordinator final {
public:
  TorrentMaterializationCoordinator(
      LocalCanonicalStorage &storage,
      config::StorageConfig::MaterializationConfig configuration,
      MaterializationObserver *observer = nullptr,
      std::function<void(std::uint64_t)> on_torrents_committed = {});
  ~TorrentMaterializationCoordinator();

  TorrentMaterializationCoordinator(const TorrentMaterializationCoordinator &) =
      delete;
  TorrentMaterializationCoordinator &
  operator=(const TorrentMaterializationCoordinator &) = delete;

  core::Result<void> start();
  void stop() noexcept;
  bool running() const noexcept { return running_.load(); }
  core::Result<index::IncrementalMaterializationResult> run_once();

private:
  core::Result<void> load_checkpoint();
  void worker(std::stop_token stop);
  void notify(index::IncrementalMaterializationResult result) noexcept;
  void notify_error(core::Error error) noexcept;

  LocalCanonicalStorage &storage_;
  config::StorageConfig::MaterializationConfig configuration_;
  LocalMaterializationCheckpointStore checkpoints_;
  MaterializationObserver *observer_{};
  std::function<void(std::uint64_t)> on_torrents_committed_;
  index::MaterializationCheckpoint checkpoint_;
  bool checkpoint_loaded_{};
  std::mutex run_mutex_;
  std::atomic<bool> running_{};
  std::mutex wait_mutex_;
  std::condition_variable_any wake_;
  std::jthread worker_;
};

} // namespace sakuin::service

namespace sakuin::service {
namespace {

core::Error io_error(std::string action, const std::filesystem::path &path,
                     std::string detail = {}) {
  std::string message = std::move(action) + ": " + path.string();
  if (!detail.empty())
    message += ": " + detail;
  return {core::ErrorCode::IoError, std::move(message)};
}

core::Result<void> sync_path(const std::filesystem::path &path,
                             bool directory) {
  const auto descriptor = ::open(
      path.c_str(), O_RDONLY | O_CLOEXEC | (directory ? O_DIRECTORY : 0));
  if (descriptor < 0)
    return std::unexpected(
        io_error("Could not open checkpoint path", path, std::strerror(errno)));
  if (::fsync(descriptor) != 0) {
    const auto detail = std::string{std::strerror(errno)};
    ::close(descriptor);
    return std::unexpected(
        io_error("Could not sync checkpoint path", path, detail));
  }
  ::close(descriptor);
  return {};
}

core::Result<std::uint64_t> parse_value(std::string_view line,
                                        std::string_view key) {
  if (!line.starts_with(key) || line.size() == key.size() ||
      line[key.size()] != '=')
    return std::unexpected(core::Error{
        core::ErrorCode::InvalidManifest,
        "Materialization checkpoint is malformed at " + std::string{key}});
  const auto text = line.substr(key.size() + 1);
  std::uint64_t result{};
  const auto [end, error] =
      std::from_chars(text.data(), text.data() + text.size(), result);
  if (error != std::errc{} || end != text.data() + text.size())
    return std::unexpected(core::Error{
        core::ErrorCode::InvalidManifest,
        "Materialization checkpoint has an invalid " + std::string{key}});
  return result;
}

} // namespace

core::Result<index::MaterializationCheckpoint>
LocalMaterializationCheckpointStore::load() const {
  std::error_code status_error;
  if (!std::filesystem::exists(path_, status_error)) {
    if (status_error)
      return std::unexpected(io_error("Could not inspect checkpoint", path_,
                                      status_error.message()));
    return index::MaterializationCheckpoint{};
  }
  std::ifstream input{path_, std::ios::binary};
  if (!input)
    return std::unexpected(io_error("Could not open checkpoint", path_));
  std::array<std::string, 4> lines;
  for (auto &line : lines)
    if (!std::getline(input, line))
      return std::unexpected(
          core::Error{core::ErrorCode::InvalidManifest,
                      "Materialization checkpoint is truncated"});
  std::string trailing;
  if (std::getline(input, trailing) && !trailing.empty())
    return std::unexpected(
        core::Error{core::ErrorCode::InvalidManifest,
                    "Materialization checkpoint has trailing data"});
  if (lines[0] != "sakuin-materialization-checkpoint-v1")
    return std::unexpected(
        core::Error{core::ErrorCode::UnsupportedFormat,
                    "Materialization checkpoint version is unsupported"});
  auto processed = parse_value(lines[1], "observations_processed");
  auto source = parse_value(lines[2], "source_generation");
  auto destination = parse_value(lines[3], "destination_generation");
  if (!processed)
    return std::unexpected(processed.error());
  if (!source)
    return std::unexpected(source.error());
  if (!destination)
    return std::unexpected(destination.error());
  return index::MaterializationCheckpoint{.observations_processed = *processed,
                                          .source_generation = *source,
                                          .destination_generation =
                                              *destination};
}

core::Result<void> LocalMaterializationCheckpointStore::save(
    const index::MaterializationCheckpoint &checkpoint) const {
  std::error_code directory_error;
  std::filesystem::create_directories(path_.parent_path(), directory_error);
  if (directory_error)
    return std::unexpected(io_error("Could not create checkpoint directory",
                                    path_.parent_path(),
                                    directory_error.message()));
  const auto contents =
      "sakuin-materialization-checkpoint-v1\nobservations_processed=" +
      std::to_string(checkpoint.observations_processed) +
      "\nsource_generation=" + std::to_string(checkpoint.source_generation) +
      "\ndestination_generation=" +
      std::to_string(checkpoint.destination_generation) + "\n";
  const auto temporary = path_.string() + ".temporary";
  {
    std::ofstream output{temporary, std::ios::binary | std::ios::trunc};
    output.write(contents.data(),
                 static_cast<std::streamsize>(contents.size()));
    output.flush();
    if (!output)
      return std::unexpected(io_error("Could not write checkpoint", temporary));
  }
  if (auto synced = sync_path(temporary, false); !synced)
    return synced;
  std::error_code rename_error;
  std::filesystem::rename(temporary, path_, rename_error);
  if (rename_error)
    return std::unexpected(io_error("Could not publish checkpoint", path_,
                                    rename_error.message()));
  return sync_path(path_.parent_path(), true);
}

TorrentMaterializationCoordinator::TorrentMaterializationCoordinator(
    LocalCanonicalStorage &storage,
    config::StorageConfig::MaterializationConfig configuration,
    MaterializationObserver *observer,
    std::function<void(std::uint64_t)> on_torrents_committed)
    : storage_(storage), configuration_(configuration),
      checkpoints_(storage.root() / "operational" /
                   "materialization.checkpoint"),
      observer_(observer),
      on_torrents_committed_(std::move(on_torrents_committed)) {}

TorrentMaterializationCoordinator::~TorrentMaterializationCoordinator() {
  stop();
}

core::Result<void> TorrentMaterializationCoordinator::start() {
  if (running_.exchange(true))
    return std::unexpected(
        core::Error{core::ErrorCode::Conflict,
                    "Torrent materialization is already running"});
  {
    std::lock_guard lock{run_mutex_};
    if (auto loaded = load_checkpoint(); !loaded) {
      running_ = false;
      return loaded;
    }
  }
  try {
    worker_ =
        std::jthread{[this](std::stop_token stop) { worker(std::move(stop)); }};
  } catch (const std::exception &exception) {
    running_ = false;
    return std::unexpected(
        core::Error{core::ErrorCode::Internal,
                    "Unable to start torrent materialization worker: " +
                        std::string{exception.what()}});
  }
  return {};
}

void TorrentMaterializationCoordinator::stop() noexcept {
  if (!running_.exchange(false))
    return;
  worker_.request_stop();
  wake_.notify_all();
  if (worker_.joinable())
    worker_.join();
}

core::Result<void> TorrentMaterializationCoordinator::load_checkpoint() {
  if (checkpoint_loaded_)
    return {};
  auto loaded = checkpoints_.load();
  if (!loaded)
    return std::unexpected(loaded.error());
  checkpoint_ = *loaded;
  checkpoint_loaded_ = true;
  return {};
}

core::Result<index::IncrementalMaterializationResult>
TorrentMaterializationCoordinator::run_once() {
  std::lock_guard lock{run_mutex_};
  if (auto loaded = load_checkpoint(); !loaded)
    return std::unexpected(loaded.error());
  if (auto flushed = storage_.flush(); !flushed)
    return std::unexpected(flushed.error());
  auto source = storage_.observation_dataset().snapshot();
  if (!source)
    return std::unexpected(source.error());
  if ((*source)->id().generation == checkpoint_.source_generation)
    return index::IncrementalMaterializationResult{.checkpoint = checkpoint_};
  auto advanced = index::ObservationMaterializer::advance(
      **source, storage_.torrents(), checkpoint_);
  if (!advanced)
    return std::unexpected(advanced.error());
  if (auto saved = checkpoints_.save(advanced->checkpoint); !saved)
    return std::unexpected(saved.error());
  checkpoint_ = advanced->checkpoint;
  if (advanced->torrents_updated != 0 && on_torrents_committed_) {
    try {
      on_torrents_committed_(checkpoint_.destination_generation);
    } catch (...) {
      // The durable torrent view and checkpoint have already committed. Search
      // refresh is a best-effort notification and will catch up independently.
    }
  }
  return advanced;
}

void TorrentMaterializationCoordinator::notify(
    index::IncrementalMaterializationResult result) noexcept {
  if (!observer_)
    return;
  try {
    observer_->on_materialization_completed(std::move(result));
  } catch (...) {
  }
}

void TorrentMaterializationCoordinator::notify_error(
    core::Error error) noexcept {
  if (!observer_)
    return;
  try {
    observer_->on_materialization_error(std::move(error));
  } catch (...) {
  }
}

void TorrentMaterializationCoordinator::worker(std::stop_token stop) {
  while (!stop.stop_requested()) {
    auto result = run_once();
    if (result)
      notify(std::move(*result));
    else
      notify_error(result.error());
    std::unique_lock lock{wait_mutex_};
    wake_.wait_for(lock, stop, configuration_.interval, [] { return false; });
  }
}

} // namespace sakuin::service

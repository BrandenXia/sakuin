export module sakuin.service.duplicates;

import std;

import sakuin.config.model;
import sakuin.core.ids;
import sakuin.core.result;
export import sakuin.index.local;
import sakuin.service.storage;

export namespace sakuin::service {

class DuplicateIndexObserver {
public:
  virtual ~DuplicateIndexObserver() = default;
  virtual void on_duplicate_index_synchronized(
      index::DuplicateSynchronizationResult result) = 0;
  virtual void on_duplicate_index_error(core::Error error) = 0;
};

// Periodically advances the disposable duplicate projection from the canonical
// torrent change stream. The jthread remains an implementation detail behind
// this Sakuin-owned lifecycle boundary and can later move to stdexec.
class DuplicateIndexCoordinator final : public index::DuplicateIndexView {
public:
  static core::Result<std::unique_ptr<DuplicateIndexCoordinator>>
  create(LocalCanonicalStorage &storage,
         config::DuplicateIndexConfig configuration,
         DuplicateIndexObserver *observer = nullptr);
  ~DuplicateIndexCoordinator();

  DuplicateIndexCoordinator(const DuplicateIndexCoordinator &) = delete;
  DuplicateIndexCoordinator &
  operator=(const DuplicateIndexCoordinator &) = delete;

  core::Result<void> start();
  void stop() noexcept;
  bool running() const noexcept { return running_.load(); }
  core::Result<index::DuplicateSynchronizationResult> run_once();

  core::Result<std::vector<index::DuplicateGroup>>
  groups(index::DuplicateFingerprintAlgorithm algorithm,
         std::size_t minimum_members = 2) const;
  std::vector<index::DuplicateGroup>
  matches(const core::InfoHash &torrent) const;
  index::DuplicateIndexStats stats() const;
  index::DuplicateIndexState snapshot() const override;

private:
  DuplicateIndexCoordinator(LocalCanonicalStorage &storage,
                            config::DuplicateIndexConfig configuration,
                            std::unique_ptr<index::LocalDuplicateIndex> index,
                            DuplicateIndexObserver *observer)
      : storage_(storage), configuration_(configuration),
        index_(std::move(index)), observer_(observer) {}

  void worker(std::stop_token stop);
  void notify(index::DuplicateSynchronizationResult result) noexcept;
  void notify_error(core::Error error) noexcept;

  LocalCanonicalStorage &storage_;
  config::DuplicateIndexConfig configuration_;
  std::unique_ptr<index::LocalDuplicateIndex> index_;
  DuplicateIndexObserver *observer_{};
  mutable std::mutex index_mutex_;
  std::atomic<bool> running_{};
  std::mutex wait_mutex_;
  std::condition_variable_any wake_;
  std::jthread worker_;
};

} // namespace sakuin::service

namespace sakuin::service {
namespace {

bool recoverable_derived_error(core::ErrorCode code) {
  return code == core::ErrorCode::CorruptSegment ||
         code == core::ErrorCode::ChecksumMismatch ||
         code == core::ErrorCode::UnsupportedFormat;
}

} // namespace

core::Result<std::unique_ptr<DuplicateIndexCoordinator>>
DuplicateIndexCoordinator::create(LocalCanonicalStorage &storage,
                                  config::DuplicateIndexConfig configuration,
                                  DuplicateIndexObserver *observer) {
  const auto path = storage.root() / "derived" / "duplicates" / "index.v1";
  auto opened = index::LocalDuplicateIndex::open(path);
  if (!opened) {
    if (!recoverable_derived_error(opened.error().code))
      return std::unexpected(opened.error());
    auto source = storage.torrents().keyed_snapshot();
    if (!source)
      return std::unexpected(source.error());
    opened = index::LocalDuplicateIndex::rebuild(path, **source);
    if (!opened)
      return std::unexpected(opened.error());
  }
  return std::unique_ptr<DuplicateIndexCoordinator>{
      new DuplicateIndexCoordinator{storage, configuration, std::move(*opened),
                                    observer}};
}

DuplicateIndexCoordinator::~DuplicateIndexCoordinator() { stop(); }

core::Result<void> DuplicateIndexCoordinator::start() {
  if (running_.exchange(true))
    return std::unexpected(
        core::Error{core::ErrorCode::Conflict,
                    "Duplicate-index coordinator is already running"});
  try {
    worker_ =
        std::jthread{[this](std::stop_token stop) { worker(std::move(stop)); }};
  } catch (const std::exception &error) {
    running_.store(false);
    return std::unexpected(
        core::Error{core::ErrorCode::Internal,
                    "Unable to start duplicate-index coordinator: " +
                        std::string{error.what()}});
  }
  return {};
}

void DuplicateIndexCoordinator::stop() noexcept {
  if (!running_.exchange(false))
    return;
  worker_.request_stop();
  wake_.notify_all();
  if (worker_.joinable())
    worker_.join();
}

core::Result<index::DuplicateSynchronizationResult>
DuplicateIndexCoordinator::run_once() {
  std::lock_guard lock{index_mutex_};
  return index_->synchronize(storage_.torrents());
}

core::Result<std::vector<index::DuplicateGroup>>
DuplicateIndexCoordinator::groups(
    index::DuplicateFingerprintAlgorithm algorithm,
    std::size_t minimum_members) const {
  std::lock_guard lock{index_mutex_};
  return index_->groups(algorithm, minimum_members);
}

std::vector<index::DuplicateGroup>
DuplicateIndexCoordinator::matches(const core::InfoHash &torrent) const {
  std::lock_guard lock{index_mutex_};
  return index_->matches(torrent);
}

index::DuplicateIndexStats DuplicateIndexCoordinator::stats() const {
  std::lock_guard lock{index_mutex_};
  return index_->stats();
}

index::DuplicateIndexState DuplicateIndexCoordinator::snapshot() const {
  std::lock_guard lock{index_mutex_};
  return index_->snapshot();
}

void DuplicateIndexCoordinator::worker(std::stop_token stop) {
  while (!stop.stop_requested()) {
    auto synchronized = run_once();
    if (synchronized)
      notify(*synchronized);
    else
      notify_error(synchronized.error());
    std::unique_lock lock{wait_mutex_};
    wake_.wait_for(lock, stop, configuration_.interval, [] { return false; });
  }
}

void DuplicateIndexCoordinator::notify(
    index::DuplicateSynchronizationResult result) noexcept {
  if (!observer_)
    return;
  try {
    observer_->on_duplicate_index_synchronized(result);
  } catch (...) {
    notify_error({core::ErrorCode::Internal,
                  "Duplicate-index observer threw an exception"});
  }
}

void DuplicateIndexCoordinator::notify_error(core::Error error) noexcept {
  if (!observer_)
    return;
  try {
    observer_->on_duplicate_index_error(std::move(error));
  } catch (...) {
    // Observers cannot unwind through coordinator-owned threads.
  }
}

} // namespace sakuin::service

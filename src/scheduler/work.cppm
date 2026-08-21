export module sakuin.scheduler.work;

import std;

import sakuin.core.bytes;
import sakuin.core.hash;
import sakuin.core.result;
import sakuin.core.time;

export namespace sakuin::scheduler {

struct WorkId {
  std::array<std::uint8_t, 16> bytes{};
  friend auto operator<=>(const WorkId &, const WorkId &) = default;
};

using LeaseId = std::uint64_t;

enum class WorkClass : std::uint8_t {
  DhtCrawl,
  MetadataFetch,
  Materialization,
  Compaction
};

struct WorkItem {
  WorkId id;
  WorkClass work_class{};
  core::ByteBuffer payload;
  std::int32_t priority{};
  core::Timestamp not_before{core::Timestamp::min()};
  std::uint32_t maximum_attempts{3};

  friend bool operator==(const WorkItem &, const WorkItem &) = default;
};

struct WorkerDescriptor {
  std::string id;
  std::vector<WorkClass> capabilities;
};

struct WorkLease {
  LeaseId id{};
  WorkItem item;
  std::uint32_t attempt{};
  core::Timestamp expires_at;
};

struct WorkCoordinatorOptions {
  std::size_t maximum_work_items{65'536};
  std::size_t maximum_payload_bytes{1U * 1024U * 1024U};
  core::Duration worker_timeout{std::chrono::seconds{30}};
  core::Duration lease_duration{std::chrono::minutes{2}};
};

struct WorkCoordinatorSnapshot {
  std::size_t pending{};
  std::size_t leased{};
  std::size_t succeeded{};
  std::size_t failed{};
  std::size_t workers{};
};

WorkId content_work_id(core::ByteView material);

class WorkCoordinator {
public:
  virtual ~WorkCoordinator() = default;

  virtual core::Result<bool> submit(WorkItem item) = 0;
  virtual core::Result<bool> register_worker(WorkerDescriptor worker,
                                             core::Timestamp now) = 0;
  virtual core::Result<void> heartbeat(std::string_view worker,
                                       core::Timestamp now) = 0;
  virtual core::Result<std::vector<WorkLease>>
  lease(std::string_view worker, std::size_t maximum, core::Timestamp now) = 0;
  virtual core::Result<void> renew(std::string_view worker, LeaseId lease,
                                   core::Timestamp now) = 0;
  virtual core::Result<void> complete(std::string_view worker, LeaseId lease,
                                      core::Timestamp now) = 0;
  virtual core::Result<void> fail(std::string_view worker, LeaseId lease,
                                  bool retryable, core::Timestamp now,
                                  core::Timestamp retry_at) = 0;
  virtual core::Result<void> unregister_worker(std::string_view worker,
                                               core::Timestamp now) = 0;
  virtual WorkCoordinatorSnapshot snapshot(core::Timestamp now) = 0;
};

// Reference operational-state implementation. It is concurrency safe and
// models the lease semantics expected from a future distributed coordinator;
// no canonical records live here.
class LocalWorkCoordinator final : public WorkCoordinator {
public:
  static core::Result<std::unique_ptr<LocalWorkCoordinator>>
  create(WorkCoordinatorOptions options = {});

  core::Result<bool> submit(WorkItem item) override;
  core::Result<bool> register_worker(WorkerDescriptor worker,
                                     core::Timestamp now) override;
  core::Result<void> heartbeat(std::string_view worker,
                               core::Timestamp now) override;
  core::Result<std::vector<WorkLease>> lease(std::string_view worker,
                                             std::size_t maximum,
                                             core::Timestamp now) override;
  core::Result<void> renew(std::string_view worker, LeaseId lease,
                           core::Timestamp now) override;
  core::Result<void> complete(std::string_view worker, LeaseId lease,
                              core::Timestamp now) override;
  core::Result<void> fail(std::string_view worker, LeaseId lease,
                          bool retryable, core::Timestamp now,
                          core::Timestamp retry_at) override;
  core::Result<void> unregister_worker(std::string_view worker,
                                       core::Timestamp now) override;
  WorkCoordinatorSnapshot snapshot(core::Timestamp now) override;

private:
  enum class State : std::uint8_t { Pending, Leased, Succeeded, Failed };
  struct Entry {
    WorkItem item;
    State state{State::Pending};
    std::uint64_t sequence{};
    std::uint32_t attempts{};
    LeaseId lease{};
    std::string worker;
    core::Timestamp lease_expires{};
  };
  struct Worker {
    std::vector<WorkClass> capabilities;
    core::Timestamp expires_at;
  };

  explicit LocalWorkCoordinator(WorkCoordinatorOptions options)
      : options_(options) {}

  void reap(core::Timestamp now);
  void release(Entry &entry, core::Timestamp retry_at);
  core::Result<Entry *> active_lease(std::string_view worker, LeaseId lease);

  WorkCoordinatorOptions options_;
  std::mutex mutex_;
  std::map<WorkId, Entry> work_;
  std::map<std::string, Worker, std::less<>> workers_;
  std::uint64_t next_sequence_{};
  LeaseId next_lease_{};
};

} // namespace sakuin::scheduler

namespace sakuin::scheduler {
namespace {

core::Timestamp after(core::Timestamp now, core::Duration duration) {
  return now + std::chrono::duration_cast<core::Timestamp::duration>(duration);
}

bool valid_worker_id(std::string_view id) {
  return !id.empty() && id.size() <= 128 &&
         std::ranges::all_of(id, [](unsigned char value) {
           return std::isalnum(value) || value == '-' || value == '_' ||
                  value == '.' || value == ':';
         });
}

} // namespace

WorkId content_work_id(core::ByteView material) {
  const auto digest = core::sha256(material);
  WorkId result;
  std::ranges::copy_n(digest.bytes.begin(), result.bytes.size(),
                      result.bytes.begin());
  return result;
}

core::Result<std::unique_ptr<LocalWorkCoordinator>>
LocalWorkCoordinator::create(WorkCoordinatorOptions options) {
  if (options.maximum_work_items == 0 || options.maximum_payload_bytes == 0 ||
      options.worker_timeout <= core::Duration::zero() ||
      options.lease_duration <= core::Duration::zero())
    return std::unexpected(
        core::Error{core::ErrorCode::InvalidArgument,
                    "Work coordinator limits and durations must be positive"});
  return std::unique_ptr<LocalWorkCoordinator>{
      new LocalWorkCoordinator{options}};
}

core::Result<bool> LocalWorkCoordinator::submit(WorkItem item) {
  if (item.maximum_attempts == 0 ||
      item.payload.size() > options_.maximum_payload_bytes)
    return std::unexpected(core::Error{
        core::ErrorCode::InvalidArgument,
        "Work item has invalid attempts or exceeds the payload limit"});
  std::lock_guard lock{mutex_};
  if (const auto found = work_.find(item.id); found != work_.end()) {
    if (found->second.item == item)
      return false;
    return std::unexpected(
        core::Error{core::ErrorCode::Conflict,
                    "Work id is already associated with different work"});
  }
  if (work_.size() >= options_.maximum_work_items)
    return std::unexpected(
        core::Error{core::ErrorCode::QuotaExceeded,
                    "Work coordinator item limit would be exceeded"});
  work_.emplace(item.id,
                Entry{.item = std::move(item), .sequence = ++next_sequence_});
  return true;
}

core::Result<bool>
LocalWorkCoordinator::register_worker(WorkerDescriptor worker,
                                      core::Timestamp now) {
  if (!valid_worker_id(worker.id) || worker.capabilities.empty())
    return std::unexpected(
        core::Error{core::ErrorCode::InvalidArgument,
                    "Worker id or capabilities are invalid"});
  std::ranges::sort(worker.capabilities);
  worker.capabilities.erase(std::ranges::unique(worker.capabilities).begin(),
                            worker.capabilities.end());
  std::lock_guard lock{mutex_};
  reap(now);
  const auto expires = after(now, options_.worker_timeout);
  const auto found = workers_.find(worker.id);
  if (found != workers_.end()) {
    if (found->second.capabilities != worker.capabilities)
      return std::unexpected(
          core::Error{core::ErrorCode::Conflict,
                      "Active worker registration has different capabilities"});
    found->second.expires_at = expires;
    return false;
  }
  workers_.emplace(std::move(worker.id),
                   Worker{.capabilities = std::move(worker.capabilities),
                          .expires_at = expires});
  return true;
}

core::Result<void> LocalWorkCoordinator::heartbeat(std::string_view worker,
                                                   core::Timestamp now) {
  std::lock_guard lock{mutex_};
  reap(now);
  const auto found = workers_.find(worker);
  if (found == workers_.end())
    return std::unexpected(
        core::Error{core::ErrorCode::NotFound, "Worker is not registered"});
  found->second.expires_at = after(now, options_.worker_timeout);
  return {};
}

void LocalWorkCoordinator::release(Entry &entry, core::Timestamp retry_at) {
  entry.lease = 0;
  entry.worker.clear();
  entry.lease_expires = {};
  if (entry.attempts >= entry.item.maximum_attempts) {
    entry.state = State::Failed;
  } else {
    entry.state = State::Pending;
    entry.item.not_before = retry_at;
  }
}

void LocalWorkCoordinator::reap(core::Timestamp now) {
  for (auto worker = workers_.begin(); worker != workers_.end();) {
    if (worker->second.expires_at > now) {
      ++worker;
      continue;
    }
    const auto id = worker->first;
    for (auto &[_, entry] : work_)
      if (entry.state == State::Leased && entry.worker == id)
        release(entry, now);
    worker = workers_.erase(worker);
  }
  for (auto &[_, entry] : work_)
    if (entry.state == State::Leased && entry.lease_expires <= now)
      release(entry, now);
}

core::Result<std::vector<WorkLease>>
LocalWorkCoordinator::lease(std::string_view worker, std::size_t maximum,
                            core::Timestamp now) {
  if (maximum == 0)
    return std::unexpected(core::Error{core::ErrorCode::InvalidArgument,
                                       "Lease maximum must be positive"});
  std::lock_guard lock{mutex_};
  reap(now);
  const auto registered = workers_.find(worker);
  if (registered == workers_.end())
    return std::unexpected(
        core::Error{core::ErrorCode::NotFound, "Worker is not registered"});

  std::vector<Entry *> ready;
  for (auto &[_, entry] : work_)
    if (entry.state == State::Pending && entry.item.not_before <= now &&
        std::ranges::contains(registered->second.capabilities,
                              entry.item.work_class))
      ready.push_back(&entry);
  std::ranges::sort(ready, [](const Entry *left, const Entry *right) {
    if (left->item.priority != right->item.priority)
      return left->item.priority > right->item.priority;
    return left->sequence < right->sequence;
  });

  std::vector<WorkLease> result;
  result.reserve(std::min(maximum, ready.size()));
  for (auto *entry : ready | std::views::take(maximum)) {
    do {
      ++next_lease_;
    } while (next_lease_ == 0);
    entry->state = State::Leased;
    entry->lease = next_lease_;
    entry->worker = worker;
    entry->lease_expires = after(now, options_.lease_duration);
    ++entry->attempts;
    result.push_back({.id = entry->lease,
                      .item = entry->item,
                      .attempt = entry->attempts,
                      .expires_at = entry->lease_expires});
  }
  return result;
}

core::Result<LocalWorkCoordinator::Entry *>
LocalWorkCoordinator::active_lease(std::string_view worker, LeaseId lease) {
  const auto found = std::ranges::find_if(work_, [&](const auto &pair) {
    return pair.second.state == State::Leased && pair.second.lease == lease;
  });
  if (found == work_.end())
    return std::unexpected(
        core::Error{core::ErrorCode::NotFound, "Work lease is not active"});
  if (found->second.worker != worker)
    return std::unexpected(core::Error{core::ErrorCode::PermissionDenied,
                                       "Work lease belongs to another worker"});
  return &found->second;
}

core::Result<void> LocalWorkCoordinator::renew(std::string_view worker,
                                               LeaseId lease,
                                               core::Timestamp now) {
  std::lock_guard lock{mutex_};
  reap(now);
  if (!workers_.contains(worker))
    return std::unexpected(
        core::Error{core::ErrorCode::NotFound, "Worker is not registered"});
  auto entry = active_lease(worker, lease);
  if (!entry)
    return std::unexpected(entry.error());
  (*entry)->lease_expires = after(now, options_.lease_duration);
  return {};
}

core::Result<void> LocalWorkCoordinator::complete(std::string_view worker,
                                                  LeaseId lease,
                                                  core::Timestamp now) {
  std::lock_guard lock{mutex_};
  reap(now);
  auto entry = active_lease(worker, lease);
  if (!entry)
    return std::unexpected(entry.error());
  (*entry)->state = State::Succeeded;
  (*entry)->lease = 0;
  (*entry)->lease_expires = {};
  (*entry)->worker.clear();
  return {};
}

core::Result<void> LocalWorkCoordinator::fail(std::string_view worker,
                                              LeaseId lease, bool retryable,
                                              core::Timestamp now,
                                              core::Timestamp retry_at) {
  std::lock_guard lock{mutex_};
  reap(now);
  auto entry = active_lease(worker, lease);
  if (!entry)
    return std::unexpected(entry.error());
  if (retryable)
    release(**entry, retry_at);
  else {
    (*entry)->state = State::Failed;
    (*entry)->lease = 0;
    (*entry)->lease_expires = {};
    (*entry)->worker.clear();
  }
  return {};
}

core::Result<void>
LocalWorkCoordinator::unregister_worker(std::string_view worker,
                                        core::Timestamp now) {
  std::lock_guard lock{mutex_};
  const auto found = workers_.find(worker);
  if (found == workers_.end())
    return std::unexpected(
        core::Error{core::ErrorCode::NotFound, "Worker is not registered"});
  workers_.erase(found);
  for (auto &[_, entry] : work_)
    if (entry.state == State::Leased && entry.worker == worker)
      release(entry, now);
  return {};
}

WorkCoordinatorSnapshot LocalWorkCoordinator::snapshot(core::Timestamp now) {
  std::lock_guard lock{mutex_};
  reap(now);
  WorkCoordinatorSnapshot result{.workers = workers_.size()};
  for (const auto &[_, entry] : work_) {
    switch (entry.state) {
    case State::Pending:
      ++result.pending;
      break;
    case State::Leased:
      ++result.leased;
      break;
    case State::Succeeded:
      ++result.succeeded;
      break;
    case State::Failed:
      ++result.failed;
      break;
    }
  }
  return result;
}

} // namespace sakuin::scheduler

import std;

import sakuin.core;
import sakuin.scheduler;

namespace {

sakuin::core::ByteBuffer bytes(std::string_view value) {
  const auto input = std::as_bytes(std::span{value});
  return {input.begin(), input.end()};
}

sakuin::scheduler::WorkItem item(std::string_view value) {
  auto payload = bytes(value);
  return {.id = sakuin::scheduler::content_work_id(payload),
          .work_class = sakuin::scheduler::WorkClass::MetadataFetch,
          .payload = std::move(payload),
          .maximum_attempts = 3};
}

std::string text(const sakuin::scheduler::WorkLease &lease) {
  return {reinterpret_cast<const char *>(lease.item.payload.data()),
          lease.item.payload.size()};
}

class Executor final : public sakuin::scheduler::WorkExecutor {
public:
  sakuin::scheduler::WorkExecutionOutcome
  execute(const sakuin::scheduler::WorkLease &lease,
          std::stop_token stop) override {
    const auto value = text(lease);
    const auto concurrent = active_.fetch_add(1) + 1;
    maximum_active_.store(std::max(maximum_active_.load(), concurrent));
    struct Release {
      std::atomic<unsigned> &active;
      ~Release() { active.fetch_sub(1); }
    } release{active_};

    {
      std::lock_guard lock{mutex_};
      ++attempts_[value];
    }
    if (value == "retry" && lease.attempt == 1)
      return sakuin::scheduler::WorkExecutionOutcome::retry(
          {sakuin::core::ErrorCode::StorageUnavailable,
           "simulated transient failure"},
          std::chrono::milliseconds{5});
    if (value == "ok")
      std::this_thread::sleep_for(std::chrono::milliseconds{30});
    if (value == "slow")
      std::this_thread::sleep_for(std::chrono::milliseconds{120});
    if (value == "cancel") {
      std::mutex mutex;
      std::condition_variable_any changed;
      std::unique_lock lock{mutex};
      changed.wait(lock, stop, [] { return false; });
      return sakuin::scheduler::WorkExecutionOutcome::retry(
          {sakuin::core::ErrorCode::StorageUnavailable, "execution canceled"});
    }
    return sakuin::scheduler::WorkExecutionOutcome::succeeded();
  }

  unsigned attempts(std::string_view value) const {
    std::lock_guard lock{mutex_};
    const auto found = attempts_.find(value);
    return found == attempts_.end() ? 0 : found->second;
  }

  unsigned maximum_active() const { return maximum_active_.load(); }

private:
  mutable std::mutex mutex_;
  std::map<std::string, unsigned, std::less<>> attempts_;
  std::atomic<unsigned> active_{};
  std::atomic<unsigned> maximum_active_{};
};

class Observer final : public sakuin::scheduler::WorkExecutionObserver {
public:
  void on_work_started(const sakuin::scheduler::WorkLease &) override {
    started_.fetch_add(1);
  }

  void
  on_work_finished(const sakuin::scheduler::WorkLease &,
                   const sakuin::scheduler::WorkExecutionOutcome &) override {
    finished_.fetch_add(1);
  }

  void on_work_runtime_error(sakuin::core::Error error) override {
    std::lock_guard lock{mutex_};
    errors_.push_back(std::move(error));
  }

  unsigned started() const { return started_.load(); }
  unsigned finished() const { return finished_.load(); }
  bool empty() const {
    std::lock_guard lock{mutex_};
    return errors_.empty();
  }

private:
  mutable std::mutex mutex_;
  std::vector<sakuin::core::Error> errors_;
  std::atomic<unsigned> started_{};
  std::atomic<unsigned> finished_{};
};

bool wait_until(std::function<bool()> predicate) {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds{5};
  while (std::chrono::steady_clock::now() < deadline) {
    if (predicate())
      return true;
    std::this_thread::sleep_for(std::chrono::milliseconds{5});
  }
  return predicate();
}

} // namespace

int main() {
  using namespace sakuin;
  auto coordinator = scheduler::LocalWorkCoordinator::create(
      {.maximum_work_items = 16,
       .maximum_payload_bytes = 128,
       .worker_timeout = std::chrono::milliseconds{80},
       .lease_duration = std::chrono::milliseconds{40}});
  if (!coordinator)
    return 1;
  for (const auto value : {"ok", "retry", "slow"})
    if (!(*coordinator)->submit(item(value)).value_or(false))
      return 2;

  Executor executor;
  Observer observer;
  auto worker = scheduler::WorkExecutionRuntime::create(
      **coordinator, executor,
      {.worker = {.id = "executor-1",
                  .capabilities = {scheduler::WorkClass::MetadataFetch}},
       .maximum_concurrency = 2,
       .poll_interval = std::chrono::milliseconds{5},
       .heartbeat_interval = std::chrono::milliseconds{15},
       .lease_renewal_interval = std::chrono::milliseconds{10},
       .default_retry_delay = std::chrono::milliseconds{5}},
      &observer);
  if (!worker || !(*worker)->start())
    return 3;
  auto duplicate_start = (*worker)->start();
  if (duplicate_start ||
      duplicate_start.error().code != core::ErrorCode::Conflict)
    return 4;

  if (!wait_until([&] {
        auto snapshot =
            (*coordinator)->snapshot(std::chrono::system_clock::now());
        return snapshot && snapshot->succeeded == 3;
      }))
    return 5;
  if (executor.attempts("ok") != 1 || executor.attempts("retry") != 2 ||
      executor.attempts("slow") != 1 || executor.maximum_active() != 2 ||
      observer.started() != 4 || observer.finished() != 4 ||
      !observer.empty()) {
    std::cerr << "attempts=" << executor.attempts("ok") << ','
              << executor.attempts("retry") << ',' << executor.attempts("slow")
              << " maximum_active=" << executor.maximum_active()
              << " events=" << observer.started() << ',' << observer.finished()
              << " errors=" << !observer.empty() << '\n';
    return 6;
  }

  if (!(*coordinator)->submit(item("cancel")).value_or(false) ||
      !wait_until([&] { return (*worker)->active_executions() == 1; }))
    return 7;
  (*worker)->stop();
  if ((*worker)->running() || observer.finished() != 5 || !observer.empty())
    return 8;
  auto stopped = (*coordinator)->snapshot(std::chrono::system_clock::now());
  if (!stopped || stopped->workers != 0 || stopped->pending != 1 ||
      stopped->leased != 0 || stopped->succeeded != 3)
    return 9;

  auto invalid = scheduler::WorkExecutionRuntime::create(
      **coordinator, executor,
      {.worker = {.id = "invalid",
                  .capabilities = {scheduler::WorkClass::MetadataFetch}},
       .maximum_concurrency = 0});
  if (invalid || invalid.error().code != core::ErrorCode::InvalidArgument)
    return 10;
  return 0;
}

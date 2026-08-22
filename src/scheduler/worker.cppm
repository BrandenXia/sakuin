export module sakuin.scheduler.worker;

import std;

import sakuin.core.result;
import sakuin.core.time;
import sakuin.scheduler.work;

export namespace sakuin::scheduler {

struct WorkExecutionOutcome {
  std::optional<core::Error> error;
  bool retryable{};
  core::Duration retry_after{};

  static WorkExecutionOutcome succeeded() { return {}; }
  static WorkExecutionOutcome failed(core::Error error) {
    return {.error = std::move(error)};
  }
  static WorkExecutionOutcome retry(core::Error error,
                                    core::Duration retry_after = {}) {
    return {.error = std::move(error),
            .retryable = true,
            .retry_after = retry_after};
  }
};

class WorkExecutor {
public:
  virtual ~WorkExecutor() = default;

  // The executor and its dependencies must outlive the runtime. Cooperative
  // cancellation keeps this boundary compatible with a future stdexec-backed
  // worker without exposing a concrete scheduler or networking library.
  virtual WorkExecutionOutcome execute(const WorkLease &lease,
                                       std::stop_token stop) = 0;
};

class WorkExecutionObserver {
public:
  virtual ~WorkExecutionObserver() = default;
  virtual void on_work_started(const WorkLease &lease) = 0;
  virtual void on_work_finished(const WorkLease &lease,
                                const WorkExecutionOutcome &outcome) = 0;
  virtual void on_work_runtime_error(core::Error error) = 0;
};

struct WorkExecutionRuntimeOptions {
  WorkerDescriptor worker;
  std::size_t maximum_concurrency{1};
  core::Duration poll_interval{std::chrono::milliseconds{250}};
  core::Duration heartbeat_interval{std::chrono::seconds{10}};
  core::Duration lease_renewal_interval{std::chrono::seconds{30}};
  core::Duration default_retry_delay{std::chrono::seconds{5}};
};

class WorkExecutionRuntime final {
public:
  using Clock = std::function<core::Timestamp()>;

  static core::Result<std::unique_ptr<WorkExecutionRuntime>> create(
      WorkCoordinator &coordinator, WorkExecutor &executor,
      WorkExecutionRuntimeOptions options,
      WorkExecutionObserver *observer = nullptr,
      Clock clock = [] { return std::chrono::system_clock::now(); });

  ~WorkExecutionRuntime();

  WorkExecutionRuntime(const WorkExecutionRuntime &) = delete;
  WorkExecutionRuntime &operator=(const WorkExecutionRuntime &) = delete;

  core::Result<void> start();
  void stop() noexcept;
  bool running() const noexcept {
    return running_.load(std::memory_order_acquire);
  }
  std::size_t active_executions() const noexcept {
    return active_count_.load(std::memory_order_acquire);
  }

private:
  struct ActiveExecution {
    explicit ActiveExecution(WorkLease value) : lease(std::move(value)) {}

    WorkLease lease;
    std::jthread thread;
    std::mutex mutex;
    std::optional<WorkExecutionOutcome> outcome;
  };

  WorkExecutionRuntime(WorkCoordinator &coordinator, WorkExecutor &executor,
                       WorkExecutionRuntimeOptions options,
                       WorkExecutionObserver *observer, Clock clock)
      : coordinator_(&coordinator), executor_(&executor),
        options_(std::move(options)), observer_(observer),
        clock_(std::move(clock)) {}

  core::Result<core::Timestamp> now() const noexcept;
  void run(std::stop_token stop) noexcept;
  void start_execution(WorkLease lease);
  void harvest(core::Timestamp now);
  bool has_finished_execution() const;
  void cancel_and_join_executions() noexcept;
  void unregister(core::Timestamp now) noexcept;
  void report(core::Error error) noexcept;
  void notify_started(const WorkLease &lease) noexcept;
  void notify_finished(const WorkLease &lease,
                       const WorkExecutionOutcome &outcome) noexcept;

  WorkCoordinator *coordinator_;
  WorkExecutor *executor_;
  WorkExecutionRuntimeOptions options_;
  WorkExecutionObserver *observer_{};
  Clock clock_;
  std::jthread owner_;
  std::vector<std::shared_ptr<ActiveExecution>> active_;
  mutable std::mutex wait_mutex_;
  std::condition_variable_any wakeup_;
  std::atomic<bool> running_{};
  std::atomic<bool> registered_{};
  std::atomic<std::size_t> active_count_{};
};

} // namespace sakuin::scheduler

namespace sakuin::scheduler {
namespace {

core::Error executor_exception(std::string detail) {
  return {core::ErrorCode::Internal,
          "Work executor threw an exception" +
              (detail.empty() ? std::string{} : ": " + std::move(detail))};
}

} // namespace

core::Result<std::unique_ptr<WorkExecutionRuntime>>
WorkExecutionRuntime::create(WorkCoordinator &coordinator,
                             WorkExecutor &executor,
                             WorkExecutionRuntimeOptions options,
                             WorkExecutionObserver *observer, Clock clock) {
  if (options.maximum_concurrency == 0 || options.maximum_concurrency > 1'024 ||
      options.poll_interval <= core::Duration::zero() ||
      options.heartbeat_interval <= core::Duration::zero() ||
      options.lease_renewal_interval <= core::Duration::zero() ||
      options.default_retry_delay < core::Duration::zero() || !clock)
    return std::unexpected(
        core::Error{core::ErrorCode::InvalidArgument,
                    "Work runtime limits, intervals, and clock are invalid"});
  return std::unique_ptr<WorkExecutionRuntime>{new WorkExecutionRuntime{
      coordinator, executor, std::move(options), observer, std::move(clock)}};
}

WorkExecutionRuntime::~WorkExecutionRuntime() { stop(); }

core::Result<core::Timestamp> WorkExecutionRuntime::now() const noexcept {
  try {
    return clock_();
  } catch (const std::exception &exception) {
    return std::unexpected(core::Error{
        core::ErrorCode::Internal,
        std::string{"Work runtime clock failed: "} + exception.what()});
  } catch (...) {
    return std::unexpected(
        core::Error{core::ErrorCode::Internal, "Work runtime clock failed"});
  }
}

core::Result<void> WorkExecutionRuntime::start() {
  bool expected = false;
  if (!running_.compare_exchange_strong(expected, true,
                                        std::memory_order_acq_rel))
    return std::unexpected(core::Error{core::ErrorCode::Conflict,
                                       "Work runtime is already running"});
  auto current = now();
  if (!current) {
    running_.store(false, std::memory_order_release);
    return std::unexpected(current.error());
  }
  auto registered = coordinator_->register_worker(options_.worker, *current);
  if (!registered) {
    running_.store(false, std::memory_order_release);
    return std::unexpected(registered.error());
  }
  if (!*registered) {
    running_.store(false, std::memory_order_release);
    return std::unexpected(core::Error{
        core::ErrorCode::Conflict, "Work runtime id is already registered"});
  }
  registered_.store(true, std::memory_order_release);
  try {
    owner_ = std::jthread{[this](std::stop_token stop) { run(stop); }};
  } catch (const std::exception &exception) {
    unregister(*current);
    running_.store(false, std::memory_order_release);
    return std::unexpected(
        core::Error{core::ErrorCode::Internal,
                    std::string{"Could not start work runtime thread: "} +
                        exception.what()});
  } catch (...) {
    unregister(*current);
    running_.store(false, std::memory_order_release);
    return std::unexpected(core::Error{core::ErrorCode::Internal,
                                       "Could not start work runtime thread"});
  }
  return {};
}

void WorkExecutionRuntime::stop() noexcept {
  running_.store(false, std::memory_order_release);
  if (!owner_.joinable()) {
    if (auto current = now())
      unregister(*current);
    return;
  }
  owner_.request_stop();
  wakeup_.notify_all();
  if (owner_.get_id() != std::this_thread::get_id())
    owner_.join();
}

void WorkExecutionRuntime::report(core::Error error) noexcept {
  if (!observer_)
    return;
  try {
    observer_->on_work_runtime_error(std::move(error));
  } catch (...) {
    // Observability callbacks cannot unwind through runtime threads.
  }
}

void WorkExecutionRuntime::notify_started(const WorkLease &lease) noexcept {
  if (!observer_)
    return;
  try {
    observer_->on_work_started(lease);
  } catch (...) {
    report({core::ErrorCode::Internal, "Work-start observer callback threw"});
  }
}

void WorkExecutionRuntime::notify_finished(
    const WorkLease &lease, const WorkExecutionOutcome &outcome) noexcept {
  if (!observer_)
    return;
  try {
    observer_->on_work_finished(lease, outcome);
  } catch (...) {
    report({core::ErrorCode::Internal, "Work-finish observer callback threw"});
  }
}

void WorkExecutionRuntime::start_execution(WorkLease lease) {
  auto execution = std::make_shared<ActiveExecution>(std::move(lease));
  notify_started(execution->lease);
  execution->thread = std::jthread{[this, execution](std::stop_token stop) {
    WorkExecutionOutcome outcome;
    try {
      outcome = executor_->execute(execution->lease, stop);
      if (!outcome.error &&
          (outcome.retryable || outcome.retry_after != core::Duration::zero()))
        outcome = WorkExecutionOutcome::retry(
            {core::ErrorCode::Internal,
             "Successful work execution returned failure policy"},
            options_.default_retry_delay);
      if (outcome.retry_after < core::Duration::zero())
        outcome = WorkExecutionOutcome::retry(
            {core::ErrorCode::Internal,
             "Work execution returned a negative retry delay"},
            options_.default_retry_delay);
    } catch (const std::exception &exception) {
      outcome = WorkExecutionOutcome::retry(
          executor_exception(exception.what()), options_.default_retry_delay);
    } catch (...) {
      outcome = WorkExecutionOutcome::retry(executor_exception({}),
                                            options_.default_retry_delay);
    }
    {
      std::lock_guard lock{execution->mutex};
      execution->outcome = std::move(outcome);
    }
    wakeup_.notify_all();
  }};
  active_.push_back(std::move(execution));
  active_count_.store(active_.size(), std::memory_order_release);
}

bool WorkExecutionRuntime::has_finished_execution() const {
  return std::ranges::any_of(active_, [](const auto &execution) {
    std::lock_guard lock{execution->mutex};
    return execution->outcome.has_value();
  });
}

void WorkExecutionRuntime::harvest(core::Timestamp current) {
  for (auto entry = active_.begin(); entry != active_.end();) {
    auto execution = *entry;
    std::optional<WorkExecutionOutcome> outcome;
    {
      std::lock_guard lock{execution->mutex};
      if (execution->outcome)
        outcome = std::move(execution->outcome);
    }
    if (!outcome) {
      ++entry;
      continue;
    }
    if (execution->thread.joinable())
      execution->thread.join();

    core::Result<void> settled;
    if (!outcome->error) {
      settled = coordinator_->complete(options_.worker.id, execution->lease.id,
                                       current);
    } else {
      const auto delay = outcome->retryable
                             ? (outcome->retry_after == core::Duration::zero()
                                    ? options_.default_retry_delay
                                    : outcome->retry_after)
                             : core::Duration::zero();
      settled = coordinator_->fail(
          options_.worker.id, execution->lease.id, outcome->retryable, current,
          current +
              std::chrono::duration_cast<core::Timestamp::duration>(delay));
    }
    if (!settled)
      report(settled.error());
    notify_finished(execution->lease, *outcome);
    entry = active_.erase(entry);
  }
  active_count_.store(active_.size(), std::memory_order_release);
}

void WorkExecutionRuntime::cancel_and_join_executions() noexcept {
  for (const auto &execution : active_)
    if (execution->thread.joinable())
      execution->thread.request_stop();
  for (const auto &execution : active_) {
    if (execution->thread.joinable() &&
        execution->thread.get_id() != std::this_thread::get_id())
      execution->thread.join();
    std::lock_guard lock{execution->mutex};
    if (execution->outcome)
      notify_finished(execution->lease, *execution->outcome);
  }
  active_.clear();
  active_count_.store(0, std::memory_order_release);
}

void WorkExecutionRuntime::unregister(core::Timestamp current) noexcept {
  if (!registered_.exchange(false, std::memory_order_acq_rel))
    return;
  auto result = coordinator_->unregister_worker(options_.worker.id, current);
  if (!result && result.error().code != core::ErrorCode::NotFound)
    report(result.error());
}

void WorkExecutionRuntime::run(std::stop_token stop) noexcept {
  auto next_heartbeat = std::chrono::steady_clock::now();
  auto next_renewal = next_heartbeat + options_.lease_renewal_interval;
  auto next_poll = next_heartbeat;

  while (!stop.stop_requested()) {
    auto current = now();
    if (!current) {
      report(current.error());
      break;
    }
    harvest(*current);
    const auto steady_now = std::chrono::steady_clock::now();

    if (steady_now >= next_heartbeat) {
      auto heartbeat = coordinator_->heartbeat(options_.worker.id, *current);
      if (!heartbeat) {
        report(heartbeat.error());
        break;
      }
      next_heartbeat = steady_now + options_.heartbeat_interval;
    }

    if (!active_.empty() && steady_now >= next_renewal) {
      bool renewal_failed{};
      for (const auto &execution : active_) {
        auto renewed = coordinator_->renew(options_.worker.id,
                                           execution->lease.id, *current);
        if (!renewed) {
          report(renewed.error());
          renewal_failed = true;
          break;
        }
      }
      if (renewal_failed)
        break;
      next_renewal = steady_now + options_.lease_renewal_interval;
    } else if (active_.empty()) {
      next_renewal = steady_now + options_.lease_renewal_interval;
    }

    if (active_.size() < options_.maximum_concurrency &&
        steady_now >= next_poll) {
      auto leases = coordinator_->lease(
          options_.worker.id, options_.maximum_concurrency - active_.size(),
          *current);
      if (!leases) {
        report(leases.error());
        break;
      }
      try {
        for (auto &lease : *leases)
          start_execution(std::move(lease));
      } catch (const std::exception &exception) {
        report({core::ErrorCode::Internal,
                std::string{"Could not start work execution: "} +
                    exception.what()});
        break;
      } catch (...) {
        report({core::ErrorCode::Internal, "Could not start work execution"});
        break;
      }
      next_poll = steady_now + options_.poll_interval;
    }

    const auto wake_at = std::min({next_heartbeat, next_renewal, next_poll});
    std::unique_lock lock{wait_mutex_};
    wakeup_.wait_until(lock, stop, wake_at,
                       [this] { return has_finished_execution(); });
  }

  cancel_and_join_executions();
  if (auto current = now())
    unregister(*current);
  running_.store(false, std::memory_order_release);
}

} // namespace sakuin::scheduler

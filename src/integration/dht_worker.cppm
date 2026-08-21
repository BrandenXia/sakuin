export module sakuin.integration.dht_worker;

import std;

import sakuin.core.result;
import sakuin.core.time;
import sakuin.dht.runtime;
import sakuin.integration.dht_runtime;

export namespace sakuin::integration {

// Shared edge-triggered signal for callback producers and the owner worker.
// Construct it before the action pump and metadata controller, then pass
// notify() lambdas through their wake_owner options.
class DhtRuntimeWakeup final {
public:
  void notify() noexcept;

private:
  friend class DhtRuntimeWorker;

  std::uint64_t generation() const noexcept;
  void wait(std::uint64_t observed, std::optional<core::Timestamp> deadline,
            std::stop_token stop);

  mutable std::mutex mutex_;
  std::condition_variable_any condition_;
  std::uint64_t generation_{};
};

struct DhtRuntimeCycle {
  DhtRuntimePoll poll;
  DhtRuntimeDispatch dispatch;
};

class DhtRuntimeWorkerObserver {
public:
  virtual ~DhtRuntimeWorkerObserver() = default;

  // Runs on the owner thread after planner datagrams have been dispatched.
  // A BEP 42 reconfiguration is an explicit request to replace this family's
  // complete node/runtime. The observer must commit the identity policy only
  // after that replacement succeeds; live node IDs are never mutated here.
  virtual void on_cycle(DhtRuntimeCycle cycle) = 0;
  virtual void on_worker_error(core::Error error) = 0;
};

// Current owner-thread implementation. It intentionally depends only on the
// Sakuin driver/pump abstractions; Standalone Asio remains behind the supplied
// transport and a future stdexec owner can replace this class without changing
// DHT protocol or persistence contracts.
class DhtRuntimeWorker final {
public:
  using Clock = std::function<core::Timestamp()>;

  DhtRuntimeWorker(
      dht::DhtRuntimeDriver &driver, DhtRuntimeActionPump &pump,
      DhtRuntimeWakeup &wakeup, DhtRuntimeWorkerObserver &observer,
      Clock clock = [] { return std::chrono::system_clock::now(); })
      : driver_(&driver), pump_(&pump), wakeup_(&wakeup), observer_(&observer),
        clock_(std::move(clock)) {}
  ~DhtRuntimeWorker();

  DhtRuntimeWorker(const DhtRuntimeWorker &) = delete;
  DhtRuntimeWorker &operator=(const DhtRuntimeWorker &) = delete;

  core::Result<void> start();
  void stop() noexcept;
  bool running() const noexcept {
    return running_.load(std::memory_order_acquire);
  }

private:
  core::Result<core::Timestamp> now() const noexcept;
  void run(std::stop_token stop) noexcept;
  void report(core::Error error) noexcept;

  dht::DhtRuntimeDriver *driver_;
  DhtRuntimeActionPump *pump_;
  DhtRuntimeWakeup *wakeup_;
  DhtRuntimeWorkerObserver *observer_;
  Clock clock_;
  std::jthread thread_;
  std::atomic<bool> running_{};
};

} // namespace sakuin::integration

namespace sakuin::integration {

void DhtRuntimeWakeup::notify() noexcept {
  {
    std::lock_guard lock{mutex_};
    ++generation_;
  }
  condition_.notify_all();
}

std::uint64_t DhtRuntimeWakeup::generation() const noexcept {
  std::lock_guard lock{mutex_};
  return generation_;
}

void DhtRuntimeWakeup::wait(std::uint64_t observed,
                            std::optional<core::Timestamp> deadline,
                            std::stop_token stop) {
  std::unique_lock lock{mutex_};
  const auto changed = [&] { return generation_ != observed; };
  if (deadline) {
    condition_.wait_until(lock, stop, *deadline, changed);
  } else {
    condition_.wait(lock, stop, changed);
  }
}

DhtRuntimeWorker::~DhtRuntimeWorker() { stop(); }

core::Result<void> DhtRuntimeWorker::start() {
  bool expected = false;
  if (!running_.compare_exchange_strong(expected, true,
                                        std::memory_order_acq_rel))
    return std::unexpected(core::Error{core::ErrorCode::Conflict,
                                       "DHT runtime worker already started"});
  auto started = driver_->start();
  if (!started) {
    running_.store(false, std::memory_order_release);
    return std::unexpected(started.error());
  }
  try {
    thread_ = std::jthread{[this](std::stop_token stop) { run(stop); }};
  } catch (const std::exception &exception) {
    driver_->stop();
    running_.store(false, std::memory_order_release);
    return std::unexpected(core::Error{
        core::ErrorCode::Internal,
        std::string{"Failed to start DHT owner thread: "} + exception.what()});
  } catch (...) {
    driver_->stop();
    running_.store(false, std::memory_order_release);
    return std::unexpected(core::Error{core::ErrorCode::Internal,
                                       "Failed to start DHT owner thread"});
  }
  return {};
}

void DhtRuntimeWorker::stop() noexcept {
  running_.store(false, std::memory_order_release);
  if (thread_.joinable()) {
    thread_.request_stop();
    wakeup_->notify();
    if (thread_.get_id() != std::this_thread::get_id())
      thread_.join();
  }
  driver_->stop();
}

core::Result<core::Timestamp> DhtRuntimeWorker::now() const noexcept {
  try {
    return clock_();
  } catch (const std::exception &exception) {
    return std::unexpected(core::Error{core::ErrorCode::Internal,
                                       std::string{"DHT owner clock failed: "} +
                                           exception.what()});
  } catch (...) {
    return std::unexpected(
        core::Error{core::ErrorCode::Internal, "DHT owner clock failed"});
  }
}

void DhtRuntimeWorker::report(core::Error error) noexcept {
  try {
    observer_->on_worker_error(std::move(error));
  } catch (...) {
    // Observer failures cannot unwind through the owner thread.
  }
}

void DhtRuntimeWorker::run(std::stop_token stop) noexcept {
  while (!stop.stop_requested()) {
    const auto observed = wakeup_->generation();
    auto current = now();
    if (!current) {
      report(current.error());
      break;
    }
    try {
      auto poll = pump_->poll(*current);
      auto dispatch = dispatch_dht_runtime(poll, *driver_, *pump_, *current);
      const auto deadline = poll.next_wakeup;
      try {
        observer_->on_cycle(DhtRuntimeCycle{.poll = std::move(poll),
                                            .dispatch = std::move(dispatch)});
      } catch (const std::exception &exception) {
        report(core::Error{core::ErrorCode::Internal,
                           std::string{"DHT owner observer threw: "} +
                               exception.what()});
      } catch (...) {
        report(core::Error{core::ErrorCode::Internal,
                           "DHT owner observer threw an unknown exception"});
      }
      if (stop.stop_requested())
        break;
      wakeup_->wait(observed, deadline, stop);
    } catch (const std::exception &exception) {
      report(core::Error{core::ErrorCode::Internal,
                         std::string{"DHT owner cycle failed: "} +
                             exception.what()});
      break;
    } catch (...) {
      report(core::Error{core::ErrorCode::Internal, "DHT owner cycle failed"});
      break;
    }
  }
  running_.store(false, std::memory_order_release);
  driver_->stop();
}

} // namespace sakuin::integration

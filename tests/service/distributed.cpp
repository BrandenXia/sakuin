import std;

import sakuin.config;
import sakuin.core;
import sakuin.runtime;
import sakuin.runtime.asio_stream;
import sakuin.scheduler;
import sakuin.service.distributed;

namespace {

class Observer final : public sakuin::service::DistributedWorkServiceObserver {
public:
  void
  on_distributed_work_error(std::optional<sakuin::runtime::StreamSessionId>,
                            sakuin::core::Error error) override {
    std::lock_guard lock{mutex_};
    errors_.push_back(std::move(error));
  }

  bool empty() const {
    std::lock_guard lock{mutex_};
    return errors_.empty();
  }

private:
  mutable std::mutex mutex_;
  std::vector<sakuin::core::Error> errors_;
};

class Executor final : public sakuin::scheduler::WorkExecutor {
public:
  sakuin::scheduler::WorkExecutionOutcome
  execute(const sakuin::scheduler::WorkLease &, std::stop_token) override {
    executions_.fetch_add(1);
    return sakuin::scheduler::WorkExecutionOutcome::succeeded();
  }

  unsigned executions() const { return executions_.load(); }

private:
  std::atomic<unsigned> executions_{};
};

sakuin::scheduler::WorkItem work(std::string_view value) {
  const auto input = std::as_bytes(std::span{value});
  sakuin::core::ByteBuffer payload{input.begin(), input.end()};
  return {.id = sakuin::scheduler::content_work_id(payload),
          .work_class = sakuin::scheduler::WorkClass::MetadataFetch,
          .payload = std::move(payload)};
}

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
      {.maximum_work_items = 64,
       .maximum_payload_bytes = 1'024,
       .worker_timeout = std::chrono::seconds{5},
       .lease_duration = std::chrono::seconds{1}});
  if (!coordinator)
    return 1;

  auto configuration = config::defaults().distributed;
  configuration.maximum_payload_bytes = 1'024;
  configuration.coordinator.enabled = true;
  configuration.coordinator.listen_port = 0;
  configuration.coordinator.idle_timeout = std::chrono::seconds{3};

  Observer observer;
  auto service = service::DistributedWorkService::create(
      configuration, **coordinator, &observer);
  if (!service) {
    std::cerr << "distributed service create: " << service.error().message
              << '\n';
    return 2;
  }
  if ((*service)->local_endpoint().port == 0) {
    std::cerr << "distributed service retained an ephemeral port\n";
    return 2;
  }
  if (!(*service)->start() || !(*service)->running())
    return 3;
  auto duplicate_start = (*service)->start();
  if (duplicate_start ||
      duplicate_start.error().code != core::ErrorCode::Conflict)
    return 4;

  runtime::AsioTcpStreamTransportFactory transports;
  auto remote = scheduler::StreamWorkCoordinator::connect(
      transports,
      {.transport = {.remote = (*service)->local_endpoint(),
                     .connect_timeout = std::chrono::seconds{2},
                     .idle_timeout = std::chrono::seconds{3},
                     .read_buffer_bytes = 41,
                     .maximum_queued_write_bytes = 2U * 1024U * 1024U},
       .request_timeout = std::chrono::seconds{2},
       .protocol_limits = {
           .maximum_frame_bytes = configuration.coordinator.maximum_frame_bytes,
           .maximum_work_payload_bytes = configuration.maximum_payload_bytes}});
  if (!remote)
    return 5;

  const scheduler::WorkerDescriptor worker{
      .id = "service-worker",
      .capabilities = {scheduler::WorkClass::MetadataFetch}};
  auto registered =
      (*remote)->register_worker(worker, std::chrono::system_clock::now());
  if (!registered || !*registered)
    return 6;
  auto snapshot = (*remote)->snapshot(std::chrono::system_clock::now());
  if (!snapshot || snapshot->workers != 1)
    return 7;

  auto submitted = (*remote)->submit(work("remote-execution"));
  Executor executor;
  auto remote_worker = scheduler::WorkExecutionRuntime::create(
      **remote, executor,
      {.worker = {.id = "remote-executor",
                  .capabilities = {scheduler::WorkClass::MetadataFetch}},
       .poll_interval = std::chrono::milliseconds{5},
       .heartbeat_interval = std::chrono::milliseconds{50},
       .lease_renewal_interval = std::chrono::milliseconds{100}});
  if (!submitted || !*submitted || !remote_worker || !(*remote_worker)->start())
    return 8;
  if (!wait_until([&] {
        auto current = (*remote)->snapshot(std::chrono::system_clock::now());
        return current && current->succeeded == 1;
      }) ||
      executor.executions() != 1)
    return 9;
  (*remote_worker)->stop();
  if (!(*remote)->unregister_worker(worker.id,
                                    std::chrono::system_clock::now()))
    return 10;

  (*remote).reset();
  (*service)->stop();
  if ((*service)->running() || !observer.empty())
    return 11;

  configuration.coordinator.listen_address = "0.0.0.0";
  auto exposed = service::DistributedWorkService::create(
      configuration, **coordinator, &observer);
  if (exposed || exposed.error().code != core::ErrorCode::PermissionDenied)
    return 12;

  configuration.coordinator.enabled = false;
  auto disabled = service::DistributedWorkService::create(
      configuration, **coordinator, &observer);
  if (disabled || disabled.error().code != core::ErrorCode::InvalidArgument)
    return 13;
  return 0;
}

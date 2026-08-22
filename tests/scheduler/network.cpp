import std;

import sakuin.core;
import sakuin.runtime;
import sakuin.runtime.asio_server;
import sakuin.runtime.asio_stream;
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
          .priority = 3,
          .maximum_attempts = 3};
}

class Observer final : public sakuin::scheduler::WorkProtocolServiceObserver,
                       public sakuin::runtime::StreamServerEvents {
public:
  void on_work_protocol_error(sakuin::runtime::StreamSessionId,
                              sakuin::core::Error error) override {
    std::lock_guard lock{mutex_};
    errors_.push_back(std::move(error));
  }

  void on_stream_server_error(sakuin::core::Error error) override {
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

} // namespace

int main() {
  using namespace sakuin;
  auto local = scheduler::LocalWorkCoordinator::create(
      {.maximum_work_items = 64,
       .maximum_payload_bytes = 1'024,
       .worker_timeout = std::chrono::seconds{5},
       .lease_duration = std::chrono::seconds{1}});
  if (!local)
    return 1;
  scheduler::WorkProtocolDispatcher dispatcher{**local};
  scheduler::LoopbackWorkProtocolAccessPolicy access;
  Observer observer;
  scheduler::WorkProtocolService service{dispatcher, access, {}, &observer};
  auto server = runtime::AsioTcpStreamServer::create(
      {.bind_to = {.address = runtime::IpAddress::loopback_v4(), .port = 0},
       .idle_timeout = std::chrono::seconds{15},
       .read_buffer_bytes = 37,
       .maximum_queued_write_bytes = 2U * 1024U * 1024U,
       .maximum_connections = 8});
  if (!server) {
    std::cerr << "server create: " << server.error().message << '\n';
    return 2;
  }
  auto server_started = (*server)->start(service, &observer);
  if (!server_started) {
    std::cerr << "server start: " << server_started.error().message << '\n';
    return 2;
  }

  runtime::AsioTcpStreamTransportFactory transports;
  auto remote = scheduler::StreamWorkCoordinator::connect(
      transports,
      {.transport = {.remote = (*server)->local_endpoint(),
                     .connect_timeout = std::chrono::seconds{10},
                     .idle_timeout = std::chrono::seconds{15},
                     .read_buffer_bytes = 29,
                     .maximum_queued_write_bytes = 2U * 1024U * 1024U},
       .request_timeout = std::chrono::seconds{10}});
  if (!remote) {
    std::cerr << "remote connect: " << remote.error().message << '\n';
    return 3;
  }

  const auto work = item("network-metadata");
  auto submitted = (*remote)->submit(work);
  auto duplicate = (*remote)->submit(work);
  if (!submitted || !*submitted || !duplicate || *duplicate)
    return 4;
  const scheduler::WorkerDescriptor worker{
      .id = "network-worker-1",
      .capabilities = {scheduler::WorkClass::MetadataFetch}};
  auto now = std::chrono::system_clock::now();
  auto registered = (*remote)->register_worker(worker, core::Timestamp::min());
  if (!registered || !*registered)
    return 5;
  auto leases = (*remote)->lease(worker.id, 1, core::Timestamp::max());
  if (!leases) {
    std::cerr << "lease: " << leases.error().message << '\n';
    return 6;
  }
  if (leases->size() != 1 || leases->front().item != work) {
    std::cerr << "lease mismatch: count=" << leases->size() << '\n';
    return 6;
  }
  if (!(*remote)->complete(worker.id, leases->front().id, now))
    return 7;
  auto snapshot = (*remote)->snapshot(core::Timestamp::min());
  if (!snapshot || snapshot->succeeded != 1 || snapshot->workers != 1)
    return 8;

  auto missing = (*remote)->heartbeat("missing", now);
  if (missing || missing.error().code != core::ErrorCode::NotFound)
    return 9;

  constexpr std::size_t parallel_count = 12;
  for (std::size_t index = 0; index < parallel_count; ++index) {
    auto submitted_parallel =
        (*remote)->submit(item("parallel-" + std::to_string(index)));
    if (!submitted_parallel || !*submitted_parallel)
      return 10;
  }
  std::atomic<std::size_t> leased_count{};
  std::vector<std::jthread> callers;
  for (std::size_t index = 0; index < 4; ++index)
    callers.emplace_back([&] {
      auto result = (*remote)->lease(worker.id, 3, now);
      if (result)
        leased_count.fetch_add(result->size(), std::memory_order_relaxed);
    });
  callers.clear();
  if (leased_count.load(std::memory_order_relaxed) != parallel_count)
    return 11;

  (*server)->stop();
  auto unavailable = (*remote)->snapshot(now);
  if (unavailable || unavailable.error().code != core::ErrorCode::IoError)
    return 12;
  if (!observer.empty())
    return 13;
  return 0;
}

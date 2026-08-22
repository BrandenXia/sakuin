import std;

import sakuin.config;
import sakuin.core;
import sakuin.integration.dht_worker;
import sakuin.runtime;
import sakuin.runtime.asio;
import sakuin.runtime.asio_stream;
import sakuin.runtime.asio_tls_stream;
import sakuin.scheduler;
import sakuin.service.distributed;
import sakuin.service.remote_worker;
import sakuin.service.runtime;

namespace {

sakuin::core::ByteBuffer bytes(std::string_view value) {
  const auto input = std::as_bytes(std::span{value});
  return {input.begin(), input.end()};
}

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

class ResultPublisher final : public sakuin::scheduler::WorkResultPublisher {
public:
  sakuin::core::Result<bool>
  publish_result(std::string_view worker,
                 sakuin::scheduler::WorkResultBatch batch) override {
    std::lock_guard lock{mutex_};
    worker_ = worker;
    batch_ = std::move(batch);
    return true;
  }

  bool received(std::string_view worker,
                const sakuin::scheduler::WorkResultBatch &batch) const {
    std::lock_guard lock{mutex_};
    return worker_ == worker && batch_ == batch;
  }

private:
  mutable std::mutex mutex_;
  std::string worker_;
  std::optional<sakuin::scheduler::WorkResultBatch> batch_;
};

class DhtObserver final : public sakuin::service::DhtRuntimeObserver {
public:
  void on_family_cycle(sakuin::runtime::AddressFamily,
                       sakuin::integration::DhtRuntimeCycle) override {}

  void on_family_error(sakuin::runtime::AddressFamily,
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

std::filesystem::path tls_fixture_directory;

std::filesystem::path tls_fixture(std::string_view name) {
  if (!tls_fixture_directory.empty())
    return tls_fixture_directory / name;
  return std::filesystem::path{__FILE__}.parent_path().parent_path() /
         "fixtures" / "tls" / name;
}

} // namespace

int main(int argc, char **argv) {
  using namespace sakuin;

  if (argc > 1)
    tls_fixture_directory = argv[1];

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
           .maximum_work_payload_bytes = configuration.maximum_payload_bytes,
           .maximum_result_payload_bytes = configuration.maximum_payload_bytes,
           .maximum_chunked_result_bytes = configuration.maximum_result_bytes,
           .maximum_result_reassembly_bytes =
               configuration.coordinator.maximum_result_reassembly_bytes,
           .maximum_result_transfers =
               configuration.coordinator.maximum_result_transfers,
           .result_transfer_timeout =
               configuration.coordinator.result_transfer_timeout}});
  if (!remote)
    return 5;

  const scheduler::WorkerDescriptor worker{
      .id = "service-worker",
      .capabilities = {scheduler::WorkClass::MetadataFetch}};
  auto registered =
      (*remote)->register_worker(worker, std::chrono::system_clock::now());
  if (!registered || !*registered) {
    if (!registered)
      std::cerr << "plain register: " << registered.error().message << '\n';
    else
      std::cerr << "plain register was not new\n";
    return 6;
  }
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

  configuration.coordinator.enabled = true;
  configuration.coordinator.listen_address = "127.0.0.1";
  configuration.coordinator.tls_trust_anchor_file = tls_fixture("ca.pem");
  configuration.coordinator.tls_certificate_chain_file =
      tls_fixture("server.pem");
  configuration.coordinator.tls_private_key_file =
      tls_fixture("server-key.pem");
  ResultPublisher result_publisher;
  auto aggregate_traffic = scheduler::FixedWindowTrafficGovernor::create(
      {.global = {.outbound = scheduler::PeriodicByteQuota{
                      .maximum_bytes = 100, .period = std::chrono::hours{1}}}});
  if (!aggregate_traffic)
    return 14;
  scheduler::GovernorTrafficGrantSource traffic_grants{**aggregate_traffic};
  auto secure_service = service::DistributedWorkService::create(
      configuration, **coordinator, &observer, &result_publisher,
      &traffic_grants);
  if (!secure_service) {
    std::cerr << "secure service create: " << secure_service.error().message
              << '\n';
    return 14;
  }
  auto secure_started = (*secure_service)->start();
  if (!secure_started) {
    std::cerr << "secure service start: " << secure_started.error().message
              << '\n';
    return 14;
  }

  runtime::AsioTlsStreamTransportFactory secure_transports{
      {.trust_anchor_file = tls_fixture("ca.pem"),
       .certificate_chain_file = tls_fixture("worker.pem"),
       .private_key_file = tls_fixture("worker-key.pem"),
       .server_name = "coordinator.test"}};
  auto secure_remote = scheduler::StreamWorkCoordinator::connect(
      secure_transports,
      {.transport = {.remote = (*secure_service)->local_endpoint(),
                     .connect_timeout = std::chrono::seconds{2},
                     .idle_timeout = std::chrono::seconds{3},
                     .read_buffer_bytes = 41,
                     .maximum_queued_write_bytes = 2U * 1024U * 1024U},
       .request_timeout = std::chrono::seconds{2},
       .protocol_limits = {
           .maximum_frame_bytes = configuration.coordinator.maximum_frame_bytes,
           .maximum_work_payload_bytes = configuration.maximum_payload_bytes,
           .maximum_result_payload_bytes = configuration.maximum_payload_bytes,
           .maximum_chunked_result_bytes = configuration.maximum_result_bytes,
           .maximum_result_reassembly_bytes =
               configuration.coordinator.maximum_result_reassembly_bytes,
           .maximum_result_transfers =
               configuration.coordinator.maximum_result_transfers,
           .result_transfer_timeout =
               configuration.coordinator.result_transfer_timeout}});
  if (!secure_remote) {
    std::cerr << "secure connect: " << secure_remote.error().message << '\n';
    return 15;
  }
  auto secure_registered =
      (*secure_remote)
          ->register_worker(
              {.id = "worker-1",
               .capabilities = {scheduler::WorkClass::MetadataFetch}},
              std::chrono::system_clock::now());
  if (!secure_registered || !*secure_registered) {
    if (!secure_registered)
      std::cerr << "secure register: " << secure_registered.error().message
                << '\n';
    else
      std::cerr << "secure register was not new\n";
    return 16;
  }
  auto forbidden_snapshot =
      (*secure_remote)->snapshot(std::chrono::system_clock::now());
  if (forbidden_snapshot ||
      forbidden_snapshot.error().code != core::ErrorCode::PermissionDenied)
    return 17;
  auto impersonation =
      (*secure_remote)
          ->register_worker(
              {.id = "worker-2",
               .capabilities = {scheduler::WorkClass::MetadataFetch}},
              std::chrono::system_clock::now());
  if (impersonation ||
      impersonation.error().code != core::ErrorCode::PermissionDenied)
    return 18;
  auto quota_impersonation =
      (*secure_remote)
          ->acquire("worker-2",
                    {.direction = runtime::TrafficDirection::Outbound,
                     .traffic_class = 4,
                     .bytes = 1},
                    std::chrono::system_clock::now());
  if (quota_impersonation ||
      quota_impersonation.error().code != core::ErrorCode::PermissionDenied)
    return 18;
  const auto result_payload = bytes("durable-observation-result");
  scheduler::WorkResultBatch result{
      .id = scheduler::content_work_result_id(
          scheduler::WorkResultKind::ObservationBatch, result_payload),
      .kind = scheduler::WorkResultKind::ObservationBatch,
      .payload = result_payload};
  auto published = (*secure_remote)->publish_result("worker-1", result);
  if (!published || !*published ||
      !result_publisher.received("worker-1", result))
    return 19;
  const auto metadata_payload = bytes("bounded-torrent-metadata-result");
  scheduler::WorkResultBatch metadata_result{
      .id = scheduler::content_work_result_id(
          scheduler::WorkResultKind::TorrentMetadataBatch, metadata_payload),
      .kind = scheduler::WorkResultKind::TorrentMetadataBatch,
      .payload = metadata_payload};
  auto metadata_published =
      (*secure_remote)->publish_result("worker-1", metadata_result);
  if (!metadata_published || !*metadata_published ||
      !result_publisher.received("worker-1", metadata_result))
    return 19;
  core::ByteBuffer chunked_payload(2'500, core::Byte{0x4d});
  scheduler::WorkResultBatch chunked_result{
      .id = scheduler::content_work_result_id(
          scheduler::WorkResultKind::TorrentMetadataBatch, chunked_payload),
      .kind = scheduler::WorkResultKind::TorrentMetadataBatch,
      .payload = std::move(chunked_payload)};
  auto chunked_published =
      (*secure_remote)->publish_result("worker-1", chunked_result);
  if (!chunked_published || !*chunked_published ||
      !result_publisher.received("worker-1", chunked_result))
    return 19;
  auto first_grant =
      (*secure_remote)
          ->acquire("worker-1",
                    {.direction = runtime::TrafficDirection::Outbound,
                     .traffic_class = 4,
                     .bytes = 64},
                    std::chrono::system_clock::now());
  auto second_grant =
      (*secure_remote)
          ->acquire("worker-1",
                    {.direction = runtime::TrafficDirection::Outbound,
                     .traffic_class = 4,
                     .bytes = 64},
                    std::chrono::system_clock::now());
  auto exhausted =
      (*secure_remote)
          ->acquire("worker-1",
                    {.direction = runtime::TrafficDirection::Outbound,
                     .traffic_class = 4,
                     .bytes = 1},
                    std::chrono::system_clock::now());
  if (!first_grant || first_grant->bytes != 64 || !second_grant ||
      second_grant->bytes != 36 || !exhausted || exhausted->bytes != 0 ||
      !exhausted->retry_after)
    return 27;

  auto udp_probe = runtime::AsioDatagramTransport::create(
      {.bind_to = {.address = runtime::IpAddress::loopback_v4(), .port = 0}});
  if (!udp_probe)
    return 20;
  const auto worker_dht_port = (*udp_probe)->local_endpoint().port;
  (*udp_probe).reset();
  auto worker_configuration = config::defaults();
  worker_configuration.network.enable_ipv4 = true;
  worker_configuration.network.enable_ipv6 = false;
  worker_configuration.network.listen_port = worker_dht_port;
  worker_configuration.network.dht.bootstrap.clear();
  worker_configuration.distributed = configuration;
  worker_configuration.distributed.worker.enabled = true;
  worker_configuration.distributed.worker.id = "worker-1";
  worker_configuration.distributed.worker.coordinator_address = "127.0.0.1";
  worker_configuration.distributed.worker.coordinator_port =
      (*secure_service)->local_endpoint().port;
  worker_configuration.distributed.worker.observation_batch_size = 2;
  worker_configuration.distributed.worker.tls_trust_anchor_file =
      tls_fixture("ca.pem");
  worker_configuration.distributed.worker.tls_certificate_chain_file =
      tls_fixture("worker.pem");
  worker_configuration.distributed.worker.tls_private_key_file =
      tls_fixture("worker-key.pem");
  worker_configuration.distributed.worker.tls_server_name = "coordinator.test";
  DhtObserver dht_observer;
  auto remote_dht = service::RemoteDhtWorkerService::create(
      worker_configuration, dht_observer);
  if (!remote_dht || !(*remote_dht)->start())
    return 21;
  if (!wait_until([&] {
        auto current =
            (*coordinator)->snapshot(std::chrono::system_clock::now());
        return current && current->workers == 2;
      }))
    return 22;
  if (auto stopped = (*remote_dht)->stop(); !stopped)
    return 23;
  if (!wait_until([&] {
        auto current =
            (*coordinator)->snapshot(std::chrono::system_clock::now());
        return current && current->workers == 1;
      }) ||
      !dht_observer.empty())
    return 24;
  if (!(*secure_remote)
           ->unregister_worker("worker-1", std::chrono::system_clock::now()))
    return 25;
  (*secure_remote).reset();
  (*secure_service)->stop();
  if (!observer.empty())
    return 26;
  return 0;
}

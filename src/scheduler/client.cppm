export module sakuin.scheduler.client;

import std;

import sakuin.core.bytes;
import sakuin.core.result;
import sakuin.core.time;
import sakuin.runtime.stream;
import sakuin.runtime.traffic;
import sakuin.scheduler.protocol;
import sakuin.scheduler.traffic;
import sakuin.scheduler.work;

export namespace sakuin::scheduler {

struct StreamWorkCoordinatorOptions {
  runtime::StreamTransportOptions transport;
  core::Duration request_timeout{std::chrono::seconds{10}};
  WorkProtocolLimits protocol_limits{};
};

// Synchronous WorkCoordinator facade over Sakuin's asynchronous owned-byte
// stream abstraction. The implementation supports concurrent callers and does
// not expose the runtime's execution mechanism to scheduler consumers.
class StreamWorkCoordinator final : public WorkCoordinator,
                                    public WorkResultPublisher,
                                    public TrafficGrantSource,
                                    private runtime::StreamReceiver {
public:
  static core::Result<std::unique_ptr<StreamWorkCoordinator>>
  connect(runtime::StreamTransportFactory &factory,
          StreamWorkCoordinatorOptions options);

  ~StreamWorkCoordinator() override;

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
  core::Result<WorkCoordinatorSnapshot> snapshot(core::Timestamp now) override;
  core::Result<bool> publish_result(std::string_view worker,
                                    WorkResultBatch result) override;
  core::Result<TrafficGrant> acquire(std::string_view worker,
                                     runtime::TrafficRequest request,
                                     core::Timestamp now) override;

private:
  struct Pending {
    std::optional<core::Result<WorkProtocolResponse>> outcome;
  };

  StreamWorkCoordinator(std::shared_ptr<runtime::StreamTransport> transport,
                        StreamWorkCoordinatorOptions options)
      : transport_(std::move(transport)), options_(std::move(options)),
        decoder_(options_.protocol_limits) {}

  core::Result<WorkProtocolResponse>
  exchange(WorkProtocolOperation operation, WorkProtocolRequestPayload payload);
  core::Result<void> void_response(WorkProtocolOperation operation,
                                   WorkProtocolRequestPayload payload);
  void fail_connection(core::Error error) noexcept;

  void on_stream_connected() override;
  void on_stream_data(core::ByteBuffer bytes) override;
  void on_stream_closed(std::optional<core::Error> error) override;

  std::shared_ptr<runtime::StreamTransport> transport_;
  StreamWorkCoordinatorOptions options_;
  WorkFrameDecoder decoder_;
  std::mutex mutex_;
  std::condition_variable condition_;
  std::map<std::uint64_t, std::shared_ptr<Pending>> pending_;
  std::atomic<std::uint64_t> next_request_{};
  std::optional<core::Error> connection_error_;
  bool connected_{};
};

} // namespace sakuin::scheduler

namespace sakuin::scheduler {
namespace {

core::Error closed_error() {
  return {core::ErrorCode::IoError,
          "Distributed-work coordinator connection closed"};
}

template <typename T>
core::Result<T> response_value(WorkProtocolResponse response) {
  if (const auto error = std::get_if<core::Error>(&response.payload))
    return std::unexpected(*error);
  if (const auto value = std::get_if<T>(&response.payload))
    return std::move(*value);
  return std::unexpected(core::Error{
      core::ErrorCode::CorruptSegment,
      "Distributed-work response payload does not match its operation"});
}

} // namespace

core::Result<std::unique_ptr<StreamWorkCoordinator>>
StreamWorkCoordinator::connect(runtime::StreamTransportFactory &factory,
                               StreamWorkCoordinatorOptions options) {
  if (options.request_timeout <= core::Duration::zero())
    return std::unexpected(
        core::Error{core::ErrorCode::InvalidArgument,
                    "Distributed-work request timeout must be positive"});
  auto transport = factory.create(options.transport);
  if (!transport)
    return std::unexpected(transport.error());
  auto result = std::unique_ptr<StreamWorkCoordinator>{
      new StreamWorkCoordinator{std::move(*transport), std::move(options)}};
  if (auto started = result->transport_->start(*result); !started)
    return std::unexpected(started.error());
  std::unique_lock lock{result->mutex_};
  const auto connected = result->condition_.wait_for(
      lock, result->options_.transport.connect_timeout,
      [&] { return result->connected_ || result->connection_error_; });
  if (!connected || !result->connected_) {
    auto error = result->connection_error_.value_or(
        core::Error{core::ErrorCode::Timeout,
                    "Distributed-work coordinator connection timed out"});
    lock.unlock();
    result->transport_->stop();
    return std::unexpected(std::move(error));
  }
  return result;
}

StreamWorkCoordinator::~StreamWorkCoordinator() {
  if (transport_)
    transport_->stop();
}

void StreamWorkCoordinator::on_stream_connected() {
  std::lock_guard lock{mutex_};
  connected_ = true;
  condition_.notify_all();
}

void StreamWorkCoordinator::fail_connection(core::Error error) noexcept {
  std::lock_guard lock{mutex_};
  if (!connection_error_)
    connection_error_ = error;
  connected_ = false;
  for (auto &[_, pending] : pending_)
    if (!pending->outcome)
      pending->outcome = std::unexpected(error);
  condition_.notify_all();
}

void StreamWorkCoordinator::on_stream_data(core::ByteBuffer bytes) {
  auto frames = decoder_.feed(bytes);
  if (!frames) {
    fail_connection(frames.error());
    transport_->stop();
    return;
  }
  for (const auto &frame : *frames) {
    auto response = decode_work_response(frame, options_.protocol_limits);
    if (!response) {
      fail_connection(response.error());
      transport_->stop();
      return;
    }
    bool unexpected_response{};
    {
      std::lock_guard lock{mutex_};
      const auto found = pending_.find(response->request_id);
      if (found == pending_.end() || found->second->outcome) {
        unexpected_response = true;
      } else {
        found->second->outcome = std::move(*response);
        condition_.notify_all();
      }
    }
    if (unexpected_response) {
      fail_connection({core::ErrorCode::CorruptSegment,
                       "Distributed-work response id is not pending"});
      transport_->stop();
      return;
    }
  }
}

void StreamWorkCoordinator::on_stream_closed(std::optional<core::Error> error) {
  fail_connection(error.value_or(closed_error()));
}

core::Result<WorkProtocolResponse>
StreamWorkCoordinator::exchange(WorkProtocolOperation operation,
                                WorkProtocolRequestPayload payload) {
  auto request_id = next_request_.fetch_add(1, std::memory_order_relaxed) + 1;
  if (request_id == 0)
    request_id = next_request_.fetch_add(1, std::memory_order_relaxed) + 1;
  WorkProtocolRequest request{.request_id = request_id,
                              .operation = operation,
                              .payload = std::move(payload)};
  auto encoded = encode_work_request(request, options_.protocol_limits);
  if (!encoded)
    return std::unexpected(encoded.error());
  auto pending = std::make_shared<Pending>();
  {
    std::lock_guard lock{mutex_};
    if (!connected_)
      return std::unexpected(connection_error_.value_or(closed_error()));
    if (!pending_.emplace(request_id, pending).second)
      return std::unexpected(
          core::Error{core::ErrorCode::Conflict,
                      "Distributed-work request id is already pending"});
  }
  if (auto sent = transport_->send(std::move(*encoded)); !sent) {
    std::lock_guard lock{mutex_};
    pending_.erase(request_id);
    return std::unexpected(sent.error());
  }
  std::unique_lock lock{mutex_};
  const auto completed =
      condition_.wait_for(lock, options_.request_timeout,
                          [&] { return pending->outcome.has_value(); });
  if (!completed) {
    pending_.erase(request_id);
    auto error = core::Error{core::ErrorCode::Timeout,
                             "Distributed-work coordinator request timed out"};
    lock.unlock();
    fail_connection(error);
    transport_->stop();
    return std::unexpected(std::move(error));
  }
  auto outcome = std::move(*pending->outcome);
  pending_.erase(request_id);
  if (!outcome)
    return std::unexpected(outcome.error());
  if (outcome->operation != operation)
    return std::unexpected(core::Error{
        core::ErrorCode::CorruptSegment,
        "Distributed-work response operation does not match its request"});
  return std::move(*outcome);
}

core::Result<void>
StreamWorkCoordinator::void_response(WorkProtocolOperation operation,
                                     WorkProtocolRequestPayload payload) {
  auto response = exchange(operation, std::move(payload));
  if (!response)
    return std::unexpected(response.error());
  return response_value<std::monostate>(std::move(*response))
      .transform([](std::monostate) {});
}

core::Result<bool> StreamWorkCoordinator::submit(WorkItem item) {
  auto response = exchange(WorkProtocolOperation::Submit,
                           SubmitWorkRequest{.item = std::move(item)});
  if (!response)
    return std::unexpected(response.error());
  return response_value<bool>(std::move(*response));
}

core::Result<bool>
StreamWorkCoordinator::register_worker(WorkerDescriptor worker,
                                       core::Timestamp) {
  auto response = exchange(WorkProtocolOperation::RegisterWorker,
                           RegisterWorkerRequest{.worker = std::move(worker)});
  if (!response)
    return std::unexpected(response.error());
  return response_value<bool>(std::move(*response));
}

core::Result<void> StreamWorkCoordinator::heartbeat(std::string_view worker,
                                                    core::Timestamp) {
  return void_response(WorkProtocolOperation::Heartbeat,
                       WorkerHeartbeatRequest{.worker = std::string{worker}});
}

core::Result<std::vector<WorkLease>>
StreamWorkCoordinator::lease(std::string_view worker, std::size_t maximum,
                             core::Timestamp) {
  auto response = exchange(
      WorkProtocolOperation::Lease,
      LeaseWorkRequest{.worker = std::string{worker}, .maximum = maximum});
  if (!response)
    return std::unexpected(response.error());
  return response_value<std::vector<WorkLease>>(std::move(*response));
}

core::Result<void> StreamWorkCoordinator::renew(std::string_view worker,
                                                LeaseId lease,
                                                core::Timestamp) {
  return void_response(
      WorkProtocolOperation::Renew,
      LeaseMutationRequest{.worker = std::string{worker}, .lease = lease});
}

core::Result<void> StreamWorkCoordinator::complete(std::string_view worker,
                                                   LeaseId lease,
                                                   core::Timestamp) {
  return void_response(
      WorkProtocolOperation::Complete,
      LeaseMutationRequest{.worker = std::string{worker}, .lease = lease});
}

core::Result<void> StreamWorkCoordinator::fail(std::string_view worker,
                                               LeaseId lease, bool retryable,
                                               core::Timestamp now,
                                               core::Timestamp retry_at) {
  if (retry_at < now)
    return std::unexpected(
        core::Error{core::ErrorCode::InvalidArgument,
                    "Retry time cannot precede failure time"});
  return void_response(
      WorkProtocolOperation::Fail,
      FailWorkRequest{.worker = std::string{worker},
                      .lease = lease,
                      .retryable = retryable,
                      .retry_delay = std::chrono::duration_cast<core::Duration>(
                          retry_at - now)});
}

core::Result<void>
StreamWorkCoordinator::unregister_worker(std::string_view worker,
                                         core::Timestamp) {
  return void_response(WorkProtocolOperation::UnregisterWorker,
                       UnregisterWorkerRequest{.worker = std::string{worker}});
}

core::Result<WorkCoordinatorSnapshot>
StreamWorkCoordinator::snapshot(core::Timestamp) {
  auto response =
      exchange(WorkProtocolOperation::Snapshot, WorkSnapshotRequest{});
  if (!response)
    return std::unexpected(response.error());
  return response_value<WorkCoordinatorSnapshot>(std::move(*response));
}

core::Result<bool>
StreamWorkCoordinator::publish_result(std::string_view worker,
                                      WorkResultBatch result) {
  auto response =
      exchange(WorkProtocolOperation::PublishResult,
               PublishWorkResultRequest{.worker = std::string{worker},
                                        .result = std::move(result)});
  if (!response)
    return std::unexpected(response.error());
  return response_value<bool>(std::move(*response));
}

core::Result<TrafficGrant> StreamWorkCoordinator::acquire(
    std::string_view worker, runtime::TrafficRequest request, core::Timestamp) {
  auto response = exchange(
      WorkProtocolOperation::AcquireTraffic,
      AcquireTrafficRequest{.worker = std::string{worker}, .traffic = request});
  if (!response)
    return std::unexpected(response.error());
  return response_value<TrafficGrant>(std::move(*response));
}

} // namespace sakuin::scheduler

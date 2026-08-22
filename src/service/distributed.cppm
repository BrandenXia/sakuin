export module sakuin.service.distributed;

import std;

import sakuin.config.model;
import sakuin.core.result;
import sakuin.runtime.asio_resolver;
import sakuin.runtime.asio_server;
import sakuin.runtime.datagram;
import sakuin.runtime.stream;
import sakuin.scheduler.protocol;
import sakuin.scheduler.server;
import sakuin.scheduler.work;

export namespace sakuin::service {

class DistributedWorkServiceObserver {
public:
  virtual ~DistributedWorkServiceObserver() = default;
  virtual void
  on_distributed_work_error(std::optional<runtime::StreamSessionId> session,
                            core::Error error) = 0;
};

// Hosts the replaceable operational work coordinator over the Sakuin stream
// boundary. The initial policy is deliberately loopback-only; authenticated
// remote admission can replace it without changing scheduler consumers.
class DistributedWorkService final
    : private scheduler::WorkProtocolServiceObserver,
      private runtime::StreamServerEvents {
public:
  static core::Result<std::unique_ptr<DistributedWorkService>>
  create(const config::DistributedConfig &configuration,
         scheduler::WorkCoordinator &coordinator,
         DistributedWorkServiceObserver *observer = nullptr);

  ~DistributedWorkService();

  core::Result<void> start();
  void stop() noexcept;
  bool running() const noexcept {
    return running_.load(std::memory_order_acquire);
  }
  runtime::StreamEndpoint local_endpoint() const noexcept {
    return server_->local_endpoint();
  }

private:
  DistributedWorkService(scheduler::WorkCoordinator &coordinator,
                         scheduler::WorkProtocolLimits limits,
                         std::unique_ptr<runtime::AsioTcpStreamServer> server,
                         DistributedWorkServiceObserver *observer)
      : dispatcher_(coordinator), protocol_(dispatcher_, access_, limits, this),
        server_(std::move(server)), observer_(observer) {}

  void on_work_protocol_error(runtime::StreamSessionId session,
                              core::Error error) override;
  void on_stream_server_error(core::Error error) override;
  void report(std::optional<runtime::StreamSessionId> session,
              core::Error error) noexcept;

  scheduler::WorkProtocolDispatcher dispatcher_;
  scheduler::LoopbackWorkProtocolAccessPolicy access_;
  scheduler::WorkProtocolService protocol_;
  std::unique_ptr<runtime::AsioTcpStreamServer> server_;
  DistributedWorkServiceObserver *observer_{};
  std::atomic<bool> running_{};
};

} // namespace sakuin::service

namespace sakuin::service {
namespace {

bool loopback(runtime::IpAddress address) {
  if (address.family == runtime::AddressFamily::IPv4)
    return address.bytes[0] == 127;
  return std::ranges::all_of(std::span{address.bytes}.first(15),
                             [](std::uint8_t value) { return value == 0; }) &&
         address.bytes[15] == 1;
}

core::Result<runtime::StreamEndpoint>
resolve_endpoint(const config::DistributedConfig::CoordinatorConfig &config) {
  runtime::AsioDatagramEndpointResolver resolver;
  // The shared startup-time resolver rejects port zero because datagram peers
  // cannot use it. A stream listener can legitimately request an ephemeral
  // port, so resolve the address with a placeholder and restore the bind port.
  const auto resolution_port =
      config.listen_port == 0 ? std::uint16_t{1} : config.listen_port;
  for (const auto family :
       {runtime::AddressFamily::IPv4, runtime::AddressFamily::IPv6}) {
    auto resolved =
        resolver.resolve(config.listen_address, resolution_port, family);
    if (!resolved)
      continue;
    const auto found =
        std::ranges::find_if(*resolved, [](const auto &endpoint) {
          return loopback(endpoint.address);
        });
    if (found != resolved->end())
      return runtime::StreamEndpoint{.address = found->address,
                                     .port = config.listen_port};
  }
  return std::unexpected(core::Error{
      core::ErrorCode::PermissionDenied,
      "The initial distributed coordinator listener must resolve to loopback"});
}

} // namespace

core::Result<std::unique_ptr<DistributedWorkService>>
DistributedWorkService::create(const config::DistributedConfig &configuration,
                               scheduler::WorkCoordinator &coordinator,
                               DistributedWorkServiceObserver *observer) {
  const auto &listener = configuration.coordinator;
  if (!listener.enabled)
    return std::unexpected(
        core::Error{core::ErrorCode::InvalidArgument,
                    "Distributed coordinator listener is not enabled"});
  auto endpoint = resolve_endpoint(listener);
  if (!endpoint)
    return std::unexpected(endpoint.error());
  scheduler::WorkProtocolLimits limits{
      .maximum_frame_bytes = listener.maximum_frame_bytes,
      .maximum_work_payload_bytes = configuration.maximum_payload_bytes};
  auto server = runtime::AsioTcpStreamServer::create(
      {.bind_to = *endpoint,
       .idle_timeout = listener.idle_timeout,
       .read_buffer_bytes = listener.read_buffer_bytes,
       .maximum_queued_write_bytes = listener.maximum_queued_write_bytes,
       .maximum_connections = listener.maximum_connections});
  if (!server)
    return std::unexpected(server.error());
  return std::unique_ptr<DistributedWorkService>{new DistributedWorkService{
      coordinator, limits, std::move(*server), observer}};
}

DistributedWorkService::~DistributedWorkService() { stop(); }

core::Result<void> DistributedWorkService::start() {
  if (running_.load(std::memory_order_acquire))
    return std::unexpected(
        core::Error{core::ErrorCode::Conflict,
                    "Distributed coordinator service is already running"});
  auto started = server_->start(protocol_, this);
  if (!started)
    return std::unexpected(started.error());
  running_.store(true, std::memory_order_release);
  return {};
}

void DistributedWorkService::stop() noexcept {
  if (!running_.exchange(false, std::memory_order_acq_rel))
    return;
  server_->stop();
}

void DistributedWorkService::report(
    std::optional<runtime::StreamSessionId> session,
    core::Error error) noexcept {
  if (!observer_)
    return;
  try {
    observer_->on_distributed_work_error(session, std::move(error));
  } catch (...) {
    // Observability callbacks may not unwind through runtime threads.
  }
}

void DistributedWorkService::on_work_protocol_error(
    runtime::StreamSessionId session, core::Error error) {
  report(session, std::move(error));
}

void DistributedWorkService::on_stream_server_error(core::Error error) {
  report(std::nullopt, std::move(error));
}

} // namespace sakuin::service

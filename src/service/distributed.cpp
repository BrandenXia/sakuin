module sakuin.service.distributed;

import std;

import sakuin.config.model;
import sakuin.core.result;
import sakuin.runtime.asio_resolver;
import sakuin.runtime.asio_server;
import sakuin.runtime.asio_tls_server;
import sakuin.runtime.datagram;
import sakuin.runtime.stream;
import sakuin.scheduler.protocol;
import sakuin.scheduler.server;
import sakuin.scheduler.work;

namespace sakuin::service {

struct DistributedWorkService::Impl final
    : scheduler::WorkProtocolServiceObserver,
      runtime::StreamServerEvents {
  Impl(scheduler::WorkCoordinator &coordinator,
       scheduler::WorkProtocolLimits limits,
       std::unique_ptr<scheduler::WorkProtocolAccessPolicy> configured_access,
       std::unique_ptr<runtime::StreamServer> configured_server,
       DistributedWorkServiceObserver *configured_observer,
       scheduler::WorkResultPublisher *results,
       scheduler::TrafficGrantSource *traffic)
      : dispatcher(
            coordinator, [] { return std::chrono::system_clock::now(); },
            results, traffic, limits),
        access(std::move(configured_access)),
        protocol(dispatcher, *access, limits, this),
        server(std::move(configured_server)), observer(configured_observer) {}

  void report(std::optional<runtime::StreamSessionId> session,
              core::Error error) noexcept {
    if (!observer)
      return;
    try {
      observer->on_distributed_work_error(session, std::move(error));
    } catch (...) {
    }
  }

  void on_work_protocol_error(runtime::StreamSessionId session,
                              core::Error error) override {
    report(session, std::move(error));
  }

  void on_stream_server_error(core::Error error) override {
    report(std::nullopt, std::move(error));
  }

  scheduler::WorkProtocolDispatcher dispatcher;
  std::unique_ptr<scheduler::WorkProtocolAccessPolicy> access;
  scheduler::WorkProtocolService protocol;
  std::unique_ptr<runtime::StreamServer> server;
  DistributedWorkServiceObserver *observer{};
  std::atomic<bool> running{};
};

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
  const auto resolution_port =
      config.listen_port == 0 ? std::uint16_t{1} : config.listen_port;
  const auto secure = config.tls_trust_anchor_file.has_value();
  for (const auto family :
       {runtime::AddressFamily::IPv4, runtime::AddressFamily::IPv6}) {
    auto resolved =
        resolver.resolve(config.listen_address, resolution_port, family);
    if (!resolved)
      continue;
    const auto found =
        secure ? resolved->begin()
               : std::ranges::find_if(*resolved, [](const auto &endpoint) {
                   return loopback(endpoint.address);
                 });
    if (found != resolved->end())
      return runtime::StreamEndpoint{.address = found->address,
                                     .port = config.listen_port};
  }
  return std::unexpected(core::Error{
      core::ErrorCode::PermissionDenied,
      "A plaintext distributed coordinator listener must resolve to loopback"});
}

} // namespace

DistributedWorkService::DistributedWorkService(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

core::Result<std::unique_ptr<DistributedWorkService>>
DistributedWorkService::create(const config::DistributedConfig &configuration,
                               scheduler::WorkCoordinator &coordinator,
                               DistributedWorkServiceObserver *observer,
                               scheduler::WorkResultPublisher *results,
                               scheduler::TrafficGrantSource *traffic) {
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
      .maximum_work_payload_bytes = configuration.maximum_payload_bytes,
      .maximum_result_payload_bytes = configuration.maximum_payload_bytes,
      .maximum_chunked_result_bytes = configuration.maximum_result_bytes,
      .maximum_result_reassembly_bytes =
          listener.maximum_result_reassembly_bytes,
      .maximum_result_transfers = listener.maximum_result_transfers,
      .result_transfer_timeout = listener.result_transfer_timeout};
  runtime::StreamServerOptions transport{
      .bind_to = *endpoint,
      .idle_timeout = listener.idle_timeout,
      .read_buffer_bytes = listener.read_buffer_bytes,
      .maximum_queued_write_bytes = listener.maximum_queued_write_bytes,
      .maximum_connections = listener.maximum_connections};
  std::unique_ptr<runtime::StreamServer> server;
  std::unique_ptr<scheduler::WorkProtocolAccessPolicy> access;
  if (listener.tls_trust_anchor_file) {
    auto secure = runtime::AsioTlsStreamServer::create(
        transport,
        {.trust_anchor_file = *listener.tls_trust_anchor_file,
         .certificate_chain_file = *listener.tls_certificate_chain_file,
         .private_key_file = *listener.tls_private_key_file});
    if (!secure)
      return std::unexpected(secure.error());
    server = std::move(*secure);
    access = std::make_unique<scheduler::MutualTlsWorkProtocolAccessPolicy>();
  } else {
    auto local = runtime::AsioTcpStreamServer::create(transport);
    if (!local)
      return std::unexpected(local.error());
    server = std::move(*local);
    access = std::make_unique<scheduler::LoopbackWorkProtocolAccessPolicy>();
  }
  auto impl =
      std::make_unique<Impl>(coordinator, limits, std::move(access),
                             std::move(server), observer, results, traffic);
  return std::unique_ptr<DistributedWorkService>{
      new DistributedWorkService{std::move(impl)}};
}

DistributedWorkService::~DistributedWorkService() { stop(); }

core::Result<void> DistributedWorkService::start() {
  if (impl_->running.load(std::memory_order_acquire))
    return std::unexpected(
        core::Error{core::ErrorCode::Conflict,
                    "Distributed coordinator service is already running"});
  auto started = impl_->server->start(impl_->protocol, impl_.get());
  if (!started)
    return std::unexpected(started.error());
  impl_->running.store(true, std::memory_order_release);
  return {};
}

void DistributedWorkService::stop() noexcept {
  if (!impl_->running.exchange(false, std::memory_order_acq_rel))
    return;
  impl_->server->stop();
}

bool DistributedWorkService::running() const noexcept {
  return impl_->running.load(std::memory_order_acquire);
}

runtime::StreamEndpoint
DistributedWorkService::local_endpoint() const noexcept {
  return impl_->server->local_endpoint();
}

} // namespace sakuin::service

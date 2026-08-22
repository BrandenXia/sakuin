module sakuin.service.remote_worker;

import std;

import sakuin.config.model;
import sakuin.config;
import sakuin.core.result;
import sakuin.integration.work_results;
import sakuin.integration.remote_metadata;
import sakuin.runtime.asio_resolver;
import sakuin.runtime.asio_tls_stream;
import sakuin.runtime.datagram;
import sakuin.runtime.stream;
import sakuin.scheduler.client;
import sakuin.scheduler.protocol;
import sakuin.scheduler.traffic;
import sakuin.service.runtime;

namespace sakuin::service {

struct RemoteDhtWorkerService::Impl {
  Impl(std::unique_ptr<scheduler::StreamWorkCoordinator> configured_coordinator,
       std::unique_ptr<integration::RemoteObservationSink>
           configured_observations,
       std::unique_ptr<integration::RemoteTorrentMetadataSink>
           configured_metadata,
       std::unique_ptr<scheduler::GrantedTrafficGovernor> configured_traffic,
       std::unique_ptr<AsioDhtRuntime> configured_runtime)
      : coordinator(std::move(configured_coordinator)),
        observations(std::move(configured_observations)),
        metadata(std::move(configured_metadata)),
        traffic(std::move(configured_traffic)),
        runtime(std::move(configured_runtime)) {}

  std::unique_ptr<scheduler::StreamWorkCoordinator> coordinator;
  std::unique_ptr<integration::RemoteObservationSink> observations;
  std::unique_ptr<integration::RemoteTorrentMetadataSink> metadata;
  std::unique_ptr<scheduler::GrantedTrafficGovernor> traffic;
  std::unique_ptr<AsioDhtRuntime> runtime;
};

namespace {

core::Result<runtime::StreamEndpoint>
resolve_coordinator(const config::DistributedConfig::WorkerConfig &worker) {
  runtime::AsioDatagramEndpointResolver resolver;
  for (const auto family :
       {runtime::AddressFamily::IPv4, runtime::AddressFamily::IPv6}) {
    auto resolved = resolver.resolve(worker.coordinator_address,
                                     worker.coordinator_port, family);
    if (resolved && !resolved->empty())
      return runtime::StreamEndpoint{.address = resolved->front().address,
                                     .port = resolved->front().port};
  }
  return std::unexpected(
      core::Error{core::ErrorCode::NotFound,
                  "Distributed worker could not resolve its coordinator"});
}

} // namespace

RemoteDhtWorkerService::RemoteDhtWorkerService(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

core::Result<std::unique_ptr<RemoteDhtWorkerService>>
RemoteDhtWorkerService::create(const config::AppConfig &configuration,
                               DhtRuntimeObserver &observer,
                               DhtRuntimeExternalAddresses external_addresses) {
  if (auto valid = config::validate(configuration); !valid)
    return std::unexpected(valid.error());
  const auto &worker = configuration.distributed.worker;
  if (!worker.enabled)
    return std::unexpected(
        core::Error{core::ErrorCode::InvalidArgument,
                    "Distributed DHT worker is not enabled"});
  auto endpoint = resolve_coordinator(worker);
  if (!endpoint)
    return std::unexpected(endpoint.error());

  runtime::AsioTlsStreamTransportFactory transports{
      {.trust_anchor_file = *worker.tls_trust_anchor_file,
       .certificate_chain_file = *worker.tls_certificate_chain_file,
       .private_key_file = *worker.tls_private_key_file,
       .server_name = worker.tls_server_name}};
  auto coordinator = scheduler::StreamWorkCoordinator::connect(
      transports,
      {.transport = {.remote = *endpoint,
                     .connect_timeout = worker.connect_timeout,
                     .idle_timeout = worker.idle_timeout,
                     .read_buffer_bytes = worker.read_buffer_bytes,
                     .maximum_queued_write_bytes =
                         worker.maximum_queued_write_bytes},
       .request_timeout = worker.request_timeout,
       .protocol_limits = {
           .maximum_frame_bytes =
               configuration.distributed.coordinator.maximum_frame_bytes,
           .maximum_work_payload_bytes =
               configuration.distributed.maximum_payload_bytes,
           .maximum_result_payload_bytes =
               configuration.distributed.maximum_payload_bytes,
           .maximum_traffic_grant_bytes = 16U * 1024U * 1024U}});
  if (!coordinator)
    return std::unexpected(coordinator.error());
  auto observations = std::make_unique<integration::RemoteObservationSink>(
      **coordinator, worker.id, worker.observation_batch_size);
  auto metadata = std::make_unique<integration::RemoteTorrentMetadataSink>(
      **coordinator, worker.id,
      configuration.distributed.maximum_payload_bytes);
  auto traffic = scheduler::GrantedTrafficGovernor::create(
      **coordinator, worker.id, configuration.network.traffic.grant_bytes);
  if (!traffic)
    return std::unexpected(traffic.error());

  auto runtime =
      AsioDhtRuntime::create(configuration.network,
                             {.observations = observations.get(),
                              .metadata_results = metadata.get(),
                              .observer = &observer,
                              .work = coordinator->get(),
                              .traffic = traffic->get(),
                              .worker_namespace = worker.id,
                              .worker_heartbeat_interval =
                                  configuration.distributed.heartbeat_interval},
                             std::move(external_addresses));
  if (!runtime)
    return std::unexpected(runtime.error());
  auto impl = std::make_unique<Impl>(
      std::move(*coordinator), std::move(observations), std::move(metadata),
      std::move(*traffic), std::move(*runtime));
  return std::unique_ptr<RemoteDhtWorkerService>{
      new RemoteDhtWorkerService{std::move(impl)}};
}

RemoteDhtWorkerService::~RemoteDhtWorkerService() { static_cast<void>(stop()); }

core::Result<void> RemoteDhtWorkerService::start() {
  return impl_->runtime->start();
}

core::Result<void> RemoteDhtWorkerService::stop() {
  impl_->runtime->stop();
  return impl_->observations->flush();
}

bool RemoteDhtWorkerService::running() const noexcept {
  return impl_->runtime->running();
}

std::optional<runtime::DatagramEndpoint> RemoteDhtWorkerService::local_endpoint(
    runtime::AddressFamily family) const noexcept {
  return impl_->runtime->local_endpoint(family);
}

} // namespace sakuin::service

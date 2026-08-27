#include <csignal>
#include <spdlog/spdlog.h>

import std;

import sakuin.config;
import sakuin.api.status;
import sakuin.core;
import sakuin.dht.krpc;
import sakuin.integration.dht_worker;
import sakuin.index.materialize;
import sakuin.runtime.datagram;
import sakuin.runtime.stream;
import sakuin.search.rebuild;
import sakuin.service.api;
import sakuin.service.application;
import sakuin.service.duplicates;
import sakuin.service.distributed;
import sakuin.service.local;
import sakuin.service.maintenance;
import sakuin.service.materialization;
import sakuin.service.runtime;
import sakuin.service.remote_worker;
import sakuin.service.storage;

extern char **environ;

namespace {

volatile std::sig_atomic_t pending_signal{};

extern "C" void handle_signal(int signal) { pending_signal = signal; }

constexpr std::string_view usage = R"usage(Usage:
  sakuin [--config PATH] [--key=value ...]
  sakuin worker [--config PATH] [--key=value ...]
  sakuin admin compact|verify|gc [--config PATH] [--key=value ...]
  sakuin --version

Configuration precedence: defaults, TOML, SAKUIN_* environment, command line.
Send SIGHUP to reload API credentials and refresh derived indexes.

Before enabling the API for the first time:
  sakuin-api-key --state-dir ./data/operational/api init
  sakuin-api-key --state-dir ./data/operational/api create --id reader --permissions search
)usage";

std::string_view family_name(sakuin::runtime::AddressFamily family) {
  return family == sakuin::runtime::AddressFamily::IPv4 ? "IPv4" : "IPv6";
}

std::string_view family_error_source(sakuin::runtime::AddressFamily family) {
  return family == sakuin::runtime::AddressFamily::IPv4 ? "dht.ipv4"
                                                        : "dht.ipv6";
}

std::string_view
maintenance_dataset_name(sakuin::service::LocalDataset dataset) {
  return dataset == sakuin::service::LocalDataset::Observations ? "observations"
                                                                : "torrents";
}

std::string_view
maintenance_operation_name(sakuin::service::MaintenanceOperation operation) {
  switch (operation) {
  case sakuin::service::MaintenanceOperation::Retention:
    return "retention";
  case sakuin::service::MaintenanceOperation::Compaction:
    return "compaction";
  case sakuin::service::MaintenanceOperation::Verification:
    return "verification";
  case sakuin::service::MaintenanceOperation::GarbageCollection:
    return "garbage_collection";
  }
  std::unreachable();
}

std::string
maintenance_error_source(sakuin::service::LocalDataset dataset,
                         sakuin::service::MaintenanceOperation operation) {
  return "maintenance." + std::string{maintenance_dataset_name(dataset)} + "." +
         std::string{maintenance_operation_name(operation)};
}

class Observer final : public sakuin::service::DhtRuntimeObserver,
                       public sakuin::service::ApiServiceObserver,
                       public sakuin::service::StorageMaintenanceObserver,
                       public sakuin::service::MaterializationObserver,
                       public sakuin::service::DuplicateIndexObserver,
                       public sakuin::service::DistributedWorkServiceObserver,
                       public sakuin::api::StatusProvider {
public:
  explicit Observer(const sakuin::config::AppConfig &configuration) {
    snapshot_.started_at_ms = now_ms();
    started_steady_ = std::chrono::steady_clock::now();
    snapshot_.ipv4.enabled = configuration.network.enable_ipv4;
    snapshot_.ipv6.enabled = configuration.network.enable_ipv6;
  }

  sakuin::api::ServiceStatus status() const override {
    std::lock_guard lock{mutex_};
    auto result = snapshot_;
    result.uptime_ms = static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - started_steady_)
            .count());
    return result;
  }

  void mark_started() {
    std::lock_guard lock{mutex_};
    snapshot_.started_at_ms = now_ms();
    started_steady_ = std::chrono::steady_clock::now();
    snapshot_.state = "running";
    snapshot_.ipv4.running = snapshot_.ipv4.enabled;
    snapshot_.ipv6.running = snapshot_.ipv6.enabled;
  }

  void mark_stopped() {
    std::lock_guard lock{mutex_};
    snapshot_.state = "stopped";
    snapshot_.ipv4.running = false;
    snapshot_.ipv6.running = false;
  }

  void on_family_cycle(sakuin::runtime::AddressFamily family,
                       sakuin::integration::DhtRuntimeCycle cycle) override {
    std::lock_guard lock{mutex_};
    auto &status = family_status(family);
    ++status.cycles;
    status.running = true;
    status.last_cycle_ms = now_ms();
    status.observations_stored += cycle.poll.observations_stored;
    status.metadata_candidates_accepted +=
        cycle.poll.metadata_candidates_accepted;
    status.metadata_attempts_started += cycle.poll.metadata_attempts_started;
    status.metadata_fetches_succeeded += cycle.poll.metadata_fetches_succeeded;
    status.metadata_retryable_failures +=
        cycle.poll.metadata_retryable_failures;
    status.metadata_permanent_failures +=
        cycle.poll.metadata_permanent_failures;
    status.metadata_failure_reasons.io +=
        cycle.poll.metadata_failure_reasons.io;
    status.metadata_failure_reasons.timeout +=
        cycle.poll.metadata_failure_reasons.timeout;
    status.metadata_failure_reasons.storage_unavailable +=
        cycle.poll.metadata_failure_reasons.storage_unavailable;
    status.metadata_failure_reasons.invalid_metadata +=
        cycle.poll.metadata_failure_reasons.invalid_metadata;
    status.metadata_failure_reasons.protocol +=
        cycle.poll.metadata_failure_reasons.protocol;
    status.metadata_failure_reasons.quota +=
        cycle.poll.metadata_failure_reasons.quota;
    status.metadata_failure_reasons.other +=
        cycle.poll.metadata_failure_reasons.other;
    status.metadata_sink_succeeded += cycle.poll.metadata_sink_succeeded;
    status.metadata_sink_failures += cycle.poll.metadata_sink_failures;
    status.routing_probes_accepted += cycle.poll.routing_probes_accepted;
    status.discovery_queries_started += cycle.poll.discovery_queries_started;
    status.peer_discovery_queries_started +=
        cycle.poll.peer_discovery_queries_started;
    status.peer_discovery_responses_received +=
        cycle.poll.peer_discovery_responses_received;
    status.peer_discovery_queries_timed_out +=
        cycle.poll.peer_discovery_queries_timed_out;
    status.peer_discovery_delivery_failures +=
        cycle.poll.peer_discovery_delivery_failures;
    status.peer_discovery_peers_found += cycle.poll.peer_discovery_peers_found;
    status.peer_discovery_succeeded += cycle.poll.peer_discovery_succeeded;
    status.peer_discovery_exhausted += cycle.poll.peer_discovery_exhausted;
    status.metadata_backfill_records_scanned +=
        cycle.poll.metadata_backfill_records_scanned;
    status.metadata_backfill_targets_offered +=
        cycle.poll.metadata_backfill_targets_offered;
    status.metadata_backfill_records_with_metadata +=
        cycle.poll.metadata_backfill_records_with_metadata;
    status.inbound_messages += cycle.poll.inbound_messages;
    status.inbound_queries += cycle.poll.inbound_queries;
    status.inbound_ping_queries += cycle.poll.inbound_ping_queries;
    status.inbound_find_node_queries += cycle.poll.inbound_find_node_queries;
    status.inbound_get_peers_queries += cycle.poll.inbound_get_peers_queries;
    status.inbound_announce_peer_queries +=
        cycle.poll.inbound_announce_peer_queries;
    status.inbound_unknown_queries += cycle.poll.inbound_unknown_queries;
    status.inbound_responses += cycle.poll.inbound_responses;
    status.inbound_protocol_errors += cycle.poll.inbound_protocol_errors;
    if (cycle.poll.last_inbound_query)
      status.last_inbound_query_ms =
          std::chrono::duration_cast<std::chrono::milliseconds>(
              cycle.poll.last_inbound_query->time_since_epoch())
              .count();
    status.queries_expired += cycle.poll.queries_expired;
    status.datagrams_attempted += cycle.dispatch.attempted;
    status.datagrams_accepted += cycle.dispatch.accepted;
    status.datagrams_failed +=
        cycle.dispatch.failed + cycle.poll.datagrams_failed;
    if (cycle.poll.last_datagram_failure &&
        cycle.poll.last_datagram_failure_at) {
      status.last_datagram_failure = cycle.poll.last_datagram_failure->message;
      status.last_datagram_failure_ms =
          std::chrono::duration_cast<std::chrono::milliseconds>(
              cycle.poll.last_datagram_failure_at->time_since_epoch())
              .count();
    }
    status.routing_nodes = cycle.poll.routing_nodes;
    status.outstanding_queries = cycle.poll.outstanding_queries;
    status.discovery_in_flight =
        cycle.poll.discovery ? cycle.poll.discovery->in_flight : 0;
    status.peer_discovery_pending =
        cycle.poll.peer_discovery ? cycle.poll.peer_discovery->pending : 0;
    status.peer_discovery_active =
        cycle.poll.peer_discovery ? cycle.poll.peer_discovery->active : 0;
    status.peer_discovery_in_flight =
        cycle.poll.peer_discovery ? cycle.poll.peer_discovery->in_flight : 0;
    status.metadata_backfill_source_generation =
        cycle.poll.metadata_backfill
            ? cycle.poll.metadata_backfill->source_generation
            : 0;
    status.metadata_backfill_scan_in_progress =
        cycle.poll.metadata_backfill
            ? cycle.poll.metadata_backfill->scan_in_progress
            : false;
    status.metadata_backfill_full_rebuild =
        cycle.poll.metadata_backfill
            ? cycle.poll.metadata_backfill->full_rebuild
            : false;
    status.pending_actions = cycle.poll.pending_actions;
    status.metadata_queued = cycle.poll.metadata_queued;
    status.metadata_in_flight = cycle.poll.metadata_in_flight;
    status.metadata_pending_storage = cycle.poll.metadata_pending_storage;
    status.metadata_backlog = cycle.poll.metadata_backlog;
    if (cycle.poll.bootstrap) {
      status.bootstrap_candidates = cycle.poll.bootstrap->known_candidates;
      status.bootstrap_complete = cycle.poll.bootstrap->complete;
      status.bootstrap_exhausted = cycle.poll.bootstrap->exhausted;
    }
    sakuin::api::note_service_recovery(snapshot_, family_error_source(family),
                                       now_ms());
  }

  void on_family_error(sakuin::runtime::AddressFamily family,
                       sakuin::core::Error error) override {
    {
      std::lock_guard lock{mutex_};
      auto &status = family_status(family);
      ++status.errors;
      status.last_error_ms = now_ms();
      status.last_error = error.message;
      sakuin::api::note_service_error(snapshot_,
                                      std::string{family_error_source(family)},
                                      error.message, *status.last_error_ms);
    }
    spdlog::error("DHT {}: {}", family_name(family), error.message);
  }

  void on_identity_reconfigured(sakuin::runtime::AddressFamily family,
                                const sakuin::dht::krpc::NodeId &,
                                const sakuin::dht::krpc::NodeId &,
                                sakuin::runtime::IpAddress) override {
    spdlog::info(
        "DHT {} identity reconfigured after external-address consensus",
        family_name(family));
  }

  void on_api_error(sakuin::core::Error error) override {
    record_service_error("api", error.message);
    spdlog::error("API: {}", error.message);
  }

  void on_search_index_refreshed(
      sakuin::search::SearchRebuildResult result) override {
    {
      std::lock_guard lock{mutex_};
      snapshot_.search_source_generation = result.source_generation;
      snapshot_.search_records_indexed += result.records_indexed;
      sakuin::api::note_service_recovery(snapshot_, "search", now_ms());
    }
    if (result.records_indexed != 0)
      spdlog::info("Search index advanced to generation {} using {} records",
                   result.source_generation, result.records_indexed);
  }

  void on_search_index_error(sakuin::core::Error error) override {
    record_service_error("search", error.message);
    if (error.code == sakuin::core::ErrorCode::Conflict)
      spdlog::warn("Search-index refresh deferred: {}", error.message);
    else
      spdlog::error("Search-index refresh: {}", error.message);
  }

  void
  on_maintenance_completed(sakuin::service::MaintenanceEvent event) override {
    const auto source =
        maintenance_error_source(event.dataset, event.operation);
    {
      std::lock_guard lock{mutex_};
      ++snapshot_.maintenance_operations;
      sakuin::api::note_service_recovery(snapshot_, source, now_ms());
    }
    const auto dataset = maintenance_dataset_name(event.dataset);
    switch (event.operation) {
    case sakuin::service::MaintenanceOperation::Retention:
      if (event.segments_affected != 0)
        spdlog::info(
            "Storage observations archived {} segments, expired {} segments "
            "and {} records ({} bytes to {} bytes)",
            event.segments_archived, event.segments_expired,
            event.records_expired, event.bytes_before, event.bytes_after);
      break;
    case sakuin::service::MaintenanceOperation::Compaction:
      if (event.segments_affected != 0)
        spdlog::info("Storage {} compacted {} segments ({} bytes to {} bytes)",
                     dataset, event.segments_affected, event.bytes_before,
                     event.bytes_after);
      break;
    case sakuin::service::MaintenanceOperation::Verification:
      spdlog::info("Storage {} verified {} segments and {} records", dataset,
                   event.segments_affected, event.records_checked);
      break;
    case sakuin::service::MaintenanceOperation::GarbageCollection:
      if (event.objects_deleted != 0)
        spdlog::info("Storage {} collected {} objects and reclaimed {} bytes",
                     dataset, event.objects_deleted, event.bytes_reclaimed);
      break;
    }
  }

  void on_maintenance_error(sakuin::service::LocalDataset dataset,
                            sakuin::service::MaintenanceOperation operation,
                            sakuin::core::Error error) override {
    const auto source = maintenance_error_source(dataset, operation);
    {
      std::lock_guard lock{mutex_};
      ++snapshot_.maintenance_errors;
      sakuin::api::note_service_error(snapshot_, source, error.message,
                                      now_ms());
    }
    const auto dataset_name = maintenance_dataset_name(dataset);
    const auto operation_name = maintenance_operation_name(operation);
    if (error.code == sakuin::core::ErrorCode::Conflict)
      spdlog::warn("Storage {} {} deferred: {}", dataset_name, operation_name,
                   error.message);
    else
      spdlog::error("Storage {} {}: {}", dataset_name, operation_name,
                    error.message);
  }

  void on_materialization_completed(
      sakuin::index::IncrementalMaterializationResult result) override {
    {
      std::lock_guard lock{mutex_};
      snapshot_.materialized_observations += result.observations_read;
      snapshot_.materialized_torrent_updates += result.torrents_updated;
      sakuin::api::note_service_recovery(snapshot_, "materialization",
                                         now_ms());
    }
    if (result.observations_read != 0)
      spdlog::info(
          "Materialized {} observations into {} torrent updates; source "
          "generation {}",
          result.observations_read, result.torrents_updated,
          result.checkpoint.source_generation);
  }

  void on_materialization_error(sakuin::core::Error error) override {
    record_service_error("materialization", error.message);
    if (error.code == sakuin::core::ErrorCode::Conflict)
      spdlog::warn("Torrent materialization deferred: {}", error.message);
    else
      spdlog::error("Torrent materialization: {}", error.message);
  }

  void on_duplicate_index_synchronized(
      sakuin::index::DuplicateSynchronizationResult result) override {
    {
      std::lock_guard lock{mutex_};
      snapshot_.duplicate_source_generation = result.source_generation;
      snapshot_.duplicate_records_processed += result.records_processed;
      sakuin::api::note_service_recovery(snapshot_, "duplicates", now_ms());
    }
    if (result.records_processed == 0)
      return;
    spdlog::info("Duplicate index advanced to generation {} using {} records{}",
                 result.source_generation, result.records_processed,
                 result.full_rebuild ? " (full rebuild)" : "");
  }

  void on_duplicate_index_error(sakuin::core::Error error) override {
    record_service_error("duplicates", error.message);
    if (error.code == sakuin::core::ErrorCode::Conflict)
      spdlog::warn("Duplicate-index refresh deferred: {}", error.message);
    else
      spdlog::error("Duplicate-index refresh: {}", error.message);
  }

  void on_distributed_work_error(
      std::optional<sakuin::runtime::StreamSessionId> session,
      sakuin::core::Error error) override {
    record_service_error("distributed", error.message);
    if (session)
      spdlog::warn("Distributed work session {}: {}", *session, error.message);
    else
      spdlog::error("Distributed coordinator: {}", error.message);
  }

private:
  static std::int64_t now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
  }

  sakuin::api::DhtFamilyStatus &
  family_status(sakuin::runtime::AddressFamily family) {
    return family == sakuin::runtime::AddressFamily::IPv4 ? snapshot_.ipv4
                                                          : snapshot_.ipv6;
  }

  void record_service_error(std::string source, std::string message) {
    std::lock_guard lock{mutex_};
    sakuin::api::note_service_error(snapshot_, std::move(source),
                                    std::move(message), now_ms());
  }

  mutable std::mutex mutex_;
  sakuin::api::ServiceStatus snapshot_;
  std::chrono::steady_clock::time_point started_steady_;
};

std::vector<std::pair<std::string, std::string>> environment_values() {
  std::vector<std::pair<std::string, std::string>> result;
  if (!environ)
    return result;
  for (auto entry = environ; *entry; ++entry) {
    const std::string_view value{*entry};
    const auto separator = value.find('=');
    if (separator == std::string_view::npos)
      continue;
    result.emplace_back(value.substr(0, separator),
                        value.substr(separator + 1));
  }
  return result;
}

int fail(std::string_view message, int status = 1) {
  spdlog::error("{}", message);
  return status;
}

int run_admin(const sakuin::config::StorageConfig &configuration,
              sakuin::service::AdminCommand command) {
  using namespace sakuin;
  auto storage = service::LocalCanonicalStorage::open(configuration);
  if (!storage)
    return fail(storage.error().message);
  constexpr std::array datasets{service::LocalDataset::Observations,
                                service::LocalDataset::Torrents};
  for (const auto dataset : datasets) {
    const auto name = dataset == service::LocalDataset::Observations
                          ? "observations"
                          : "torrents";
    if (command == service::AdminCommand::Compact) {
      auto result = (*storage)->compact(dataset);
      if (!result)
        return fail("Storage " + std::string{name} +
                    " compaction: " + result.error().message);
      spdlog::info("Storage {} compacted {} segments into {} ({} to {} bytes)",
                   name, result->segments_removed, result->segments_created,
                   result->bytes_before, result->bytes_after);
    } else if (command == service::AdminCommand::Verify) {
      auto result = (*storage)->verify(dataset);
      if (!result)
        return fail("Storage " + std::string{name} +
                    " verification: " + result.error().message);
      spdlog::info("Storage {} verified {} segments and {} records", name,
                   result->segments_checked, result->records_checked);
    } else {
      auto result = (*storage)->garbage_collect(dataset);
      if (!result)
        return fail("Storage " + std::string{name} +
                    " garbage collection: " + result.error().message);
      spdlog::info("Storage {} collected {} objects and reclaimed {} bytes",
                   name, result->objects_deleted, result->bytes_reclaimed);
    }
  }
  return 0;
}

} // namespace

int main(int argc, char **argv) {
  using namespace sakuin;
  spdlog::set_pattern("%Y-%m-%dT%H:%M:%S.%e%z [%^%l%$] %v");
  std::vector<std::string_view> raw_arguments;
  raw_arguments.reserve(static_cast<std::size_t>(std::max(0, argc - 1)));
  for (int index = 1; index < argc; ++index)
    raw_arguments.emplace_back(argv[index]);
  auto arguments = service::parse_daemon_arguments(raw_arguments);
  if (!arguments)
    return fail(arguments.error().message, 2);
  if (arguments->help) {
    std::cout << usage;
    return 0;
  }
  if (arguments->version) {
    std::cout << "sakuin " << core::version << '\n';
    return 0;
  }

  const auto environment = environment_values();
  std::vector<std::string_view> overrides;
  overrides.reserve(arguments->configuration_overrides.size());
  for (const auto &argument : arguments->configuration_overrides)
    overrides.push_back(argument);
  std::vector<std::filesystem::path> bootstrap_fallback_files;
  if (arguments->configuration_file)
    bootstrap_fallback_files.push_back(
        arguments->configuration_file->parent_path() / "dht-bootstrap.txt");
  std::error_code executable_error;
  std::filesystem::path executable;
#if defined(__linux__)
  executable =
      std::filesystem::read_symlink("/proc/self/exe", executable_error);
#endif
  if (executable_error || executable.empty()) {
    executable_error.clear();
    executable = std::filesystem::weakly_canonical(argv[0], executable_error);
  }
  if (!executable_error)
    bootstrap_fallback_files.push_back(executable.parent_path().parent_path() /
                                       "share" / "sakuin" /
                                       "dht-bootstrap.txt");
  bootstrap_fallback_files.emplace_back("config/dht-bootstrap.txt");
  auto configuration =
      config::load({.toml_file = arguments->configuration_file,
                    .environment = environment,
                    .command_line = overrides,
                    .bootstrap_fallback_files = bootstrap_fallback_files});
  if (!configuration)
    return fail(configuration.error().message, 2);
  if (arguments->admin_command)
    return run_admin(configuration->storage, *arguments->admin_command);

  if (!configuration->network.dht.private_network &&
      configuration->network.dht.bootstrap.empty())
    spdlog::warn(
        "No public DHT bootstrap contacts are configured; a fresh node may "
        "remain isolated until it learns a contact from inbound traffic");

  Observer observer{*configuration};
  if (std::signal(SIGINT, handle_signal) == SIG_ERR ||
      std::signal(SIGTERM, handle_signal) == SIG_ERR ||
      std::signal(SIGHUP, handle_signal) == SIG_ERR)
    return fail("Unable to install process signal handlers");
  if (arguments->worker_mode) {
    auto worker =
        service::RemoteDhtWorkerService::create(*configuration, observer);
    if (!worker)
      return fail(worker.error().message);
    if (auto started = (*worker)->start(); !started)
      return fail(started.error().message);
    if (const auto endpoint =
            (*worker)->local_endpoint(runtime::AddressFamily::IPv4))
      spdlog::info("Remote DHT worker IPv4 listening on UDP port {}",
                   endpoint->port);
    if (const auto endpoint =
            (*worker)->local_endpoint(runtime::AddressFamily::IPv6))
      spdlog::info("Remote DHT worker IPv6 listening on UDP port {}",
                   endpoint->port);
    spdlog::info("Remote DHT worker {} connected",
                 configuration->distributed.worker.id);
    while (pending_signal != SIGINT && pending_signal != SIGTERM) {
      if (pending_signal == SIGHUP)
        pending_signal = 0;
      std::this_thread::sleep_for(std::chrono::milliseconds{100});
    }
    if (auto stopped = (*worker)->stop(); !stopped)
      return fail(stopped.error().message);
    spdlog::info("Remote DHT worker stopped");
    return 0;
  }
  auto service = service::LocalSakuinService::create(
      *configuration, observer, observer, {}, &observer, &observer, &observer,
      &observer, &observer);
  if (!service)
    return fail(service.error().message);
  if (auto started = (*service)->start(); !started)
    return fail(started.error().message);
  observer.mark_started();

  if (const auto endpoint = (*service)->api_endpoint())
    spdlog::info("API listening on port {}", endpoint->port);
  else
    spdlog::info("API disabled");
  if (const auto endpoint = (*service)->coordinator_endpoint())
    spdlog::info("Distributed coordinator listening on port {}{}",
                 endpoint->port,
                 configuration->distributed.coordinator.tls_trust_anchor_file
                     ? " with mutual TLS"
                     : " on loopback");
  else
    spdlog::info("Distributed coordinator listener disabled");
  spdlog::info("Sakuin started");

  while (true) {
    const auto signal = pending_signal;
    if (signal == SIGINT || signal == SIGTERM)
      break;
    if (signal == SIGHUP) {
      pending_signal = 0;
      if (configuration->api.enabled) {
        if (auto reloaded = (*service)->reload_api_credentials(); !reloaded)
          spdlog::error("API credential reload: {}", reloaded.error().message);
        if (auto refreshed = (*service)->refresh_search(); !refreshed)
          spdlog::error("Search index rebuild: {}", refreshed.error().message);
      }
      if (configuration->indexing.duplicates.enabled) {
        if (auto refreshed = (*service)->refresh_duplicates(); !refreshed)
          spdlog::error("Duplicate-index refresh: {}",
                        refreshed.error().message);
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds{100});
  }

  if (auto stopped = (*service)->stop(); !stopped)
    return fail(stopped.error().message);
  observer.mark_stopped();
  spdlog::info("Sakuin stopped");
  return 0;
}

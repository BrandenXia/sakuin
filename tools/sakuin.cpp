#include <csignal>
#include <spdlog/spdlog.h>

import std;

import sakuin.config;
import sakuin.core;
import sakuin.dht.krpc;
import sakuin.integration.dht_worker;
import sakuin.index.materialize;
import sakuin.runtime.datagram;
import sakuin.search.rebuild;
import sakuin.service.api;
import sakuin.service.application;
import sakuin.service.duplicates;
import sakuin.service.local;
import sakuin.service.maintenance;
import sakuin.service.materialization;
import sakuin.service.runtime;
import sakuin.service.storage;

extern char **environ;

namespace {

volatile std::sig_atomic_t pending_signal{};

extern "C" void handle_signal(int signal) { pending_signal = signal; }

constexpr std::string_view usage = R"usage(Usage:
  sakuin [--config PATH] [--key=value ...]
  sakuin admin compact|verify|gc [--config PATH] [--key=value ...]

Configuration precedence: defaults, TOML, SAKUIN_* environment, command line.
Send SIGHUP to reload API credentials and refresh derived indexes.

Before enabling the API for the first time:
  sakuin-api-key --state-dir ./data/operational/api init
  sakuin-api-key --state-dir ./data/operational/api create --id reader --permissions search
)usage";

std::string_view family_name(sakuin::runtime::AddressFamily family) {
  return family == sakuin::runtime::AddressFamily::IPv4 ? "IPv4" : "IPv6";
}

class Observer final : public sakuin::service::DhtRuntimeObserver,
                       public sakuin::service::ApiServiceObserver,
                       public sakuin::service::StorageMaintenanceObserver,
                       public sakuin::service::MaterializationObserver,
                       public sakuin::service::DuplicateIndexObserver {
public:
  void on_family_cycle(sakuin::runtime::AddressFamily,
                       sakuin::integration::DhtRuntimeCycle) override {}

  void on_family_error(sakuin::runtime::AddressFamily family,
                       sakuin::core::Error error) override {
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
    spdlog::error("API: {}", error.message);
  }

  void on_search_index_refreshed(
      sakuin::search::SearchRebuildResult result) override {
    if (result.records_indexed != 0)
      spdlog::info("Search index advanced to generation {} using {} records",
                   result.source_generation, result.records_indexed);
  }

  void
  on_maintenance_completed(sakuin::service::MaintenanceEvent event) override {
    const auto dataset =
        event.dataset == sakuin::service::LocalDataset::Observations
            ? "observations"
            : "torrents";
    switch (event.operation) {
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
    const auto dataset_name =
        dataset == sakuin::service::LocalDataset::Observations ? "observations"
                                                               : "torrents";
    const auto operation_name = [&] {
      switch (operation) {
      case sakuin::service::MaintenanceOperation::Compaction:
        return "compaction";
      case sakuin::service::MaintenanceOperation::Verification:
        return "verification";
      case sakuin::service::MaintenanceOperation::GarbageCollection:
        return "garbage collection";
      }
      std::unreachable();
    }();
    if (error.code == sakuin::core::ErrorCode::Conflict)
      spdlog::warn("Storage {} {} deferred: {}", dataset_name, operation_name,
                   error.message);
    else
      spdlog::error("Storage {} {}: {}", dataset_name, operation_name,
                    error.message);
  }

  void on_materialization_completed(
      sakuin::index::IncrementalMaterializationResult result) override {
    if (result.observations_read != 0)
      spdlog::info(
          "Materialized {} observations into {} torrent updates; source "
          "generation {}",
          result.observations_read, result.torrents_updated,
          result.checkpoint.source_generation);
  }

  void on_materialization_error(sakuin::core::Error error) override {
    if (error.code == sakuin::core::ErrorCode::Conflict)
      spdlog::warn("Torrent materialization deferred: {}", error.message);
    else
      spdlog::error("Torrent materialization: {}", error.message);
  }

  void on_duplicate_index_synchronized(
      sakuin::index::DuplicateSynchronizationResult result) override {
    if (result.records_processed == 0)
      return;
    spdlog::info("Duplicate index advanced to generation {} using {} records{}",
                 result.source_generation, result.records_processed,
                 result.full_rebuild ? " (full rebuild)" : "");
  }

  void on_duplicate_index_error(sakuin::core::Error error) override {
    if (error.code == sakuin::core::ErrorCode::Conflict)
      spdlog::warn("Duplicate-index refresh deferred: {}", error.message);
    else
      spdlog::error("Duplicate-index refresh: {}", error.message);
  }
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

  const auto environment = environment_values();
  std::vector<std::string_view> overrides;
  overrides.reserve(arguments->configuration_overrides.size());
  for (const auto &argument : arguments->configuration_overrides)
    overrides.push_back(argument);
  auto configuration = config::load({.toml_file = arguments->configuration_file,
                                     .environment = environment,
                                     .command_line = overrides});
  if (!configuration)
    return fail(configuration.error().message, 2);
  if (arguments->admin_command)
    return run_admin(configuration->storage, *arguments->admin_command);

  Observer observer;
  if (std::signal(SIGINT, handle_signal) == SIG_ERR ||
      std::signal(SIGTERM, handle_signal) == SIG_ERR ||
      std::signal(SIGHUP, handle_signal) == SIG_ERR)
    return fail("Unable to install process signal handlers");
  auto service = service::LocalSakuinService::create(
      *configuration, observer, observer, {}, &observer, &observer, &observer);
  if (!service)
    return fail(service.error().message);
  if (auto started = (*service)->start(); !started)
    return fail(started.error().message);

  if (const auto endpoint = (*service)->api_endpoint())
    spdlog::info("Sakuin started; API port {}", endpoint->port);
  else
    spdlog::info("Sakuin started; API disabled");

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
  spdlog::info("Sakuin stopped");
  return 0;
}

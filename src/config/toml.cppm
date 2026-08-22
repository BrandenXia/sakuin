module;

#include <toml++/toml.hpp>

export module sakuin.config.toml;

import std;

import sakuin.config.model;
import sakuin.core.result;

export namespace sakuin::config {

struct ConfigLoadRequest {
  std::optional<std::filesystem::path> toml_file;
  std::span<const std::pair<std::string, std::string>> environment;
  std::span<const std::string_view> command_line;
};

core::Result<ConfigOverlay> parse_toml(std::string_view source,
                                       std::string_view source_name = {});
core::Result<ConfigOverlay> environment_overlay(
    std::span<const std::pair<std::string, std::string>> environment);
core::Result<ConfigOverlay>
command_line_overlay(std::span<const std::string_view> arguments);
core::Result<AppConfig> load(const ConfigLoadRequest &request);

} // namespace sakuin::config

namespace sakuin::config {
namespace {

core::Error invalid(std::string message) {
  return {core::ErrorCode::InvalidArgument, std::move(message)};
}

core::Result<void> check_keys(const toml::table &table,
                              std::initializer_list<std::string_view> allowed,
                              std::string_view prefix) {
  for (const auto &[key, _] : table) {
    const auto name = key.str();
    if (std::ranges::find(allowed, name) == allowed.end())
      return std::unexpected(invalid("Unknown configuration key: " +
                                     std::string{prefix} + std::string{name}));
  }
  return {};
}

core::Result<const toml::table *>
optional_table(const toml::table &parent, std::string_view name,
               std::string_view qualified_name) {
  const auto view = parent[name];
  if (!view)
    return static_cast<const toml::table *>(nullptr);
  if (const auto *table = view.as_table())
    return table;
  return std::unexpected(
      invalid(std::string{qualified_name} + " must be a TOML table"));
}

template <typename T>
core::Result<void> scalar(const toml::table &table, std::string_view key,
                          std::string_view path, ConfigOverlay &overlay) {
  const auto view = table[key];
  if (!view)
    return {};
  const auto value = view.value<T>();
  if (!value)
    return std::unexpected(
        invalid(std::string{path} + " has the wrong TOML type"));
  if constexpr (std::same_as<T, std::string>)
    overlay.scalars[std::string{path}] = *value;
  else if constexpr (std::same_as<T, bool>)
    overlay.scalars[std::string{path}] = *value ? "true" : "false";
  else
    overlay.scalars[std::string{path}] = std::to_string(*value);
  return {};
}

core::Result<void> parse_identity(const toml::table &table,
                                  ConfigOverlay &overlay) {
  if (auto checked = check_keys(table,
                                {"mode", "observation_quorum", "vote_window_ms",
                                 "fixed_ipv4_node_id", "fixed_ipv6_node_id"},
                                "network.dht.identity.");
      !checked)
    return checked;
  for (auto result :
       {scalar<std::string>(table, "mode", "network.dht.identity.mode",
                            overlay),
        scalar<std::int64_t>(table, "observation_quorum",
                             "network.dht.identity.observation_quorum",
                             overlay),
        scalar<std::int64_t>(table, "vote_window_ms",
                             "network.dht.identity.vote_window_ms", overlay),
        scalar<std::string>(table, "fixed_ipv4_node_id",
                            "network.dht.identity.fixed_ipv4_node_id", overlay),
        scalar<std::string>(table, "fixed_ipv6_node_id",
                            "network.dht.identity.fixed_ipv6_node_id",
                            overlay)})
    if (!result)
      return result;
  return {};
}

core::Result<void> parse_metadata(const toml::table &table,
                                  ConfigOverlay &overlay) {
  if (auto checked = check_keys(
          table,
          {"enabled", "maximum_in_flight", "maximum_queued",
           "maximum_attempts_per_peer", "initial_retry_delay_ms",
           "maximum_retry_delay_ms", "connect_timeout_ms", "idle_timeout_ms",
           "maximum_metadata_bytes", "maximum_outstanding_requests",
           "maximum_queued_write_bytes", "storage_conflict_attempts",
           "storage_retry_delay_ms"},
          "network.dht.metadata.");
      !checked)
    return checked;
  for (auto result :
       {scalar<bool>(table, "enabled", "network.dht.metadata.enabled", overlay),
        scalar<std::int64_t>(table, "maximum_in_flight",
                             "network.dht.metadata.maximum_in_flight", overlay),
        scalar<std::int64_t>(table, "maximum_queued",
                             "network.dht.metadata.maximum_queued", overlay),
        scalar<std::int64_t>(table, "maximum_attempts_per_peer",
                             "network.dht.metadata.maximum_attempts_per_peer",
                             overlay),
        scalar<std::int64_t>(table, "initial_retry_delay_ms",
                             "network.dht.metadata.initial_retry_delay_ms",
                             overlay),
        scalar<std::int64_t>(table, "maximum_retry_delay_ms",
                             "network.dht.metadata.maximum_retry_delay_ms",
                             overlay),
        scalar<std::int64_t>(table, "connect_timeout_ms",
                             "network.dht.metadata.connect_timeout_ms",
                             overlay),
        scalar<std::int64_t>(table, "idle_timeout_ms",
                             "network.dht.metadata.idle_timeout_ms", overlay),
        scalar<std::int64_t>(table, "maximum_metadata_bytes",
                             "network.dht.metadata.maximum_metadata_bytes",
                             overlay),
        scalar<std::int64_t>(
            table, "maximum_outstanding_requests",
            "network.dht.metadata.maximum_outstanding_requests", overlay),
        scalar<std::int64_t>(table, "maximum_queued_write_bytes",
                             "network.dht.metadata.maximum_queued_write_bytes",
                             overlay),
        scalar<std::int64_t>(table, "storage_conflict_attempts",
                             "network.dht.metadata.storage_conflict_attempts",
                             overlay),
        scalar<std::int64_t>(table, "storage_retry_delay_ms",
                             "network.dht.metadata.storage_retry_delay_ms",
                             overlay)})
    if (!result)
      return result;
  return {};
}

core::Result<void> parse_routing(const toml::table &table,
                                 ConfigOverlay &overlay) {
  if (auto checked = check_keys(table,
                                {"maximum_queued", "maximum_in_flight",
                                 "maximum_attempts", "retry_delay_ms"},
                                "network.dht.routing.");
      !checked)
    return checked;
  for (auto result :
       {scalar<std::int64_t>(table, "maximum_queued",
                             "network.dht.routing.maximum_queued", overlay),
        scalar<std::int64_t>(table, "maximum_in_flight",
                             "network.dht.routing.maximum_in_flight", overlay),
        scalar<std::int64_t>(table, "maximum_attempts",
                             "network.dht.routing.maximum_attempts", overlay),
        scalar<std::int64_t>(table, "retry_delay_ms",
                             "network.dht.routing.retry_delay_ms", overlay)})
    if (!result)
      return result;
  return {};
}

core::Result<void> parse_dht(const toml::table &table, ConfigOverlay &overlay) {
  if (auto checked =
          check_keys(table,
                     {"private_network", "maximum_in_flight",
                      "query_timeout_ms", "bootstrap_maximum_in_flight",
                      "bootstrap_maximum_attempts", "bootstrap_retry_delay_ms",
                      "bootstrap", "identity", "routing", "metadata"},
                     "network.dht.");
      !checked)
    return checked;
  for (auto result :
       {scalar<bool>(table, "private_network", "network.dht.private_network",
                     overlay),
        scalar<std::int64_t>(table, "maximum_in_flight",
                             "network.dht.maximum_in_flight", overlay),
        scalar<std::int64_t>(table, "query_timeout_ms",
                             "network.dht.query_timeout_ms", overlay),
        scalar<std::int64_t>(table, "bootstrap_maximum_in_flight",
                             "network.dht.bootstrap_maximum_in_flight",
                             overlay),
        scalar<std::int64_t>(table, "bootstrap_maximum_attempts",
                             "network.dht.bootstrap_maximum_attempts", overlay),
        scalar<std::int64_t>(table, "bootstrap_retry_delay_ms",
                             "network.dht.bootstrap_retry_delay_ms", overlay)})
    if (!result)
      return result;

  if (const auto bootstrap = table["bootstrap"]) {
    const auto *array = bootstrap.as_array();
    if (!array)
      return std::unexpected(
          invalid("network.dht.bootstrap must be an array of strings"));
    std::vector<std::string> values;
    values.reserve(array->size());
    for (const auto &entry : *array) {
      const auto value = entry.value<std::string>();
      if (!value)
        return std::unexpected(
            invalid("network.dht.bootstrap must contain only strings"));
      values.push_back(*value);
    }
    overlay.bootstrap = std::move(values);
  }
  auto identity = optional_table(table, "identity", "network.dht.identity");
  if (!identity)
    return std::unexpected(identity.error());
  if (*identity)
    if (auto result = parse_identity(**identity, overlay); !result)
      return result;
  auto routing = optional_table(table, "routing", "network.dht.routing");
  if (!routing)
    return std::unexpected(routing.error());
  if (*routing)
    if (auto result = parse_routing(**routing, overlay); !result)
      return result;
  auto metadata = optional_table(table, "metadata", "network.dht.metadata");
  if (!metadata)
    return std::unexpected(metadata.error());
  return *metadata ? parse_metadata(**metadata, overlay) : core::Result<void>{};
}

core::Result<void> parse_traffic(const toml::table &table,
                                 ConfigOverlay &overlay) {
  if (auto checked =
          check_keys(table, {"window_ms", "inbound_bytes", "outbound_bytes"},
                     "network.traffic.");
      !checked)
    return checked;
  for (auto result :
       {scalar<std::int64_t>(table, "window_ms", "network.traffic.window_ms",
                             overlay),
        scalar<std::int64_t>(table, "inbound_bytes",
                             "network.traffic.inbound_bytes", overlay),
        scalar<std::int64_t>(table, "outbound_bytes",
                             "network.traffic.outbound_bytes", overlay)})
    if (!result)
      return result;
  return {};
}

core::Result<void> parse_network(const toml::table &table,
                                 ConfigOverlay &overlay) {
  if (auto checked = check_keys(
          table,
          {"enable_ipv4", "enable_ipv6", "listen_port", "dht", "traffic"},
          "network.");
      !checked)
    return checked;
  for (auto result :
       {scalar<bool>(table, "enable_ipv4", "network.enable_ipv4", overlay),
        scalar<bool>(table, "enable_ipv6", "network.enable_ipv6", overlay),
        scalar<std::int64_t>(table, "listen_port", "network.listen_port",
                             overlay)})
    if (!result)
      return result;
  auto dht = optional_table(table, "dht", "network.dht");
  if (!dht)
    return std::unexpected(dht.error());
  if (*dht)
    if (auto result = parse_dht(**dht, overlay); !result)
      return result;
  auto traffic = optional_table(table, "traffic", "network.traffic");
  if (!traffic)
    return std::unexpected(traffic.error());
  return *traffic ? parse_traffic(**traffic, overlay) : core::Result<void>{};
}

core::Result<void> parse_storage(const toml::table &table,
                                 ConfigOverlay &overlay) {
  if (auto checked =
          check_keys(table,
                     {"backend", "local_root", "block_target_bytes",
                      "segment_target_bytes", "compression", "compaction",
                      "maintenance", "materialization"},
                     "storage.");
      !checked)
    return checked;
  for (auto result :
       {scalar<std::string>(table, "backend", "storage.backend", overlay),
        scalar<std::string>(table, "local_root", "storage.local_root", overlay),
        scalar<std::int64_t>(table, "block_target_bytes",
                             "storage.block_target_bytes", overlay),
        scalar<std::int64_t>(table, "segment_target_bytes",
                             "storage.segment_target_bytes", overlay)})
    if (!result)
      return result;
  auto compression =
      optional_table(table, "compression", "storage.compression");
  if (!compression)
    return std::unexpected(compression.error());
  if (*compression) {
    if (auto checked = check_keys(**compression, {"codec", "level"},
                                  "storage.compression.");
        !checked)
      return checked;
    for (auto result :
         {scalar<std::string>(**compression, "codec",
                              "storage.compression.codec", overlay),
          scalar<std::int64_t>(**compression, "level",
                               "storage.compression.level", overlay)})
      if (!result)
        return result;
  }
  auto compaction = optional_table(table, "compaction", "storage.compaction");
  if (!compaction)
    return std::unexpected(compaction.error());
  if (*compaction) {
    if (auto checked = check_keys(**compaction,
                                  {"minimum_segments", "maximum_warm_segments",
                                   "warm_block_target_bytes"},
                                  "storage.compaction.");
        !checked)
      return checked;
    for (auto parsed :
         {scalar<std::int64_t>(**compaction, "minimum_segments",
                               "storage.compaction.minimum_segments", overlay),
          scalar<std::int64_t>(**compaction, "maximum_warm_segments",
                               "storage.compaction.maximum_warm_segments",
                               overlay),
          scalar<std::int64_t>(**compaction, "warm_block_target_bytes",
                               "storage.compaction.warm_block_target_bytes",
                               overlay)})
      if (!parsed)
        return parsed;
  }
  auto maintenance =
      optional_table(table, "maintenance", "storage.maintenance");
  if (!maintenance)
    return std::unexpected(maintenance.error());
  if (*maintenance) {
    if (auto checked =
            check_keys(**maintenance,
                       {"enabled", "interval_ms", "verification_interval_ms"},
                       "storage.maintenance.");
        !checked)
      return checked;
    for (auto result :
         {scalar<bool>(**maintenance, "enabled", "storage.maintenance.enabled",
                       overlay),
          scalar<std::int64_t>(**maintenance, "interval_ms",
                               "storage.maintenance.interval_ms", overlay),
          scalar<std::int64_t>(**maintenance, "verification_interval_ms",
                               "storage.maintenance.verification_interval_ms",
                               overlay)})
      if (!result)
        return result;
  }
  auto materialization =
      optional_table(table, "materialization", "storage.materialization");
  if (!materialization)
    return std::unexpected(materialization.error());
  if (!*materialization)
    return {};
  if (auto checked = check_keys(**materialization, {"enabled", "interval_ms"},
                                "storage.materialization.");
      !checked)
    return checked;
  for (auto result :
       {scalar<bool>(**materialization, "enabled",
                     "storage.materialization.enabled", overlay),
        scalar<std::int64_t>(**materialization, "interval_ms",
                             "storage.materialization.interval_ms", overlay)})
    if (!result)
      return result;
  return {};
}

core::Result<void> parse_api(const toml::table &table, ConfigOverlay &overlay) {
  if (auto checked = check_keys(
          table,
          {"enabled", "credential_store_directory", "listen_address",
           "listen_port", "maximum_connections", "read_buffer_bytes",
           "request_timeout_ms", "maximum_target_bytes", "maximum_header_bytes",
           "maximum_header_count", "maximum_body_bytes",
           "tls_certificate_chain_file", "tls_private_key_file", "rate_limit"},
          "api.");
      !checked)
    return checked;
  for (auto result :
       {scalar<bool>(table, "enabled", "api.enabled", overlay),
        scalar<std::string>(table, "credential_store_directory",
                            "api.credential_store_directory", overlay),
        scalar<std::string>(table, "listen_address", "api.listen_address",
                            overlay),
        scalar<std::int64_t>(table, "listen_port", "api.listen_port", overlay),
        scalar<std::int64_t>(table, "maximum_connections",
                             "api.maximum_connections", overlay),
        scalar<std::int64_t>(table, "read_buffer_bytes",
                             "api.read_buffer_bytes", overlay),
        scalar<std::int64_t>(table, "request_timeout_ms",
                             "api.request_timeout_ms", overlay),
        scalar<std::int64_t>(table, "maximum_target_bytes",
                             "api.maximum_target_bytes", overlay),
        scalar<std::int64_t>(table, "maximum_header_bytes",
                             "api.maximum_header_bytes", overlay),
        scalar<std::int64_t>(table, "maximum_header_count",
                             "api.maximum_header_count", overlay),
        scalar<std::int64_t>(table, "maximum_body_bytes",
                             "api.maximum_body_bytes", overlay),
        scalar<std::string>(table, "tls_certificate_chain_file",
                            "api.tls_certificate_chain_file", overlay),
        scalar<std::string>(table, "tls_private_key_file",
                            "api.tls_private_key_file", overlay)})
    if (!result)
      return result;
  auto rate_limit = optional_table(table, "rate_limit", "api.rate_limit");
  if (!rate_limit)
    return std::unexpected(rate_limit.error());
  if (*rate_limit) {
    if (auto checked = check_keys(
            **rate_limit, {"enabled", "requests_per_window", "window_ms"},
            "api.rate_limit.");
        !checked)
      return checked;
    for (auto result :
         {scalar<bool>(**rate_limit, "enabled", "api.rate_limit.enabled",
                       overlay),
          scalar<std::int64_t>(**rate_limit, "requests_per_window",
                               "api.rate_limit.requests_per_window", overlay),
          scalar<std::int64_t>(**rate_limit, "window_ms",
                               "api.rate_limit.window_ms", overlay)})
      if (!result)
        return result;
  }
  return {};
}

core::Result<void> parse_indexing(const toml::table &table,
                                  ConfigOverlay &overlay) {
  if (auto checked = check_keys(table, {"duplicates"}, "indexing."); !checked)
    return checked;
  auto duplicates = optional_table(table, "duplicates", "indexing.duplicates");
  if (!duplicates)
    return std::unexpected(duplicates.error());
  if (!*duplicates)
    return {};
  if (auto checked = check_keys(**duplicates, {"enabled", "interval_ms"},
                                "indexing.duplicates.");
      !checked)
    return checked;
  for (auto result :
       {scalar<bool>(**duplicates, "enabled", "indexing.duplicates.enabled",
                     overlay),
        scalar<std::int64_t>(**duplicates, "interval_ms",
                             "indexing.duplicates.interval_ms", overlay)})
    if (!result)
      return result;
  return {};
}

core::Result<void> parse_distributed_coordinator(const toml::table &table,
                                                 ConfigOverlay &overlay) {
  if (auto checked = check_keys(
          table,
          {"enabled", "listen_address", "listen_port", "maximum_connections",
           "read_buffer_bytes", "maximum_frame_bytes",
           "maximum_queued_write_bytes", "idle_timeout_ms"},
          "distributed.coordinator.");
      !checked)
    return checked;
  for (auto result :
       {scalar<bool>(table, "enabled", "distributed.coordinator.enabled",
                     overlay),
        scalar<std::string>(table, "listen_address",
                            "distributed.coordinator.listen_address", overlay),
        scalar<std::int64_t>(table, "listen_port",
                             "distributed.coordinator.listen_port", overlay),
        scalar<std::int64_t>(table, "maximum_connections",
                             "distributed.coordinator.maximum_connections",
                             overlay),
        scalar<std::int64_t>(table, "read_buffer_bytes",
                             "distributed.coordinator.read_buffer_bytes",
                             overlay),
        scalar<std::int64_t>(table, "maximum_frame_bytes",
                             "distributed.coordinator.maximum_frame_bytes",
                             overlay),
        scalar<std::int64_t>(
            table, "maximum_queued_write_bytes",
            "distributed.coordinator.maximum_queued_write_bytes", overlay),
        scalar<std::int64_t>(table, "idle_timeout_ms",
                             "distributed.coordinator.idle_timeout_ms",
                             overlay)})
    if (!result)
      return result;
  return {};
}

core::Result<void> parse_distributed(const toml::table &table,
                                     ConfigOverlay &overlay) {
  if (auto checked = check_keys(table,
                                {"maximum_work_items", "maximum_payload_bytes",
                                 "worker_timeout_ms", "lease_duration_ms",
                                 "heartbeat_interval_ms", "coordinator"},
                                "distributed.");
      !checked)
    return checked;
  for (auto result :
       {scalar<std::int64_t>(table, "maximum_work_items",
                             "distributed.maximum_work_items", overlay),
        scalar<std::int64_t>(table, "maximum_payload_bytes",
                             "distributed.maximum_payload_bytes", overlay),
        scalar<std::int64_t>(table, "worker_timeout_ms",
                             "distributed.worker_timeout_ms", overlay),
        scalar<std::int64_t>(table, "lease_duration_ms",
                             "distributed.lease_duration_ms", overlay),
        scalar<std::int64_t>(table, "heartbeat_interval_ms",
                             "distributed.heartbeat_interval_ms", overlay)})
    if (!result)
      return result;
  auto coordinator =
      optional_table(table, "coordinator", "distributed.coordinator");
  if (!coordinator)
    return std::unexpected(coordinator.error());
  if (*coordinator)
    return parse_distributed_coordinator(**coordinator, overlay);
  return {};
}

std::vector<std::string> split_list(std::string_view input) {
  std::vector<std::string> result;
  while (!input.empty()) {
    const auto separator = input.find(',');
    auto entry = input.substr(0, separator);
    result.emplace_back(entry);
    if (separator == std::string_view::npos)
      break;
    input.remove_prefix(separator + 1);
  }
  return result;
}

} // namespace

core::Result<ConfigOverlay> parse_toml(std::string_view source,
                                       std::string_view source_name) {
  try {
    const auto document = toml::parse(source, source_name);
    if (auto checked = check_keys(
            document, {"network", "storage", "indexing", "api", "distributed"},
            "");
        !checked)
      return std::unexpected(checked.error());
    ConfigOverlay overlay;
    auto network = optional_table(document, "network", "network");
    if (!network)
      return std::unexpected(network.error());
    if (*network)
      if (auto result = parse_network(**network, overlay); !result)
        return std::unexpected(result.error());
    auto storage = optional_table(document, "storage", "storage");
    if (!storage)
      return std::unexpected(storage.error());
    if (*storage)
      if (auto result = parse_storage(**storage, overlay); !result)
        return std::unexpected(result.error());
    auto indexing = optional_table(document, "indexing", "indexing");
    if (!indexing)
      return std::unexpected(indexing.error());
    if (*indexing)
      if (auto result = parse_indexing(**indexing, overlay); !result)
        return std::unexpected(result.error());
    auto api = optional_table(document, "api", "api");
    if (!api)
      return std::unexpected(api.error());
    if (*api)
      if (auto result = parse_api(**api, overlay); !result)
        return std::unexpected(result.error());
    auto distributed = optional_table(document, "distributed", "distributed");
    if (!distributed)
      return std::unexpected(distributed.error());
    if (*distributed)
      if (auto result = parse_distributed(**distributed, overlay); !result)
        return std::unexpected(result.error());
    return overlay;
  } catch (const toml::parse_error &error) {
    return std::unexpected(
        invalid("Invalid TOML: " + std::string{error.description()}));
  }
}

core::Result<ConfigOverlay> environment_overlay(
    std::span<const std::pair<std::string, std::string>> environment) {
  static const std::map<std::string_view, std::string_view> names{
      {"SAKUIN_NETWORK_ENABLE_IPV4", "network.enable_ipv4"},
      {"SAKUIN_NETWORK_ENABLE_IPV6", "network.enable_ipv6"},
      {"SAKUIN_NETWORK_LISTEN_PORT", "network.listen_port"},
      {"SAKUIN_DHT_MAXIMUM_IN_FLIGHT", "network.dht.maximum_in_flight"},
      {"SAKUIN_DHT_PRIVATE_NETWORK", "network.dht.private_network"},
      {"SAKUIN_DHT_QUERY_TIMEOUT_MS", "network.dht.query_timeout_ms"},
      {"SAKUIN_DHT_BOOTSTRAP_MAXIMUM_IN_FLIGHT",
       "network.dht.bootstrap_maximum_in_flight"},
      {"SAKUIN_DHT_BOOTSTRAP_MAXIMUM_ATTEMPTS",
       "network.dht.bootstrap_maximum_attempts"},
      {"SAKUIN_DHT_BOOTSTRAP_RETRY_DELAY_MS",
       "network.dht.bootstrap_retry_delay_ms"},
      {"SAKUIN_DHT_ROUTING_MAXIMUM_QUEUED",
       "network.dht.routing.maximum_queued"},
      {"SAKUIN_DHT_ROUTING_MAXIMUM_IN_FLIGHT",
       "network.dht.routing.maximum_in_flight"},
      {"SAKUIN_DHT_ROUTING_MAXIMUM_ATTEMPTS",
       "network.dht.routing.maximum_attempts"},
      {"SAKUIN_DHT_ROUTING_RETRY_DELAY_MS",
       "network.dht.routing.retry_delay_ms"},
      {"SAKUIN_DHT_IDENTITY_MODE", "network.dht.identity.mode"},
      {"SAKUIN_DHT_IDENTITY_QUORUM", "network.dht.identity.observation_quorum"},
      {"SAKUIN_DHT_IDENTITY_VOTE_WINDOW_MS",
       "network.dht.identity.vote_window_ms"},
      {"SAKUIN_DHT_IDENTITY_FIXED_IPV4_NODE_ID",
       "network.dht.identity.fixed_ipv4_node_id"},
      {"SAKUIN_DHT_IDENTITY_FIXED_IPV6_NODE_ID",
       "network.dht.identity.fixed_ipv6_node_id"},
      {"SAKUIN_DHT_METADATA_ENABLED", "network.dht.metadata.enabled"},
      {"SAKUIN_DHT_METADATA_MAXIMUM_IN_FLIGHT",
       "network.dht.metadata.maximum_in_flight"},
      {"SAKUIN_DHT_METADATA_MAXIMUM_QUEUED",
       "network.dht.metadata.maximum_queued"},
      {"SAKUIN_DHT_METADATA_MAXIMUM_ATTEMPTS_PER_PEER",
       "network.dht.metadata.maximum_attempts_per_peer"},
      {"SAKUIN_DHT_METADATA_INITIAL_RETRY_DELAY_MS",
       "network.dht.metadata.initial_retry_delay_ms"},
      {"SAKUIN_DHT_METADATA_MAXIMUM_RETRY_DELAY_MS",
       "network.dht.metadata.maximum_retry_delay_ms"},
      {"SAKUIN_DHT_METADATA_CONNECT_TIMEOUT_MS",
       "network.dht.metadata.connect_timeout_ms"},
      {"SAKUIN_DHT_METADATA_IDLE_TIMEOUT_MS",
       "network.dht.metadata.idle_timeout_ms"},
      {"SAKUIN_DHT_METADATA_MAXIMUM_METADATA_BYTES",
       "network.dht.metadata.maximum_metadata_bytes"},
      {"SAKUIN_DHT_METADATA_MAXIMUM_OUTSTANDING_REQUESTS",
       "network.dht.metadata.maximum_outstanding_requests"},
      {"SAKUIN_DHT_METADATA_MAXIMUM_QUEUED_WRITE_BYTES",
       "network.dht.metadata.maximum_queued_write_bytes"},
      {"SAKUIN_DHT_METADATA_STORAGE_CONFLICT_ATTEMPTS",
       "network.dht.metadata.storage_conflict_attempts"},
      {"SAKUIN_DHT_METADATA_STORAGE_RETRY_DELAY_MS",
       "network.dht.metadata.storage_retry_delay_ms"},
      {"SAKUIN_TRAFFIC_WINDOW_MS", "network.traffic.window_ms"},
      {"SAKUIN_TRAFFIC_INBOUND_BYTES", "network.traffic.inbound_bytes"},
      {"SAKUIN_TRAFFIC_OUTBOUND_BYTES", "network.traffic.outbound_bytes"},
      {"SAKUIN_STORAGE_BACKEND", "storage.backend"},
      {"SAKUIN_STORAGE_LOCAL_ROOT", "storage.local_root"},
      {"SAKUIN_STORAGE_BLOCK_TARGET_BYTES", "storage.block_target_bytes"},
      {"SAKUIN_STORAGE_SEGMENT_TARGET_BYTES", "storage.segment_target_bytes"},
      {"SAKUIN_STORAGE_COMPRESSION", "storage.compression.codec"},
      {"SAKUIN_STORAGE_COMPRESSION_LEVEL", "storage.compression.level"},
      {"SAKUIN_STORAGE_COMPACTION_MINIMUM_SEGMENTS",
       "storage.compaction.minimum_segments"},
      {"SAKUIN_STORAGE_COMPACTION_MAXIMUM_WARM_SEGMENTS",
       "storage.compaction.maximum_warm_segments"},
      {"SAKUIN_STORAGE_COMPACTION_WARM_BLOCK_TARGET_BYTES",
       "storage.compaction.warm_block_target_bytes"},
      {"SAKUIN_STORAGE_MAINTENANCE_ENABLED", "storage.maintenance.enabled"},
      {"SAKUIN_STORAGE_MAINTENANCE_INTERVAL_MS",
       "storage.maintenance.interval_ms"},
      {"SAKUIN_STORAGE_MAINTENANCE_VERIFICATION_INTERVAL_MS",
       "storage.maintenance.verification_interval_ms"},
      {"SAKUIN_STORAGE_MATERIALIZATION_ENABLED",
       "storage.materialization.enabled"},
      {"SAKUIN_STORAGE_MATERIALIZATION_INTERVAL_MS",
       "storage.materialization.interval_ms"},
      {"SAKUIN_DUPLICATE_INDEX_ENABLED", "indexing.duplicates.enabled"},
      {"SAKUIN_DUPLICATE_INDEX_INTERVAL_MS", "indexing.duplicates.interval_ms"},
      {"SAKUIN_API_ENABLED", "api.enabled"},
      {"SAKUIN_API_CREDENTIAL_STORE_DIRECTORY",
       "api.credential_store_directory"},
      {"SAKUIN_API_LISTEN_ADDRESS", "api.listen_address"},
      {"SAKUIN_API_LISTEN_PORT", "api.listen_port"},
      {"SAKUIN_API_MAXIMUM_CONNECTIONS", "api.maximum_connections"},
      {"SAKUIN_API_READ_BUFFER_BYTES", "api.read_buffer_bytes"},
      {"SAKUIN_API_REQUEST_TIMEOUT_MS", "api.request_timeout_ms"},
      {"SAKUIN_API_MAXIMUM_TARGET_BYTES", "api.maximum_target_bytes"},
      {"SAKUIN_API_MAXIMUM_HEADER_BYTES", "api.maximum_header_bytes"},
      {"SAKUIN_API_MAXIMUM_HEADER_COUNT", "api.maximum_header_count"},
      {"SAKUIN_API_MAXIMUM_BODY_BYTES", "api.maximum_body_bytes"},
      {"SAKUIN_API_TLS_CERTIFICATE_CHAIN_FILE",
       "api.tls_certificate_chain_file"},
      {"SAKUIN_API_TLS_PRIVATE_KEY_FILE", "api.tls_private_key_file"},
      {"SAKUIN_API_RATE_LIMIT_ENABLED", "api.rate_limit.enabled"},
      {"SAKUIN_API_RATE_LIMIT_REQUESTS_PER_WINDOW",
       "api.rate_limit.requests_per_window"},
      {"SAKUIN_API_RATE_LIMIT_WINDOW_MS", "api.rate_limit.window_ms"},
      {"SAKUIN_DISTRIBUTED_MAXIMUM_WORK_ITEMS",
       "distributed.maximum_work_items"},
      {"SAKUIN_DISTRIBUTED_MAXIMUM_PAYLOAD_BYTES",
       "distributed.maximum_payload_bytes"},
      {"SAKUIN_DISTRIBUTED_WORKER_TIMEOUT_MS", "distributed.worker_timeout_ms"},
      {"SAKUIN_DISTRIBUTED_LEASE_DURATION_MS", "distributed.lease_duration_ms"},
      {"SAKUIN_DISTRIBUTED_HEARTBEAT_INTERVAL_MS",
       "distributed.heartbeat_interval_ms"},
      {"SAKUIN_DISTRIBUTED_COORDINATOR_ENABLED",
       "distributed.coordinator.enabled"},
      {"SAKUIN_DISTRIBUTED_COORDINATOR_LISTEN_ADDRESS",
       "distributed.coordinator.listen_address"},
      {"SAKUIN_DISTRIBUTED_COORDINATOR_LISTEN_PORT",
       "distributed.coordinator.listen_port"},
      {"SAKUIN_DISTRIBUTED_COORDINATOR_MAXIMUM_CONNECTIONS",
       "distributed.coordinator.maximum_connections"},
      {"SAKUIN_DISTRIBUTED_COORDINATOR_READ_BUFFER_BYTES",
       "distributed.coordinator.read_buffer_bytes"},
      {"SAKUIN_DISTRIBUTED_COORDINATOR_MAXIMUM_FRAME_BYTES",
       "distributed.coordinator.maximum_frame_bytes"},
      {"SAKUIN_DISTRIBUTED_COORDINATOR_MAXIMUM_QUEUED_WRITE_BYTES",
       "distributed.coordinator.maximum_queued_write_bytes"},
      {"SAKUIN_DISTRIBUTED_COORDINATOR_IDLE_TIMEOUT_MS",
       "distributed.coordinator.idle_timeout_ms"},
  };
  ConfigOverlay overlay;
  for (const auto &[name, value] : environment) {
    if (name == "SAKUIN_DHT_BOOTSTRAP") {
      overlay.bootstrap = split_list(value);
      continue;
    }
    const auto found = names.find(name);
    if (found != names.end()) {
      overlay.scalars[std::string{found->second}] = value;
    } else if (name.starts_with("SAKUIN_")) {
      return std::unexpected(
          invalid("Unknown Sakuin environment variable: " + name));
    }
  }
  return overlay;
}

core::Result<ConfigOverlay>
command_line_overlay(std::span<const std::string_view> arguments) {
  ConfigOverlay overlay;
  for (const auto argument : arguments) {
    if (!argument.starts_with("--"))
      return std::unexpected(
          invalid("Configuration argument must begin with --"));
    const auto assignment = argument.substr(2);
    const auto separator = assignment.find('=');
    if (separator == std::string_view::npos || separator == 0)
      return std::unexpected(
          invalid("Configuration argument must use --key=value"));
    const auto name = assignment.substr(0, separator);
    const auto value = assignment.substr(separator + 1);
    if (name == "network.dht.bootstrap")
      overlay.bootstrap = split_list(value);
    else
      overlay.scalars[std::string{name}] = value;
  }
  return overlay;
}

core::Result<AppConfig> load(const ConfigLoadRequest &request) {
  AppConfig result = defaults();
  if (request.toml_file) {
    std::ifstream input{*request.toml_file, std::ios::binary};
    if (!input)
      return std::unexpected(core::Error{core::ErrorCode::IoError,
                                         "Unable to open configuration file: " +
                                             request.toml_file->string()});
    std::string contents{std::istreambuf_iterator<char>{input}, {}};
    auto overlay = parse_toml(contents, request.toml_file->string());
    if (!overlay)
      return std::unexpected(overlay.error());
    if (auto applied = apply(result, *overlay); !applied)
      return std::unexpected(applied.error());
  }
  auto environment = environment_overlay(request.environment);
  if (!environment)
    return std::unexpected(environment.error());
  if (auto applied = apply(result, *environment); !applied)
    return std::unexpected(applied.error());
  auto command_line = command_line_overlay(request.command_line);
  if (!command_line)
    return std::unexpected(command_line.error());
  if (auto applied = apply(result, *command_line); !applied)
    return std::unexpected(applied.error());
  if (auto valid = validate(result); !valid)
    return std::unexpected(valid.error());
  return result;
}

} // namespace sakuin::config

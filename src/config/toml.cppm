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
  std::span<const std::filesystem::path> bootstrap_fallback_files;
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
           "storage_retry_delay_ms", "discovery"},
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
  auto discovery =
      optional_table(table, "discovery", "network.dht.metadata.discovery");
  if (!discovery)
    return std::unexpected(discovery.error());
  if (!*discovery)
    return {};
  if (auto checked =
          check_keys(**discovery,
                     {"enabled", "maximum_pending", "maximum_in_flight",
                      "parallelism_per_hash", "maximum_queries_per_hash",
                      "retry_delay_ms", "backfill"},
                     "network.dht.metadata.discovery.");
      !checked)
    return checked;
  for (auto result :
       {scalar<bool>(**discovery, "enabled",
                     "network.dht.metadata.discovery.enabled", overlay),
        scalar<std::int64_t>(**discovery, "maximum_pending",
                             "network.dht.metadata.discovery.maximum_pending",
                             overlay),
        scalar<std::int64_t>(**discovery, "maximum_in_flight",
                             "network.dht.metadata.discovery.maximum_in_flight",
                             overlay),
        scalar<std::int64_t>(
            **discovery, "parallelism_per_hash",
            "network.dht.metadata.discovery.parallelism_per_hash", overlay),
        scalar<std::int64_t>(
            **discovery, "maximum_queries_per_hash",
            "network.dht.metadata.discovery.maximum_queries_per_hash", overlay),
        scalar<std::int64_t>(**discovery, "retry_delay_ms",
                             "network.dht.metadata.discovery.retry_delay_ms",
                             overlay)})
    if (!result)
      return result;
  auto backfill = optional_table(**discovery, "backfill",
                                 "network.dht.metadata.discovery.backfill");
  if (!backfill)
    return std::unexpected(backfill.error());
  if (!*backfill)
    return {};
  if (auto checked = check_keys(**backfill,
                                {"enabled", "maximum_records_per_poll",
                                 "refresh_interval_ms",
                                 "full_rescan_interval_ms", "retry_delay_ms"},
                                "network.dht.metadata.discovery.backfill.");
      !checked)
    return checked;
  for (auto result :
       {scalar<bool>(**backfill, "enabled",
                     "network.dht.metadata.discovery.backfill.enabled",
                     overlay),
        scalar<std::int64_t>(
            **backfill, "maximum_records_per_poll",
            "network.dht.metadata.discovery.backfill.maximum_records_per_poll",
            overlay),
        scalar<std::int64_t>(
            **backfill, "refresh_interval_ms",
            "network.dht.metadata.discovery.backfill.refresh_interval_ms",
            overlay),
        scalar<std::int64_t>(
            **backfill, "full_rescan_interval_ms",
            "network.dht.metadata.discovery.backfill.full_rescan_interval_ms",
            overlay),
        scalar<std::int64_t>(
            **backfill, "retry_delay_ms",
            "network.dht.metadata.discovery.backfill.retry_delay_ms", overlay)})
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
  if (auto checked = check_keys(
          table,
          {"private_network", "maximum_in_flight", "query_timeout_ms",
           "bootstrap_maximum_in_flight", "bootstrap_maximum_attempts",
           "bootstrap_retry_delay_ms", "bootstrap", "bootstrap_file",
           "identity", "routing", "metadata"},
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
                             "network.dht.bootstrap_retry_delay_ms", overlay),
        scalar<std::string>(table, "bootstrap_file",
                            "network.dht.bootstrap_file", overlay)})
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
  if (auto checked = check_keys(
          table,
          {"window_ms", "inbound_bytes", "outbound_bytes", "grant_bytes"},
          "network.traffic.");
      !checked)
    return checked;
  for (auto result :
       {scalar<std::int64_t>(table, "window_ms", "network.traffic.window_ms",
                             overlay),
        scalar<std::int64_t>(table, "inbound_bytes",
                             "network.traffic.inbound_bytes", overlay),
        scalar<std::int64_t>(table, "outbound_bytes",
                             "network.traffic.outbound_bytes", overlay),
        scalar<std::int64_t>(table, "grant_bytes",
                             "network.traffic.grant_bytes", overlay)})
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
                      "retention", "maintenance", "materialization", "s3"},
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
  auto s3 = optional_table(table, "s3", "storage.s3");
  if (!s3)
    return std::unexpected(s3.error());
  if (*s3) {
    if (auto checked =
            check_keys(**s3,
                       {"endpoint", "bucket", "region", "prefix",
                        "connect_timeout_ms", "request_timeout_ms",
                        "maximum_attempts", "retry_delay_ms", "verify_tls"},
                       "storage.s3.");
        !checked)
      return checked;
    for (auto result :
         {scalar<std::string>(**s3, "endpoint", "storage.s3.endpoint", overlay),
          scalar<std::string>(**s3, "bucket", "storage.s3.bucket", overlay),
          scalar<std::string>(**s3, "region", "storage.s3.region", overlay),
          scalar<std::string>(**s3, "prefix", "storage.s3.prefix", overlay),
          scalar<std::int64_t>(**s3, "connect_timeout_ms",
                               "storage.s3.connect_timeout_ms", overlay),
          scalar<std::int64_t>(**s3, "request_timeout_ms",
                               "storage.s3.request_timeout_ms", overlay),
          scalar<std::int64_t>(**s3, "maximum_attempts",
                               "storage.s3.maximum_attempts", overlay),
          scalar<std::int64_t>(**s3, "retry_delay_ms",
                               "storage.s3.retry_delay_ms", overlay),
          scalar<bool>(**s3, "verify_tls", "storage.s3.verify_tls", overlay)})
      if (!result)
        return result;
  }
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
  auto retention = optional_table(table, "retention", "storage.retention");
  if (!retention)
    return std::unexpected(retention.error());
  if (*retention) {
    if (auto checked =
            check_keys(**retention,
                       {"enabled", "observation_cold_age_ms",
                        "observation_max_age_ms", "cold_block_target_bytes",
                        "cold_compression_codec", "cold_compression_level"},
                       "storage.retention.");
        !checked)
      return checked;
    for (auto parsed :
         {scalar<bool>(**retention, "enabled", "storage.retention.enabled",
                       overlay),
          scalar<std::int64_t>(**retention, "observation_cold_age_ms",
                               "storage.retention.observation_cold_age_ms",
                               overlay),
          scalar<std::int64_t>(**retention, "observation_max_age_ms",
                               "storage.retention.observation_max_age_ms",
                               overlay),
          scalar<std::int64_t>(**retention, "cold_block_target_bytes",
                               "storage.retention.cold_block_target_bytes",
                               overlay),
          scalar<std::string>(**retention, "cold_compression_codec",
                              "storage.retention.cold_compression_codec",
                              overlay),
          scalar<std::int64_t>(**retention, "cold_compression_level",
                               "storage.retention.cold_compression_level",
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
  if (auto checked =
          check_keys(table, {"duplicates", "classification"}, "indexing.");
      !checked)
    return checked;
  auto duplicates = optional_table(table, "duplicates", "indexing.duplicates");
  if (!duplicates)
    return std::unexpected(duplicates.error());
  if (*duplicates) {
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
  }
  auto classification =
      optional_table(table, "classification", "indexing.classification");
  if (!classification)
    return std::unexpected(classification.error());
  if (!*classification)
    return {};
  if (auto checked =
          check_keys(**classification,
                     {"enabled", "learned_fallback_enabled",
                      "adult_detection_enabled", "adult_content_policy",
                      "adult_minimum_confidence", "maximum_files_to_inspect",
                      "maximum_path_bytes", "maximum_tokens", "rules"},
                     "indexing.classification.");
      !checked)
    return checked;
  for (auto result :
       {scalar<bool>(**classification, "enabled",
                     "indexing.classification.enabled", overlay),
        scalar<bool>(**classification, "learned_fallback_enabled",
                     "indexing.classification.learned_fallback_enabled",
                     overlay),
        scalar<bool>(**classification, "adult_detection_enabled",
                     "indexing.classification.adult_detection_enabled",
                     overlay),
        scalar<std::string>(**classification, "adult_content_policy",
                            "indexing.classification.adult_content_policy",
                            overlay),
        scalar<std::string>(**classification, "adult_minimum_confidence",
                            "indexing.classification.adult_minimum_confidence",
                            overlay),
        scalar<std::int64_t>(**classification, "maximum_files_to_inspect",
                             "indexing.classification.maximum_files_to_inspect",
                             overlay),
        scalar<std::int64_t>(**classification, "maximum_path_bytes",
                             "indexing.classification.maximum_path_bytes",
                             overlay),
        scalar<std::int64_t>(**classification, "maximum_tokens",
                             "indexing.classification.maximum_tokens",
                             overlay)})
    if (!result)
      return result;

  const auto rules_view = (**classification)["rules"];
  if (!rules_view)
    return {};
  const auto *rules = rules_view.as_array();
  if (!rules)
    return std::unexpected(invalid(
        "indexing.classification.rules must be an array of TOML tables"));
  std::vector<ClassificationRuleConfig> parsed_rules;
  parsed_rules.reserve(rules->size());
  for (std::size_t index = 0; index < rules->size(); ++index) {
    const auto *rule = (*rules)[index].as_table();
    const auto prefix =
        "indexing.classification.rules[" + std::to_string(index) + "].";
    if (!rule)
      return std::unexpected(invalid(prefix + "value must be a TOML table"));
    if (auto checked = check_keys(
            *rule, {"id", "kind", "match", "tokens", "weight"}, prefix);
        !checked)
      return checked;
    const auto id = (*rule)["id"].value<std::string>();
    const auto kind = (*rule)["kind"].value<std::string>();
    const auto *tokens = (*rule)["tokens"].as_array();
    if (!id || !kind || !tokens)
      return std::unexpected(
          invalid(prefix + "id, kind, and tokens are required"));

    ClassificationRuleConfig parsed{.id = *id};
    if (*kind == "movie")
      parsed.kind = ClassificationRuleKind::Movie;
    else if (*kind == "series")
      parsed.kind = ClassificationRuleKind::Series;
    else if (*kind == "music")
      parsed.kind = ClassificationRuleKind::Music;
    else if (*kind == "audiobook")
      parsed.kind = ClassificationRuleKind::Audiobook;
    else if (*kind == "ebook")
      parsed.kind = ClassificationRuleKind::Ebook;
    else if (*kind == "game")
      parsed.kind = ClassificationRuleKind::Game;
    else if (*kind == "application")
      parsed.kind = ClassificationRuleKind::Application;
    else
      return std::unexpected(invalid(
          prefix +
          "kind must be movie, series, music, audiobook, ebook, game, or "
          "application"));

    const auto match_view = (*rule)["match"];
    if (match_view) {
      const auto match = match_view.value<std::string>();
      if (!match || (*match != "any" && *match != "all"))
        return std::unexpected(invalid(prefix + "match must be any or all"));
      parsed.match = *match == "any" ? ClassificationRuleMatch::Any
                                     : ClassificationRuleMatch::All;
    }
    const auto weight_view = (*rule)["weight"];
    if (weight_view) {
      const auto weight = weight_view.value<std::int64_t>();
      if (!weight || *weight < std::numeric_limits<int>::min() ||
          *weight > std::numeric_limits<int>::max())
        return std::unexpected(invalid(prefix + "weight must be an integer"));
      parsed.weight = static_cast<int>(*weight);
    }
    parsed.tokens.reserve(tokens->size());
    for (const auto &token : *tokens) {
      const auto value = token.value<std::string>();
      if (!value)
        return std::unexpected(
            invalid(prefix + "tokens must contain only strings"));
      parsed.tokens.push_back(*value);
    }
    parsed_rules.push_back(std::move(parsed));
  }
  overlay.classification_rules = std::move(parsed_rules);
  return {};
}

core::Result<void> parse_distributed_coordinator(const toml::table &table,
                                                 ConfigOverlay &overlay) {
  if (auto checked = check_keys(
          table,
          {"enabled", "recovery_enabled", "recovery_file",
           "recovery_maximum_bytes", "maximum_terminal_work_items",
           "terminal_work_retention_ms", "listen_address", "listen_port",
           "maximum_connections", "read_buffer_bytes", "maximum_frame_bytes",
           "maximum_queued_write_bytes", "maximum_result_reassembly_bytes",
           "maximum_result_transfers", "result_transfer_timeout_ms",
           "idle_timeout_ms", "tls_trust_anchor_file",
           "tls_certificate_chain_file", "tls_private_key_file"},
          "distributed.coordinator.");
      !checked)
    return checked;
  for (auto result :
       {scalar<bool>(table, "enabled", "distributed.coordinator.enabled",
                     overlay),
        scalar<bool>(table, "recovery_enabled",
                     "distributed.coordinator.recovery_enabled", overlay),
        scalar<std::string>(table, "recovery_file",
                            "distributed.coordinator.recovery_file", overlay),
        scalar<std::int64_t>(table, "recovery_maximum_bytes",
                             "distributed.coordinator.recovery_maximum_bytes",
                             overlay),
        scalar<std::int64_t>(
            table, "maximum_terminal_work_items",
            "distributed.coordinator.maximum_terminal_work_items", overlay),
        scalar<std::int64_t>(
            table, "terminal_work_retention_ms",
            "distributed.coordinator.terminal_work_retention_ms", overlay),
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
        scalar<std::int64_t>(
            table, "maximum_result_reassembly_bytes",
            "distributed.coordinator.maximum_result_reassembly_bytes", overlay),
        scalar<std::int64_t>(table, "maximum_result_transfers",
                             "distributed.coordinator.maximum_result_transfers",
                             overlay),
        scalar<std::int64_t>(
            table, "result_transfer_timeout_ms",
            "distributed.coordinator.result_transfer_timeout_ms", overlay),
        scalar<std::int64_t>(table, "idle_timeout_ms",
                             "distributed.coordinator.idle_timeout_ms",
                             overlay),
        scalar<std::string>(table, "tls_trust_anchor_file",
                            "distributed.coordinator.tls_trust_anchor_file",
                            overlay),
        scalar<std::string>(
            table, "tls_certificate_chain_file",
            "distributed.coordinator.tls_certificate_chain_file", overlay),
        scalar<std::string>(table, "tls_private_key_file",
                            "distributed.coordinator.tls_private_key_file",
                            overlay)})
    if (!result)
      return result;
  return {};
}

core::Result<void> parse_distributed_worker(const toml::table &table,
                                            ConfigOverlay &overlay) {
  if (auto checked = check_keys(
          table,
          {"enabled", "id", "coordinator_address", "coordinator_port",
           "observation_batch_size", "read_buffer_bytes",
           "maximum_queued_write_bytes", "connect_timeout_ms",
           "request_timeout_ms", "idle_timeout_ms", "tls_trust_anchor_file",
           "tls_certificate_chain_file", "tls_private_key_file",
           "tls_server_name"},
          "distributed.worker.");
      !checked)
    return checked;
  for (auto result :
       {scalar<bool>(table, "enabled", "distributed.worker.enabled", overlay),
        scalar<std::string>(table, "id", "distributed.worker.id", overlay),
        scalar<std::string>(table, "coordinator_address",
                            "distributed.worker.coordinator_address", overlay),
        scalar<std::int64_t>(table, "coordinator_port",
                             "distributed.worker.coordinator_port", overlay),
        scalar<std::int64_t>(table, "observation_batch_size",
                             "distributed.worker.observation_batch_size",
                             overlay),
        scalar<std::int64_t>(table, "read_buffer_bytes",
                             "distributed.worker.read_buffer_bytes", overlay),
        scalar<std::int64_t>(table, "maximum_queued_write_bytes",
                             "distributed.worker.maximum_queued_write_bytes",
                             overlay),
        scalar<std::int64_t>(table, "connect_timeout_ms",
                             "distributed.worker.connect_timeout_ms", overlay),
        scalar<std::int64_t>(table, "request_timeout_ms",
                             "distributed.worker.request_timeout_ms", overlay),
        scalar<std::int64_t>(table, "idle_timeout_ms",
                             "distributed.worker.idle_timeout_ms", overlay),
        scalar<std::string>(table, "tls_trust_anchor_file",
                            "distributed.worker.tls_trust_anchor_file",
                            overlay),
        scalar<std::string>(table, "tls_certificate_chain_file",
                            "distributed.worker.tls_certificate_chain_file",
                            overlay),
        scalar<std::string>(table, "tls_private_key_file",
                            "distributed.worker.tls_private_key_file", overlay),
        scalar<std::string>(table, "tls_server_name",
                            "distributed.worker.tls_server_name", overlay)})
    if (!result)
      return result;
  return {};
}

core::Result<void> parse_distributed(const toml::table &table,
                                     ConfigOverlay &overlay) {
  if (auto checked = check_keys(table,
                                {"maximum_work_items", "maximum_payload_bytes",
                                 "maximum_result_bytes", "worker_timeout_ms",
                                 "lease_duration_ms", "heartbeat_interval_ms",
                                 "coordinator", "worker"},
                                "distributed.");
      !checked)
    return checked;
  for (auto result :
       {scalar<std::int64_t>(table, "maximum_work_items",
                             "distributed.maximum_work_items", overlay),
        scalar<std::int64_t>(table, "maximum_payload_bytes",
                             "distributed.maximum_payload_bytes", overlay),
        scalar<std::int64_t>(table, "maximum_result_bytes",
                             "distributed.maximum_result_bytes", overlay),
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
    if (auto parsed = parse_distributed_coordinator(**coordinator, overlay);
        !parsed)
      return parsed;
  auto worker = optional_table(table, "worker", "distributed.worker");
  if (!worker)
    return std::unexpected(worker.error());
  if (*worker)
    return parse_distributed_worker(**worker, overlay);
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
      {"SAKUIN_DHT_BOOTSTRAP_FILE", "network.dht.bootstrap_file"},
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
      {"SAKUIN_DHT_METADATA_DISCOVERY_ENABLED",
       "network.dht.metadata.discovery.enabled"},
      {"SAKUIN_DHT_METADATA_DISCOVERY_MAXIMUM_PENDING",
       "network.dht.metadata.discovery.maximum_pending"},
      {"SAKUIN_DHT_METADATA_DISCOVERY_MAXIMUM_IN_FLIGHT",
       "network.dht.metadata.discovery.maximum_in_flight"},
      {"SAKUIN_DHT_METADATA_DISCOVERY_PARALLELISM_PER_HASH",
       "network.dht.metadata.discovery.parallelism_per_hash"},
      {"SAKUIN_DHT_METADATA_DISCOVERY_MAXIMUM_QUERIES_PER_HASH",
       "network.dht.metadata.discovery.maximum_queries_per_hash"},
      {"SAKUIN_DHT_METADATA_DISCOVERY_RETRY_DELAY_MS",
       "network.dht.metadata.discovery.retry_delay_ms"},
      {"SAKUIN_DHT_METADATA_DISCOVERY_BACKFILL_ENABLED",
       "network.dht.metadata.discovery.backfill.enabled"},
      {"SAKUIN_DHT_METADATA_DISCOVERY_BACKFILL_MAXIMUM_RECORDS_PER_POLL",
       "network.dht.metadata.discovery.backfill.maximum_records_per_poll"},
      {"SAKUIN_DHT_METADATA_DISCOVERY_BACKFILL_REFRESH_INTERVAL_MS",
       "network.dht.metadata.discovery.backfill.refresh_interval_ms"},
      {"SAKUIN_DHT_METADATA_DISCOVERY_BACKFILL_FULL_RESCAN_INTERVAL_MS",
       "network.dht.metadata.discovery.backfill.full_rescan_interval_ms"},
      {"SAKUIN_DHT_METADATA_DISCOVERY_BACKFILL_RETRY_DELAY_MS",
       "network.dht.metadata.discovery.backfill.retry_delay_ms"},
      {"SAKUIN_TRAFFIC_WINDOW_MS", "network.traffic.window_ms"},
      {"SAKUIN_TRAFFIC_INBOUND_BYTES", "network.traffic.inbound_bytes"},
      {"SAKUIN_TRAFFIC_OUTBOUND_BYTES", "network.traffic.outbound_bytes"},
      {"SAKUIN_TRAFFIC_GRANT_BYTES", "network.traffic.grant_bytes"},
      {"SAKUIN_STORAGE_BACKEND", "storage.backend"},
      {"SAKUIN_STORAGE_LOCAL_ROOT", "storage.local_root"},
      {"SAKUIN_STORAGE_S3_ENDPOINT", "storage.s3.endpoint"},
      {"SAKUIN_STORAGE_S3_BUCKET", "storage.s3.bucket"},
      {"SAKUIN_STORAGE_S3_REGION", "storage.s3.region"},
      {"SAKUIN_STORAGE_S3_PREFIX", "storage.s3.prefix"},
      {"SAKUIN_STORAGE_S3_CONNECT_TIMEOUT_MS", "storage.s3.connect_timeout_ms"},
      {"SAKUIN_STORAGE_S3_REQUEST_TIMEOUT_MS", "storage.s3.request_timeout_ms"},
      {"SAKUIN_STORAGE_S3_MAXIMUM_ATTEMPTS", "storage.s3.maximum_attempts"},
      {"SAKUIN_STORAGE_S3_RETRY_DELAY_MS", "storage.s3.retry_delay_ms"},
      {"SAKUIN_STORAGE_S3_VERIFY_TLS", "storage.s3.verify_tls"},
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
      {"SAKUIN_STORAGE_RETENTION_ENABLED", "storage.retention.enabled"},
      {"SAKUIN_STORAGE_RETENTION_OBSERVATION_COLD_AGE_MS",
       "storage.retention.observation_cold_age_ms"},
      {"SAKUIN_STORAGE_RETENTION_OBSERVATION_MAX_AGE_MS",
       "storage.retention.observation_max_age_ms"},
      {"SAKUIN_STORAGE_RETENTION_COLD_BLOCK_TARGET_BYTES",
       "storage.retention.cold_block_target_bytes"},
      {"SAKUIN_STORAGE_RETENTION_COLD_COMPRESSION_CODEC",
       "storage.retention.cold_compression_codec"},
      {"SAKUIN_STORAGE_RETENTION_COLD_COMPRESSION_LEVEL",
       "storage.retention.cold_compression_level"},
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
      {"SAKUIN_CLASSIFICATION_ENABLED", "indexing.classification.enabled"},
      {"SAKUIN_CLASSIFICATION_LEARNED_FALLBACK_ENABLED",
       "indexing.classification.learned_fallback_enabled"},
      {"SAKUIN_CLASSIFICATION_ADULT_DETECTION_ENABLED",
       "indexing.classification.adult_detection_enabled"},
      {"SAKUIN_CLASSIFICATION_ADULT_CONTENT_POLICY",
       "indexing.classification.adult_content_policy"},
      {"SAKUIN_CLASSIFICATION_ADULT_MINIMUM_CONFIDENCE",
       "indexing.classification.adult_minimum_confidence"},
      {"SAKUIN_CLASSIFICATION_MAXIMUM_FILES_TO_INSPECT",
       "indexing.classification.maximum_files_to_inspect"},
      {"SAKUIN_CLASSIFICATION_MAXIMUM_PATH_BYTES",
       "indexing.classification.maximum_path_bytes"},
      {"SAKUIN_CLASSIFICATION_MAXIMUM_TOKENS",
       "indexing.classification.maximum_tokens"},
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
      {"SAKUIN_DISTRIBUTED_MAXIMUM_RESULT_BYTES",
       "distributed.maximum_result_bytes"},
      {"SAKUIN_DISTRIBUTED_WORKER_TIMEOUT_MS", "distributed.worker_timeout_ms"},
      {"SAKUIN_DISTRIBUTED_LEASE_DURATION_MS", "distributed.lease_duration_ms"},
      {"SAKUIN_DISTRIBUTED_HEARTBEAT_INTERVAL_MS",
       "distributed.heartbeat_interval_ms"},
      {"SAKUIN_DISTRIBUTED_COORDINATOR_ENABLED",
       "distributed.coordinator.enabled"},
      {"SAKUIN_DISTRIBUTED_COORDINATOR_RECOVERY_ENABLED",
       "distributed.coordinator.recovery_enabled"},
      {"SAKUIN_DISTRIBUTED_COORDINATOR_RECOVERY_FILE",
       "distributed.coordinator.recovery_file"},
      {"SAKUIN_DISTRIBUTED_COORDINATOR_RECOVERY_MAXIMUM_BYTES",
       "distributed.coordinator.recovery_maximum_bytes"},
      {"SAKUIN_DISTRIBUTED_COORDINATOR_MAXIMUM_TERMINAL_WORK_ITEMS",
       "distributed.coordinator.maximum_terminal_work_items"},
      {"SAKUIN_DISTRIBUTED_COORDINATOR_TERMINAL_WORK_RETENTION_MS",
       "distributed.coordinator.terminal_work_retention_ms"},
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
      {"SAKUIN_DISTRIBUTED_COORDINATOR_MAXIMUM_RESULT_REASSEMBLY_BYTES",
       "distributed.coordinator.maximum_result_reassembly_bytes"},
      {"SAKUIN_DISTRIBUTED_COORDINATOR_MAXIMUM_RESULT_TRANSFERS",
       "distributed.coordinator.maximum_result_transfers"},
      {"SAKUIN_DISTRIBUTED_COORDINATOR_RESULT_TRANSFER_TIMEOUT_MS",
       "distributed.coordinator.result_transfer_timeout_ms"},
      {"SAKUIN_DISTRIBUTED_COORDINATOR_IDLE_TIMEOUT_MS",
       "distributed.coordinator.idle_timeout_ms"},
      {"SAKUIN_DISTRIBUTED_COORDINATOR_TLS_TRUST_ANCHOR_FILE",
       "distributed.coordinator.tls_trust_anchor_file"},
      {"SAKUIN_DISTRIBUTED_COORDINATOR_TLS_CERTIFICATE_CHAIN_FILE",
       "distributed.coordinator.tls_certificate_chain_file"},
      {"SAKUIN_DISTRIBUTED_COORDINATOR_TLS_PRIVATE_KEY_FILE",
       "distributed.coordinator.tls_private_key_file"},
      {"SAKUIN_DISTRIBUTED_WORKER_ENABLED", "distributed.worker.enabled"},
      {"SAKUIN_DISTRIBUTED_WORKER_ID", "distributed.worker.id"},
      {"SAKUIN_DISTRIBUTED_WORKER_COORDINATOR_ADDRESS",
       "distributed.worker.coordinator_address"},
      {"SAKUIN_DISTRIBUTED_WORKER_COORDINATOR_PORT",
       "distributed.worker.coordinator_port"},
      {"SAKUIN_DISTRIBUTED_WORKER_OBSERVATION_BATCH_SIZE",
       "distributed.worker.observation_batch_size"},
      {"SAKUIN_DISTRIBUTED_WORKER_READ_BUFFER_BYTES",
       "distributed.worker.read_buffer_bytes"},
      {"SAKUIN_DISTRIBUTED_WORKER_MAXIMUM_QUEUED_WRITE_BYTES",
       "distributed.worker.maximum_queued_write_bytes"},
      {"SAKUIN_DISTRIBUTED_WORKER_CONNECT_TIMEOUT_MS",
       "distributed.worker.connect_timeout_ms"},
      {"SAKUIN_DISTRIBUTED_WORKER_REQUEST_TIMEOUT_MS",
       "distributed.worker.request_timeout_ms"},
      {"SAKUIN_DISTRIBUTED_WORKER_IDLE_TIMEOUT_MS",
       "distributed.worker.idle_timeout_ms"},
      {"SAKUIN_DISTRIBUTED_WORKER_TLS_TRUST_ANCHOR_FILE",
       "distributed.worker.tls_trust_anchor_file"},
      {"SAKUIN_DISTRIBUTED_WORKER_TLS_CERTIFICATE_CHAIN_FILE",
       "distributed.worker.tls_certificate_chain_file"},
      {"SAKUIN_DISTRIBUTED_WORKER_TLS_PRIVATE_KEY_FILE",
       "distributed.worker.tls_private_key_file"},
      {"SAKUIN_DISTRIBUTED_WORKER_TLS_SERVER_NAME",
       "distributed.worker.tls_server_name"},
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
  if (result.network.dht.bootstrap.empty()) {
    const auto read_bootstrap = [](const std::filesystem::path &path)
        -> core::Result<std::vector<std::string>> {
      std::ifstream input{path};
      if (!input)
        return std::unexpected(
            core::Error{core::ErrorCode::IoError,
                        "Unable to open DHT bootstrap file: " + path.string()});
      std::vector<std::string> entries;
      std::string line;
      while (std::getline(input, line)) {
        auto first = line.find_first_not_of(" \t\r");
        if (first == std::string::npos || line[first] == '#')
          continue;
        auto last = line.find_last_not_of(" \t\r");
        auto entry = line.substr(first, last - first + 1);
        if (const auto comment = entry.find('#');
            comment != std::string::npos) {
          entry.erase(comment);
          if (const auto end = entry.find_last_not_of(" \t\r");
              end != std::string::npos)
            entry.erase(end + 1);
        }
        if (!entry.empty())
          entries.push_back(std::move(entry));
      }
      if (!input.eof())
        return std::unexpected(
            core::Error{core::ErrorCode::IoError,
                        "Unable to read DHT bootstrap file: " + path.string()});
      return entries;
    };

    if (result.network.dht.bootstrap_file) {
      auto path = *result.network.dht.bootstrap_file;
      if (path.is_relative() && request.toml_file)
        path = request.toml_file->parent_path() / path;
      auto entries = read_bootstrap(path);
      if (!entries)
        return std::unexpected(entries.error());
      result.network.dht.bootstrap = std::move(*entries);
      result.network.dht.bootstrap_file = std::move(path);
    } else {
      for (const auto &candidate : request.bootstrap_fallback_files) {
        std::error_code status_error;
        const bool exists =
            std::filesystem::is_regular_file(candidate, status_error);
        if (status_error || !exists)
          continue;
        auto entries = read_bootstrap(candidate);
        if (!entries)
          return std::unexpected(entries.error());
        result.network.dht.bootstrap = std::move(*entries);
        result.network.dht.bootstrap_file = candidate;
        break;
      }
    }
  }
  if (auto valid = validate(result); !valid)
    return std::unexpected(valid.error());
  return result;
}

} // namespace sakuin::config

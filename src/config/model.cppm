export module sakuin.config.model;

import std;

import sakuin.core.result;
import sakuin.core.time;

export namespace sakuin::config {

enum class DhtIdentityMode { Bep42, Fixed };
enum class StorageBackend { Local };
enum class CompressionCodec { None, Zstd };

struct DhtIdentityConfig {
  DhtIdentityMode mode{DhtIdentityMode::Bep42};
  std::size_t observation_quorum{3};
  core::Duration vote_window{std::chrono::minutes{10}};
  std::optional<std::string> fixed_ipv4_node_id;
  std::optional<std::string> fixed_ipv6_node_id;
};

struct MetadataAcquisitionConfig {
  bool enabled{true};
  std::size_t maximum_in_flight{64};
  std::size_t maximum_queued{4'096};
  std::size_t maximum_attempts_per_peer{3};
  core::Duration initial_retry_delay{std::chrono::seconds{30}};
  core::Duration maximum_retry_delay{std::chrono::minutes{30}};
  core::Duration connect_timeout{std::chrono::seconds{10}};
  core::Duration idle_timeout{std::chrono::seconds{30}};
  std::size_t maximum_metadata_bytes{4U * 1024U * 1024U};
  std::size_t maximum_outstanding_requests{4};
  std::size_t maximum_queued_write_bytes{1024U * 1024U};
  std::size_t storage_conflict_attempts{3};
  core::Duration storage_retry_delay{std::chrono::seconds{1}};
};

struct RoutingMaintenanceConfig {
  std::size_t maximum_queued{1'024};
  std::size_t maximum_in_flight{8};
  std::size_t maximum_attempts{2};
  core::Duration retry_delay{std::chrono::seconds{5}};
};

struct DhtConfig {
  bool private_network{};
  std::size_t maximum_in_flight{128};
  core::Duration query_timeout{std::chrono::seconds{15}};
  std::size_t bootstrap_maximum_in_flight{8};
  std::size_t bootstrap_maximum_attempts{3};
  core::Duration bootstrap_retry_delay{std::chrono::seconds{5}};
  std::vector<std::string> bootstrap;
  DhtIdentityConfig identity;
  RoutingMaintenanceConfig routing;
  MetadataAcquisitionConfig metadata;
};

struct PeriodicTrafficConfig {
  core::Duration window{std::chrono::hours{1}};
  std::optional<std::uint64_t> inbound_bytes;
  std::optional<std::uint64_t> outbound_bytes;
};

struct NetworkConfig {
  bool enable_ipv4{true};
  bool enable_ipv6{true};
  std::uint16_t listen_port{6881};
  DhtConfig dht;
  PeriodicTrafficConfig traffic;
};

struct StorageConfig {
  struct MaintenanceConfig {
    bool enabled{true};
    core::Duration interval{std::chrono::minutes{15}};
    core::Duration verification_interval{std::chrono::hours{24}};
  };

  struct MaterializationConfig {
    bool enabled{true};
    core::Duration interval{std::chrono::minutes{1}};
  };

  StorageBackend backend{StorageBackend::Local};
  std::filesystem::path local_root{"./data"};
  std::uint64_t block_target_bytes{2U * 1024U * 1024U};
  std::uint64_t segment_target_bytes{128U * 1024U * 1024U};
  CompressionCodec compression{CompressionCodec::Zstd};
  int compression_level{3};
  std::size_t compaction_minimum_segments{4};
  std::size_t compaction_maximum_warm_segments{8};
  std::uint64_t compaction_warm_block_target_bytes{64U * 1024U};
  MaintenanceConfig maintenance;
  MaterializationConfig materialization;
};

struct ApiRateLimitConfig {
  bool enabled{true};
  std::uint64_t requests_per_window{120};
  core::Duration window{std::chrono::minutes{1}};
};

struct ApiConfig {
  bool enabled{true};
  std::filesystem::path credential_store_directory{"./data/operational/api"};
  std::string listen_address{"127.0.0.1"};
  std::uint16_t listen_port{8080};
  std::size_t maximum_connections{256};
  std::size_t read_buffer_bytes{16U * 1024U};
  core::Duration request_timeout{std::chrono::seconds{15}};
  std::size_t maximum_target_bytes{8U * 1024U};
  std::size_t maximum_header_bytes{32U * 1024U};
  std::size_t maximum_header_count{100};
  std::size_t maximum_body_bytes{1024U * 1024U};
  std::optional<std::filesystem::path> tls_certificate_chain_file;
  std::optional<std::filesystem::path> tls_private_key_file;
  ApiRateLimitConfig rate_limit;
};

struct DuplicateIndexConfig {
  bool enabled{true};
  core::Duration interval{std::chrono::minutes{5}};
};

struct IndexingConfig {
  DuplicateIndexConfig duplicates;
};

struct DistributedConfig {
  struct CoordinatorConfig {
    bool enabled{};
    std::string listen_address{"127.0.0.1"};
    std::uint16_t listen_port{7100};
    std::size_t maximum_connections{256};
    std::size_t read_buffer_bytes{16U * 1024U};
    std::size_t maximum_frame_bytes{2U * 1024U * 1024U};
    std::size_t maximum_queued_write_bytes{4U * 1024U * 1024U};
    core::Duration idle_timeout{std::chrono::seconds{30}};
  };

  std::size_t maximum_work_items{65'536};
  std::size_t maximum_payload_bytes{1U * 1024U * 1024U};
  core::Duration worker_timeout{std::chrono::seconds{30}};
  core::Duration lease_duration{std::chrono::minutes{2}};
  core::Duration heartbeat_interval{std::chrono::seconds{10}};
  CoordinatorConfig coordinator;
};

struct AppConfig {
  NetworkConfig network;
  StorageConfig storage;
  IndexingConfig indexing;
  ApiConfig api;
  DistributedConfig distributed;
};

struct ConfigOverlay {
  std::map<std::string, std::string, std::less<>> scalars;
  std::optional<std::vector<std::string>> bootstrap;
};

AppConfig defaults();
core::Result<void> apply(AppConfig &config, const ConfigOverlay &overlay);
core::Result<void> validate(const AppConfig &config);

} // namespace sakuin::config

namespace sakuin::config {
namespace {

core::Error invalid(std::string message) {
  return {core::ErrorCode::InvalidArgument, std::move(message)};
}

template <std::unsigned_integral T>
core::Result<T> unsigned_value(std::string_view text, std::string_view name) {
  T value{};
  const auto [end, error] =
      std::from_chars(text.data(), text.data() + text.size(), value);
  if (error != std::errc{} || end != text.data() + text.size())
    return std::unexpected(
        invalid(std::string{name} + " must be an unsigned integer"));
  return value;
}

core::Result<int> integer_value(std::string_view text, std::string_view name) {
  int value{};
  const auto [end, error] =
      std::from_chars(text.data(), text.data() + text.size(), value);
  if (error != std::errc{} || end != text.data() + text.size())
    return std::unexpected(invalid(std::string{name} + " must be an integer"));
  return value;
}

core::Result<bool> boolean_value(std::string_view text, std::string_view name) {
  if (text == "true")
    return true;
  if (text == "false")
    return false;
  return std::unexpected(invalid(std::string{name} + " must be true or false"));
}

core::Result<core::Duration> duration_ms(std::string_view text,
                                         std::string_view name) {
  auto milliseconds = unsigned_value<std::uint64_t>(text, name);
  if (!milliseconds || *milliseconds == 0 ||
      *milliseconds >
          static_cast<std::uint64_t>(std::chrono::milliseconds::max().count()))
    return std::unexpected(
        milliseconds ? invalid(std::string{name} + " must be positive")
                     : milliseconds.error());
  return std::chrono::duration_cast<core::Duration>(
      std::chrono::milliseconds{*milliseconds});
}

bool valid_node_id(std::string_view value) {
  return value.size() == 40 && std::ranges::all_of(value, [](char digit) {
           return (digit >= '0' && digit <= '9') ||
                  (digit >= 'a' && digit <= 'f') ||
                  (digit >= 'A' && digit <= 'F');
         });
}

} // namespace

AppConfig defaults() { return {}; }

core::Result<void> apply(AppConfig &config, const ConfigOverlay &overlay) {
  for (const auto &[name, text] : overlay.scalars) {
    if (name == "network.enable_ipv4") {
      auto value = boolean_value(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.network.enable_ipv4 = *value;
    } else if (name == "network.enable_ipv6") {
      auto value = boolean_value(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.network.enable_ipv6 = *value;
    } else if (name == "network.listen_port") {
      auto value = unsigned_value<std::uint16_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.network.listen_port = *value;
    } else if (name == "network.dht.maximum_in_flight") {
      auto value = unsigned_value<std::size_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.network.dht.maximum_in_flight = *value;
    } else if (name == "network.dht.private_network") {
      auto value = boolean_value(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.network.dht.private_network = *value;
    } else if (name == "network.dht.query_timeout_ms") {
      auto value = duration_ms(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.network.dht.query_timeout = *value;
    } else if (name == "network.dht.bootstrap_maximum_in_flight") {
      auto value = unsigned_value<std::size_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.network.dht.bootstrap_maximum_in_flight = *value;
    } else if (name == "network.dht.bootstrap_maximum_attempts") {
      auto value = unsigned_value<std::size_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.network.dht.bootstrap_maximum_attempts = *value;
    } else if (name == "network.dht.bootstrap_retry_delay_ms") {
      auto value = duration_ms(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.network.dht.bootstrap_retry_delay = *value;
    } else if (name == "network.dht.routing.maximum_queued") {
      auto value = unsigned_value<std::size_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.network.dht.routing.maximum_queued = *value;
    } else if (name == "network.dht.routing.maximum_in_flight") {
      auto value = unsigned_value<std::size_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.network.dht.routing.maximum_in_flight = *value;
    } else if (name == "network.dht.routing.maximum_attempts") {
      auto value = unsigned_value<std::size_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.network.dht.routing.maximum_attempts = *value;
    } else if (name == "network.dht.routing.retry_delay_ms") {
      auto value = duration_ms(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.network.dht.routing.retry_delay = *value;
    } else if (name == "network.dht.identity.mode") {
      if (text == "bep42")
        config.network.dht.identity.mode = DhtIdentityMode::Bep42;
      else if (text == "fixed")
        config.network.dht.identity.mode = DhtIdentityMode::Fixed;
      else
        return std::unexpected(
            invalid("network.dht.identity.mode must be bep42 or fixed"));
    } else if (name == "network.dht.identity.observation_quorum") {
      auto value = unsigned_value<std::size_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.network.dht.identity.observation_quorum = *value;
    } else if (name == "network.dht.identity.vote_window_ms") {
      auto value = duration_ms(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.network.dht.identity.vote_window = *value;
    } else if (name == "network.dht.identity.fixed_ipv4_node_id") {
      config.network.dht.identity.fixed_ipv4_node_id = text;
    } else if (name == "network.dht.identity.fixed_ipv6_node_id") {
      config.network.dht.identity.fixed_ipv6_node_id = text;
    } else if (name == "network.dht.metadata.enabled") {
      auto value = boolean_value(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.network.dht.metadata.enabled = *value;
    } else if (name == "network.dht.metadata.maximum_in_flight") {
      auto value = unsigned_value<std::size_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.network.dht.metadata.maximum_in_flight = *value;
    } else if (name == "network.dht.metadata.maximum_queued") {
      auto value = unsigned_value<std::size_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.network.dht.metadata.maximum_queued = *value;
    } else if (name == "network.dht.metadata.maximum_attempts_per_peer") {
      auto value = unsigned_value<std::size_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.network.dht.metadata.maximum_attempts_per_peer = *value;
    } else if (name == "network.dht.metadata.initial_retry_delay_ms") {
      auto value = duration_ms(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.network.dht.metadata.initial_retry_delay = *value;
    } else if (name == "network.dht.metadata.maximum_retry_delay_ms") {
      auto value = duration_ms(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.network.dht.metadata.maximum_retry_delay = *value;
    } else if (name == "network.dht.metadata.connect_timeout_ms") {
      auto value = duration_ms(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.network.dht.metadata.connect_timeout = *value;
    } else if (name == "network.dht.metadata.idle_timeout_ms") {
      auto value = duration_ms(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.network.dht.metadata.idle_timeout = *value;
    } else if (name == "network.dht.metadata.maximum_metadata_bytes") {
      auto value = unsigned_value<std::size_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.network.dht.metadata.maximum_metadata_bytes = *value;
    } else if (name == "network.dht.metadata.maximum_outstanding_requests") {
      auto value = unsigned_value<std::size_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.network.dht.metadata.maximum_outstanding_requests = *value;
    } else if (name == "network.dht.metadata.maximum_queued_write_bytes") {
      auto value = unsigned_value<std::size_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.network.dht.metadata.maximum_queued_write_bytes = *value;
    } else if (name == "network.dht.metadata.storage_conflict_attempts") {
      auto value = unsigned_value<std::size_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.network.dht.metadata.storage_conflict_attempts = *value;
    } else if (name == "network.dht.metadata.storage_retry_delay_ms") {
      auto value = duration_ms(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.network.dht.metadata.storage_retry_delay = *value;
    } else if (name == "network.traffic.window_ms") {
      auto value = duration_ms(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.network.traffic.window = *value;
    } else if (name == "network.traffic.inbound_bytes") {
      auto value = unsigned_value<std::uint64_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.network.traffic.inbound_bytes = *value;
    } else if (name == "network.traffic.outbound_bytes") {
      auto value = unsigned_value<std::uint64_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.network.traffic.outbound_bytes = *value;
    } else if (name == "storage.backend") {
      if (text != "local")
        return std::unexpected(
            invalid("storage.backend currently supports only local"));
      config.storage.backend = StorageBackend::Local;
    } else if (name == "storage.local_root") {
      config.storage.local_root = text;
    } else if (name == "storage.block_target_bytes") {
      auto value = unsigned_value<std::uint64_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.storage.block_target_bytes = *value;
    } else if (name == "storage.segment_target_bytes") {
      auto value = unsigned_value<std::uint64_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.storage.segment_target_bytes = *value;
    } else if (name == "storage.compression.codec") {
      if (text == "none")
        config.storage.compression = CompressionCodec::None;
      else if (text == "zstd")
        config.storage.compression = CompressionCodec::Zstd;
      else
        return std::unexpected(
            invalid("storage.compression.codec must be none or zstd"));
    } else if (name == "storage.compression.level") {
      auto value = integer_value(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.storage.compression_level = *value;
    } else if (name == "storage.compaction.minimum_segments") {
      auto value = unsigned_value<std::size_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.storage.compaction_minimum_segments = *value;
    } else if (name == "storage.compaction.maximum_warm_segments") {
      auto value = unsigned_value<std::size_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.storage.compaction_maximum_warm_segments = *value;
    } else if (name == "storage.compaction.warm_block_target_bytes") {
      auto value = unsigned_value<std::uint64_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.storage.compaction_warm_block_target_bytes = *value;
    } else if (name == "storage.maintenance.enabled") {
      auto value = boolean_value(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.storage.maintenance.enabled = *value;
    } else if (name == "storage.maintenance.interval_ms") {
      auto value = duration_ms(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.storage.maintenance.interval = *value;
    } else if (name == "storage.maintenance.verification_interval_ms") {
      auto value = duration_ms(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.storage.maintenance.verification_interval = *value;
    } else if (name == "storage.materialization.enabled") {
      auto value = boolean_value(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.storage.materialization.enabled = *value;
    } else if (name == "storage.materialization.interval_ms") {
      auto value = duration_ms(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.storage.materialization.interval = *value;
    } else if (name == "indexing.duplicates.enabled") {
      auto value = boolean_value(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.indexing.duplicates.enabled = *value;
    } else if (name == "indexing.duplicates.interval_ms") {
      auto value = duration_ms(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.indexing.duplicates.interval = *value;
    } else if (name == "api.enabled") {
      auto value = boolean_value(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.api.enabled = *value;
    } else if (name == "api.credential_store_directory") {
      config.api.credential_store_directory = text;
    } else if (name == "api.listen_address") {
      config.api.listen_address = text;
    } else if (name == "api.listen_port") {
      auto value = unsigned_value<std::uint16_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.api.listen_port = *value;
    } else if (name == "api.maximum_connections") {
      auto value = unsigned_value<std::size_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.api.maximum_connections = *value;
    } else if (name == "api.read_buffer_bytes") {
      auto value = unsigned_value<std::size_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.api.read_buffer_bytes = *value;
    } else if (name == "api.request_timeout_ms") {
      auto value = duration_ms(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.api.request_timeout = *value;
    } else if (name == "api.maximum_target_bytes") {
      auto value = unsigned_value<std::size_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.api.maximum_target_bytes = *value;
    } else if (name == "api.maximum_header_bytes") {
      auto value = unsigned_value<std::size_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.api.maximum_header_bytes = *value;
    } else if (name == "api.maximum_header_count") {
      auto value = unsigned_value<std::size_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.api.maximum_header_count = *value;
    } else if (name == "api.maximum_body_bytes") {
      auto value = unsigned_value<std::size_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.api.maximum_body_bytes = *value;
    } else if (name == "api.tls_certificate_chain_file") {
      config.api.tls_certificate_chain_file = text;
    } else if (name == "api.tls_private_key_file") {
      config.api.tls_private_key_file = text;
    } else if (name == "api.rate_limit.enabled") {
      auto value = boolean_value(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.api.rate_limit.enabled = *value;
    } else if (name == "api.rate_limit.requests_per_window") {
      auto value = unsigned_value<std::uint64_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.api.rate_limit.requests_per_window = *value;
    } else if (name == "api.rate_limit.window_ms") {
      auto value = duration_ms(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.api.rate_limit.window = *value;
    } else if (name == "distributed.maximum_work_items") {
      auto value = unsigned_value<std::size_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.distributed.maximum_work_items = *value;
    } else if (name == "distributed.maximum_payload_bytes") {
      auto value = unsigned_value<std::size_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.distributed.maximum_payload_bytes = *value;
    } else if (name == "distributed.worker_timeout_ms") {
      auto value = duration_ms(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.distributed.worker_timeout = *value;
    } else if (name == "distributed.lease_duration_ms") {
      auto value = duration_ms(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.distributed.lease_duration = *value;
    } else if (name == "distributed.heartbeat_interval_ms") {
      auto value = duration_ms(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.distributed.heartbeat_interval = *value;
    } else if (name == "distributed.coordinator.enabled") {
      auto value = boolean_value(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.distributed.coordinator.enabled = *value;
    } else if (name == "distributed.coordinator.listen_address") {
      config.distributed.coordinator.listen_address = text;
    } else if (name == "distributed.coordinator.listen_port") {
      auto value = unsigned_value<std::uint16_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.distributed.coordinator.listen_port = *value;
    } else if (name == "distributed.coordinator.maximum_connections") {
      auto value = unsigned_value<std::size_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.distributed.coordinator.maximum_connections = *value;
    } else if (name == "distributed.coordinator.read_buffer_bytes") {
      auto value = unsigned_value<std::size_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.distributed.coordinator.read_buffer_bytes = *value;
    } else if (name == "distributed.coordinator.maximum_frame_bytes") {
      auto value = unsigned_value<std::size_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.distributed.coordinator.maximum_frame_bytes = *value;
    } else if (name == "distributed.coordinator.maximum_queued_write_bytes") {
      auto value = unsigned_value<std::size_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.distributed.coordinator.maximum_queued_write_bytes = *value;
    } else if (name == "distributed.coordinator.idle_timeout_ms") {
      auto value = duration_ms(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.distributed.coordinator.idle_timeout = *value;
    } else {
      return std::unexpected(invalid("Unknown configuration key: " + name));
    }
  }
  if (overlay.bootstrap)
    config.network.dht.bootstrap = *overlay.bootstrap;
  return {};
}

core::Result<void> validate(const AppConfig &config) {
  if (!config.network.enable_ipv4 && !config.network.enable_ipv6)
    return std::unexpected(
        invalid("At least one DHT address family must be enabled"));
  if (config.network.listen_port == 0)
    return std::unexpected(invalid("network.listen_port must be nonzero"));
  if (config.network.dht.maximum_in_flight == 0 ||
      config.network.dht.bootstrap_maximum_in_flight == 0 ||
      config.network.dht.bootstrap_maximum_attempts == 0)
    return std::unexpected(
        invalid("DHT concurrency and attempt counts must be positive"));
  if (config.network.dht.query_timeout <= core::Duration::zero() ||
      config.network.dht.bootstrap_retry_delay <= core::Duration::zero())
    return std::unexpected(invalid("DHT timeouts must be positive"));
  const auto &routing = config.network.dht.routing;
  if (routing.maximum_queued == 0 || routing.maximum_queued > 1'000'000 ||
      routing.maximum_in_flight == 0 ||
      routing.maximum_in_flight > config.network.dht.maximum_in_flight ||
      routing.maximum_attempts == 0 || routing.maximum_attempts > 32 ||
      routing.retry_delay <= core::Duration::zero())
    return std::unexpected(
        invalid("DHT routing-maintenance limits are invalid"));
  if (config.network.dht.identity.mode == DhtIdentityMode::Bep42 &&
      (config.network.dht.identity.observation_quorum < 2 ||
       config.network.dht.identity.vote_window <= core::Duration::zero()))
    return std::unexpected(invalid(
        "BEP 42 identity requires a quorum of at least two and a positive "
        "vote window"));
  if (config.network.dht.identity.mode == DhtIdentityMode::Fixed) {
    if (!config.network.dht.private_network)
      return std::unexpected(invalid(
          "Fixed DHT identities are restricted to private/test networks"));
    if (config.network.enable_ipv4 &&
        (!config.network.dht.identity.fixed_ipv4_node_id ||
         !valid_node_id(*config.network.dht.identity.fixed_ipv4_node_id)))
      return std::unexpected(invalid(
          "Fixed IPv4 identity requires a 40-digit hexadecimal node ID"));
    if (config.network.enable_ipv6 &&
        (!config.network.dht.identity.fixed_ipv6_node_id ||
         !valid_node_id(*config.network.dht.identity.fixed_ipv6_node_id)))
      return std::unexpected(invalid(
          "Fixed IPv6 identity requires a 40-digit hexadecimal node ID"));
  }
  const auto &metadata = config.network.dht.metadata;
  if (metadata.maximum_in_flight == 0 || metadata.maximum_in_flight > 4'096 ||
      metadata.maximum_queued == 0 || metadata.maximum_queued > 1'000'000 ||
      metadata.maximum_attempts_per_peer == 0 ||
      metadata.maximum_attempts_per_peer > 32 ||
      metadata.initial_retry_delay <= core::Duration::zero() ||
      metadata.maximum_retry_delay < metadata.initial_retry_delay ||
      metadata.connect_timeout <= core::Duration::zero() ||
      metadata.idle_timeout <= core::Duration::zero() ||
      metadata.maximum_metadata_bytes == 0 ||
      metadata.maximum_metadata_bytes > 64U * 1024U * 1024U ||
      metadata.maximum_outstanding_requests == 0 ||
      metadata.maximum_outstanding_requests > 256 ||
      metadata.maximum_queued_write_bytes < 68 ||
      metadata.maximum_queued_write_bytes > 64U * 1024U * 1024U ||
      metadata.storage_conflict_attempts == 0 ||
      metadata.storage_conflict_attempts > 32 ||
      metadata.storage_retry_delay <= core::Duration::zero())
    return std::unexpected(
        invalid("DHT metadata acquisition limits are invalid"));
  if (config.network.traffic.window <= core::Duration::zero() ||
      (config.network.traffic.inbound_bytes == 0) ||
      (config.network.traffic.outbound_bytes == 0))
    return std::unexpected(invalid(
        "Configured traffic budgets and their window must be positive"));
  if (config.storage.local_root.empty() ||
      config.storage.block_target_bytes == 0 ||
      config.storage.segment_target_bytes == 0 ||
      config.storage.block_target_bytes > config.storage.segment_target_bytes)
    return std::unexpected(
        invalid("Storage paths and size targets are invalid"));
  if (config.storage.compaction_minimum_segments < 2)
    return std::unexpected(
        invalid("Compaction requires at least two input segments"));
  if (config.storage.compaction_maximum_warm_segments == 0)
    return std::unexpected(
        invalid("Compaction requires at least one WARM segment slot"));
  if (config.storage.compaction_warm_block_target_bytes < 4U * 1024U ||
      config.storage.compaction_warm_block_target_bytes >
          config.storage.segment_target_bytes ||
      config.storage.compaction_warm_block_target_bytes >
          std::numeric_limits<std::uint32_t>::max())
    return std::unexpected(
        invalid("WARM compaction block target is outside storage limits"));
  if (config.storage.compression_level < -131'072 ||
      config.storage.compression_level > 22)
    return std::unexpected(
        invalid("storage.compression.level is outside the Zstandard range"));
  if (config.storage.maintenance.interval <= core::Duration::zero() ||
      config.storage.maintenance.verification_interval <=
          core::Duration::zero())
    return std::unexpected(
        invalid("Storage maintenance intervals must be positive"));
  if (config.storage.materialization.interval <= core::Duration::zero())
    return std::unexpected(
        invalid("Storage materialization interval must be positive"));
  if (config.indexing.duplicates.interval <= core::Duration::zero())
    return std::unexpected(
        invalid("Duplicate-index interval must be positive"));
  if (config.api.enabled &&
      (config.api.credential_store_directory.empty() ||
       config.api.listen_address.empty() || config.api.listen_port == 0 ||
       config.api.maximum_connections == 0 ||
       config.api.read_buffer_bytes == 0 ||
       config.api.read_buffer_bytes > 1024U * 1024U ||
       config.api.request_timeout <= core::Duration::zero() ||
       config.api.maximum_target_bytes == 0 ||
       config.api.maximum_header_bytes == 0 ||
       config.api.maximum_header_count == 0))
    return std::unexpected(
        invalid("Enabled API listener settings are invalid"));
  if (config.api.tls_certificate_chain_file.has_value() !=
      config.api.tls_private_key_file.has_value())
    return std::unexpected(invalid("API TLS certificate chain and private key "
                                   "must be configured together"));
  if ((config.api.tls_certificate_chain_file &&
       config.api.tls_certificate_chain_file->empty()) ||
      (config.api.tls_private_key_file &&
       config.api.tls_private_key_file->empty()))
    return std::unexpected(invalid("API TLS file paths must not be empty"));
  if (config.api.rate_limit.enabled &&
      (config.api.rate_limit.requests_per_window == 0 ||
       config.api.rate_limit.window <= core::Duration::zero()))
    return std::unexpected(invalid("Enabled API rate limit is invalid"));
  if (config.distributed.maximum_work_items == 0 ||
      config.distributed.maximum_work_items > 10'000'000 ||
      config.distributed.maximum_payload_bytes == 0 ||
      config.distributed.maximum_payload_bytes > 64U * 1024U * 1024U ||
      config.distributed.worker_timeout <= core::Duration::zero() ||
      config.distributed.lease_duration <= core::Duration::zero() ||
      config.distributed.heartbeat_interval <= core::Duration::zero() ||
      config.distributed.heartbeat_interval >=
          config.distributed.worker_timeout)
    return std::unexpected(
        invalid("Distributed work and heartbeat limits are invalid"));
  const auto &coordinator = config.distributed.coordinator;
  if (coordinator.listen_address.empty() ||
      coordinator.maximum_connections == 0 ||
      coordinator.maximum_connections > 1'000'000 ||
      coordinator.read_buffer_bytes == 0 ||
      coordinator.read_buffer_bytes > 1024U * 1024U ||
      coordinator.maximum_frame_bytes < 64 ||
      coordinator.maximum_frame_bytes > 64U * 1024U * 1024U ||
      config.distributed.maximum_payload_bytes >
          coordinator.maximum_frame_bytes - 64 ||
      coordinator.maximum_queued_write_bytes <
          coordinator.maximum_frame_bytes ||
      coordinator.maximum_queued_write_bytes > 64U * 1024U * 1024U ||
      coordinator.idle_timeout <= core::Duration::zero())
    return std::unexpected(
        invalid("Distributed coordinator listener limits are invalid"));
  for (const auto &endpoint : config.network.dht.bootstrap) {
    if (endpoint.empty())
      return std::unexpected(
          invalid("DHT bootstrap entries must not be empty"));
  }
  return {};
}

} // namespace sakuin::config

export module sakuin.config.model;

import std;

import sakuin.core.result;
import sakuin.core.time;

export namespace sakuin::config {

enum class DhtIdentityMode { Bep42, Fixed };
enum class StorageBackend { Local, S3 };
enum class CompressionCodec { None, Zstd };
enum class AdultContentPolicy { Include, Exclude, Only };
enum class ClassificationConfidence { Low, Medium, High };
enum class ClassificationRuleKind {
  Movie,
  Series,
  Music,
  Audiobook,
  Ebook,
  Game,
  Application
};
enum class ClassificationRuleMatch { Any, All };

struct ClassificationRuleConfig {
  std::string id;
  ClassificationRuleKind kind{ClassificationRuleKind::Movie};
  ClassificationRuleMatch match{ClassificationRuleMatch::All};
  std::vector<std::string> tokens;
  int weight{100};

  friend bool operator==(const ClassificationRuleConfig &,
                         const ClassificationRuleConfig &) = default;
};

struct DhtIdentityConfig {
  DhtIdentityMode mode{DhtIdentityMode::Bep42};
  std::size_t observation_quorum{3};
  core::Duration vote_window{std::chrono::minutes{10}};
  std::optional<std::string> fixed_ipv4_node_id;
  std::optional<std::string> fixed_ipv6_node_id;
};

struct MetadataDiscoveryBackfillConfig {
  bool enabled{true};
  std::size_t maximum_records_per_poll{256};
  core::Duration refresh_interval{std::chrono::minutes{1}};
  core::Duration full_rescan_interval{std::chrono::minutes{30}};
  core::Duration retry_delay{std::chrono::seconds{1}};
};

struct MetadataDiscoveryConfig {
  bool enabled{true};
  std::size_t maximum_pending{8'192};
  std::size_t maximum_in_flight{64};
  std::size_t parallelism_per_hash{3};
  std::size_t maximum_queries_per_hash{24};
  core::Duration retry_delay{std::chrono::minutes{5}};
  MetadataDiscoveryBackfillConfig backfill;
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
  MetadataDiscoveryConfig discovery;
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
  std::optional<std::filesystem::path> bootstrap_file;
  DhtIdentityConfig identity;
  RoutingMaintenanceConfig routing;
  MetadataAcquisitionConfig metadata;
};

struct PeriodicTrafficConfig {
  core::Duration window{std::chrono::hours{1}};
  std::optional<std::uint64_t> inbound_bytes;
  std::optional<std::uint64_t> outbound_bytes;
  std::uint64_t grant_bytes{64U * 1024U};
};

struct NetworkConfig {
  bool enable_ipv4{true};
  bool enable_ipv6{true};
  std::uint16_t listen_port{6881};
  DhtConfig dht;
  PeriodicTrafficConfig traffic;
};

struct StorageConfig {
  struct S3Config {
    std::string endpoint{"https://s3.amazonaws.com"};
    std::string bucket;
    std::string region{"us-east-1"};
    std::string prefix{"sakuin"};
    core::Duration connect_timeout{std::chrono::seconds{10}};
    core::Duration request_timeout{std::chrono::minutes{5}};
    std::size_t maximum_attempts{3};
    core::Duration retry_delay{std::chrono::milliseconds{200}};
    bool verify_tls{true};
  };

  struct MaintenanceConfig {
    bool enabled{true};
    core::Duration interval{std::chrono::minutes{15}};
    core::Duration verification_interval{std::chrono::hours{24}};
  };

  struct MaterializationConfig {
    bool enabled{true};
    core::Duration interval{std::chrono::minutes{1}};
  };

  struct RetentionConfig {
    // Deletion is opt-in. When enabled, only observation history is eligible;
    // canonical torrent metadata remains retained indefinitely.
    bool enabled{};
    core::Duration observation_cold_age{std::chrono::hours{24 * 30}};
    core::Duration observation_max_age{std::chrono::hours{24 * 365}};
    std::uint64_t cold_block_target_bytes{8U * 1024U * 1024U};
    CompressionCodec cold_compression{CompressionCodec::Zstd};
    int cold_compression_level{9};
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
  RetentionConfig retention;
  S3Config s3;
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

struct ClassificationConfig {
  bool enabled{true};
  // Learns content-kind vocabulary from this node's high-confidence
  // deterministic results and only fills metadata-complete Unknown/Ambiguous
  // cases. Labels and deterministic Classified results are never overridden.
  bool learned_fallback_enabled{true};
  bool adult_detection_enabled{true};
  // Classification only supplies a label. Search visibility is an explicit
  // operator policy and defaults to retaining every result.
  AdultContentPolicy adult_content_policy{AdultContentPolicy::Include};
  ClassificationConfidence adult_minimum_confidence{
      ClassificationConfidence::High};
  std::size_t maximum_files_to_inspect{100'000};
  std::size_t maximum_path_bytes{4'096};
  std::size_t maximum_tokens{16'384};
  std::vector<ClassificationRuleConfig> rules;
};

struct IndexingConfig {
  DuplicateIndexConfig duplicates;
  ClassificationConfig classification;
};

struct DistributedConfig {
  struct CoordinatorConfig {
    bool enabled{};
    bool recovery_enabled{true};
    std::optional<std::filesystem::path> recovery_file;
    std::size_t recovery_maximum_bytes{512U * 1024U * 1024U};
    // Zero selects one quarter of distributed.maximum_work_items.
    std::size_t maximum_terminal_work_items{};
    core::Duration terminal_work_retention{std::chrono::hours{24 * 7}};
    std::string listen_address{"127.0.0.1"};
    std::uint16_t listen_port{7100};
    std::size_t maximum_connections{256};
    std::size_t read_buffer_bytes{16U * 1024U};
    std::size_t maximum_frame_bytes{2U * 1024U * 1024U};
    std::size_t maximum_queued_write_bytes{4U * 1024U * 1024U};
    std::size_t maximum_result_reassembly_bytes{128U * 1024U * 1024U};
    std::size_t maximum_result_transfers{64};
    core::Duration result_transfer_timeout{std::chrono::minutes{2}};
    core::Duration idle_timeout{std::chrono::seconds{30}};
    std::optional<std::filesystem::path> tls_trust_anchor_file;
    std::optional<std::filesystem::path> tls_certificate_chain_file;
    std::optional<std::filesystem::path> tls_private_key_file;
  };

  struct WorkerConfig {
    bool enabled{};
    std::string id{"worker-1"};
    std::string coordinator_address{"127.0.0.1"};
    std::uint16_t coordinator_port{7100};
    std::size_t observation_batch_size{4'096};
    std::size_t read_buffer_bytes{16U * 1024U};
    std::size_t maximum_queued_write_bytes{4U * 1024U * 1024U};
    core::Duration connect_timeout{std::chrono::seconds{10}};
    core::Duration request_timeout{std::chrono::seconds{10}};
    core::Duration idle_timeout{std::chrono::seconds{30}};
    std::optional<std::filesystem::path> tls_trust_anchor_file;
    std::optional<std::filesystem::path> tls_certificate_chain_file;
    std::optional<std::filesystem::path> tls_private_key_file;
    std::string tls_server_name;
  };

  std::size_t maximum_work_items{65'536};
  std::size_t maximum_payload_bytes{1U * 1024U * 1024U};
  std::size_t maximum_result_bytes{8U * 1024U * 1024U};
  core::Duration worker_timeout{std::chrono::seconds{30}};
  core::Duration lease_duration{std::chrono::minutes{2}};
  core::Duration heartbeat_interval{std::chrono::seconds{10}};
  CoordinatorConfig coordinator;
  WorkerConfig worker;
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
  std::optional<std::vector<ClassificationRuleConfig>> classification_rules;
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

bool valid_rule_id(std::string_view value) {
  return !value.empty() && value.size() <= 64 &&
         std::ranges::all_of(value, [](unsigned char character) {
           return (character >= 'a' && character <= 'z') ||
                  (character >= 'A' && character <= 'Z') ||
                  (character >= '0' && character <= '9') || character == '_' ||
                  character == '-' || character == '.';
         });
}

bool valid_rule_token(std::string_view value) {
  return !value.empty() && value.size() <= 64 &&
         std::ranges::all_of(value, [](unsigned char character) {
           return (character >= 'a' && character <= 'z') ||
                  (character >= '0' && character <= '9');
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
    } else if (name == "network.dht.bootstrap_file") {
      config.network.dht.bootstrap_file = text;
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
    } else if (name == "network.dht.metadata.discovery.enabled") {
      auto value = boolean_value(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.network.dht.metadata.discovery.enabled = *value;
    } else if (name == "network.dht.metadata.discovery.maximum_pending") {
      auto value = unsigned_value<std::size_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.network.dht.metadata.discovery.maximum_pending = *value;
    } else if (name == "network.dht.metadata.discovery.maximum_in_flight") {
      auto value = unsigned_value<std::size_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.network.dht.metadata.discovery.maximum_in_flight = *value;
    } else if (name == "network.dht.metadata.discovery.parallelism_per_hash") {
      auto value = unsigned_value<std::size_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.network.dht.metadata.discovery.parallelism_per_hash = *value;
    } else if (name ==
               "network.dht.metadata.discovery.maximum_queries_per_hash") {
      auto value = unsigned_value<std::size_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.network.dht.metadata.discovery.maximum_queries_per_hash = *value;
    } else if (name == "network.dht.metadata.discovery.retry_delay_ms") {
      auto value = duration_ms(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.network.dht.metadata.discovery.retry_delay = *value;
    } else if (name == "network.dht.metadata.discovery.backfill.enabled") {
      auto value = boolean_value(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.network.dht.metadata.discovery.backfill.enabled = *value;
    } else if (name == "network.dht.metadata.discovery.backfill.maximum_"
                       "records_per_poll") {
      auto value = unsigned_value<std::size_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.network.dht.metadata.discovery.backfill.maximum_records_per_poll =
          *value;
    } else if (name ==
               "network.dht.metadata.discovery.backfill.refresh_interval_ms") {
      auto value = duration_ms(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.network.dht.metadata.discovery.backfill.refresh_interval = *value;
    } else if (name == "network.dht.metadata.discovery.backfill.full_rescan_"
                       "interval_ms") {
      auto value = duration_ms(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.network.dht.metadata.discovery.backfill.full_rescan_interval =
          *value;
    } else if (name ==
               "network.dht.metadata.discovery.backfill.retry_delay_ms") {
      auto value = duration_ms(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.network.dht.metadata.discovery.backfill.retry_delay = *value;
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
    } else if (name == "network.traffic.grant_bytes") {
      auto value = unsigned_value<std::uint64_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.network.traffic.grant_bytes = *value;
    } else if (name == "storage.backend") {
      if (text == "local")
        config.storage.backend = StorageBackend::Local;
      else if (text == "s3")
        config.storage.backend = StorageBackend::S3;
      else
        return std::unexpected(invalid("storage.backend must be local or s3"));
    } else if (name == "storage.local_root") {
      config.storage.local_root = text;
    } else if (name == "storage.s3.endpoint") {
      config.storage.s3.endpoint = text;
    } else if (name == "storage.s3.bucket") {
      config.storage.s3.bucket = text;
    } else if (name == "storage.s3.region") {
      config.storage.s3.region = text;
    } else if (name == "storage.s3.prefix") {
      config.storage.s3.prefix = text;
    } else if (name == "storage.s3.connect_timeout_ms") {
      auto value = duration_ms(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.storage.s3.connect_timeout = *value;
    } else if (name == "storage.s3.request_timeout_ms") {
      auto value = duration_ms(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.storage.s3.request_timeout = *value;
    } else if (name == "storage.s3.maximum_attempts") {
      auto value = unsigned_value<std::size_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.storage.s3.maximum_attempts = *value;
    } else if (name == "storage.s3.retry_delay_ms") {
      auto value = duration_ms(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.storage.s3.retry_delay = *value;
    } else if (name == "storage.s3.verify_tls") {
      auto value = boolean_value(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.storage.s3.verify_tls = *value;
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
    } else if (name == "storage.retention.enabled") {
      auto value = boolean_value(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.storage.retention.enabled = *value;
    } else if (name == "storage.retention.observation_cold_age_ms") {
      auto value = duration_ms(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.storage.retention.observation_cold_age = *value;
    } else if (name == "storage.retention.observation_max_age_ms") {
      auto value = duration_ms(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.storage.retention.observation_max_age = *value;
    } else if (name == "storage.retention.cold_block_target_bytes") {
      auto value = unsigned_value<std::uint64_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.storage.retention.cold_block_target_bytes = *value;
    } else if (name == "storage.retention.cold_compression_codec") {
      if (text == "none")
        config.storage.retention.cold_compression = CompressionCodec::None;
      else if (text == "zstd")
        config.storage.retention.cold_compression = CompressionCodec::Zstd;
      else
        return std::unexpected(invalid(
            "storage.retention.cold_compression_codec must be none or zstd"));
    } else if (name == "storage.retention.cold_compression_level") {
      auto value = integer_value(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.storage.retention.cold_compression_level = *value;
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
    } else if (name == "indexing.classification.enabled") {
      auto value = boolean_value(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.indexing.classification.enabled = *value;
    } else if (name == "indexing.classification.learned_fallback_enabled") {
      auto value = boolean_value(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.indexing.classification.learned_fallback_enabled = *value;
    } else if (name == "indexing.classification.adult_detection_enabled") {
      auto value = boolean_value(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.indexing.classification.adult_detection_enabled = *value;
    } else if (name == "indexing.classification.adult_content_policy") {
      if (text == "include")
        config.indexing.classification.adult_content_policy =
            AdultContentPolicy::Include;
      else if (text == "exclude")
        config.indexing.classification.adult_content_policy =
            AdultContentPolicy::Exclude;
      else if (text == "only")
        config.indexing.classification.adult_content_policy =
            AdultContentPolicy::Only;
      else
        return std::unexpected(invalid(
            "indexing.classification.adult_content_policy must be include, "
            "exclude, or only"));
    } else if (name == "indexing.classification.adult_minimum_confidence") {
      if (text == "low")
        config.indexing.classification.adult_minimum_confidence =
            ClassificationConfidence::Low;
      else if (text == "medium")
        config.indexing.classification.adult_minimum_confidence =
            ClassificationConfidence::Medium;
      else if (text == "high")
        config.indexing.classification.adult_minimum_confidence =
            ClassificationConfidence::High;
      else
        return std::unexpected(invalid(
            "indexing.classification.adult_minimum_confidence must be low, "
            "medium, or high"));
    } else if (name == "indexing.classification.maximum_files_to_inspect") {
      auto value = unsigned_value<std::size_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.indexing.classification.maximum_files_to_inspect = *value;
    } else if (name == "indexing.classification.maximum_path_bytes") {
      auto value = unsigned_value<std::size_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.indexing.classification.maximum_path_bytes = *value;
    } else if (name == "indexing.classification.maximum_tokens") {
      auto value = unsigned_value<std::size_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.indexing.classification.maximum_tokens = *value;
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
    } else if (name == "distributed.maximum_result_bytes") {
      auto value = unsigned_value<std::size_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.distributed.maximum_result_bytes = *value;
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
    } else if (name == "distributed.coordinator.recovery_enabled") {
      auto value = boolean_value(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.distributed.coordinator.recovery_enabled = *value;
    } else if (name == "distributed.coordinator.recovery_file") {
      config.distributed.coordinator.recovery_file = text;
    } else if (name == "distributed.coordinator.recovery_maximum_bytes") {
      auto value = unsigned_value<std::size_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.distributed.coordinator.recovery_maximum_bytes = *value;
    } else if (name == "distributed.coordinator.maximum_terminal_work_items") {
      auto value = unsigned_value<std::size_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.distributed.coordinator.maximum_terminal_work_items = *value;
    } else if (name == "distributed.coordinator.terminal_work_retention_ms") {
      auto value = duration_ms(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.distributed.coordinator.terminal_work_retention = *value;
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
    } else if (name ==
               "distributed.coordinator.maximum_result_reassembly_bytes") {
      auto value = unsigned_value<std::size_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.distributed.coordinator.maximum_result_reassembly_bytes = *value;
    } else if (name == "distributed.coordinator.maximum_result_transfers") {
      auto value = unsigned_value<std::size_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.distributed.coordinator.maximum_result_transfers = *value;
    } else if (name == "distributed.coordinator.result_transfer_timeout_ms") {
      auto value = duration_ms(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.distributed.coordinator.result_transfer_timeout = *value;
    } else if (name == "distributed.coordinator.idle_timeout_ms") {
      auto value = duration_ms(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.distributed.coordinator.idle_timeout = *value;
    } else if (name == "distributed.coordinator.tls_trust_anchor_file") {
      config.distributed.coordinator.tls_trust_anchor_file = text;
    } else if (name == "distributed.coordinator.tls_certificate_chain_file") {
      config.distributed.coordinator.tls_certificate_chain_file = text;
    } else if (name == "distributed.coordinator.tls_private_key_file") {
      config.distributed.coordinator.tls_private_key_file = text;
    } else if (name == "distributed.worker.enabled") {
      auto value = boolean_value(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.distributed.worker.enabled = *value;
    } else if (name == "distributed.worker.id") {
      config.distributed.worker.id = text;
    } else if (name == "distributed.worker.coordinator_address") {
      config.distributed.worker.coordinator_address = text;
    } else if (name == "distributed.worker.coordinator_port") {
      auto value = unsigned_value<std::uint16_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.distributed.worker.coordinator_port = *value;
    } else if (name == "distributed.worker.observation_batch_size") {
      auto value = unsigned_value<std::size_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.distributed.worker.observation_batch_size = *value;
    } else if (name == "distributed.worker.read_buffer_bytes") {
      auto value = unsigned_value<std::size_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.distributed.worker.read_buffer_bytes = *value;
    } else if (name == "distributed.worker.maximum_queued_write_bytes") {
      auto value = unsigned_value<std::size_t>(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.distributed.worker.maximum_queued_write_bytes = *value;
    } else if (name == "distributed.worker.connect_timeout_ms") {
      auto value = duration_ms(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.distributed.worker.connect_timeout = *value;
    } else if (name == "distributed.worker.request_timeout_ms") {
      auto value = duration_ms(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.distributed.worker.request_timeout = *value;
    } else if (name == "distributed.worker.idle_timeout_ms") {
      auto value = duration_ms(text, name);
      if (!value)
        return std::unexpected(value.error());
      config.distributed.worker.idle_timeout = *value;
    } else if (name == "distributed.worker.tls_trust_anchor_file") {
      config.distributed.worker.tls_trust_anchor_file = text;
    } else if (name == "distributed.worker.tls_certificate_chain_file") {
      config.distributed.worker.tls_certificate_chain_file = text;
    } else if (name == "distributed.worker.tls_private_key_file") {
      config.distributed.worker.tls_private_key_file = text;
    } else if (name == "distributed.worker.tls_server_name") {
      config.distributed.worker.tls_server_name = text;
    } else {
      return std::unexpected(invalid("Unknown configuration key: " + name));
    }
  }
  if (overlay.bootstrap)
    config.network.dht.bootstrap = *overlay.bootstrap;
  if (overlay.classification_rules)
    config.indexing.classification.rules = *overlay.classification_rules;
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
  const auto &metadata_discovery = metadata.discovery;
  if (metadata_discovery.maximum_pending == 0 ||
      metadata_discovery.maximum_pending > 1'000'000 ||
      metadata_discovery.maximum_in_flight == 0 ||
      metadata_discovery.maximum_in_flight >
          config.network.dht.maximum_in_flight ||
      metadata_discovery.parallelism_per_hash == 0 ||
      metadata_discovery.parallelism_per_hash >
          metadata_discovery.maximum_in_flight ||
      metadata_discovery.maximum_queries_per_hash <
          metadata_discovery.parallelism_per_hash ||
      metadata_discovery.maximum_queries_per_hash > 4'096 ||
      metadata_discovery.retry_delay <= core::Duration::zero())
    return std::unexpected(
        invalid("DHT active metadata-discovery limits are invalid"));
  const auto &metadata_backfill = metadata_discovery.backfill;
  if (metadata_backfill.maximum_records_per_poll == 0 ||
      metadata_backfill.maximum_records_per_poll > 1'000'000 ||
      metadata_backfill.refresh_interval <= core::Duration::zero() ||
      metadata_backfill.full_rescan_interval <= core::Duration::zero() ||
      metadata_backfill.retry_delay <= core::Duration::zero())
    return std::unexpected(
        invalid("DHT metadata-discovery backfill limits are invalid"));
  if (config.network.traffic.window <= core::Duration::zero() ||
      (config.network.traffic.inbound_bytes == 0) ||
      (config.network.traffic.outbound_bytes == 0) ||
      config.network.traffic.grant_bytes == 0 ||
      config.network.traffic.grant_bytes > 16U * 1024U * 1024U)
    return std::unexpected(invalid(
        "Configured traffic budgets and their window must be positive"));
  if (config.storage.local_root.empty() ||
      config.storage.block_target_bytes == 0 ||
      config.storage.segment_target_bytes == 0 ||
      config.storage.block_target_bytes > config.storage.segment_target_bytes)
    return std::unexpected(
        invalid("Storage paths and size targets are invalid"));
  if (config.storage.backend == StorageBackend::S3 &&
      ((!config.storage.s3.endpoint.starts_with("https://") &&
        !config.storage.s3.endpoint.starts_with("http://")) ||
       config.storage.s3.bucket.empty() || config.storage.s3.region.empty() ||
       config.storage.s3.connect_timeout <= core::Duration::zero() ||
       config.storage.s3.request_timeout <= core::Duration::zero() ||
       config.storage.s3.maximum_attempts == 0 ||
       config.storage.s3.maximum_attempts > 10 ||
       config.storage.s3.retry_delay < core::Duration::zero()))
    return std::unexpected(invalid(
        "S3 storage requires an HTTP(S) endpoint, bucket, region, and positive "
        "timeouts and one to ten request attempts"));
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
  if (config.storage.retention.enabled &&
      (config.storage.retention.observation_cold_age <=
           core::Duration::zero() ||
       config.storage.retention.observation_max_age <=
           config.storage.retention.observation_cold_age ||
       config.storage.retention.cold_block_target_bytes < 4U * 1024U ||
       config.storage.retention.cold_block_target_bytes >
           config.storage.segment_target_bytes ||
       config.storage.retention.cold_block_target_bytes >
           std::numeric_limits<std::uint32_t>::max() ||
       config.storage.retention.cold_compression_level < -131'072 ||
       config.storage.retention.cold_compression_level > 22))
    return std::unexpected(
        invalid("Observation retention and COLD storage settings are invalid"));
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
  if (config.indexing.classification.maximum_files_to_inspect == 0 ||
      config.indexing.classification.maximum_files_to_inspect > 1'000'000 ||
      config.indexing.classification.maximum_path_bytes == 0 ||
      config.indexing.classification.maximum_path_bytes > 1024U * 1024U ||
      config.indexing.classification.maximum_tokens == 0 ||
      config.indexing.classification.maximum_tokens > 1'000'000)
    return std::unexpected(
        invalid("Classification inspection limits are invalid"));
  const auto &classification_rules = config.indexing.classification.rules;
  if (classification_rules.size() > 128)
    return std::unexpected(
        invalid("At most 128 operator classification rules are allowed"));
  std::set<std::string_view> classification_rule_ids;
  for (const auto &rule : classification_rules) {
    if (!valid_rule_id(rule.id) || rule.tokens.empty() ||
        rule.tokens.size() > 32 || rule.weight <= 0 || rule.weight > 200 ||
        !std::ranges::all_of(rule.tokens, valid_rule_token) ||
        !classification_rule_ids.insert(rule.id).second)
      return std::unexpected(invalid(
          "Operator classification rules require unique bounded IDs, one to "
          "32 lowercase alphanumeric tokens, and weights from 1 to 200"));
  }
  if (config.indexing.classification.adult_content_policy !=
          AdultContentPolicy::Include &&
      (!config.indexing.classification.enabled ||
       !config.indexing.classification.adult_detection_enabled))
    return std::unexpected(
        invalid("Adult exclude/only policy requires classification and adult "
                "detection to be enabled"));
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
      config.distributed.maximum_result_bytes <
          config.distributed.maximum_payload_bytes ||
      config.distributed.maximum_result_bytes > 64U * 1024U * 1024U ||
      config.distributed.worker_timeout <= core::Duration::zero() ||
      config.distributed.lease_duration <= core::Duration::zero() ||
      config.distributed.heartbeat_interval <= core::Duration::zero() ||
      config.distributed.heartbeat_interval >=
          config.distributed.worker_timeout)
    return std::unexpected(
        invalid("Distributed work and heartbeat limits are invalid"));
  const auto &coordinator = config.distributed.coordinator;
  if (coordinator.listen_address.empty() ||
      (coordinator.recovery_enabled &&
       (coordinator.recovery_maximum_bytes < 4U * 1024U ||
        coordinator.recovery_maximum_bytes > 16ULL * 1024U * 1024U * 1024U ||
        (coordinator.recovery_file && coordinator.recovery_file->empty()))) ||
      (coordinator.maximum_terminal_work_items != 0 &&
       coordinator.maximum_terminal_work_items >
           config.distributed.maximum_work_items) ||
      coordinator.terminal_work_retention < std::chrono::seconds{1} ||
      coordinator.terminal_work_retention > std::chrono::hours{24 * 365} ||
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
      coordinator.maximum_result_reassembly_bytes <
          config.distributed.maximum_result_bytes ||
      coordinator.maximum_result_reassembly_bytes >
          4ULL * 1024U * 1024U * 1024U ||
      coordinator.maximum_result_transfers == 0 ||
      coordinator.maximum_result_transfers > 65'536 ||
      coordinator.result_transfer_timeout < std::chrono::seconds{1} ||
      coordinator.result_transfer_timeout > std::chrono::hours{24} ||
      coordinator.idle_timeout <= core::Duration::zero())
    return std::unexpected(
        invalid("Distributed coordinator listener limits are invalid"));
  const auto tls_paths =
      static_cast<unsigned>(coordinator.tls_trust_anchor_file.has_value()) +
      static_cast<unsigned>(
          coordinator.tls_certificate_chain_file.has_value()) +
      static_cast<unsigned>(coordinator.tls_private_key_file.has_value());
  if (tls_paths != 0 && tls_paths != 3)
    return std::unexpected(invalid(
        "Distributed coordinator mTLS trust anchor, certificate chain, and "
        "private key must be configured together"));
  if ((coordinator.tls_trust_anchor_file &&
       coordinator.tls_trust_anchor_file->empty()) ||
      (coordinator.tls_certificate_chain_file &&
       coordinator.tls_certificate_chain_file->empty()) ||
      (coordinator.tls_private_key_file &&
       coordinator.tls_private_key_file->empty()))
    return std::unexpected(
        invalid("Distributed coordinator mTLS paths must not be empty"));
  const auto &worker = config.distributed.worker;
  const auto valid_worker_id =
      !worker.id.empty() && worker.id.size() <= 128 &&
      std::ranges::all_of(worker.id, [](unsigned char value) {
        return std::isalnum(value) || value == '-' || value == '_' ||
               value == '.' || value == ':';
      });
  if (worker.enabled &&
      (!valid_worker_id || worker.coordinator_address.empty() ||
       worker.coordinator_port == 0 || worker.observation_batch_size == 0 ||
       worker.observation_batch_size > 65'536 ||
       config.distributed.maximum_result_bytes <
           config.network.dht.metadata.maximum_metadata_bytes ||
       config.distributed.maximum_payload_bytes <= 12 ||
       worker.observation_batch_size >
           (config.distributed.maximum_payload_bytes - 12) / 32 ||
       worker.read_buffer_bytes == 0 ||
       worker.read_buffer_bytes > 1024U * 1024U ||
       worker.maximum_queued_write_bytes < coordinator.maximum_frame_bytes ||
       worker.maximum_queued_write_bytes > 64U * 1024U * 1024U ||
       worker.connect_timeout <= core::Duration::zero() ||
       worker.request_timeout <= core::Duration::zero() ||
       worker.idle_timeout <= core::Duration::zero()))
    return std::unexpected(invalid("Distributed worker settings are invalid"));
  const auto worker_tls_paths =
      static_cast<unsigned>(worker.tls_trust_anchor_file.has_value()) +
      static_cast<unsigned>(worker.tls_certificate_chain_file.has_value()) +
      static_cast<unsigned>(worker.tls_private_key_file.has_value());
  if (worker_tls_paths != 0 && worker_tls_paths != 3)
    return std::unexpected(
        invalid("Distributed worker mTLS trust anchor, certificate chain, and "
                "private key must be configured together"));
  if (worker.enabled &&
      (worker_tls_paths != 3 || worker.tls_server_name.empty()))
    return std::unexpected(invalid(
        "Enabled distributed worker requires mTLS trust, certificate, key, "
        "and server name"));
  if ((worker.tls_trust_anchor_file && worker.tls_trust_anchor_file->empty()) ||
      (worker.tls_certificate_chain_file &&
       worker.tls_certificate_chain_file->empty()) ||
      (worker.tls_private_key_file && worker.tls_private_key_file->empty()))
    return std::unexpected(
        invalid("Distributed worker mTLS paths must not be empty"));
  for (const auto &endpoint : config.network.dht.bootstrap) {
    if (endpoint.empty())
      return std::unexpected(
          invalid("DHT bootstrap entries must not be empty"));
  }
  if (config.network.dht.bootstrap_file &&
      config.network.dht.bootstrap_file->empty())
    return std::unexpected(
        invalid("network.dht.bootstrap_file must not be empty"));
  return {};
}

} // namespace sakuin::config

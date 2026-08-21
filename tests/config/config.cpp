import std;

import sakuin.config;
import sakuin.core;

int main() {
  using namespace sakuin;
  constexpr std::string_view source = R"toml(
[network]
enable_ipv4 = true
enable_ipv6 = false
listen_port = 7000

[network.dht]
maximum_in_flight = 64
query_timeout_ms = 12000
bootstrap = ["router.example:6881", "router6.example:6881"]

[network.dht.identity]
mode = "bep42"
observation_quorum = 4
vote_window_ms = 300000

[network.dht.routing]
maximum_queued = 512
maximum_in_flight = 6
maximum_attempts = 4
retry_delay_ms = 2500

[network.dht.metadata]
maximum_in_flight = 32
maximum_queued = 2048
maximum_attempts_per_peer = 4
initial_retry_delay_ms = 15000
maximum_retry_delay_ms = 600000
connect_timeout_ms = 7000
idle_timeout_ms = 20000
maximum_metadata_bytes = 2097152
maximum_outstanding_requests = 8
maximum_queued_write_bytes = 524288
storage_conflict_attempts = 5
storage_retry_delay_ms = 250

[network.traffic]
window_ms = 3600000
outbound_bytes = 1000000

[storage]
backend = "local"
local_root = "/srv/sakuin"
block_target_bytes = 1048576
segment_target_bytes = 67108864

[storage.compression]
codec = "zstd"
level = 5

[storage.compaction]
minimum_segments = 6

[storage.maintenance]
enabled = true
interval_ms = 600000
verification_interval_ms = 43200000

[storage.materialization]
enabled = true
interval_ms = 30000

[indexing.duplicates]
enabled = true
interval_ms = 120000

[api]
credential_store_directory = "/srv/sakuin/api-credentials"
listen_address = "::1"
listen_port = 9000
maximum_connections = 64
request_timeout_ms = 5000

[api.rate_limit]
requests_per_window = 30
window_ms = 10000

[distributed]
maximum_work_items = 8192
maximum_payload_bytes = 524288
worker_timeout_ms = 20000
lease_duration_ms = 60000
heartbeat_interval_ms = 5000
)toml";

  auto file = config::parse_toml(source, "test.toml");
  if (!file)
    return 1;
  const std::array environment{
      std::pair{std::string{"SAKUIN_NETWORK_LISTEN_PORT"}, std::string{"7100"}},
      std::pair{std::string{"SAKUIN_API_LISTEN_PORT"}, std::string{"9001"}},
      std::pair{std::string{"SAKUIN_API_CREDENTIAL_STORE_DIRECTORY"},
                std::string{"/var/lib/sakuin/api-credentials"}},
      std::pair{std::string{"SAKUIN_DHT_METADATA_MAXIMUM_IN_FLIGHT"},
                std::string{"48"}},
      std::pair{std::string{"SAKUIN_DHT_ROUTING_MAXIMUM_QUEUED"},
                std::string{"768"}},
      std::pair{std::string{"SAKUIN_DHT_BOOTSTRAP_MAXIMUM_ATTEMPTS"},
                std::string{"7"}},
      std::pair{std::string{"SAKUIN_DHT_BOOTSTRAP_MAXIMUM_IN_FLIGHT"},
                std::string{"5"}},
      std::pair{std::string{"SAKUIN_DHT_BOOTSTRAP_RETRY_DELAY_MS"},
                std::string{"9000"}},
      std::pair{std::string{"SAKUIN_DHT_IDENTITY_FIXED_IPV4_NODE_ID"},
                std::string(40, 'a')},
      std::pair{std::string{"SAKUIN_DHT_IDENTITY_FIXED_IPV6_NODE_ID"},
                std::string(40, 'b')},
      std::pair{std::string{"SAKUIN_STORAGE_COMPACTION_MINIMUM_SEGMENTS"},
                std::string{"8"}},
      std::pair{std::string{"SAKUIN_STORAGE_MAINTENANCE_INTERVAL_MS"},
                std::string{"300000"}},
      std::pair{std::string{"SAKUIN_STORAGE_MATERIALIZATION_INTERVAL_MS"},
                std::string{"15000"}},
      std::pair{std::string{"SAKUIN_DUPLICATE_INDEX_INTERVAL_MS"},
                std::string{"60000"}},
      std::pair{std::string{"SAKUIN_DISTRIBUTED_MAXIMUM_WORK_ITEMS"},
                std::string{"16384"}},
      std::pair{std::string{"UNRELATED"}, std::string{"ignored"}}};
  auto env = config::environment_overlay(environment);
  if (!env)
    return 2;
  constexpr std::array cli{
      std::string_view{"--network.dht.maximum_in_flight=256"}};
  auto command_line = config::command_line_overlay(cli);
  if (!command_line)
    return 3;

  auto loaded = config::defaults();
  for (const auto *overlay : {&*file, &*env, &*command_line})
    if (auto applied = config::apply(loaded, *overlay); !applied)
      return 4;
  if (auto valid = config::validate(loaded); !valid)
    return 5;
  if (loaded.network.enable_ipv6 || loaded.network.listen_port != 7100 ||
      loaded.network.dht.maximum_in_flight != 256 ||
      loaded.network.dht.metadata.maximum_in_flight != 48 ||
      loaded.network.dht.metadata.maximum_queued != 2048 ||
      loaded.network.dht.metadata.maximum_attempts_per_peer != 4 ||
      loaded.network.dht.metadata.initial_retry_delay !=
          std::chrono::seconds{15} ||
      loaded.network.dht.metadata.maximum_retry_delay !=
          std::chrono::minutes{10} ||
      loaded.network.dht.metadata.connect_timeout != std::chrono::seconds{7} ||
      loaded.network.dht.metadata.maximum_metadata_bytes !=
          2U * 1024U * 1024U ||
      loaded.network.dht.metadata.maximum_outstanding_requests != 8 ||
      loaded.network.dht.metadata.storage_conflict_attempts != 5 ||
      loaded.network.dht.metadata.storage_retry_delay !=
          std::chrono::milliseconds{250} ||
      loaded.network.dht.identity.observation_quorum != 4 ||
      loaded.network.dht.routing.maximum_queued != 768 ||
      loaded.network.dht.routing.maximum_in_flight != 6 ||
      loaded.network.dht.routing.maximum_attempts != 4 ||
      loaded.network.dht.routing.retry_delay !=
          std::chrono::milliseconds{2500} ||
      loaded.network.dht.bootstrap_maximum_attempts != 7 ||
      loaded.network.dht.bootstrap_maximum_in_flight != 5 ||
      loaded.network.dht.bootstrap_retry_delay != std::chrono::seconds{9} ||
      loaded.network.dht.identity.fixed_ipv4_node_id != std::string(40, 'a') ||
      loaded.network.dht.identity.fixed_ipv6_node_id != std::string(40, 'b') ||
      loaded.network.dht.bootstrap.size() != 2 ||
      loaded.storage.local_root != "/srv/sakuin" ||
      loaded.storage.compression_level != 5 ||
      loaded.storage.compaction_minimum_segments != 8 ||
      !loaded.storage.maintenance.enabled ||
      loaded.storage.maintenance.interval != std::chrono::minutes{5} ||
      loaded.storage.maintenance.verification_interval !=
          std::chrono::hours{12} ||
      !loaded.storage.materialization.enabled ||
      loaded.storage.materialization.interval != std::chrono::seconds{15} ||
      !loaded.indexing.duplicates.enabled ||
      loaded.indexing.duplicates.interval != std::chrono::minutes{1} ||
      loaded.api.credential_store_directory !=
          "/var/lib/sakuin/api-credentials" ||
      loaded.api.listen_address != "::1" || loaded.api.listen_port != 9001 ||
      loaded.api.maximum_connections != 64 ||
      loaded.api.rate_limit.requests_per_window != 30 ||
      loaded.api.rate_limit.window != std::chrono::seconds{10} ||
      loaded.distributed.maximum_work_items != 16'384 ||
      loaded.distributed.maximum_payload_bytes != 512U * 1024U ||
      loaded.distributed.worker_timeout != std::chrono::seconds{20} ||
      loaded.distributed.lease_duration != std::chrono::minutes{1} ||
      loaded.distributed.heartbeat_interval != std::chrono::seconds{5})
    return 6;

  if (config::parse_toml("[network]\nlistne_port = 1") ||
      config::command_line_overlay(std::array{std::string_view{"bad"}}))
    return 7;

  config::ConfigOverlay invalid;
  invalid.scalars["network.enable_ipv4"] = "false";
  invalid.scalars["network.enable_ipv6"] = "false";
  auto invalid_config = config::defaults();
  if (!config::apply(invalid_config, invalid) ||
      config::validate(invalid_config))
    return 8;
  config::ConfigOverlay incomplete_tls;
  incomplete_tls.scalars["api.tls_certificate_chain_file"] = "chain.pem";
  auto tls_config = config::defaults();
  if (!config::apply(tls_config, incomplete_tls) ||
      config::validate(tls_config))
    return 9;
  auto invalid_distributed = config::defaults();
  invalid_distributed.distributed.heartbeat_interval =
      invalid_distributed.distributed.worker_timeout;
  if (config::validate(invalid_distributed))
    return 10;
  auto invalid_compression = config::defaults();
  invalid_compression.storage.compression_level = 23;
  if (config::validate(invalid_compression))
    return 11;
  return 0;
}

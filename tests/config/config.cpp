import std;

import sakuin.config;
import sakuin.core;

int main(int argument_count, char **arguments) {
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
bootstrap_file = "bootstrap.txt"

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

[network.dht.metadata.discovery]
enabled = true
maximum_pending = 4096
maximum_in_flight = 12
parallelism_per_hash = 4
maximum_queries_per_hash = 32
retry_delay_ms = 120000

[network.dht.metadata.discovery.backfill]
enabled = true
maximum_records_per_poll = 128
refresh_interval_ms = 45000
full_rescan_interval_ms = 900000
retry_delay_ms = 750

[network.traffic]
window_ms = 3600000
outbound_bytes = 1000000
grant_bytes = 32768

[storage]
backend = "local"
local_root = "/srv/sakuin"
block_target_bytes = 1048576
segment_target_bytes = 67108864

[storage.s3]
endpoint = "https://objects.example.test"
bucket = "sakuin-data"
region = "us-test-1"
prefix = "production/catalog"
connect_timeout_ms = 4000
request_timeout_ms = 120000
maximum_attempts = 4
retry_delay_ms = 350
verify_tls = true

[storage.compression]
codec = "zstd"
level = 5

[storage.compaction]
minimum_segments = 6
maximum_warm_segments = 3
warm_block_target_bytes = 131072

[storage.retention]
enabled = true
observation_cold_age_ms = 2592000000
observation_max_age_ms = 31536000000
cold_block_target_bytes = 4194304
cold_compression_codec = "zstd"
cold_compression_level = 10

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

[indexing.classification]
enabled = true
learned_fallback_enabled = true
adult_detection_enabled = true
adult_content_policy = "only"
adult_minimum_confidence = "medium"
maximum_files_to_inspect = 50000
maximum_path_bytes = 2048
maximum_tokens = 8192

[[indexing.classification.rules]]
id = "workbench-tools"
kind = "application"
match = "all"
tokens = ["workbench", "tool"]
weight = 110

[[indexing.classification.rules]]
id = "episodic-groups"
kind = "series"
match = "any"
tokens = ["examplegroup", "anothergroup"]

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
maximum_result_bytes = 4194304
worker_timeout_ms = 20000
lease_duration_ms = 60000
heartbeat_interval_ms = 5000

[distributed.coordinator]
enabled = true
recovery_enabled = true
recovery_file = "/srv/sakuin/operational/custom-work.checkpoint"
recovery_maximum_bytes = 268435456
maximum_terminal_work_items = 4096
terminal_work_retention_ms = 259200000
listen_address = "127.0.0.1"
listen_port = 7101
maximum_connections = 32
read_buffer_bytes = 8192
maximum_frame_bytes = 1048576
maximum_queued_write_bytes = 2097152
maximum_result_reassembly_bytes = 33554432
maximum_result_transfers = 8
result_transfer_timeout_ms = 30000
idle_timeout_ms = 15000
tls_trust_anchor_file = "/etc/sakuin/workers/ca.pem"
tls_certificate_chain_file = "/etc/sakuin/workers/coordinator.pem"
tls_private_key_file = "/etc/sakuin/workers/coordinator-key.pem"

[distributed.worker]
enabled = true
id = "crawler-a"
coordinator_address = "coordinator.internal"
coordinator_port = 7102
observation_batch_size = 2048
read_buffer_bytes = 4096
maximum_queued_write_bytes = 2097152
connect_timeout_ms = 8000
request_timeout_ms = 9000
idle_timeout_ms = 14000
tls_trust_anchor_file = "/etc/sakuin/coordinator/ca.pem"
tls_certificate_chain_file = "/etc/sakuin/workers/crawler-a.pem"
tls_private_key_file = "/etc/sakuin/workers/crawler-a-key.pem"
tls_server_name = "coordinator.internal"
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
      std::pair{std::string{"SAKUIN_DHT_METADATA_DISCOVERY_MAXIMUM_IN_FLIGHT"},
                std::string{"10"}},
      std::pair{
          std::string{"SAKUIN_DHT_METADATA_DISCOVERY_BACKFILL_MAXIMUM_RECORDS_"
                      "PER_POLL"},
          std::string{"192"}},
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
      std::pair{std::string{"SAKUIN_TRAFFIC_GRANT_BYTES"},
                std::string{"131072"}},
      std::pair{std::string{"SAKUIN_STORAGE_COMPACTION_MINIMUM_SEGMENTS"},
                std::string{"8"}},
      std::pair{std::string{"SAKUIN_STORAGE_S3_BUCKET"},
                std::string{"sakuin-data-override"}},
      std::pair{std::string{"SAKUIN_STORAGE_S3_MAXIMUM_ATTEMPTS"},
                std::string{"5"}},
      std::pair{std::string{"SAKUIN_STORAGE_S3_RETRY_DELAY_MS"},
                std::string{"450"}},
      std::pair{std::string{"SAKUIN_STORAGE_COMPACTION_MAXIMUM_WARM_SEGMENTS"},
                std::string{"5"}},
      std::pair{
          std::string{"SAKUIN_STORAGE_COMPACTION_WARM_BLOCK_TARGET_BYTES"},
          std::string{"65536"}},
      std::pair{std::string{"SAKUIN_STORAGE_RETENTION_OBSERVATION_COLD_AGE_MS"},
                std::string{"1209600000"}},
      std::pair{std::string{"SAKUIN_STORAGE_RETENTION_COLD_BLOCK_TARGET_BYTES"},
                std::string{"2097152"}},
      std::pair{std::string{"SAKUIN_STORAGE_MAINTENANCE_INTERVAL_MS"},
                std::string{"300000"}},
      std::pair{std::string{"SAKUIN_STORAGE_MATERIALIZATION_INTERVAL_MS"},
                std::string{"15000"}},
      std::pair{std::string{"SAKUIN_DUPLICATE_INDEX_INTERVAL_MS"},
                std::string{"60000"}},
      std::pair{std::string{"SAKUIN_CLASSIFICATION_ADULT_CONTENT_POLICY"},
                std::string{"exclude"}},
      std::pair{std::string{"SAKUIN_CLASSIFICATION_LEARNED_FALLBACK_ENABLED"},
                std::string{"false"}},
      std::pair{std::string{"SAKUIN_CLASSIFICATION_ADULT_MINIMUM_CONFIDENCE"},
                std::string{"low"}},
      std::pair{std::string{"SAKUIN_DISTRIBUTED_MAXIMUM_WORK_ITEMS"},
                std::string{"16384"}},
      std::pair{std::string{"SAKUIN_DISTRIBUTED_COORDINATOR_LISTEN_PORT"},
                std::string{"7201"}},
      std::pair{
          std::string{"SAKUIN_DISTRIBUTED_COORDINATOR_RECOVERY_MAXIMUM_BYTES"},
          std::string{"134217728"}},
      std::pair{
          std::string{
              "SAKUIN_DISTRIBUTED_COORDINATOR_MAXIMUM_TERMINAL_WORK_ITEMS"},
          std::string{"8192"}},
      std::pair{
          std::string{
              "SAKUIN_DISTRIBUTED_COORDINATOR_TERMINAL_WORK_RETENTION_MS"},
          std::string{"345600000"}},
      std::pair{
          std::string{
              "SAKUIN_DISTRIBUTED_COORDINATOR_MAXIMUM_RESULT_REASSEMBLY_BYTES"},
          std::string{"67108864"}},
      std::pair{std::string{"SAKUIN_DISTRIBUTED_WORKER_OBSERVATION_BATCH_SIZE"},
                std::string{"1024"}},
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
      !loaded.network.dht.metadata.discovery.enabled ||
      loaded.network.dht.metadata.discovery.maximum_pending != 4096 ||
      loaded.network.dht.metadata.discovery.maximum_in_flight != 10 ||
      loaded.network.dht.metadata.discovery.parallelism_per_hash != 4 ||
      loaded.network.dht.metadata.discovery.maximum_queries_per_hash != 32 ||
      loaded.network.dht.metadata.discovery.retry_delay !=
          std::chrono::minutes{2} ||
      !loaded.network.dht.metadata.discovery.backfill.enabled ||
      loaded.network.dht.metadata.discovery.backfill.maximum_records_per_poll !=
          192 ||
      loaded.network.dht.metadata.discovery.backfill.refresh_interval !=
          std::chrono::seconds{45} ||
      loaded.network.dht.metadata.discovery.backfill.full_rescan_interval !=
          std::chrono::minutes{15} ||
      loaded.network.dht.metadata.discovery.backfill.retry_delay !=
          std::chrono::milliseconds{750} ||
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
      loaded.network.dht.bootstrap_file != "bootstrap.txt" ||
      loaded.network.traffic.grant_bytes != 131072 ||
      loaded.storage.local_root != "/srv/sakuin" ||
      loaded.storage.s3.endpoint != "https://objects.example.test" ||
      loaded.storage.s3.bucket != "sakuin-data-override" ||
      loaded.storage.s3.region != "us-test-1" ||
      loaded.storage.s3.prefix != "production/catalog" ||
      loaded.storage.s3.connect_timeout != std::chrono::seconds{4} ||
      loaded.storage.s3.request_timeout != std::chrono::minutes{2} ||
      loaded.storage.s3.maximum_attempts != 5 ||
      loaded.storage.s3.retry_delay != std::chrono::milliseconds{450} ||
      !loaded.storage.s3.verify_tls || loaded.storage.compression_level != 5 ||
      loaded.storage.compaction_minimum_segments != 8 ||
      loaded.storage.compaction_maximum_warm_segments != 5 ||
      loaded.storage.compaction_warm_block_target_bytes != 65536 ||
      !loaded.storage.retention.enabled ||
      loaded.storage.retention.observation_cold_age !=
          std::chrono::hours{24 * 14} ||
      loaded.storage.retention.observation_max_age !=
          std::chrono::hours{24 * 365} ||
      loaded.storage.retention.cold_block_target_bytes != 2U * 1024U * 1024U ||
      loaded.storage.retention.cold_compression !=
          config::CompressionCodec::Zstd ||
      loaded.storage.retention.cold_compression_level != 10 ||
      !loaded.storage.maintenance.enabled ||
      loaded.storage.maintenance.interval != std::chrono::minutes{5} ||
      loaded.storage.maintenance.verification_interval !=
          std::chrono::hours{12} ||
      !loaded.storage.materialization.enabled ||
      loaded.storage.materialization.interval != std::chrono::seconds{15} ||
      !loaded.indexing.duplicates.enabled ||
      loaded.indexing.duplicates.interval != std::chrono::minutes{1} ||
      !loaded.indexing.classification.enabled ||
      loaded.indexing.classification.learned_fallback_enabled ||
      !loaded.indexing.classification.adult_detection_enabled ||
      loaded.indexing.classification.adult_content_policy !=
          config::AdultContentPolicy::Exclude ||
      loaded.indexing.classification.adult_minimum_confidence !=
          config::ClassificationConfidence::Low ||
      loaded.indexing.classification.maximum_files_to_inspect != 50'000 ||
      loaded.indexing.classification.maximum_path_bytes != 2'048 ||
      loaded.indexing.classification.maximum_tokens != 8'192 ||
      loaded.indexing.classification.rules.size() != 2 ||
      loaded.indexing.classification.rules[0].id != "workbench-tools" ||
      loaded.indexing.classification.rules[0].kind !=
          config::ClassificationRuleKind::Application ||
      loaded.indexing.classification.rules[0].match !=
          config::ClassificationRuleMatch::All ||
      loaded.indexing.classification.rules[0].tokens !=
          std::vector<std::string>{"workbench", "tool"} ||
      loaded.indexing.classification.rules[0].weight != 110 ||
      loaded.indexing.classification.rules[1].match !=
          config::ClassificationRuleMatch::Any ||
      loaded.indexing.classification.rules[1].weight != 100 ||
      loaded.api.credential_store_directory !=
          "/var/lib/sakuin/api-credentials" ||
      loaded.api.listen_address != "::1" || loaded.api.listen_port != 9001 ||
      loaded.api.maximum_connections != 64 ||
      loaded.api.rate_limit.requests_per_window != 30 ||
      loaded.api.rate_limit.window != std::chrono::seconds{10} ||
      loaded.distributed.maximum_work_items != 16'384 ||
      loaded.distributed.maximum_payload_bytes != 512U * 1024U ||
      loaded.distributed.maximum_result_bytes != 4U * 1024U * 1024U ||
      loaded.distributed.worker_timeout != std::chrono::seconds{20} ||
      loaded.distributed.lease_duration != std::chrono::minutes{1} ||
      loaded.distributed.heartbeat_interval != std::chrono::seconds{5} ||
      !loaded.distributed.coordinator.enabled ||
      !loaded.distributed.coordinator.recovery_enabled ||
      loaded.distributed.coordinator.recovery_file !=
          "/srv/sakuin/operational/custom-work.checkpoint" ||
      loaded.distributed.coordinator.recovery_maximum_bytes !=
          128U * 1024U * 1024U ||
      loaded.distributed.coordinator.maximum_terminal_work_items != 8192 ||
      loaded.distributed.coordinator.terminal_work_retention !=
          std::chrono::hours{24 * 4} ||
      loaded.distributed.coordinator.listen_address != "127.0.0.1" ||
      loaded.distributed.coordinator.listen_port != 7201 ||
      loaded.distributed.coordinator.maximum_connections != 32 ||
      loaded.distributed.coordinator.read_buffer_bytes != 8192 ||
      loaded.distributed.coordinator.maximum_frame_bytes != 1024U * 1024U ||
      loaded.distributed.coordinator.maximum_queued_write_bytes !=
          2U * 1024U * 1024U ||
      loaded.distributed.coordinator.maximum_result_reassembly_bytes !=
          64U * 1024U * 1024U ||
      loaded.distributed.coordinator.maximum_result_transfers != 8 ||
      loaded.distributed.coordinator.result_transfer_timeout !=
          std::chrono::seconds{30} ||
      loaded.distributed.coordinator.idle_timeout != std::chrono::seconds{15} ||
      loaded.distributed.coordinator.tls_trust_anchor_file !=
          "/etc/sakuin/workers/ca.pem" ||
      loaded.distributed.coordinator.tls_certificate_chain_file !=
          "/etc/sakuin/workers/coordinator.pem" ||
      loaded.distributed.coordinator.tls_private_key_file !=
          "/etc/sakuin/workers/coordinator-key.pem" ||
      !loaded.distributed.worker.enabled ||
      loaded.distributed.worker.id != "crawler-a" ||
      loaded.distributed.worker.coordinator_address != "coordinator.internal" ||
      loaded.distributed.worker.coordinator_port != 7102 ||
      loaded.distributed.worker.observation_batch_size != 1024 ||
      loaded.distributed.worker.read_buffer_bytes != 4096 ||
      loaded.distributed.worker.maximum_queued_write_bytes !=
          2U * 1024U * 1024U ||
      loaded.distributed.worker.connect_timeout != std::chrono::seconds{8} ||
      loaded.distributed.worker.request_timeout != std::chrono::seconds{9} ||
      loaded.distributed.worker.idle_timeout != std::chrono::seconds{14} ||
      loaded.distributed.worker.tls_trust_anchor_file !=
          "/etc/sakuin/coordinator/ca.pem" ||
      loaded.distributed.worker.tls_certificate_chain_file !=
          "/etc/sakuin/workers/crawler-a.pem" ||
      loaded.distributed.worker.tls_private_key_file !=
          "/etc/sakuin/workers/crawler-a-key.pem" ||
      loaded.distributed.worker.tls_server_name != "coordinator.internal")
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
  auto invalid_metadata_discovery = config::defaults();
  invalid_metadata_discovery.network.dht.metadata.discovery
      .parallelism_per_hash = invalid_metadata_discovery.network.dht.metadata
                                  .discovery.maximum_in_flight +
                              1;
  if (config::validate(invalid_metadata_discovery))
    return 22;
  auto invalid_metadata_backfill = config::defaults();
  invalid_metadata_backfill.network.dht.metadata.discovery.backfill
      .maximum_records_per_poll = 0;
  if (config::validate(invalid_metadata_backfill))
    return 23;
  auto invalid_tiering = config::defaults();
  invalid_tiering.storage.compaction_maximum_warm_segments = 0;
  if (config::validate(invalid_tiering))
    return 12;
  invalid_tiering = config::defaults();
  invalid_tiering.storage.compaction_warm_block_target_bytes = 1024;
  if (config::validate(invalid_tiering))
    return 13;
  auto invalid_classification_rules = config::defaults();
  invalid_classification_rules.indexing.classification.rules = {
      {.id = "duplicate", .tokens = {"valid"}},
      {.id = "duplicate", .tokens = {"alsovalid"}}};
  if (config::validate(invalid_classification_rules))
    return 24;
  if (config::parse_toml(
          "[[indexing.classification.rules]]\n"
          "id = \"bad\"\nkind = \"unknown\"\ntokens = [\"token\"]\n"))
    return 25;
  auto invalid_s3 = config::defaults();
  invalid_s3.storage.backend = config::StorageBackend::S3;
  if (config::validate(invalid_s3))
    return 17;
  invalid_s3.storage.s3.bucket = "configured";
  invalid_s3.storage.s3.maximum_attempts = 11;
  if (config::validate(invalid_s3))
    return 27;
  auto invalid_retention = config::defaults();
  invalid_retention.storage.retention.enabled = true;
  invalid_retention.storage.retention.observation_max_age =
      invalid_retention.storage.retention.observation_cold_age;
  if (config::validate(invalid_retention))
    return 14;
  auto invalid_coordinator = config::defaults();
  invalid_coordinator.distributed.coordinator.maximum_frame_bytes = 128;
  if (config::validate(invalid_coordinator))
    return 15;
  invalid_coordinator = config::defaults();
  invalid_coordinator.distributed.coordinator.recovery_maximum_bytes = 1024;
  if (config::validate(invalid_coordinator))
    return 18;
  invalid_coordinator = config::defaults();
  invalid_coordinator.distributed.coordinator.maximum_result_reassembly_bytes =
      1024;
  if (config::validate(invalid_coordinator))
    return 19;
  invalid_coordinator = config::defaults();
  invalid_coordinator.distributed.coordinator.maximum_terminal_work_items =
      invalid_coordinator.distributed.maximum_work_items + 1;
  if (config::validate(invalid_coordinator))
    return 20;
  auto incomplete_worker_tls = config::defaults();
  incomplete_worker_tls.distributed.coordinator.tls_trust_anchor_file =
      "ca.pem";
  if (config::validate(incomplete_worker_tls))
    return 16;
  auto enabled_worker_without_tls = config::defaults();
  enabled_worker_without_tls.distributed.worker.enabled = true;
  if (config::validate(enabled_worker_without_tls))
    return 17;
  const auto fallback_file =
      std::filesystem::temp_directory_path() /
      ("sakuin-bootstrap-" +
       std::to_string(
           std::chrono::steady_clock::now().time_since_epoch().count()) +
       ".txt");
  {
    std::ofstream output{fallback_file};
    output << "# fallback contacts\nrouter.example:6881 # primary\n\n"
              "router6.example:6881\n";
  }
  const std::array fallback_files{fallback_file};
  auto fallback = config::load({.bootstrap_fallback_files = fallback_files});
  std::filesystem::remove(fallback_file);
  if (!fallback || fallback->network.dht.bootstrap.size() != 2 ||
      fallback->network.dht.bootstrap[0] != "router.example:6881" ||
      fallback->network.dht.bootstrap_file != fallback_file)
    return 21;
  if (argument_count != 2)
    return 24;
  const std::array docker_environment{std::pair{
      std::string{"SAKUIN_DHT_BOOTSTRAP"}, std::string{"router.example:6881"}}};
  auto docker = config::load({.toml_file = std::filesystem::path{arguments[1]},
                              .environment = docker_environment});
  if (!docker) {
    std::cerr << docker.error().message << '\n';
    return 25;
  }
  if (docker->storage.local_root != "/var/lib/sakuin" ||
      docker->api.credential_store_directory !=
          "/var/lib/sakuin/operational/api" ||
      docker->api.listen_address != "0.0.0.0" ||
      docker->network.dht.bootstrap_file !=
          "/opt/sakuin/share/sakuin/dht-bootstrap.txt" ||
      docker->network.dht.maximum_in_flight != 128 ||
      docker->network.dht.metadata.discovery.maximum_in_flight != 64 ||
      !docker->network.dht.metadata.discovery.backfill.enabled ||
      !docker->indexing.classification.learned_fallback_enabled)
    return 26;
  return 0;
}

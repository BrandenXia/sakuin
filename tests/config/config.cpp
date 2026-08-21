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

[api]
credential_store_directory = "/srv/sakuin/api-credentials"
listen_address = "::1"
listen_port = 9000
maximum_connections = 64
request_timeout_ms = 5000

[api.rate_limit]
requests_per_window = 30
window_ms = 10000
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
      loaded.network.dht.identity.observation_quorum != 4 ||
      loaded.network.dht.routing.maximum_queued != 768 ||
      loaded.network.dht.routing.maximum_in_flight != 6 ||
      loaded.network.dht.routing.maximum_attempts != 4 ||
      loaded.network.dht.routing.retry_delay !=
          std::chrono::milliseconds{2500} ||
      loaded.network.dht.bootstrap.size() != 2 ||
      loaded.storage.local_root != "/srv/sakuin" ||
      loaded.storage.compression_level != 5 ||
      loaded.api.credential_store_directory !=
          "/var/lib/sakuin/api-credentials" ||
      loaded.api.listen_address != "::1" || loaded.api.listen_port != 9001 ||
      loaded.api.maximum_connections != 64 ||
      loaded.api.rate_limit.requests_per_window != 30 ||
      loaded.api.rate_limit.window != std::chrono::seconds{10})
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
  return 0;
}

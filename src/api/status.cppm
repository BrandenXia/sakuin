module;

#include <nlohmann/json.hpp>

export module sakuin.api.status;

import std;

import sakuin.core.bytes;
import sakuin.core.result;

export namespace sakuin::api {

struct DhtFamilyStatus {
  bool enabled{};
  bool running{};
  std::uint64_t cycles{};
  std::uint64_t errors{};
  std::uint64_t observations_stored{};
  std::uint64_t metadata_candidates_accepted{};
  std::uint64_t routing_probes_accepted{};
  std::uint64_t queries_expired{};
  std::uint64_t datagrams_attempted{};
  std::uint64_t datagrams_accepted{};
  std::uint64_t datagrams_failed{};
  std::size_t routing_nodes{};
  std::size_t outstanding_queries{};
  std::size_t pending_actions{};
  std::size_t metadata_queued{};
  std::size_t metadata_in_flight{};
  std::size_t metadata_pending_storage{};
  std::size_t bootstrap_candidates{};
  std::optional<bool> bootstrap_complete;
  std::optional<std::int64_t> last_cycle_ms;
  std::optional<std::int64_t> last_error_ms;
  std::optional<std::string> last_error;
};

struct ServiceStatus {
  std::string state{"starting"};
  std::int64_t started_at_ms{};
  std::uint64_t uptime_ms{};
  DhtFamilyStatus ipv4;
  DhtFamilyStatus ipv6;
  std::uint64_t search_source_generation{};
  std::uint64_t search_records_indexed{};
  std::uint64_t materialized_observations{};
  std::uint64_t materialized_torrent_updates{};
  std::uint64_t duplicate_source_generation{};
  std::uint64_t duplicate_records_processed{};
  std::uint64_t maintenance_operations{};
  std::uint64_t maintenance_errors{};
  std::optional<std::string> last_service_error;
};

class StatusProvider {
public:
  virtual ~StatusProvider() = default;
  virtual ServiceStatus status() const = 0;
};

core::Result<core::ByteBuffer> json_status(const ServiceStatus &status);

} // namespace sakuin::api

namespace sakuin::api {
namespace {

nlohmann::json family_json(const DhtFamilyStatus &family) {
  nlohmann::json result{
      {"enabled", family.enabled},
      {"running", family.running},
      {"cycles", family.cycles},
      {"errors", family.errors},
      {"observations_stored", family.observations_stored},
      {"metadata_candidates_accepted", family.metadata_candidates_accepted},
      {"routing_probes_accepted", family.routing_probes_accepted},
      {"queries_expired", family.queries_expired},
      {"datagrams_attempted", family.datagrams_attempted},
      {"datagrams_accepted", family.datagrams_accepted},
      {"datagrams_failed", family.datagrams_failed},
      {"routing_nodes", family.routing_nodes},
      {"outstanding_queries", family.outstanding_queries},
      {"pending_actions", family.pending_actions},
      {"metadata_queued", family.metadata_queued},
      {"metadata_in_flight", family.metadata_in_flight},
      {"metadata_pending_storage", family.metadata_pending_storage},
      {"bootstrap_candidates", family.bootstrap_candidates}};
  result["bootstrap_complete"] =
      family.bootstrap_complete ? nlohmann::json(*family.bootstrap_complete)
                                : nlohmann::json(nullptr);
  result["last_cycle_ms"] = family.last_cycle_ms
                                ? nlohmann::json(*family.last_cycle_ms)
                                : nlohmann::json(nullptr);
  result["last_error_ms"] = family.last_error_ms
                                ? nlohmann::json(*family.last_error_ms)
                                : nlohmann::json(nullptr);
  result["last_error"] = family.last_error ? nlohmann::json(*family.last_error)
                                           : nlohmann::json(nullptr);
  return result;
}

} // namespace

core::Result<core::ByteBuffer> json_status(const ServiceStatus &status) {
  try {
    nlohmann::json document{
        {"state", status.state},
        {"started_at_ms", status.started_at_ms},
        {"uptime_ms", status.uptime_ms},
        {"dht",
         {{"ipv4", family_json(status.ipv4)},
          {"ipv6", family_json(status.ipv6)}}},
        {"search",
         {{"source_generation", status.search_source_generation},
          {"records_indexed", status.search_records_indexed}}},
        {"materialization",
         {{"observations_processed", status.materialized_observations},
          {"torrent_updates", status.materialized_torrent_updates}}},
        {"duplicates",
         {{"source_generation", status.duplicate_source_generation},
          {"records_processed", status.duplicate_records_processed}}},
        {"maintenance",
         {{"operations", status.maintenance_operations},
          {"errors", status.maintenance_errors}}}};
    document["last_service_error"] =
        status.last_service_error ? nlohmann::json(*status.last_service_error)
                                  : nlohmann::json(nullptr);
    const auto text = document.dump();
    const auto bytes = std::as_bytes(std::span{text});
    return core::ByteBuffer{bytes.begin(), bytes.end()};
  } catch (const std::exception &exception) {
    return std::unexpected(
        core::Error{core::ErrorCode::Internal,
                    std::string{"Could not serialize status response: "} +
                        exception.what()});
  }
}

} // namespace sakuin::api

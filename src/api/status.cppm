module;

#include <nlohmann/json.hpp>

export module sakuin.api.status;

import std;

import sakuin.classification;
import sakuin.core.bytes;
import sakuin.core.result;
import sakuin.core.version;
import sakuin.search.index;

export namespace sakuin::api {

struct DhtFamilyStatus {
  bool enabled{};
  bool running{};
  std::uint64_t cycles{};
  std::uint64_t errors{};
  std::uint64_t observations_stored{};
  std::uint64_t metadata_candidates_accepted{};
  std::uint64_t metadata_attempts_started{};
  std::uint64_t metadata_fetches_succeeded{};
  std::uint64_t metadata_retryable_failures{};
  std::uint64_t metadata_permanent_failures{};
  std::uint64_t metadata_sink_succeeded{};
  std::uint64_t metadata_sink_failures{};
  std::uint64_t routing_probes_accepted{};
  std::uint64_t discovery_queries_started{};
  std::uint64_t peer_discovery_queries_started{};
  std::uint64_t peer_discovery_peers_found{};
  std::uint64_t peer_discovery_exhausted{};
  std::uint64_t metadata_backfill_records_scanned{};
  std::uint64_t metadata_backfill_targets_offered{};
  std::uint64_t metadata_backfill_records_with_metadata{};
  std::uint64_t inbound_messages{};
  std::uint64_t inbound_queries{};
  std::uint64_t inbound_ping_queries{};
  std::uint64_t inbound_find_node_queries{};
  std::uint64_t inbound_get_peers_queries{};
  std::uint64_t inbound_announce_peer_queries{};
  std::uint64_t inbound_unknown_queries{};
  std::uint64_t inbound_responses{};
  std::uint64_t inbound_protocol_errors{};
  std::uint64_t queries_expired{};
  std::uint64_t datagrams_attempted{};
  std::uint64_t datagrams_accepted{};
  std::uint64_t datagrams_failed{};
  std::size_t routing_nodes{};
  std::size_t outstanding_queries{};
  std::size_t discovery_in_flight{};
  std::size_t peer_discovery_pending{};
  std::size_t peer_discovery_in_flight{};
  std::uint64_t metadata_backfill_source_generation{};
  bool metadata_backfill_scan_in_progress{};
  bool metadata_backfill_full_rebuild{};
  std::size_t pending_actions{};
  std::size_t metadata_queued{};
  std::size_t metadata_in_flight{};
  std::size_t metadata_pending_storage{};
  std::size_t metadata_backlog{};
  std::size_t bootstrap_candidates{};
  std::optional<bool> bootstrap_complete;
  std::optional<bool> bootstrap_exhausted;
  std::optional<std::int64_t> last_cycle_ms;
  std::optional<std::int64_t> last_inbound_query_ms;
  std::optional<std::int64_t> last_error_ms;
  std::optional<std::string> last_error;
};

struct ServiceStatus {
  std::string version{core::version};
  std::string state{"starting"};
  std::int64_t started_at_ms{};
  std::uint64_t uptime_ms{};
  DhtFamilyStatus ipv4;
  DhtFamilyStatus ipv6;
  std::uint64_t search_source_generation{};
  std::uint64_t search_records_indexed{};
  search::ClassificationIndexStats search_classification;
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

bool service_ready(const ServiceStatus &status) noexcept;
core::Result<core::ByteBuffer> json_readiness(bool ready);
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
      {"metadata_attempts_started", family.metadata_attempts_started},
      {"metadata_fetches_succeeded", family.metadata_fetches_succeeded},
      {"metadata_retryable_failures", family.metadata_retryable_failures},
      {"metadata_permanent_failures", family.metadata_permanent_failures},
      {"metadata_sink_succeeded", family.metadata_sink_succeeded},
      {"metadata_sink_failures", family.metadata_sink_failures},
      {"routing_probes_accepted", family.routing_probes_accepted},
      {"discovery_queries_started", family.discovery_queries_started},
      {"peer_discovery_queries_started", family.peer_discovery_queries_started},
      {"peer_discovery_peers_found", family.peer_discovery_peers_found},
      {"peer_discovery_exhausted", family.peer_discovery_exhausted},
      {"metadata_backfill_records_scanned",
       family.metadata_backfill_records_scanned},
      {"metadata_backfill_targets_offered",
       family.metadata_backfill_targets_offered},
      {"metadata_backfill_records_with_metadata",
       family.metadata_backfill_records_with_metadata},
      {"inbound_messages", family.inbound_messages},
      {"inbound_queries", family.inbound_queries},
      {"inbound_ping_queries", family.inbound_ping_queries},
      {"inbound_find_node_queries", family.inbound_find_node_queries},
      {"inbound_get_peers_queries", family.inbound_get_peers_queries},
      {"inbound_announce_peer_queries", family.inbound_announce_peer_queries},
      {"inbound_unknown_queries", family.inbound_unknown_queries},
      {"inbound_responses", family.inbound_responses},
      {"inbound_protocol_errors", family.inbound_protocol_errors},
      {"queries_expired", family.queries_expired},
      {"datagrams_attempted", family.datagrams_attempted},
      {"datagrams_accepted", family.datagrams_accepted},
      {"datagrams_failed", family.datagrams_failed},
      {"routing_nodes", family.routing_nodes},
      {"outstanding_queries", family.outstanding_queries},
      {"discovery_in_flight", family.discovery_in_flight},
      {"peer_discovery_pending", family.peer_discovery_pending},
      {"peer_discovery_in_flight", family.peer_discovery_in_flight},
      {"metadata_backfill_source_generation",
       family.metadata_backfill_source_generation},
      {"metadata_backfill_scan_in_progress",
       family.metadata_backfill_scan_in_progress},
      {"metadata_backfill_full_rebuild", family.metadata_backfill_full_rebuild},
      {"pending_actions", family.pending_actions},
      {"metadata_queued", family.metadata_queued},
      {"metadata_in_flight", family.metadata_in_flight},
      {"metadata_pending_storage", family.metadata_pending_storage},
      {"metadata_backlog", family.metadata_backlog},
      {"bootstrap_candidates", family.bootstrap_candidates}};
  result["bootstrap_complete"] =
      family.bootstrap_complete ? nlohmann::json(*family.bootstrap_complete)
                                : nlohmann::json(nullptr);
  result["bootstrap_exhausted"] =
      family.bootstrap_exhausted ? nlohmann::json(*family.bootstrap_exhausted)
                                 : nlohmann::json(nullptr);
  result["last_cycle_ms"] = family.last_cycle_ms
                                ? nlohmann::json(*family.last_cycle_ms)
                                : nlohmann::json(nullptr);
  result["last_inbound_query_ms"] =
      family.last_inbound_query_ms
          ? nlohmann::json(*family.last_inbound_query_ms)
          : nlohmann::json(nullptr);
  result["last_error_ms"] = family.last_error_ms
                                ? nlohmann::json(*family.last_error_ms)
                                : nlohmann::json(nullptr);
  result["last_error"] = family.last_error ? nlohmann::json(*family.last_error)
                                           : nlohmann::json(nullptr);
  return result;
}

std::string_view category_name(classification::MediaCategory category) {
  using enum classification::MediaCategory;
  switch (category) {
  case Movie:
    return "movie";
  case MovieSd:
    return "movie_sd";
  case MovieHd:
    return "movie_hd";
  case MovieUhd:
    return "movie_uhd";
  case Series:
    return "series";
  case SeriesSd:
    return "series_sd";
  case SeriesHd:
    return "series_hd";
  case SeriesUhd:
    return "series_uhd";
  case SeriesAnime:
    return "series_anime";
  case Audio:
    return "audio";
  case Audiobook:
    return "audiobook";
  case Application:
    return "application";
  case Game:
    return "game";
  case Books:
    return "books";
  case Ebook:
    return "ebook";
  case Adult:
    return "adult";
  case Other:
    return "other";
  }
  std::unreachable();
}

nlohmann::json
classification_json(const search::ClassificationIndexStats &classification) {
  nlohmann::json categories = nlohmann::json::object();
  for (std::size_t index = 0; index < search::MediaCategoryCount; ++index) {
    const auto category = static_cast<classification::MediaCategory>(index);
    categories[category_name(category)] =
        classification.category_count(category);
  }
  using enum classification::ClassificationState;
  return {{"enabled", classification.enabled},
          {"algorithm_version", classification.algorithm_version},
          {"total_records", classification.total_records},
          {"states",
           {{"awaiting_metadata", classification.state_count(AwaitingMetadata)},
            {"classified", classification.state_count(Classified)},
            {"ambiguous", classification.state_count(Ambiguous)},
            {"unknown", classification.state_count(Unknown)}}},
          {"input_truncated", classification.input_truncated},
          {"adult_labeled", classification.adult_labeled},
          {"categories", std::move(categories)}};
}

} // namespace

bool service_ready(const ServiceStatus &status) noexcept {
  if (status.state != "running")
    return false;
  const bool family_enabled = status.ipv4.enabled || status.ipv6.enabled;
  return family_enabled && (!status.ipv4.enabled || status.ipv4.running) &&
         (!status.ipv6.enabled || status.ipv6.running);
}

core::Result<core::ByteBuffer> json_readiness(bool ready) {
  try {
    const auto text =
        nlohmann::json{{"status", ready ? "ready" : "not_ready"}}.dump();
    const auto bytes = std::as_bytes(std::span{text});
    return core::ByteBuffer{bytes.begin(), bytes.end()};
  } catch (const std::exception &exception) {
    return std::unexpected(
        core::Error{core::ErrorCode::Internal,
                    std::string{"Could not serialize readiness response: "} +
                        exception.what()});
  }
}

core::Result<core::ByteBuffer> json_status(const ServiceStatus &status) {
  try {
    nlohmann::json document{
        {"version", status.version},
        {"state", status.state},
        {"started_at_ms", status.started_at_ms},
        {"uptime_ms", status.uptime_ms},
        {"dht",
         {{"ipv4", family_json(status.ipv4)},
          {"ipv6", family_json(status.ipv6)}}},
        {"search",
         {{"source_generation", status.search_source_generation},
          {"records_indexed", status.search_records_indexed},
          {"classification",
           classification_json(status.search_classification)}}},
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

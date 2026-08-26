export module sakuin.api.metrics;

import std;

import sakuin.api.status;
import sakuin.classification;
import sakuin.core.bytes;
import sakuin.core.result;
import sakuin.search.index;

export namespace sakuin::api {

// Serializes a status snapshot using the Prometheus text exposition format.
// Labels are deliberately limited to service state and address family so that
// operational errors and network peers cannot create unbounded cardinality.
core::Result<core::ByteBuffer> prometheus_metrics(const ServiceStatus &status);

} // namespace sakuin::api

namespace sakuin::api {
namespace {

void metadata(std::string &output, std::string_view name, std::string_view help,
              std::string_view type) {
  output.append("# HELP ").append(name).push_back(' ');
  output.append(help).push_back('\n');
  output.append("# TYPE ").append(name).push_back(' ');
  output.append(type).push_back('\n');
}

template <typename Value>
void sample(std::string &output, std::string_view name, std::string_view labels,
            Value value) {
  output.append(name);
  if (!labels.empty())
    output.push_back('{'), output.append(labels), output.push_back('}');
  output.push_back(' ');
  output.append(std::to_string(value)).push_back('\n');
}

std::string escaped_label(std::string_view value) {
  std::string result;
  result.reserve(value.size());
  for (const auto character : value) {
    if (character == '\\' || character == '"' || character == '\n') {
      result.push_back('\\');
      result.push_back(character == '\n' ? 'n' : character);
    } else {
      result.push_back(character);
    }
  }
  return result;
}

void family_samples(std::string &output, std::string_view name,
                    const DhtFamilyStatus &family) {
  const auto labels = "family=\"" + std::string{name} + "\"";
  sample(output, "sakuin_dht_enabled", labels, family.enabled ? 1 : 0);
  sample(output, "sakuin_dht_running", labels, family.running ? 1 : 0);
  sample(output, "sakuin_dht_cycles_total", labels, family.cycles);
  sample(output, "sakuin_dht_errors_total", labels, family.errors);
  sample(output, "sakuin_dht_observations_stored_total", labels,
         family.observations_stored);
  sample(output, "sakuin_dht_metadata_candidates_accepted_total", labels,
         family.metadata_candidates_accepted);
  sample(output, "sakuin_dht_metadata_attempts_started_total", labels,
         family.metadata_attempts_started);
  sample(output, "sakuin_dht_metadata_fetches_succeeded_total", labels,
         family.metadata_fetches_succeeded);
  sample(output, "sakuin_dht_metadata_fetch_failures_total",
         labels + ",outcome=\"retryable\"", family.metadata_retryable_failures);
  sample(output, "sakuin_dht_metadata_fetch_failures_total",
         labels + ",outcome=\"permanent\"", family.metadata_permanent_failures);
  sample(output, "sakuin_dht_metadata_sink_succeeded_total", labels,
         family.metadata_sink_succeeded);
  sample(output, "sakuin_dht_metadata_sink_failures_total", labels,
         family.metadata_sink_failures);
  sample(output, "sakuin_dht_routing_probes_accepted_total", labels,
         family.routing_probes_accepted);
  sample(output, "sakuin_dht_discovery_queries_started_total", labels,
         family.discovery_queries_started);
  sample(output, "sakuin_dht_peer_discovery_queries_started_total", labels,
         family.peer_discovery_queries_started);
  sample(output, "sakuin_dht_peer_discovery_peers_found_total", labels,
         family.peer_discovery_peers_found);
  sample(output, "sakuin_dht_peer_discovery_exhausted_total", labels,
         family.peer_discovery_exhausted);
  sample(output, "sakuin_dht_metadata_backfill_records_scanned_total", labels,
         family.metadata_backfill_records_scanned);
  sample(output, "sakuin_dht_metadata_backfill_targets_offered_total", labels,
         family.metadata_backfill_targets_offered);
  sample(output, "sakuin_dht_metadata_backfill_records_with_metadata_total",
         labels, family.metadata_backfill_records_with_metadata);
  sample(output, "sakuin_dht_inbound_messages_total", labels,
         family.inbound_messages);
  sample(output, "sakuin_dht_inbound_queries_total", labels,
         family.inbound_queries);
  sample(output, "sakuin_dht_inbound_query_methods_total",
         labels + ",method=\"ping\"", family.inbound_ping_queries);
  sample(output, "sakuin_dht_inbound_query_methods_total",
         labels + ",method=\"find_node\"", family.inbound_find_node_queries);
  sample(output, "sakuin_dht_inbound_query_methods_total",
         labels + ",method=\"get_peers\"", family.inbound_get_peers_queries);
  sample(output, "sakuin_dht_inbound_query_methods_total",
         labels + ",method=\"announce_peer\"",
         family.inbound_announce_peer_queries);
  sample(output, "sakuin_dht_inbound_query_methods_total",
         labels + ",method=\"unknown\"", family.inbound_unknown_queries);
  sample(output, "sakuin_dht_inbound_responses_total", labels,
         family.inbound_responses);
  sample(output, "sakuin_dht_inbound_protocol_errors_total", labels,
         family.inbound_protocol_errors);
  sample(output, "sakuin_dht_queries_expired_total", labels,
         family.queries_expired);
  sample(output, "sakuin_dht_datagrams_attempted_total", labels,
         family.datagrams_attempted);
  sample(output, "sakuin_dht_datagrams_accepted_total", labels,
         family.datagrams_accepted);
  sample(output, "sakuin_dht_datagrams_failed_total", labels,
         family.datagrams_failed);
  sample(output, "sakuin_dht_routing_nodes", labels, family.routing_nodes);
  sample(output, "sakuin_dht_outstanding_queries", labels,
         family.outstanding_queries);
  sample(output, "sakuin_dht_discovery_in_flight", labels,
         family.discovery_in_flight);
  sample(output, "sakuin_dht_peer_discovery_pending", labels,
         family.peer_discovery_pending);
  sample(output, "sakuin_dht_peer_discovery_in_flight", labels,
         family.peer_discovery_in_flight);
  sample(output, "sakuin_dht_metadata_backfill_source_generation", labels,
         family.metadata_backfill_source_generation);
  sample(output, "sakuin_dht_metadata_backfill_scan_in_progress", labels,
         family.metadata_backfill_scan_in_progress ? 1 : 0);
  sample(output, "sakuin_dht_metadata_backfill_full_rebuild", labels,
         family.metadata_backfill_full_rebuild ? 1 : 0);
  sample(output, "sakuin_dht_pending_actions", labels, family.pending_actions);
  sample(output, "sakuin_dht_metadata_queued", labels, family.metadata_queued);
  sample(output, "sakuin_dht_metadata_in_flight", labels,
         family.metadata_in_flight);
  sample(output, "sakuin_dht_metadata_pending_storage", labels,
         family.metadata_pending_storage);
  sample(output, "sakuin_dht_metadata_backlog", labels,
         family.metadata_backlog);
  sample(output, "sakuin_dht_bootstrap_candidates", labels,
         family.bootstrap_candidates);
  if (family.bootstrap_complete)
    sample(output, "sakuin_dht_bootstrap_complete", labels,
           *family.bootstrap_complete ? 1 : 0);
  if (family.bootstrap_exhausted)
    sample(output, "sakuin_dht_bootstrap_exhausted", labels,
           *family.bootstrap_exhausted ? 1 : 0);
  if (family.last_cycle_ms)
    sample(output, "sakuin_dht_last_cycle_timestamp_seconds", labels,
           static_cast<double>(*family.last_cycle_ms) / 1000.0);
  if (family.last_inbound_query_ms)
    sample(output, "sakuin_dht_last_inbound_query_timestamp_seconds", labels,
           static_cast<double>(*family.last_inbound_query_ms) / 1000.0);
  if (family.last_error_ms)
    sample(output, "sakuin_dht_last_error_timestamp_seconds", labels,
           static_cast<double>(*family.last_error_ms) / 1000.0);
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

std::string_view state_name(classification::ClassificationState state) {
  using enum classification::ClassificationState;
  switch (state) {
  case AwaitingMetadata:
    return "awaiting_metadata";
  case Classified:
    return "classified";
  case Ambiguous:
    return "ambiguous";
  case Unknown:
    return "unknown";
  }
  std::unreachable();
}

} // namespace

core::Result<core::ByteBuffer> prometheus_metrics(const ServiceStatus &status) {
  try {
    std::string output;
    output.reserve(8192);

    metadata(output, "sakuin_service_info",
             "Static information about the Sakuin service state.", "gauge");
    sample(output, "sakuin_service_info",
           "state=\"" + escaped_label(status.state) + "\",version=\"" +
               escaped_label(status.version) + "\"",
           1);
    metadata(output, "sakuin_service_ready",
             "Whether the composed service is ready to receive traffic.",
             "gauge");
    sample(output, "sakuin_service_ready", {}, service_ready(status) ? 1 : 0);
    metadata(output, "sakuin_start_time_seconds",
             "Unix time when the current service run started.", "gauge");
    sample(output, "sakuin_start_time_seconds", {},
           static_cast<double>(status.started_at_ms) / 1000.0);
    metadata(output, "sakuin_uptime_seconds",
             "Time elapsed since the current service run started.", "gauge");
    sample(output, "sakuin_uptime_seconds", {},
           static_cast<double>(status.uptime_ms) / 1000.0);
    metadata(output, "sakuin_service_errors_total",
             "Service errors observed during the current process run by "
             "bounded subsystem.",
             "counter");
    metadata(output, "sakuin_service_error_active",
             "Whether a service subsystem error remains unrecovered.", "gauge");
    metadata(output, "sakuin_service_error_last_seen_timestamp_seconds",
             "Unix time when a service subsystem last reported an error.",
             "gauge");
    metadata(output, "sakuin_service_error_recovered_timestamp_seconds",
             "Unix time when a service subsystem most recently recovered.",
             "gauge");
    for (const auto &error : status.service_errors) {
      const auto labels = "source=\"" + escaped_label(error.source) + "\"";
      sample(output, "sakuin_service_errors_total", labels, error.count);
      sample(output, "sakuin_service_error_active", labels,
             error.active ? 1 : 0);
      sample(output, "sakuin_service_error_last_seen_timestamp_seconds", labels,
             static_cast<double>(error.last_seen_ms) / 1000.0);
      if (error.recovered_at_ms)
        sample(output, "sakuin_service_error_recovered_timestamp_seconds",
               labels, static_cast<double>(*error.recovered_at_ms) / 1000.0);
    }

    metadata(output, "sakuin_dht_enabled",
             "Whether the DHT address family is enabled.", "gauge");
    metadata(output, "sakuin_dht_running",
             "Whether the DHT address family worker is running.", "gauge");
    metadata(output, "sakuin_dht_cycles_total",
             "Completed DHT worker polling cycles.", "counter");
    metadata(output, "sakuin_dht_errors_total", "DHT worker errors observed.",
             "counter");
    metadata(output, "sakuin_dht_observations_stored_total",
             "DHT observations stored in canonical storage.", "counter");
    metadata(output, "sakuin_dht_metadata_candidates_accepted_total",
             "Metadata acquisition candidates accepted.", "counter");
    metadata(output, "sakuin_dht_metadata_attempts_started_total",
             "Metadata acquisition attempts started.", "counter");
    metadata(output, "sakuin_dht_metadata_fetches_succeeded_total",
             "Metadata payloads downloaded and verified successfully.",
             "counter");
    metadata(output, "sakuin_dht_metadata_fetch_failures_total",
             "Metadata acquisition failures by retry outcome.", "counter");
    metadata(output, "sakuin_dht_metadata_sink_succeeded_total",
             "Fetched metadata accepted by the configured result or storage "
             "sink.",
             "counter");
    metadata(output, "sakuin_dht_metadata_sink_failures_total",
             "Fetched metadata delivery attempts rejected by the configured "
             "result or storage sink.",
             "counter");
    metadata(output, "sakuin_dht_routing_probes_accepted_total",
             "Routing probes accepted.", "counter");
    metadata(output, "sakuin_dht_discovery_queries_started_total",
             "Periodic DHT routing-discovery queries started.", "counter");
    metadata(output, "sakuin_dht_peer_discovery_queries_started_total",
             "Active get_peers discovery queries started for observed "
             "infohashes.",
             "counter");
    metadata(output, "sakuin_dht_peer_discovery_peers_found_total",
             "Peer endpoints found by active DHT discovery.", "counter");
    metadata(output, "sakuin_dht_peer_discovery_exhausted_total",
             "Observed infohash discovery traversals exhausted without a "
             "peer.",
             "counter");
    metadata(output, "sakuin_dht_metadata_backfill_records_scanned_total",
             "Canonical torrent records scanned for metadata backfill.",
             "counter");
    metadata(output, "sakuin_dht_metadata_backfill_targets_offered_total",
             "Metadata-missing records offered to active peer discovery.",
             "counter");
    metadata(output, "sakuin_dht_metadata_backfill_records_with_metadata_total",
             "Backfill records skipped because metadata is already present.",
             "counter");
    metadata(output, "sakuin_dht_inbound_messages_total",
             "Valid inbound KRPC messages received.", "counter");
    metadata(output, "sakuin_dht_inbound_queries_total",
             "Valid inbound KRPC queries received.", "counter");
    metadata(output, "sakuin_dht_inbound_query_methods_total",
             "Valid inbound KRPC queries by method.", "counter");
    metadata(output, "sakuin_dht_inbound_responses_total",
             "Valid inbound KRPC responses received.", "counter");
    metadata(output, "sakuin_dht_inbound_protocol_errors_total",
             "Valid inbound KRPC protocol-error messages received.", "counter");
    metadata(output, "sakuin_dht_queries_expired_total",
             "DHT queries expired before completion.", "counter");
    metadata(output, "sakuin_dht_datagrams_attempted_total",
             "DHT datagrams submitted for dispatch.", "counter");
    metadata(output, "sakuin_dht_datagrams_accepted_total",
             "DHT datagrams accepted by the transport.", "counter");
    metadata(output, "sakuin_dht_datagrams_failed_total",
             "DHT datagrams rejected or failed by the transport.", "counter");
    metadata(output, "sakuin_dht_routing_nodes",
             "Nodes currently present in the routing table.", "gauge");
    metadata(output, "sakuin_dht_outstanding_queries",
             "DHT queries currently awaiting responses.", "gauge");
    metadata(output, "sakuin_dht_discovery_in_flight",
             "Periodic routing-discovery queries awaiting responses.", "gauge");
    metadata(output, "sakuin_dht_peer_discovery_pending",
             "Observed infohashes awaiting or undergoing peer discovery.",
             "gauge");
    metadata(output, "sakuin_dht_peer_discovery_in_flight",
             "Active get_peers discovery queries awaiting responses.", "gauge");
    metadata(output, "sakuin_dht_metadata_backfill_source_generation",
             "Canonical torrent generation represented by the backfill scan.",
             "gauge");
    metadata(output, "sakuin_dht_metadata_backfill_scan_in_progress",
             "Whether a metadata backfill change scan is in progress.",
             "gauge");
    metadata(output, "sakuin_dht_metadata_backfill_full_rebuild",
             "Whether the current backfill scan is a complete keyed scan.",
             "gauge");
    metadata(output, "sakuin_dht_pending_actions",
             "DHT actions waiting to be dispatched.", "gauge");
    metadata(output, "sakuin_dht_metadata_queued",
             "Metadata candidates waiting for acquisition.", "gauge");
    metadata(output, "sakuin_dht_metadata_in_flight",
             "Metadata acquisitions currently in flight.", "gauge");
    metadata(output, "sakuin_dht_metadata_pending_storage",
             "Acquired metadata waiting for the configured result or storage "
             "sink.",
             "gauge");
    metadata(output, "sakuin_dht_metadata_backlog",
             "Metadata work queued, in flight, or waiting for its sink.",
             "gauge");
    metadata(output, "sakuin_dht_bootstrap_candidates",
             "Known bootstrap candidates.", "gauge");
    metadata(output, "sakuin_dht_bootstrap_complete",
             "Whether DHT bootstrap settled after a successful response.",
             "gauge");
    metadata(output, "sakuin_dht_bootstrap_exhausted",
             "Whether DHT bootstrap settled without any successful response.",
             "gauge");
    metadata(output, "sakuin_dht_last_cycle_timestamp_seconds",
             "Unix time of the last completed DHT cycle.", "gauge");
    metadata(output, "sakuin_dht_last_inbound_query_timestamp_seconds",
             "Unix time of the last valid inbound DHT query.", "gauge");
    metadata(output, "sakuin_dht_last_error_timestamp_seconds",
             "Unix time of the last DHT worker error.", "gauge");
    family_samples(output, "ipv4", status.ipv4);
    family_samples(output, "ipv6", status.ipv6);

    metadata(output, "sakuin_search_source_generation",
             "Canonical generation represented by the search index.", "gauge");
    sample(output, "sakuin_search_source_generation", {},
           status.search_source_generation);
    metadata(output, "sakuin_search_records_indexed_total",
             "Torrent records processed by search refreshes.", "counter");
    sample(output, "sakuin_search_records_indexed_total", {},
           status.search_records_indexed);
    metadata(output, "sakuin_classification_enabled",
             "Whether torrent classification is enabled.", "gauge");
    sample(output, "sakuin_classification_enabled", {},
           status.search_classification.enabled ? 1 : 0);
    metadata(output, "sakuin_classification_algorithm_version",
             "Classifier algorithm version represented by the search index.",
             "gauge");
    sample(output, "sakuin_classification_algorithm_version", {},
           status.search_classification.algorithm_version);
    metadata(output, "sakuin_classification_records",
             "Current search-index records by classification state.", "gauge");
    for (std::size_t index = 0; index < search::ClassificationStateCount;
         ++index) {
      const auto state =
          static_cast<classification::ClassificationState>(index);
      sample(output, "sakuin_classification_records",
             "state=\"" + std::string{state_name(state)} + "\"",
             status.search_classification.state_count(state));
    }
    metadata(output, "sakuin_classification_input_truncated_records",
             "Current records classified from bounded, truncated input.",
             "gauge");
    sample(output, "sakuin_classification_input_truncated_records", {},
           status.search_classification.input_truncated);
    metadata(output, "sakuin_classification_adult_labeled_records",
             "Current records carrying any classifier-produced Adult label.",
             "gauge");
    sample(output, "sakuin_classification_adult_labeled_records", {},
           status.search_classification.adult_labeled);
    metadata(output, "sakuin_classification_learned_enabled",
             "Whether the learned content-kind fallback is enabled.", "gauge");
    sample(output, "sakuin_classification_learned_enabled", {},
           status.search_classification.learned_enabled ? 1 : 0);
    metadata(output, "sakuin_classification_learned_ready",
             "Whether the local learned model has enough training coverage.",
             "gauge");
    sample(output, "sakuin_classification_learned_ready", {},
           status.search_classification.learned_ready ? 1 : 0);
    metadata(output, "sakuin_classification_learned_training_records",
             "High-confidence deterministic records used to train the local "
             "content-kind model.",
             "gauge");
    sample(output, "sakuin_classification_learned_training_records", {},
           status.search_classification.learned_training_records);
    metadata(output, "sakuin_classification_learned_classified_records",
             "Current records whose content kind was supplied by the learned "
             "fallback.",
             "gauge");
    sample(output, "sakuin_classification_learned_classified_records", {},
           status.search_classification.learned_classified_records);
    metadata(output, "sakuin_classification_learned_eligible_kinds",
             "Content kinds with enough examples in the local learned model.",
             "gauge");
    sample(output, "sakuin_classification_learned_eligible_kinds", {},
           status.search_classification.learned_eligible_kinds);
    metadata(output, "sakuin_classification_learned_vocabulary_size",
             "Bounded feature vocabulary size of the local learned model.",
             "gauge");
    sample(output, "sakuin_classification_learned_vocabulary_size", {},
           status.search_classification.learned_vocabulary_size);
    metadata(output, "sakuin_classification_category_records",
             "Current search-index records by semantic category.", "gauge");
    for (std::size_t index = 0; index < search::MediaCategoryCount; ++index) {
      const auto category = static_cast<classification::MediaCategory>(index);
      sample(output, "sakuin_classification_category_records",
             "category=\"" + std::string{category_name(category)} + "\"",
             status.search_classification.category_count(category));
    }
    metadata(output, "sakuin_materialization_observations_processed_total",
             "Observations processed by torrent materialization.", "counter");
    sample(output, "sakuin_materialization_observations_processed_total", {},
           status.materialized_observations);
    metadata(output, "sakuin_materialization_torrent_updates_total",
             "Torrent records updated by materialization.", "counter");
    sample(output, "sakuin_materialization_torrent_updates_total", {},
           status.materialized_torrent_updates);
    metadata(output, "sakuin_duplicate_source_generation",
             "Canonical generation represented by the duplicate index.",
             "gauge");
    sample(output, "sakuin_duplicate_source_generation", {},
           status.duplicate_source_generation);
    metadata(output, "sakuin_duplicate_records_processed_total",
             "Torrent records processed by duplicate indexing.", "counter");
    sample(output, "sakuin_duplicate_records_processed_total", {},
           status.duplicate_records_processed);
    metadata(output, "sakuin_maintenance_operations_total",
             "Completed storage maintenance operations.", "counter");
    sample(output, "sakuin_maintenance_operations_total", {},
           status.maintenance_operations);
    metadata(output, "sakuin_maintenance_errors_total",
             "Storage maintenance errors.", "counter");
    sample(output, "sakuin_maintenance_errors_total", {},
           status.maintenance_errors);

    const auto bytes = std::as_bytes(std::span{output});
    return core::ByteBuffer{bytes.begin(), bytes.end()};
  } catch (const std::exception &exception) {
    return std::unexpected(
        core::Error{core::ErrorCode::Internal,
                    std::string{"Could not serialize Prometheus metrics: "} +
                        exception.what()});
  }
}

} // namespace sakuin::api

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
  sample(output, "sakuin_dht_routing_probes_accepted_total", labels,
         family.routing_probes_accepted);
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
  sample(output, "sakuin_dht_pending_actions", labels, family.pending_actions);
  sample(output, "sakuin_dht_metadata_queued", labels, family.metadata_queued);
  sample(output, "sakuin_dht_metadata_in_flight", labels,
         family.metadata_in_flight);
  sample(output, "sakuin_dht_metadata_pending_storage", labels,
         family.metadata_pending_storage);
  sample(output, "sakuin_dht_bootstrap_candidates", labels,
         family.bootstrap_candidates);
  if (family.bootstrap_complete)
    sample(output, "sakuin_dht_bootstrap_complete", labels,
           *family.bootstrap_complete ? 1 : 0);
  if (family.last_cycle_ms)
    sample(output, "sakuin_dht_last_cycle_timestamp_seconds", labels,
           static_cast<double>(*family.last_cycle_ms) / 1000.0);
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
    metadata(output, "sakuin_dht_routing_probes_accepted_total",
             "Routing probes accepted.", "counter");
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
    metadata(output, "sakuin_dht_pending_actions",
             "DHT actions waiting to be dispatched.", "gauge");
    metadata(output, "sakuin_dht_metadata_queued",
             "Metadata candidates waiting for acquisition.", "gauge");
    metadata(output, "sakuin_dht_metadata_in_flight",
             "Metadata acquisitions currently in flight.", "gauge");
    metadata(output, "sakuin_dht_metadata_pending_storage",
             "Acquired metadata waiting for canonical storage.", "gauge");
    metadata(output, "sakuin_dht_bootstrap_candidates",
             "Known bootstrap candidates.", "gauge");
    metadata(output, "sakuin_dht_bootstrap_complete",
             "Whether DHT bootstrap has completed.", "gauge");
    metadata(output, "sakuin_dht_last_cycle_timestamp_seconds",
             "Unix time of the last completed DHT cycle.", "gauge");
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

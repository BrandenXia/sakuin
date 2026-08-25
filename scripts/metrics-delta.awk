# Compare two Prometheus text snapshots. The first input is the older sample,
# the second is the newer sample. Only bounded crawler-pipeline counters are
# reported; gauges remain available in the normal metrics/status output.

function tracked(name) {
  return name == "sakuin_dht_observations_stored_total" || \
         name == "sakuin_dht_peer_discovery_queries_started_total" || \
         name == "sakuin_dht_peer_discovery_peers_found_total" || \
         name == "sakuin_dht_peer_discovery_exhausted_total" || \
         name == "sakuin_dht_metadata_candidates_accepted_total" || \
         name == "sakuin_dht_metadata_attempts_started_total" || \
         name == "sakuin_dht_metadata_fetches_succeeded_total" || \
         name == "sakuin_dht_metadata_fetch_failures_total" || \
         name == "sakuin_dht_metadata_sink_succeeded_total" || \
         name == "sakuin_dht_metadata_sink_failures_total"
}

function split_key(key, parts, opening) {
  opening = index(key, "{")
  if (opening == 0) {
    parts["name"] = key
    parts["labels"] = "-"
  } else {
    parts["name"] = substr(key, 1, opening - 1)
    parts["labels"] = substr(key, opening + 1, length(key) - opening - 1)
  }
}

BEGIN {
  if (window_seconds !~ /^[0-9]+$/ || window_seconds < 1) {
    print "metrics-delta.awk: window_seconds must be a positive integer" > "/dev/stderr"
    exit 2
  }
  print "metric\tlabels\tdelta\tper_second\tcontinuity"
}

NR == FNR && $1 !~ /^#/ && NF >= 2 {
  delete fields
  split_key($1, fields)
  if (tracked(fields["name"]))
    before[$1] = $2 + 0
  next
}

$1 !~ /^#/ && NF >= 2 {
  delete fields
  split_key($1, fields)
  if (!tracked(fields["name"]))
    next

  current = $2 + 0
  if (!($1 in before)) {
    change = current
    continuity = "new"
  } else if (current < before[$1]) {
    change = current
    continuity = "reset"
  } else {
    change = current - before[$1]
    continuity = "continuous"
  }
  printf "%s\t%s\t%.0f\t%.6f\t%s\n", fields["name"], \
         fields["labels"], change, change / window_seconds, continuity
}

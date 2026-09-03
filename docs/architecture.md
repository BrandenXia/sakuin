# Sakuin architecture

This document describes Sakuin's internal design and the invariants that make
its local index portable, recoverable, and suitable for later distributed
deployment. The README intentionally stays focused on features and usage.

## Design goals

Sakuin is a specialized DHT indexing and storage system rather than a web
application whose durable state happens to live in a database. It is designed
to:

- discover infohashes and acquire torrent metadata from the BitTorrent DHT;
- retain historical observations efficiently;
- materialize current torrent records and rebuildable search indexes;
- detect duplicate content beyond infohash equality;
- enforce local or fleet-wide network budgets;
- scale from one host to disposable remote crawlers; and
- migrate canonical data without a database export or search-index conversion.

The core durability invariant is:

```text
immutable storage objects + segment metadata + manifest generations
```

Canonical data is not held in PostgreSQL, Redis, SQLite, a search index, a
worker's memory, or the coordinator checkpoint. Deleting every derived index
must still leave enough canonical data to reconstruct the catalog.

## Subsystem boundaries

The codebase uses C++23 modules grouped by stable domains:

```text
API ───────────────► search / duplicate views
                           │
DHT runtime ─► observations ─► materialization ─► torrent records
     │                     │                         │
     └── scheduler         └──────── storage ───────┘
                                      │
                                  BlobStore
                                      │
                                local filesystem
```

`sakuin.core` contains only universal values and error handling. Protocol,
storage, filesystem, search, and application behavior remain in their own
domains. Standalone Asio implements the current runtime behind Sakuin datagram,
stream, resolver, HTTP-server, and TLS interfaces; application APIs are not
committed to `asio::awaitable`. This leaves room for a future scheduler layer
such as stdexec without changing domain interfaces.

## Configuration

Configuration is TOML with deterministic precedence:

```text
defaults → TOML file → SAKUIN_* environment → command-line overrides
```

Parsing produces a runtime-independent configuration model. Validation occurs
before services or storage are opened. Configuration covers DHT concurrency and
timeouts, BEP 42 identity, traffic budgets, storage sizing and retention,
materialization, duplicate indexing, API authentication, and distributed work.

[`config/sakuin.example.toml`](../config/sakuin.example.toml) documents the full
surface. [`config/sakuin.docker.toml`](../config/sakuin.docker.toml) mirrors
those defaults with only the container paths and API bind address adjusted.
Release CI compares the two profiles so new defaults cannot silently leave the
container deployment behind.

## Canonical model

Sakuin stores two logical datasets.

Observation records are append-oriented facts: an infohash was observed at a
specific time. They preserve crawl history and do not pretend to offer keyed
database semantics.

Torrent records are keyed materialized state. They combine first/last-seen
times with acquired names, sizes, and file paths. A new observation can update
the temporal bounds without discarding already acquired metadata.

Search ranking, duplicate memberships, live routing nodes, work leases, and
traffic counters are intentionally not fields in the canonical torrent record.

## Storage stack

Storage responsibilities are layered:

```text
logical record
    ↓ RecordCodec<Record>
encoded record bytes
    ↓ RowV1 writer/reader
checksummed compressed blocks
    ↓ immutable segment
manifest generation
    ↓ BlobStore
local filesystem or S3-compatible object storage
```

The segment reader knows about blocks, checksums, sparse indexes, Bloom filters,
tiers, and format versions, but not torrent-domain objects. Dataset codecs are
the bridge between logical records and physical bytes.

### Segments and tiers

Segments contain a versioned header, independently compressed blocks, optional
key indexes and Bloom filters, and a checksummed footer. Zstandard is the
default codec.

- **HOT** segments favor cheap append-oriented writes.
- **WARM** segments are compacted, sorted where useful, and indexed for keyed
  access.
- **COLD** observation segments use larger scan-oriented blocks and stronger
  compression.

The RowV1 format has independent storage-format and record-schema versions.
Readers reject unknown versions explicitly. The migration path can read legacy
HOT segments and publish upgraded replacements without changing committed
objects in place.

### Manifests, snapshots, and commits

The manifest generation is the transaction boundary. A write session first
publishes all immutable objects and then atomically publishes the new manifest.
New snapshots see either the old or new generation, never a partial commit.

A snapshot pins its exact generation for repeatable reads. Compaction creates
replacement segments and publishes a new manifest, while existing snapshots
continue reading the old objects. Garbage collection removes an object only
after it is unreachable from every live pinned generation.

Concurrent canonical writers use optimistic publication. A generation change
is normal contention rather than corruption: torrent materialization rebases
its already-aggregated observation ranges, and retention or compaction rebuilds
from a fresh manifest. Each path retries a bounded number of times before
reporting an operational error, so sustained contention remains visible while
short-lived races do not leave the service in a failed state.

The optional S3 backend moves immutable content-addressed blobs behind the
`BlobStore` boundary. It deliberately leaves manifests, the single-writer
lock, derived indexes, and operational state under `storage.local_root`, so the
manifest remains the local atomic transaction boundary. Uploads and downloads
use bounded local staging; a downloaded object is size-checked and hashed
before a reader can observe it. Requests use SigV4 with credentials supplied
through `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and the optional
`AWS_SESSION_TOKEN`. Path-style bucket URLs support AWS S3 and compatible
endpoints without adding provider-specific configuration.

### Compaction and retention

Compaction merges immutable segments, applies safe keyed-record replacement or
observation deduplication, rebuilds statistics and indexes, and performs tier
transitions. Replacement objects are verified before the manifest changes.

Observation expiration is disabled by default because it permanently reduces
historical resolution. When enabled, only whole segments whose maximum
observation time crosses the configured boundary are archived or expired.
Torrent metadata is retained indefinitely by the current policy.

## DHT and metadata pipeline

The DHT layer implements KRPC parsing, tokens, routing, bootstrap, iterative
queries, observation admission, and BEP 9 metadata exchange. BEP 42 node
identities are the public-network default and can be reconfigured after a
quorum agrees on the node's observed external address.

The runtime emits logical observations rather than writing segment internals.
Metadata candidates pass through bounded queues, connection and idle timeouts,
piece validation, metainfo hashing, retry policy, and storage conflict handling.
An observation also feeds a bounded, iterative peer-discovery planner. It
queries routing contacts with `get_peers`, follows closer contacts returned in
responses, and converts compact peer endpoints into metadata candidates. Each
infohash has independent parallelism and query limits plus a cooldown, so
improved metadata coverage does not require unbounded DHT fan-out. IPv4 and
IPv6 traversals remain isolated even when a response contains contacts for both
families. A persistent round-robin cursor grants at most one new query to a
hash per scheduling pass. Scheduling admits a FIFO working set sized to keep
the configured query concurrency full at the per-hash parallelism limit. This
lets iterative traversals reach peers or exhaustion before a large backfill
queue can dilute every hash to an occasional first-hop query. The default query
window uses half of the DHT node's global outstanding-query allowance, leaving
capacity for bootstrap and routing maintenance.

The runtime action bridge applies its queue limit to ordinary inbound work but
always preserves query completions. The protocol node removes a completed query
before publishing that action, so dropping it would strand the corresponding
owner-thread planner entry without a future response or timeout. Planner and
dispatch errors collected during a cycle are also forwarded to the service
observer after the cycle snapshot, keeping operational error state visible
until a later clean cycle records recovery.

Existing canonical records without fetched metadata are also fed into this
planner by a bounded backfill scan. The scan starts with a complete keyed view,
then follows the torrent dataset change stream so an unchanged catalog is not
re-read continuously. Compaction or an invalidated change cursor safely causes
a new complete scan. Planner backpressure retains the current record and
resumes later instead of dropping discovery work. A separate, slower complete
rescan permits previously exhausted hashes to be tried again after their
cooldown without turning normal change polling into repeated catalog scans.

The local flow is:

```text
DHT packet
  → validated observation
  → immutable observation segment
  → incremental torrent materialization
  → local search and duplicate projections
  → authenticated HTTP query
```

A fresh public node needs at least one contact before it can bootstrap
proactively. Explicit TOML, environment, and command-line contacts take
precedence. When they are absent, Sakuin may load one `host:port` entry per
line from a standalone bootstrap file; release bundles provide an initial
public-router list that operators can replace. After bootstrap, a bounded
routing-discovery planner periodically issues `find_node` queries toward
rotating Kademlia distance buckets. This expands a sparse routing table beyond
the bootstrap routers without creating an unbounded query fan-out. If no list
is available, the node warns and permits passive discovery.

DNS absence is evaluated per address family. An IPv4-only router therefore
does not prevent a dual-stack node from starting its IPv6 family with other
contacts, while startup still fails when none of the configured contacts
resolve for any enabled family.

Bootstrap status separates success from terminal failure. `bootstrap_complete`
becomes true only after the bootstrap-owned traversal settles with at least one
successful `find_node` response; exhausting every seed and retry instead sets
`bootstrap_exhausted`. Unrelated routing, discovery, and peer queries do not
delay either state.

The release Compose file creates a dual-stack bridge and enables both DHT
families by default. Docker assigns the bridge an IPv6 subnet when none is
specified and performs the host-side forwarding for the published UDP port.
An external Compose network must be created with IPv6 enabled separately;
Compose cannot change the address-family settings of an existing external
network.

## Search and duplicate indexes

Search and duplicate state is derived and stored separately from canonical
segments. Both can be removed and rebuilt.

The search view indexes torrent name, file path, infohash, total size, file
count, observation timestamps, and derived classifications. It supports text,
structured metadata filters, semantic categories, and optional classifier
state, kind, confidence, and label facets with bounded pagination. Classifier
facets do not override the independently configured Adult visibility policy.
The in-memory search view is a compact projection rather than a copy of the
canonical torrent model: it retains display/search text, metadata summaries,
classification output, and a fixed 4096-bit trigram filter per record. The
filter cheaply rejects impossible candidates while the authoritative substring
matcher and scorer preserve name, path, and infohash semantics. It replaces the
unbounded trigram posting map and avoids retaining per-file sizes that search
does not use. Observation-only placeholders remain canonical and eligible for
metadata backfill but are omitted from the search projection until metadata
makes them searchable. Incremental refreshes append a checksummed derived
update log and mutate the projection under its ownership lock; a full rebuild
replaces the base file and clears the log. Canonical torrent segments remain
the source of truth after any derived-index loss. Query evaluation counts every
match for exact API totals but retains and sorts only the best `offset + limit`
matches, bounding the transient working set for normal first-page searches.

Duplicate detection publishes four explicitly versioned fingerprints: an exact
sorted file-layout identity, a normalized name/path identity, a conservative
payload-layout similarity signature, and a fuzzy release identity. The payload
signature ignores names and directories while retaining exact file sizes and
normalized extensions, so it can group renamed or reorganized payloads. Small
single-file torrents are excluded from that signature because their layout is
not discriminative enough. Release identity normalizes separators and ignores
a deliberately narrow vocabulary of resolution, source, codec, audio, and
repack markers. A trailing alphanumeric release-group suffix is ignored only
when one of those packaging markers precedes it, while titles, years, episode
IDs, and versions remain significant. Weak single-token identities are
excluded. These are likely-duplicate signals rather than content hashes.
Versioning the algorithms avoids silently changing group semantics as detection
expands.

## API and credentials

The HTTP service exposes health, detailed operator status, Prometheus metrics,
search, duplicate queries, and an authenticated search-refresh operation.
Status snapshots
aggregate DHT family cycles, bootstrap progress, datagram dispatch, derived
index generations, materialization, maintenance, active routing discovery, and
active metadata-peer discovery without exposing runtime or Asio types. Peer
discovery reports pending hashes, the traversals admitted to its active working
set, in-flight queries, discovered peers, and exhausted traversals per address
family. It also separates received responses, network timeouts, local delivery
failures, and successful hash traversals so operators can compare IPv4 and IPv6
scheduling with time-series deltas.
Reaching the bounded pending-hash limit is normal backpressure: newly stored
observations remain canonical and the metadata backfill offers them later.
Malformed packets from public peers increment inbound protocol-error telemetry
without becoming service incidents or error-level logs; socket/runtime,
storage, and internal failures still follow the service-error path.
Destination-specific asynchronous UDP failures are likewise delivery telemetry:
they increment `datagrams_failed` and retain the latest delivery error in the
status API. Socket and receive failures remain service errors. This distinction
keeps an unreachable public IPv6 contact from impersonating a family-wide
runtime outage while still exposing persistent local routing problems through
time-series failure ratios.
Metadata backfill additionally reports records scanned, targets offered,
records already carrying metadata, source generation, and whether a full scan
is in progress. These counters distinguish a discovery bottleneck from an
acquisition or storage bottleneck without exposing peer addresses.
Metadata acquisition exposes attempts, verified fetches, retryable and
permanent failures, and result/storage-sink outcomes as monotonic counters.
Fetch failures are further divided into the bounded reasons `io`, `timeout`,
`storage_unavailable`, `invalid_metadata`, `protocol`, `quota`, and `other`;
peer endpoints and free-form error text never become metric labels.
Its backlog gauge is the sum of candidates queued, acquisitions in flight, and
verified records waiting for their configured sink; the three component gauges
remain available for locating the congested stage. Inbound
query counts are split into `ping`, `find_node`, `get_peers`, `announce_peer`,
and unknown methods; responses and protocol errors are counted separately.
This provides direct evidence that published UDP ingress is reaching Sakuin,
while avoiding peer-address labels and their unbounded cardinality. Search and
duplicate data is returned through domain views rather than exposing manifests
or segment paths.

Classification observability is derived from the currently published search
snapshot. Status exposes state and category distributions, bounded-input
truncation, Adult label counts, and the projection's estimated retained bytes.
Prometheus exports the same data, including
`sakuin_search_index_estimated_memory_bytes`, as gauges with fixed
state/category label sets. Adult label and Adult category counts are
descriptive: the latter reflects the configured confidence threshold, while
neither metric changes the operator-selected visibility policy.

Tagged release builds inject one shared build version into both executables.
The daemon exposes it through the authenticated status response and the bounded
`sakuin_service_info` metric; local builds use the explicit `dev` identity.

The public `/openapi.json` route serves a process-cached OpenAPI 3.1 document
for the native JSON endpoints. It describes bearer authentication and required
Sakuin permissions without embedding credentials. Torznab stays outside that
document because its XML discovery and capability contract is
`/torznab/api?t=caps`.

The unauthenticated `/v1/health` route is a narrow HTTP liveness probe.
`/v1/ready` reports success only after the service state is running and every
enabled DHT family worker is running. It returns no component details; those
remain behind the authenticated status and metrics routes.

`/metrics` and `/v1/metrics` serialize that same snapshot using the Prometheus
text format. Both require an operator credential. Labels are bounded to service
state and IP address family; peer addresses and error messages are omitted to
avoid sensitive output and unbounded time-series cardinality.
Service errors are retained per bounded subsystem with a cumulative count,
last-seen timestamp, and explicit active or recovered state. Successful
subsystem callbacks close recoverable incidents without erasing their history;
`last_service_error` therefore describes the most recent incident rather than
acting as an indefinitely sticky error string. Prometheus exports the same
counts, state, and timestamps using subsystem labels while continuing to omit
error text.
Operational decisions use deltas or rates across sampled counter values, never
the absolute magnitude of a cumulative counter. The deployment helper can
sample the discovery-to-metadata pipeline without an external time-series
database and identifies new or reset series explicitly. Prometheus deployments
retain the cumulative counters and derive equivalent windows with `rate` or
`increase`.

When maintenance is enabled, authenticated operators may enqueue a pass through
`POST /v1/operations/storage-maintenance`, optionally including verification.
The HTTP request only signals the maintenance coordinator and returns `202`;
compaction, retention, garbage collection, and verification remain serialized
on the maintenance owner thread rather than blocking an Asio request thread.

The `/torznab/api` compatibility route implements Torznab capabilities and
UTF-8 XML/RSS search responses; `/api` remains a legacy alias. Semantic
classifications map to the advertised Movies, TV, Audio, PC, Books, XXX, and
Other category families. Applications use PC while games use its standard
Games subcategory. Generic and specialized movie, TV, music/audio, and book
searches apply category filters inside the derived index before totals and
pagination. Unknown category IDs are ignored as Torznab specifies; a request
containing only unknown IDs is empty. Results use magnet links because the
canonical store retains decoded torrent metadata rather than downloadable
`.torrent` payloads.

The advertised category set is part of the compatibility contract: every
category emitted in an item must also appear in the capabilities document.
This matters for clients such as Prowlarr, which build a tracker-to-standard
category map from `t=caps` and retain that map in memory. Sakuin versions before
classification advertised only Other, so a Prowlarr process that cached those
old capabilities can temporarily map `8000` while dropping newer category IDs.
Using the canonical `/torznab/api` path instead of the legacy `/api` alias
changes the client-side cache identity and forces capability discovery; a
Prowlarr restart has the same effect.

Classification is a deterministic, rebuildable projection over torrent names,
file paths, sizes, and layout. It records a semantic content kind, confidence,
bounded evidence identifiers, and independent labels such as Anime and Adult;
none of these derived values mutate canonical torrent metadata. Records without
a fetched file list remain explicitly awaiting metadata rather than being
treated as unknown. Adult detection defaults on, while search visibility is a
separate operator choice (`include`, `exclude`, or `only`) that defaults to
including all content. Operators also choose the minimum Adult-label confidence
used by that policy (`low`, `medium`, or `high`, defaulting to `high`).
Disabling detection suppresses only the label and never
filters a torrent. The local search projection recomputes these values from
canonical records when it opens or refreshes, so rule changes do not require a
canonical storage migration. Native JSON results expose the derived kind,
confidence, labels, categories, resolution, algorithm version, and truncation
state. They also include each bounded evidence code, subject, and signed rule
weight so clients and operators can explain a classification without inspecting
classifier internals. The rules inspect bounded untrusted strings without
opening paths, extracting archives, or performing network lookups.
Strong content-kind signals also tolerate one insertion, deletion,
substitution, or adjacent transposition using a bounded deterministic matcher.
Fuzzy evidence is weighted below an exact token and is exposed separately for
auditing. Short signals, structured episode/resolution tokens, and Adult or
Anime labels remain exact-only to limit false positives. The built-in rules
require no operator setup. Operators may optionally add bounded exact-token
content-kind rules in TOML. A rule uses `any` or `all` matching, contributes a
bounded weight to the same scorer instead of forcibly replacing its decision,
and exposes its configured ID as `operator_rule` evidence. Operator rules do
not assign Adult or Anime labels and cannot change visibility policy.
TV evidence covers compact season/episode forms, explicit TV/series words,
season packs, and hyphenated two- or three-digit absolute episode numbers. This
evidence outranks the generic single-video movie prior, which exists only as a
fallback when no episodic signal is present.

Classifier changes are checked against a version-controlled regression corpus
with explicit expected state, kind, minimum confidence, label requirements, and
label exclusions. The corpus includes adult detection enabled and disabled
cases so detection remains a configurable annotation policy rather than a
visibility decision. `xmake test sakuin-classifier-eval/classification-corpus`
runs the corpus. The evaluator emits deterministic coverage counts by resulting
state, kind, and label. These counts describe regression coverage only; the
hand-curated cases are not presented as population accuracy, precision, or
recall measurements.

An optional learned content-kind fallback complements those rules without
replacing them. During each derived search rebuild, Sakuin trains a bounded
multinomial Naive Bayes model from the same node's high-confidence deterministic
results. Candidate features and per-record extraction are capped, low-support
features are discarded, the final vocabulary is bounded, and a kind needs a
minimum local training set before it is eligible. Uniform kind priors prevent
the most common local media kind from winning solely because it is common.

The learned stage runs only for records that contain metadata and remain
`Unknown` or `Ambiguous` after deterministic classification. Accepted
predictions require both posterior and margin thresholds, are capped at Medium
confidence, and carry `learned_content_model` evidence. Deterministic
`Classified` results are authoritative. Adult and Anime labels, resolution, and
all operator visibility policy remain deterministic and are never learned or
overridden. The model is disposable derived state rebuilt from canonical
torrent metadata; no model artifact becomes a source of truth. Status and
metrics expose whether it is ready, its training/vocabulary coverage, and how
many current records use it.

API credentials live in a separate operational store. The CLI generates the
secret once and persists a salted verifier, owner-only pepper, and permission
set. Writes are locked and atomically replaced. Credentials can be created,
listed, and disabled; `SIGHUP` reloads them without restarting the daemon.
Requests also pass through connection, request-size, timeout, and per-key rate
limits. TLS can terminate in Sakuin or at a loopback-facing reverse proxy.

## Traffic budgets

Traffic is admitted through a runtime-level periodic byte budget rather than
independent sleeps scattered through DHT code. IPv4/IPv6 traffic and metadata
operations share the configured inbound and outbound ceilings.

When remote workers are enabled, the coordinator grants bounded byte chunks.
Workers charge individual datagrams locally, avoiding a coordinator round trip
per packet. Grants are charged when reserved so unused quota cannot make the
fleet exceed the configured window.

## Distributed work

Single-node operation uses the same work abstraction as the distributed path.
The TCP protocol is framed and versioned, with bounded payloads, leases,
heartbeats, renewal, cancellation, retry policy, and ordered chunked results.
Logical results are content-ID checked and published only by the coordinator.

Remote workers are disposable and never require access to canonical storage.
Remote coordination is allowed only with mutual TLS; the certificate identity
namespaces the protocol worker ID. Plaintext coordination is loopback-only.

Canonical result receipts are durable and prevent an acknowledged retry from
duplicating facts after restart or compaction. Receipts contain the result
identity and kind rather than a second payload copy.

Coordinator work is operational state, stored in a checksummed atomic
checkpoint. Interrupted non-final leases return to pending after a crash.
Completed and failed work is retained only within configurable age and count
bounds; durable result receipts remain independent. Version-1 checkpoints are
read and migrated to the current version.

This is crash-safe local recovery, not replicated coordinator high
availability. A future distributed scheduler can replace it behind the work
interface.

## Local deployment, images, and release bundles

The self-contained Compose deployment pulls `ghcr.io/brandenxia/sakuin`, stores
canonical, derived, and operational state in one named volume mounted at
`/var/lib/sakuin`, and initializes scoped reader/operator credentials before
starting the daemon. The API listens inside the container on TCP 8080 and the
DHT on UDP 6881. Containers run as UID/GID 10001 with a read-only root
filesystem, no Linux capabilities, and a writable temporary filesystem.

The Dockerfile does not compile the project. It downloads and checksum-verifies
a release asset named `sakuin-linux-amd64.tar.gz` or
`sakuin-linux-arm64.tar.gz`. Each archive has this layout:

```text
bin/sakuin
bin/sakuin-api-key
lib/                    # non-system shared libraries, when required
share/sakuin/dht-bootstrap.txt
```

The adjacent `.sha256` release asset is mandatory. `scripts/package-linux.sh`
creates this bundle from prebuilt Linux binaries. The release workflow builds
native `amd64` and `arm64` bundles on pushes to the `release` branch. A `v*`
tag must point to a commit reachable from that branch; after both builds pass,
the workflow creates or updates the GitHub Release, attaches the bundles,
checksums, and Compose file, then publishes a provenance- and SBOM-bearing
multi-architecture GHCR image tagged with the release and `latest`. The final
workflow step verifies an anonymous pull. If GitHub creates the package as
private on its first publication, an owner must make it public once in the
package settings before rerunning that step.

## Administration, migration, and recovery

The administration CLI verifies all segment checksums and record counts,
compacts datasets, and garbage-collects unreachable objects. Backend-neutral
migration copies a pinned snapshot's reachable objects, verifies their content
IDs, and only then publishes the destination manifest.

Derived search and duplicate state is stored beneath `derived/`; credentials,
locks, work checkpoints, receipts, and counters live beneath `operational/`.
Neither is confused with canonical observations and torrents.

## Testing and benchmarks

The test suite covers codecs, corrupted segments, format versions, manifests,
snapshot pinning, compaction, retention, migration, local and fake-S3 blob
stores, DHT protocol and runtime, metadata acquisition, search, API security,
traffic budgets, remote work, result idempotency, and crash recovery.

The separate storage benchmark measures append and scan throughput,
compression ratio, compaction and COLD archival throughput, write amplification,
keyed lookup rate, and WARM/COLD scan rate:

```bash
xmake build sakuin-storage-benchmark
xmake run sakuin-storage-benchmark 100000 65536
```

The search microbenchmark builds a path-heavy compact projection and compares
broad browsing, absent, sparse-hit, and common-term query costs. Optional
arguments select the record and query counts:

```bash
xmake build sakuin-search-benchmark
xmake run sakuin-search-benchmark 5000 100
```

`scripts/benchmark-deployment.sh` provides a read-only post-deployment
benchmark for the public health endpoint, native and Torznab searches, operator
status, and Prometheus serialization. It reports request throughput and latency
percentiles, the search projection's estimated retained bytes, and Docker
memory/CPU samples when the target container is locally visible. Its default
request volume stays below the deployment's default authenticated API budget;
larger runs warn when rate limiting may dominate the result. The default
200 MiB container-memory comparison is informational rather than a fixed
service limit, since retained metadata scales with the searchable dataset.
For suspected growth, `--soak-seconds` keeps sampling the otherwise normally
running container and reports start, end, peak, absolute growth, and a
least-squares MiB/hour slope. That slope must be interpreted alongside the
search projection and record-count deltas: growth while metadata is being
indexed is expected, while repeatable unaccounted growth after the projection
stabilizes warrants a runtime investigation.

## Known boundaries

- S3-compatible storage currently offloads immutable segment blobs only;
  manifests, coordination, and derived state remain local and single-writer.
- The search engine is a local derived implementation rather than an external
  cluster.
- Coordinator recovery is durable on one host but not replicated.
- Duplicate matching uses exact content IDs, normalized metadata, renamed
  payload-layout fingerprints, and conservative fuzzy release identities; it
  does not yet perform probabilistic cross-title semantic similarity.
- There is currently no browser UI. Classification is metadata-based and
  combines built-in deterministic evidence with optional operator-defined
  content-kind rules and an optional locally learned fallback.

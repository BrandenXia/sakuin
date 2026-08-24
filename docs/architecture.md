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
surface. [`config/sakuin.docker.toml`](../config/sakuin.docker.toml) is the small
single-node container profile.

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
local filesystem
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
public-router list that operators can replace. If no list is available, the
node warns and permits passive discovery.

DNS absence is evaluated per address family. An IPv4-only router therefore
does not prevent a dual-stack node from starting its IPv6 family with other
contacts, while startup still fails when none of the configured contacts
resolve for any enabled family.

## Search and duplicate indexes

Search and duplicate state is derived and stored separately from canonical
segments. Both can be removed and rebuilt.

The search view indexes torrent name, file path, infohash, total size, file
count, and observation timestamps. It supports text and structured filters with
bounded pagination.

Duplicate detection currently publishes two explicitly versioned fingerprints:
an exact sorted file-layout identity and a normalized name/path identity.
Versioning the algorithm avoids silently changing group semantics when future
normalization or fuzzy matching is introduced.

## API and credentials

The HTTP service exposes health, detailed operator status, search, duplicate
queries, and an authenticated search-refresh operation. Status snapshots
aggregate DHT family cycles, bootstrap progress, datagram dispatch, derived
index generations, materialization, and maintenance without exposing runtime
or Asio types. Search and duplicate data is returned through domain views
rather than exposing manifests or segment paths.

The `/api` compatibility route implements Torznab capabilities and generic
search responses as UTF-8 XML/RSS. It publishes the `Other` category because
classification is not yet available, and returns magnet links because the
canonical store retains decoded torrent metadata rather than downloadable
`.torrent` payloads.

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
snapshot pinning, compaction, retention, migration, DHT protocol and runtime,
metadata acquisition, search, API security, traffic budgets, remote work,
result idempotency, and crash recovery.

The separate storage benchmark measures append and scan throughput,
compression ratio, compaction and COLD archival throughput, write amplification,
keyed lookup rate, and WARM/COLD scan rate:

```bash
xmake build sakuin-storage-benchmark
xmake run sakuin-storage-benchmark 100000 65536
```

## Known boundaries

- The canonical backend is currently the local filesystem; S3-compatible
  storage remains future work.
- The search engine is a local derived implementation rather than an external
  cluster.
- Coordinator recovery is durable on one host but not replicated.
- Duplicate matching is deterministic metadata fingerprinting, not semantic or
  fuzzy similarity.
- There is currently no browser UI or media classifier. Torznab support is
  limited to generic search until classification is available.

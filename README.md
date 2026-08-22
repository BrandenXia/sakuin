# Sakuin

Sakuin is a self-hosted BitTorrent DHT indexer built around portable immutable
canonical storage rather than a database. It discovers infohashes, acquires
torrent metadata, materializes searchable torrent records, and maintains
rebuildable search and duplicate indexes.

The working local deployment requires no PostgreSQL, Redis, SQLite, or external
search service. Canonical state consists of content-addressed segment objects
and atomic manifest generations under the configured storage root.

## Current architecture

- C++23 modules built with xmake
- Standalone Asio hidden behind Sakuin datagram, stream, and server interfaces
- TOML configuration with defaults → file → environment → CLI precedence
- BEP 42 DHT identities by default
- RowV1 HOT, indexed WARM, and scan-oriented COLD storage tiers
- Zstandard block compression, checksums, immutable compaction, snapshot pins,
  and reachability-based garbage collection
- Incremental observation materialization into keyed torrent metadata
- Durable, rebuildable local search and duplicate projections
- API-key authentication, permissions, request limits, and optional TLS
- Local work coordination plus a versioned TCP coordinator/worker protocol
  with leases, heartbeats, cancellation, renewal, bounded durable results,
  and retry handling
- Crash-safe, checksummed coordinator work checkpoints with bounded recovery;
  interrupted leases return to the pending queue without restoring stale
  worker sessions or lease ownership
- Mutually authenticated remote workers whose certificate identity scopes
  their protocol worker namespace
- Coordinator-reserved traffic grants that enforce one aggregate periodic DHT
  byte budget across its local crawler and all remote workers
- spdlog operational logging

Plaintext coordination is deliberately loopback-only. With a dedicated worker
CA and mTLS configured, disposable `sakuin worker` processes can crawl and
fetch metadata remotely without access to the storage backend. Observation and
logical torrent-metadata batches are content-ID validated, materialized only by
the coordinator, and recorded in a durable result receipt log before success is
acknowledged. Retrying an acknowledged batch does not duplicate canonical
facts, including after process restart or observation compaction.
Receipts store only the 17-byte result identity and kind, not a second copy of
the result payload. Normal storage maintenance deduplicates them into compact
RowV1 segments and garbage-collects replaced objects. Receipt identities do
not expire: without a bounded delivery lifetime, deleting one could make a
late acknowledged retry unsafe after canonical compaction.

Coordinator work state is atomically checkpointed under
`<storage.local_root>/operational/scheduler` by default. A checkpoint failure
puts the coordinator into a fail-stop state before it acknowledges the
mutation, preserving at-least-once recovery. This is a local durable baseline,
not replicated coordinator high availability; the storage abstraction leaves
room for a distributed implementation later.

## Build and test

Dependencies are declared in [xmake.lua](xmake.lua): OpenSSL, Zstandard,
Standalone Asio, toml++, llhttp, nlohmann/json, and spdlog.

```bash
# macOS with Homebrew LLVM
xmake f --toolchain=clang --sdk=/opt/homebrew/opt/llvm

# Linux with a compiler that supports C++ modules
# xmake f --toolchain=gcc

xmake build
xmake test
```

The storage benchmark is intentionally separate from the default build:

```bash
xmake build sakuin-storage-benchmark
xmake run sakuin-storage-benchmark 100000 65536
```

The arguments are record count and WARM block target bytes. Output includes
append/scan throughput, compression ratio, compaction and COLD-archive
throughput, write amplification, keyed lookup rate, and WARM/COLD scan rate.

## Configure and run

Start with [config/sakuin.example.toml](config/sakuin.example.toml):

```bash
cp config/sakuin.example.toml sakuin.toml
xmake run sakuin --config=sakuin.toml
```

Normal settings can also be overridden through `SAKUIN_*` environment variables
or `--section.key=value` arguments. The example keeps both the API and work
coordinator on loopback.

For a remote crawler and metadata fetcher, configure both
`[distributed.coordinator]` and `[distributed.worker]` as shown in the example,
issue the worker a certificate whose common name exactly matches
`distributed.worker.id`, then run:

```bash
xmake run sakuin worker --config=worker.toml
```

One worker process registers `id:v4` and/or `id:v6` beneath that authenticated
identity. The worker needs DHT network access and coordinator TCP access, but no
canonical-storage mount or database credentials. `distributed.maximum_payload_bytes`
is the per-frame payload ceiling. Results up to
`distributed.maximum_result_bytes` are transferred as ordered bounded chunks,
then content-ID verified and published atomically by the coordinator. Global
reassembly byte/count limits and expiration prevent abandoned transfers from
becoming an unbounded operational-state sink.

`network.traffic.inbound_bytes` and `outbound_bytes` are coordinator-wide when
the distributed listener is enabled. Workers reserve bounded chunks using
`network.traffic.grant_bytes`, then admit individual datagrams locally; this
avoids a coordinator round trip per packet. Reservations are conservatively
charged immediately, so an unused remainder cannot cause the fleet to exceed
the configured window.

Observation retention is deletion-capable and therefore disabled by default.
When enabled, only whole segments whose maximum observation time has crossed a
cutoff are archived or expired. Torrent metadata is retained indefinitely, and
live snapshots continue to pin replaced objects until garbage collection is
safe.

## API credentials

Initialize the credential store before enabling the API:

```bash
xmake run sakuin-api-key --state-dir=./data/operational/api init
xmake run sakuin-api-key --state-dir=./data/operational/api \
  create --id reader --permissions search
```

The creation command prints the bearer credential once. The store persists
salted verifier material rather than plaintext secrets. Use the same CLI to
list, disable, enable, rotate, and remove keys.

Authenticated endpoints include:

- `GET /v1/health`
- `GET /v1/search`
- `GET /v1/duplicates`
- `GET /v1/duplicates/{infohash}`

Search supports text over torrent names, paths, and infohashes together with
size, file-count, first-seen, last-seen, offset, and limit filters.

## Storage administration and migration

```bash
xmake run sakuin admin verify --config=sakuin.toml
xmake run sakuin admin compact --config=sakuin.toml
xmake run sakuin admin gc --config=sakuin.toml
```

The storage administration API also provides backend-neutral pinned-snapshot
migration. It copies and content-ID-verifies reachable objects before publishing
the destination manifest. A separate RowV1 migration operation upgrades legacy
HOT 1.0 segments to schema-aware HOT 1.1 without mutating committed objects.

Derived search and duplicate state lives under `derived/` and can be discarded
and rebuilt from canonical torrent segments. Operational credentials, locks,
leases, and counters are kept separate from canonical data.

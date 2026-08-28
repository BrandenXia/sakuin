# Sakuin

Sakuin is a self-hosted BitTorrent DHT indexer. It discovers torrent
infohashes, fetches metadata, keeps a searchable local catalog, and groups
likely duplicate torrents without relying on PostgreSQL, Redis, or an external
search service.

Sakuin is designed for people who want to own and move their index as ordinary
files. Its source of truth is a compact, immutable local dataset; search and
duplicate indexes can be rebuilt from it.

## Features

- Crawls the public BitTorrent DHT and acquires torrent metadata.
- Searches torrent names, paths, sizes, file counts, dates, and infohashes.
- Detects exact, normalized-metadata, and renamed payload-layout duplicates.
- Preserves historical observations with configurable retention and
  compression.
- Enforces configurable DHT traffic budgets.
- Protects the HTTP API with scoped API keys and rate limits.
- Runs as one local service, with optional authenticated remote workers.
- Stores its portable catalog state in a local volume, with optional
  S3-compatible storage for immutable segment blobs.

## How it compares

[Bitmagnet](https://bitmagnet.io/) is the closest established project. Both
projects crawl the DHT and provide a self-hosted searchable torrent catalog,
but they make different tradeoffs.

| | Sakuin | Bitmagnet |
|---|---|---|
| Primary storage | Portable immutable files; optional S3 blob offload | PostgreSQL |
| Minimum deployment | One application container and one data volume | Application plus PostgreSQL |
| Historical data | Explicit observation history and tiered retention | Database-oriented current catalog |
| Duplicate grouping | File-layout and normalized-metadata fingerprints | Classification-oriented processing |
| Distributed crawling | Built-in coordinator and disposable remote workers | Worker model centered on the shared database |
| User interface | HTTP API only | Mature web UI and integrations |
| Project maturity | New; operational interfaces may still evolve | Established and more feature-complete |

Sakuin's main advantages are simple data ownership, no database administration,
storage-efficient history, and a design that can scale crawling separately
from canonical storage. Its current disadvantages are equally important: there
is no web UI, the S3 mode still keeps transactional catalog state on one local
volume, there are fewer media/classification integrations, and there is less
real-world deployment history than Bitmagnet.

## Quick deployment

Requirement: Docker with the Compose plugin. Each tagged release publishes a
multi-architecture image for AMD64 and ARM64 plus a self-contained Compose
file. Download that one file, start it, and read the one-time credentials:

```bash
curl -LO https://github.com/BrandenXia/sakuin/releases/latest/download/docker-compose.yaml
docker compose up --detach
docker compose logs sakuin-init
```

Save the displayed `reader` and `operator` bearer tokens, then remove the
completed initializer and its token-bearing logs with
`docker compose rm --force sakuin-init`. Data is kept in the `sakuin-data`
Docker volume. The API is available at `http://127.0.0.1:8080` by default; UDP
port 6881 is exposed for the DHT. The image includes a small public DHT
bootstrap list. The bundled Compose network is dual-stack and Sakuin uses both
IPv4 and IPv6 by default; set `SAKUIN_ENABLE_IPV6=false` in an adjacent `.env`
file only when the Docker host cannot route IPv6.

```bash
curl http://127.0.0.1:8080/v1/health
curl http://127.0.0.1:8080/v1/ready
curl http://127.0.0.1:8080/openapi.json
curl -H "Authorization: Bearer YOUR_TOKEN" \
  "http://127.0.0.1:8080/v1/search?q=ubuntu"
curl -H "Authorization: Bearer YOUR_OPERATOR_TOKEN" \
  http://127.0.0.1:8080/v1/status
curl -H "Authorization: Bearer YOUR_OPERATOR_TOKEN" \
  http://127.0.0.1:8080/metrics
curl -X POST -H "Authorization: Bearer YOUR_OPERATOR_TOKEN" \
  http://127.0.0.1:8080/v1/operations/search-refresh
curl -X POST -H "Authorization: Bearer YOUR_OPERATOR_TOKEN" \
  "http://127.0.0.1:8080/v1/operations/storage-maintenance?verify=true"
```

The detailed status response and Prometheus `sakuin_service_info` metric include
the running release version. Native binaries also report it with
`sakuin --version` and `sakuin-api-key --version`.

`/openapi.json` publishes the native JSON API contract for client generation
and interactive API tools. It contains no credentials and is available without
authentication. Torznab clients should continue to discover their XML contract
through `/torznab/api?t=caps`.

Useful commands from a repository checkout:

```bash
./scripts/deploy.sh status YOUR_OPERATOR_TOKEN
./scripts/deploy.sh metrics YOUR_OPERATOR_TOKEN
./scripts/deploy.sh activity YOUR_OPERATOR_TOKEN 30
./scripts/deploy.sh maintenance YOUR_OPERATOR_TOKEN
./scripts/deploy.sh maintenance YOUR_OPERATOR_TOKEN verify
./scripts/deploy.sh logs
./scripts/deploy.sh verify
./scripts/deploy.sh key reader-2 search
./scripts/deploy.sh down
```

Those helper commands are available when deploying from a repository checkout.
The script pulls the published image and handles first-run credentials. For the
single-file deployment, use the corresponding Compose commands directly:
`docker compose ps`, `docker compose logs --follow sakuin`, and
`docker compose down`.
Set `SAKUIN_IMAGE=ghcr.io/brandenxia/sakuin:vX.Y.Z` in an adjacent `.env` file
to pin a release.

After deployment, the read-only benchmark helper measures health, native
search, Torznab search, status, and metrics latency and throughput. It also
samples container CPU and memory when it can find the Compose service, and
reports the search projection's estimated memory separately. Run it without a
token argument to enter the operator token at the hidden prompt:

```bash
./scripts/benchmark-deployment.sh
./scripts/benchmark-deployment.sh --requests 50 --concurrency 8
./scripts/benchmark-deployment.sh --container sakuin-sakuin-1 --memory-budget 200
./scripts/benchmark-deployment.sh --container sakuin-sakuin-1 --soak-seconds 1800
```

The soak option samples background container memory after the request run and
reports both absolute growth and a least-squares MiB/hour slope. A positive
slope is a diagnostic signal, not proof of a leak: compare it with the search
projection and searchable-record deltas while the index is still growing.

For unattended runs, use `--token-file PATH` or `SAKUIN_BENCH_TOKEN`. Results
are written to standard output as tab-separated sections, so redirecting them
to a dated file makes deployments easy to compare. Runs above the default API
rate-limit budget emit a warning instead of changing production configuration.
The memory budget is an informational comparison and does not make the run
fail, because retained metadata necessarily scales with the searchable dataset.

The Compose file also supports an optional S3-compatible blob backend. In the
same `.env` file, set `SAKUIN_STORAGE_BACKEND=s3`,
`SAKUIN_STORAGE_S3_BUCKET`, and the standard `AWS_ACCESS_KEY_ID` and
`AWS_SECRET_ACCESS_KEY` variables; set the endpoint and region variables for a
non-default provider. Keep the data volume: manifests, locks, derived indexes,
and bounded transfer staging remain local. Temporary AWS session credentials
can include `AWS_SESSION_TOKEN`.

Torznab clients can use `http://127.0.0.1:8080/torznab/api` as the indexer URL
and the complete `sakuin_...` reader token as `apikey`. In Prowlarr's Generic
Torznab form, enter `http://127.0.0.1:8080` as the URL and `/torznab/api` as the
advanced API Path. Sakuin advertises generic, movie, TV, music/audio, and book
search, with standard Torznab categories, including distinct PC application
and game results.

If Prowlarr shows `Unknown` categories after a Sakuin upgrade, change its API
Path from the legacy `/api` alias to `/torznab/api`, test, and save the indexer.
This gives Prowlarr a fresh capability cache immediately; restarting Prowlarr
also clears its in-memory cache.

Results use magnet links because Sakuin indexes metadata rather than hosting
`.torrent` files. Adult classification is only a label: visibility is an
operator setting and defaults to including every result. Native JSON search
also returns bounded rule evidence so classifications can be audited.
Strong content-kind hints tolerate simple one-character misspellings out of the
box, while sensitive labels remain exact-token-only.
Optional exact-token rules in `sakuin.toml` let operators teach local release
vocabulary without replacing the built-in classifier; matched rule IDs appear
in native API evidence.
For metadata-complete results that deterministic rules still leave unknown or
ambiguous, an enabled-by-default local learned fallback adapts to recurring
content-kind vocabulary in the node's own high-confidence records. Its
predictions are conservative and auditable; it never learns sensitive labels,
changes Adult visibility, or overrides a deterministic classified result.
For classifier review workflows, `/v1/search` can filter by exact state and
kind, minimum confidence, and required labels; the published OpenAPI document
lists the accepted values. These facets never bypass the operator's Adult
visibility setting.

Prometheus can scrape `/metrics` (or `/v1/metrics`) with the operator token as
an `Authorization: Bearer` header. The endpoint reports service uptime, DHT
activity and queues, search and duplicate generations, materialization, and
storage-maintenance counters without including peer addresses or error text.
It also distinguishes inbound DHT query methods from responses and reports
active routing-discovery work, which makes passive reachability and crawler
progress visible without packet captures.
Classifier coverage, ambiguity, bounded-input truncation, and semantic category
counts are included so operators can assess classification quality in place.
Counters are cumulative for Prometheus compatibility; assess live crawler
behavior with counter deltas or rates over a time window. From a repository
deployment, `./scripts/deploy.sh activity OPERATOR_TOKEN 30` samples twice and
reports reset-aware deltas and per-second rates for the discovery-to-metadata
pipeline. In Prometheus, use `rate(..._total[5m])` or
`increase(..._total[5m])` for the same purpose instead of comparing totals.

`/v1/health` is a liveness check for the HTTP process. `/v1/ready` stays at
HTTP 503 until the composed service and every enabled DHT address-family worker
are running, then returns HTTP 200. Neither endpoint exposes operator details
or requires a credential, so container orchestrators can use them directly.

When automatic storage maintenance is enabled, operators can enqueue
compaction, retention, and garbage collection with
`POST /v1/operations/storage-maintenance`. Add `?verify=true` to include a full
checksum and record verification pass. The endpoint returns `202` immediately;
the existing maintenance owner thread performs the work, and progress is
visible through `/v1/status`, `/metrics`, and service logs.

## Native usage

Sakuin uses C++23 modules and Xmake. After building, copy
[`config/sakuin.example.toml`](config/sakuin.example.toml), copy or replace the
provided DHT bootstrap list, initialize the credential store, and start the
daemon:

```bash
xmake build
cp config/sakuin.example.toml sakuin.toml
cp config/dht-bootstrap.txt dht-bootstrap.txt
xmake run sakuin-api-key --state-dir ./data/operational/api init
xmake run sakuin-api-key --state-dir ./data/operational/api \
  create --id reader --permissions search
xmake run sakuin --config=sakuin.toml
```

For S3-compatible blob storage, select `backend = "s3"` and fill in the
commented `[storage.s3]` example, then provide credentials through the standard
AWS environment variables. Sakuin uses path-style bucket URLs and verifies
downloaded blobs against their content IDs before exposing them to readers.
The local root is still required for the transactional catalog and operational
state.

Storage administration is available through `sakuin admin verify`, `compact`,
and `gc`. See [`docs/architecture.md`](docs/architecture.md) for the design,
data lifecycle, distributed runtime, persistent formats, and testing strategy.

Only index and access content that you are legally permitted to handle. A DHT
index can observe untrusted or unlawful metadata; operators are responsible for
retention, access control, and compliance in their jurisdiction.

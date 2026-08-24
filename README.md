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
- Detects exact file-layout and normalized-metadata duplicates.
- Preserves historical observations with configurable retention and
  compression.
- Enforces configurable DHT traffic budgets.
- Protects the HTTP API with scoped API keys and rate limits.
- Runs as one local service, with optional authenticated remote workers.
- Stores all persistent data in a portable local volume.

## How it compares

[Bitmagnet](https://bitmagnet.io/) is the closest established project. Both
projects crawl the DHT and provide a self-hosted searchable torrent catalog,
but they make different tradeoffs.

| | Sakuin | Bitmagnet |
|---|---|---|
| Primary storage | Portable immutable files | PostgreSQL |
| Minimum deployment | One application container and one data volume | Application plus PostgreSQL |
| Historical data | Explicit observation history and tiered retention | Database-oriented current catalog |
| Duplicate grouping | File-layout and normalized-metadata fingerprints | Classification-oriented processing |
| Distributed crawling | Built-in coordinator and disposable remote workers | Worker model centered on the shared database |
| User interface | HTTP API only | Mature web UI and integrations |
| Project maturity | New; operational interfaces may still evolve | Established and more feature-complete |

Sakuin's main advantages are simple data ownership, no database administration,
storage-efficient history, and a design that can scale crawling separately
from canonical storage. Its current disadvantages are equally important: there
is no web UI, no S3 backend yet, fewer media/classification integrations, and
less real-world deployment history than Bitmagnet.

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
bootstrap list.

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
through `/api?t=caps`.

Useful commands from a repository checkout:

```bash
./scripts/deploy.sh status YOUR_OPERATOR_TOKEN
./scripts/deploy.sh metrics YOUR_OPERATOR_TOKEN
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

Torznab clients can use `http://127.0.0.1:8080/api` as the indexer URL and the
complete `sakuin_...` reader token as `apikey`. Sakuin advertises generic,
movie, TV, music/audio, and book search, with standard Torznab categories.
Results use magnet links because Sakuin indexes metadata rather than hosting
`.torrent` files. Adult classification is only a label: visibility is an
operator setting and defaults to including every result.

Prometheus can scrape `/metrics` (or `/v1/metrics`) with the operator token as
an `Authorization: Bearer` header. The endpoint reports service uptime, DHT
activity and queues, search and duplicate generations, materialization, and
storage-maintenance counters without including peer addresses or error text.
Classifier coverage, ambiguity, bounded-input truncation, and semantic category
counts are included so operators can assess classification quality in place.

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

Storage administration is available through `sakuin admin verify`, `compact`,
and `gc`. See [`docs/architecture.md`](docs/architecture.md) for the design,
data lifecycle, distributed runtime, persistent formats, and testing strategy.

Only index and access content that you are legally permitted to handle. A DHT
index can observe untrusted or unlawful metadata; operators are responsible for
retention, access control, and compliance in their jurisdiction.

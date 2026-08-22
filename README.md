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

Requirements: Docker with the Compose plugin, and one or more DHT bootstrap
contacts that you trust.

```bash
cp .env.example .env
# Edit .env and set SAKUIN_DHT_BOOTSTRAP=host:port[,host:port...]
./scripts/deploy.sh
```

The first deployment creates a `reader` API key and prints its bearer token
once. Data is kept in the `sakuin-data` Docker volume. The API is available at
`http://127.0.0.1:8080` by default; UDP port 6881 is exposed for the DHT.

```bash
curl http://127.0.0.1:8080/v1/health
curl -H "Authorization: Bearer YOUR_TOKEN" \
  "http://127.0.0.1:8080/v1/search?q=ubuntu"
```

Useful deployment commands:

```bash
./scripts/deploy.sh status
./scripts/deploy.sh logs
./scripts/deploy.sh verify
./scripts/deploy.sh key reader-2 search
./scripts/deploy.sh down
```

The image uses verified prebuilt Linux release bundles rather than compiling
Sakuin during deployment. Set `SAKUIN_VERSION` in `.env` to pin a release.

## Native usage

Sakuin uses C++23 modules and Xmake. After building, copy
[`config/sakuin.example.toml`](config/sakuin.example.toml), add trusted DHT
bootstrap contacts, initialize the credential store, and start the daemon:

```bash
xmake build
cp config/sakuin.example.toml sakuin.toml
xmake run sakuin-api-key --state-dir=./data/operational/api init
xmake run sakuin-api-key --state-dir=./data/operational/api \
  create --id reader --permissions search
xmake run sakuin --config=sakuin.toml
```

Storage administration is available through `sakuin admin verify`, `compact`,
and `gc`. See [`docs/architecture.md`](docs/architecture.md) for the design,
data lifecycle, distributed runtime, persistent formats, and testing strategy.

Only index and access content that you are legally permitted to handle. A DHT
index can observe untrusted or unlawful metadata; operators are responsible for
retention, access control, and compliance in their jurisdiction.

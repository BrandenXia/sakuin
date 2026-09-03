add_rules("mode.debug", "mode.release")

set_languages("c++23")

set_policy("build.c++.modules", true)

option("sakuin_version")
  set_default("dev")
  set_showmenu(true)
  set_description("Version embedded in Sakuin binaries")
option_end()

add_requires("openssl")
add_requires("zstd")
add_requires("asio")
add_requires("toml++ 3.4.0")
add_requires("llhttp 9.4.3")
add_requires("nlohmann_json 3.12.0")
add_requires("spdlog")
add_requires("libcurl")

target("sakuin-core")
  set_kind("static")
  add_files("src/core/**.cpp")
  add_files("src/core/**.cppm", {public = true})
  add_options("sakuin_version")
  on_load(function (target)
    local version = get_config("sakuin_version") or "dev"
    if not version:match("^[%w%.%+%-]+$") then
      raise("sakuin_version may only contain letters, digits, '.', '+', and '-'")
    end
    target:add("defines", 'SAKUIN_VERSION="' .. version .. '"')
  end)
  add_packages("openssl")

target("sakuin-config")
  set_kind("static")
  add_files("src/config/**.cppm", {public = true})
  add_deps("sakuin-core")
  add_packages("toml++")

target("sakuin-api")
  set_kind("static")
  add_files("src/api/**.cppm", {public = true})
  add_deps("sakuin-core", "sakuin-search", "sakuin-index")
  add_packages("nlohmann_json")

target("sakuin-api-credentials")
  set_kind("static")
  add_files("src/api_credentials/**.cppm", {public = true})
  add_deps("sakuin-core", "sakuin-api")
  add_packages("toml++")

target("sakuin-api-key")
  set_kind("binary")
  add_files("tools/api_key.cpp")
  add_deps("sakuin-core", "sakuin-api", "sakuin-api-credentials")

target("sakuin-http")
  set_kind("static")
  add_files("src/http/**.cppm", {public = true})
  add_deps("sakuin-core", "sakuin-api")
  add_packages("llhttp")

target("sakuin-storage")
  set_kind("static")
  add_files("src/storage/**.cppm", {public = true})
  add_deps("sakuin-core")
  add_packages("zstd", "libcurl")

target("sakuin-model")
  set_kind("static")
  add_files("src/model/**.cppm", {public = true})
  add_deps("sakuin-core")

target("sakuin-classification")
  set_kind("static")
  add_files("src/classification/**.cppm", {public = true})
  add_deps("sakuin-core", "sakuin-model")

target("sakuin-model-codecs")
  set_kind("static")
  add_files("src/codec/**.cppm", {public = true})
  add_deps("sakuin-core", "sakuin-model", "sakuin-storage")

target("sakuin-runtime")
  set_kind("static")
  add_files("src/runtime/**.cppm", {public = true})
  add_deps("sakuin-core")

target("sakuin-dht")
  set_kind("static")
  add_files("src/dht/**.cppm", {public = true})
  add_deps("sakuin-core", "sakuin-model", "sakuin-runtime")

target("sakuin-runtime-asio")
  set_kind("static")
  add_files("src/runtime_asio/**.cppm", {public = true})
  add_deps("sakuin-core", "sakuin-runtime")
  add_packages("asio", "openssl")

target("sakuin-runtime-http")
  set_kind("static")
  add_files("src/runtime_http/**.cppm", {public = true})
  add_deps("sakuin-core", "sakuin-runtime", "sakuin-api", "sakuin-http")

target("sakuin-runtime-asio-http")
  set_kind("static")
  add_files("src/runtime_asio_http/**.cppm", {public = true})
  add_deps("sakuin-core", "sakuin-runtime", "sakuin-api", "sakuin-http",
           "sakuin-runtime-http")
  add_packages("asio", "openssl")

target("sakuin-scheduler")
  set_kind("static")
  add_files("src/scheduler/**.cppm", {public = true})
  add_deps("sakuin-core", "sakuin-runtime")

target("sakuin-integrations")
  set_kind("static")
  add_files("src/integration/**.cppm", {public = true})
  add_deps("sakuin-core", "sakuin-config", "sakuin-model", "sakuin-storage",
           "sakuin-model-codecs", "sakuin-dht", "sakuin-scheduler")

target("sakuin-service")
  set_kind("static")
  add_files("src/service/**.cpp")
  add_files("src/service/**.cppm", {public = true})
  add_deps("sakuin-core", "sakuin-config", "sakuin-model", "sakuin-storage",
           "sakuin-model-codecs", "sakuin-runtime", "sakuin-dht",
           "sakuin-integrations", "sakuin-runtime-asio", "sakuin-scheduler",
           "sakuin-api", "sakuin-api-credentials", "sakuin-http",
           "sakuin-runtime-http", "sakuin-runtime-asio-http", "sakuin-search",
           "sakuin-search-local", "sakuin-search-pipeline", "sakuin-index",
           "sakuin-index-local", "sakuin-classification")

target("sakuin")
  set_kind("binary")
  add_files("tools/sakuin.cpp")
  add_deps("sakuin-core", "sakuin-config", "sakuin-dht", "sakuin-service")
  add_packages("spdlog")

target("sakuin-index")
  set_kind("static")
  add_files("src/index/**.cppm", {public = true})
  add_deps("sakuin-core", "sakuin-model", "sakuin-storage",
           "sakuin-model-codecs")

target("sakuin-index-local")
  set_kind("static")
  add_files("src/index_local/**.cppm", {public = true})
  add_deps("sakuin-core", "sakuin-model", "sakuin-storage",
           "sakuin-model-codecs", "sakuin-index")

target("sakuin-search")
  set_kind("static")
  add_files("src/search/**.cppm", {public = true})
  add_deps("sakuin-core", "sakuin-model", "sakuin-classification")

target("sakuin-search-local")
  set_kind("static")
  add_files("src/search_local/**.cppm", {public = true})
  add_deps("sakuin-core", "sakuin-model", "sakuin-model-codecs",
           "sakuin-search", "sakuin-classification")

target("sakuin-search-pipeline")
  set_kind("static")
  add_files("src/search_integration/**.cppm", {public = true})
  add_deps("sakuin-core", "sakuin-model", "sakuin-storage",
           "sakuin-model-codecs", "sakuin-search")

target("sakuin-storage-benchmark")
  set_kind("binary")
  set_default(false)
  add_files("benchmarks/storage.cpp")
  add_deps("sakuin-core", "sakuin-model", "sakuin-storage",
           "sakuin-model-codecs")

target("sakuin-search-benchmark")
  set_kind("binary")
  set_default(false)
  add_files("benchmarks/search.cpp")
  add_deps("sakuin-core", "sakuin-model", "sakuin-search")

target("sakuin-storage-value-tests")
  set_kind("binary")
  add_files("tests/storage/value_types.cpp")
  add_deps("sakuin-core", "sakuin-model", "sakuin-storage")
  add_tests("storage-values")

target("sakuin-config-tests")
  set_kind("binary")
  add_files("tests/config/config.cpp")
  add_deps("sakuin-core", "sakuin-config")
  add_tests("config", {
    runargs = {path.absolute("config/sakuin.docker.toml")}
  })

target("sakuin-classification-tests")
  set_kind("binary")
  add_files("tests/classification/rules.cpp")
  add_deps("sakuin-core", "sakuin-model", "sakuin-classification")
  add_tests("classification")

target("sakuin-learned-classification-tests")
  set_kind("binary")
  add_files("tests/classification/learned.cpp")
  add_deps("sakuin-core", "sakuin-model", "sakuin-classification")
  add_tests("learned-classification")

target("sakuin-classifier-eval")
  set_kind("binary")
  add_files("tools/classifier_eval.cpp")
  add_deps("sakuin-core", "sakuin-model", "sakuin-classification")
  add_tests("classification-corpus", {
    runargs = {path.absolute("tests/fixtures/classification/corpus.tsv")}
  })

target("sakuin-api-auth-tests")
  set_kind("binary")
  add_files("tests/api/auth.cpp")
  add_deps("sakuin-core", "sakuin-api")
  add_tests("api-auth")

target("sakuin-api-search-tests")
  set_kind("binary")
  add_files("tests/api/search_http.cpp")
  add_deps("sakuin-core", "sakuin-model", "sakuin-search", "sakuin-index",
           "sakuin-api")
  add_tests("api-search")

target("sakuin-api-rate-limit-tests")
  set_kind("binary")
  add_files("tests/api/rate_limit.cpp")
  add_deps("sakuin-core", "sakuin-api")
  add_tests("api-rate-limit")

target("sakuin-api-credential-store-tests")
  set_kind("binary")
  add_files("tests/api_credentials/store.cpp")
  add_deps("sakuin-core", "sakuin-api", "sakuin-api-credentials")
  add_tests("api-credential-store")

target("sakuin-api-key-cli-tests")
  set_kind("binary")
  add_files("tests/api_credentials/cli.cpp")
  add_deps("sakuin-core", "sakuin-api", "sakuin-api-credentials")
  add_tests("api-key-cli")

target("sakuin-http-tests")
  set_kind("binary")
  add_files("tests/http/http.cpp")
  add_deps("sakuin-core", "sakuin-api", "sakuin-http")
  add_tests("http")

target("sakuin-local-blob-store-tests")
  set_kind("binary")
  add_files("tests/storage/local_blob_store.cpp")
  add_deps("sakuin-core", "sakuin-storage")
  add_tests("local-blob-store")

target("sakuin-s3-blob-store-tests")
  set_kind("binary")
  add_files("tests/storage/s3_blob_store.cpp")
  add_deps("sakuin-core", "sakuin-storage")
  add_tests("s3-blob-store")

target("sakuin-model-codec-tests")
  set_kind("binary")
  add_files("tests/storage/model_codecs.cpp")
  add_deps("sakuin-core", "sakuin-model", "sakuin-storage",
           "sakuin-model-codecs")
  add_tests("model-codecs")

target("sakuin-row-v1-tests")
  set_kind("binary")
  add_files("tests/storage/row_v1.cpp")
  add_deps("sakuin-core", "sakuin-storage")
  add_tests("row-v1")

target("sakuin-local-manifest-tests")
  set_kind("binary")
  add_files("tests/storage/local_manifest.cpp")
  add_deps("sakuin-core", "sakuin-storage")
  add_tests("local-manifest")

target("sakuin-observation-dataset-tests")
  set_kind("binary")
  add_files("tests/storage/observation_dataset.cpp")
  add_deps("sakuin-core", "sakuin-model", "sakuin-storage",
           "sakuin-model-codecs")
  add_tests("observation-dataset")

target("sakuin-torrent-dataset-tests")
  set_kind("binary")
  add_files("tests/storage/torrent_dataset.cpp")
  add_deps("sakuin-core", "sakuin-model", "sakuin-storage",
           "sakuin-model-codecs")
  add_tests("torrent-dataset")

target("sakuin-compaction-tests")
  set_kind("binary")
  add_files("tests/storage/compaction.cpp")
  add_deps("sakuin-core", "sakuin-model", "sakuin-storage",
           "sakuin-model-codecs")
  add_tests("compaction")

target("sakuin-retention-tests")
  set_kind("binary")
  add_files("tests/storage/retention.cpp")
  add_deps("sakuin-core", "sakuin-model", "sakuin-storage",
           "sakuin-model-codecs")
  add_tests("retention")

target("sakuin-migration-tests")
  set_kind("binary")
  add_files("tests/storage/migration.cpp")
  add_deps("sakuin-core", "sakuin-model", "sakuin-storage",
           "sakuin-model-codecs")
  add_tests("migration")

target("sakuin-dht-observation-tests")
  set_kind("binary")
  add_files("tests/dht/observation_ingestion.cpp")
  add_deps("sakuin-core", "sakuin-model", "sakuin-storage",
           "sakuin-model-codecs", "sakuin-dht", "sakuin-integrations")
  add_tests("dht-observation-ingestion")

target("sakuin-krpc-tests")
  set_kind("binary")
  add_files("tests/dht/krpc.cpp")
  add_deps("sakuin-core", "sakuin-dht")
  add_tests("krpc")

target("sakuin-metadata-exchange-tests")
  set_kind("binary")
  add_files("tests/dht/metadata.cpp")
  add_deps("sakuin-core", "sakuin-model", "sakuin-runtime", "sakuin-dht")
  add_tests("metadata-exchange")

target("sakuin-metainfo-tests")
  set_kind("binary")
  add_files("tests/dht/metainfo.cpp")
  add_deps("sakuin-core", "sakuin-model", "sakuin-dht")
  add_tests("metainfo")

target("sakuin-metadata-fetch-tests")
  set_kind("binary")
  add_files("tests/dht/metadata_fetch.cpp")
  add_deps("sakuin-core", "sakuin-model", "sakuin-runtime", "sakuin-dht")
  add_tests("metadata-fetch")

target("sakuin-metadata-storage-tests")
  set_kind("binary")
  add_files("tests/dht/metadata_storage.cpp")
  add_deps("sakuin-core", "sakuin-config", "sakuin-model", "sakuin-storage",
           "sakuin-model-codecs", "sakuin-dht", "sakuin-integrations")
  add_tests("metadata-storage")

target("sakuin-metadata-queue-tests")
  set_kind("binary")
  add_files("tests/dht/metadata_queue.cpp")
  add_deps("sakuin-core", "sakuin-model", "sakuin-runtime", "sakuin-dht")
  add_tests("metadata-queue")

target("sakuin-metadata-controller-tests")
  set_kind("binary")
  add_files("tests/dht/metadata_controller.cpp")
  add_deps("sakuin-core", "sakuin-model", "sakuin-runtime", "sakuin-dht")
  add_tests("metadata-controller")

target("sakuin-dht-runtime-integration-tests")
  set_kind("binary")
  add_files("tests/dht/runtime_integration.cpp")
  add_deps("sakuin-core", "sakuin-config", "sakuin-model", "sakuin-runtime",
           "sakuin-dht", "sakuin-integrations")
  add_tests("dht-runtime-integration")

target("sakuin-dht-runtime-worker-tests")
  set_kind("binary")
  add_files("tests/dht/runtime_worker.cpp")
  add_deps("sakuin-core", "sakuin-model", "sakuin-runtime", "sakuin-dht",
           "sakuin-integrations")
  add_tests("dht-runtime-worker")

target("sakuin-dht-service-tests")
  set_kind("binary")
  add_files("tests/service/dht.cpp")
  add_deps("sakuin-core", "sakuin-config", "sakuin-model", "sakuin-runtime",
           "sakuin-dht", "sakuin-integrations", "sakuin-api-credentials",
           "sakuin-service")
  add_tests("dht-service")

target("sakuin-distributed-service-tests")
  set_kind("binary")
  add_files("tests/service/distributed.cpp")
  add_deps("sakuin-core", "sakuin-config", "sakuin-runtime",
           "sakuin-runtime-asio", "sakuin-scheduler", "sakuin-service")
  add_packages("asio", "openssl")
  add_tests("distributed-service", {
    runargs = {path.absolute("tests/fixtures/tls")}
  })

target("sakuin-work-result-inbox-tests")
  set_kind("binary")
  add_files("tests/integration/work_results.cpp")
  add_deps("sakuin-core", "sakuin-model", "sakuin-storage",
           "sakuin-model-codecs", "sakuin-runtime", "sakuin-dht",
           "sakuin-scheduler", "sakuin-integrations")
  add_tests("work-result-inbox")

target("sakuin-storage-service-tests")
  set_kind("binary")
  add_files("tests/service/storage.cpp")
  add_deps("sakuin-core", "sakuin-config", "sakuin-model", "sakuin-storage",
           "sakuin-model-codecs", "sakuin-dht", "sakuin-integrations",
           "sakuin-service")
  add_tests("storage-service")

target("sakuin-maintenance-service-tests")
  set_kind("binary")
  add_files("tests/service/maintenance.cpp")
  add_deps("sakuin-core", "sakuin-config", "sakuin-model", "sakuin-storage",
           "sakuin-model-codecs", "sakuin-dht", "sakuin-integrations",
           "sakuin-service")
  add_tests("maintenance-service")

target("sakuin-materialization-service-tests")
  set_kind("binary")
  add_files("tests/service/materialization.cpp")
  add_deps("sakuin-core", "sakuin-config", "sakuin-model", "sakuin-storage",
           "sakuin-model-codecs", "sakuin-index", "sakuin-dht",
           "sakuin-integrations", "sakuin-service")
  add_tests("materialization-service")

target("sakuin-duplicate-index-service-tests")
  set_kind("binary")
  add_files("tests/service/duplicates.cpp")
  add_deps("sakuin-core", "sakuin-config", "sakuin-model", "sakuin-storage",
           "sakuin-model-codecs", "sakuin-index", "sakuin-index-local",
           "sakuin-service")
  add_tests("duplicate-index-service")

target("sakuin-api-service-tests")
  set_kind("binary")
  add_files("tests/service/api.cpp")
  add_deps("sakuin-core", "sakuin-config", "sakuin-model", "sakuin-storage",
           "sakuin-model-codecs", "sakuin-index", "sakuin-api",
           "sakuin-api-credentials", "sakuin-service")
  add_packages("asio")
  add_tests("api-service")

target("sakuin-application-tests")
  set_kind("binary")
  add_files("tests/service/application.cpp")
  add_deps("sakuin-core", "sakuin-service")
  add_tests("application")

target("sakuin-routing-tests")
  set_kind("binary")
  add_files("tests/dht/routing.cpp")
  add_deps("sakuin-core", "sakuin-runtime", "sakuin-dht")
  add_tests("routing")

target("sakuin-routing-maintenance-tests")
  set_kind("binary")
  add_files("tests/dht/routing_maintenance.cpp")
  add_deps("sakuin-core", "sakuin-runtime", "sakuin-dht")
  add_tests("routing-maintenance")

target("sakuin-routing-discovery-tests")
  set_kind("binary")
  add_files("tests/dht/discovery.cpp")
  add_deps("sakuin-core", "sakuin-runtime", "sakuin-dht")
  add_tests("routing-discovery")

target("sakuin-peer-discovery-tests")
  set_kind("binary")
  add_files("tests/dht/peer_discovery.cpp")
  add_deps("sakuin-core", "sakuin-runtime", "sakuin-dht")
  add_tests("peer-discovery")

target("sakuin-metadata-backfill-tests")
  set_kind("binary")
  add_files("tests/dht/metadata_backfill.cpp")
  add_deps("sakuin-core", "sakuin-model", "sakuin-storage", "sakuin-dht",
           "sakuin-integrations")
  add_tests("metadata-backfill")

target("sakuin-dht-identity-tests")
  set_kind("binary")
  add_files("tests/dht/identity.cpp")
  add_deps("sakuin-core", "sakuin-runtime", "sakuin-dht")
  add_tests("dht-identity")

target("sakuin-dht-node-tests")
  set_kind("binary")
  add_files("tests/dht/node.cpp")
  add_deps("sakuin-core", "sakuin-model", "sakuin-runtime", "sakuin-dht")
  add_tests("dht-node")

target("sakuin-bootstrap-tests")
  set_kind("binary")
  add_files("tests/dht/bootstrap.cpp")
  add_deps("sakuin-core", "sakuin-model", "sakuin-runtime", "sakuin-dht")
  add_tests("bootstrap")

target("sakuin-materialization-tests")
  set_kind("binary")
  add_files("tests/index/materialization.cpp")
  add_deps("sakuin-core", "sakuin-model", "sakuin-storage",
           "sakuin-model-codecs", "sakuin-index")
  add_tests("observation-materialization")

target("sakuin-duplicate-index-tests")
  set_kind("binary")
  add_files("tests/index/duplicates.cpp")
  add_deps("sakuin-core", "sakuin-model", "sakuin-storage",
           "sakuin-model-codecs", "sakuin-index")
  add_tests("duplicate-index")

target("sakuin-local-duplicate-index-tests")
  set_kind("binary")
  add_files("tests/index/local_duplicates.cpp")
  add_deps("sakuin-core", "sakuin-model", "sakuin-storage",
           "sakuin-model-codecs", "sakuin-index", "sakuin-index-local")
  add_tests("local-duplicate-index")

target("sakuin-search-tests")
  set_kind("binary")
  add_files("tests/search/search.cpp")
  add_deps("sakuin-core", "sakuin-model", "sakuin-storage",
           "sakuin-model-codecs", "sakuin-search", "sakuin-search-pipeline")
  add_tests("search")

target("sakuin-local-search-tests")
  set_kind("binary")
  add_files("tests/search/local.cpp")
  add_deps("sakuin-core", "sakuin-model", "sakuin-storage",
           "sakuin-model-codecs", "sakuin-search", "sakuin-search-local",
           "sakuin-search-pipeline")
  add_tests("local-search")

target("sakuin-asio-datagram-tests")
  set_kind("binary")
  add_files("tests/runtime/asio_datagram.cpp")
  add_deps("sakuin-core", "sakuin-model", "sakuin-storage",
           "sakuin-model-codecs", "sakuin-dht", "sakuin-integrations",
           "sakuin-runtime", "sakuin-runtime-asio")
  add_tests("asio-datagram")

target("sakuin-asio-stream-tests")
  set_kind("binary")
  add_files("tests/runtime/asio_stream.cpp")
  add_deps("sakuin-core", "sakuin-runtime", "sakuin-runtime-asio")
  add_packages("asio")
  add_tests("asio-stream")

target("sakuin-asio-server-tests")
  set_kind("binary")
  add_files("tests/runtime/asio_server.cpp")
  add_deps("sakuin-core", "sakuin-runtime", "sakuin-runtime-asio")
  add_packages("asio")
  add_tests("asio-server")

target("sakuin-asio-http-tests")
  set_kind("binary")
  add_files("tests/runtime/asio_http.cpp")
  add_deps("sakuin-core", "sakuin-runtime", "sakuin-api", "sakuin-http",
           "sakuin-runtime-http", "sakuin-runtime-asio-http")
  add_packages("asio")
  add_tests("asio-http")

target("sakuin-dht-runtime-tests")
  set_kind("binary")
  add_files("tests/runtime/dht_runtime.cpp")
  add_deps("sakuin-core", "sakuin-model", "sakuin-runtime", "sakuin-dht",
           "sakuin-runtime-asio")
  add_tests("dht-runtime")

target("sakuin-traffic-budget-tests")
  set_kind("binary")
  add_files("tests/scheduler/traffic.cpp")
  add_deps("sakuin-core", "sakuin-runtime", "sakuin-scheduler")
  add_tests("traffic-budget")

target("sakuin-work-coordinator-tests")
  set_kind("binary")
  add_files("tests/scheduler/work.cpp")
  add_deps("sakuin-core", "sakuin-runtime", "sakuin-scheduler")
  add_tests("work-coordinator")

target("sakuin-work-recovery-tests")
  set_kind("binary")
  add_files("tests/scheduler/recovery.cpp")
  add_deps("sakuin-core", "sakuin-runtime", "sakuin-scheduler")
  add_tests("work-recovery")

target("sakuin-work-worker-tests")
  set_kind("binary")
  add_files("tests/scheduler/worker.cpp")
  add_deps("sakuin-core", "sakuin-runtime", "sakuin-scheduler")
  add_tests("work-worker")

target("sakuin-work-protocol-tests")
  set_kind("binary")
  add_files("tests/scheduler/protocol.cpp")
  add_deps("sakuin-core", "sakuin-runtime", "sakuin-scheduler")
  add_tests("work-protocol")

target("sakuin-work-network-tests")
  set_kind("binary")
  add_files("tests/scheduler/network.cpp")
  add_deps("sakuin-core", "sakuin-runtime", "sakuin-runtime-asio",
           "sakuin-scheduler")
  add_packages("asio")
  add_tests("work-network")

-- target("dht")
--   set_kind("static")
--   add_deps("core", "model")
--
-- target("index")
--   set_kind("static")
--   add_deps("core", "model", "storage")
--
-- target("api")
--   set_kind("static")
--   add_deps("core", "model", "index", "scheduler", "storage")
--
-- target("app")
--   set_kind("binary")
--   add_deps("core", "storage", "model", "dht", "index", "scheduler", "api")

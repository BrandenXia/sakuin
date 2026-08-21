add_rules("mode.debug", "mode.release")

set_languages("c++23")

set_policy("build.c++.modules", true)

add_requires("openssl")
add_requires("zstd")
add_requires("asio")
add_requires("toml++ 3.4.0")
add_requires("llhttp 9.4.3")
add_requires("nlohmann_json 3.12.0")

target("sakuin-core")
  set_kind("static")
  add_files("src/core/**.cpp")
  add_files("src/core/**.cppm", {public = true})
  add_packages("openssl")

target("sakuin-config")
  set_kind("static")
  add_files("src/config/**.cppm", {public = true})
  add_deps("sakuin-core")
  add_packages("toml++")

target("sakuin-api")
  set_kind("static")
  add_files("src/api/**.cppm", {public = true})
  add_deps("sakuin-core", "sakuin-search")
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
  add_packages("zstd")

target("sakuin-model")
  set_kind("static")
  add_files("src/model/**.cppm", {public = true})
  add_deps("sakuin-core")

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
  add_packages("asio")

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
           "sakuin-model-codecs", "sakuin-dht")

target("sakuin-index")
  set_kind("static")
  add_files("src/index/**.cppm", {public = true})
  add_deps("sakuin-core", "sakuin-model", "sakuin-storage",
           "sakuin-model-codecs")

target("sakuin-search")
  set_kind("static")
  add_files("src/search/**.cppm", {public = true})
  add_deps("sakuin-core", "sakuin-model")

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

target("sakuin-storage-value-tests")
  set_kind("binary")
  add_files("tests/storage/value_types.cpp")
  add_deps("sakuin-core", "sakuin-model", "sakuin-storage")
  add_tests("storage-values")

target("sakuin-config-tests")
  set_kind("binary")
  add_files("tests/config/config.cpp")
  add_deps("sakuin-core", "sakuin-config")
  add_tests("config")

target("sakuin-api-auth-tests")
  set_kind("binary")
  add_files("tests/api/auth.cpp")
  add_deps("sakuin-core", "sakuin-api")
  add_tests("api-auth")

target("sakuin-api-search-tests")
  set_kind("binary")
  add_files("tests/api/search_http.cpp")
  add_deps("sakuin-core", "sakuin-model", "sakuin-search", "sakuin-api")
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

target("sakuin-search-tests")
  set_kind("binary")
  add_files("tests/search/search.cpp")
  add_deps("sakuin-core", "sakuin-model", "sakuin-storage",
           "sakuin-model-codecs", "sakuin-search", "sakuin-search-pipeline")
  add_tests("search")

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

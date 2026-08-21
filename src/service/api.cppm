export module sakuin.service.api;

import std;

import sakuin.api.auth;
import sakuin.api.credentials.store;
import sakuin.api.rate_limit;
import sakuin.api.search;
import sakuin.config.model;
import sakuin.core.bytes;
import sakuin.core.result;
import sakuin.http.llhttp;
import sakuin.index.duplicates;
import sakuin.runtime.asio_http;
import sakuin.runtime.asio_resolver;
import sakuin.runtime.datagram;
import sakuin.runtime.http;
import sakuin.search.checkpoint;
import sakuin.search.index;
import sakuin.search.local;
import sakuin.search.memory;
import sakuin.search.rebuild;
import sakuin.storage.dataset.torrents;

export namespace sakuin::service {

// Callbacks may arrive from the HTTP runtime and search refresh worker.
// Implementations must serialize shared state.
class ApiServiceObserver {
public:
  virtual ~ApiServiceObserver() = default;
  virtual void on_api_error(core::Error error) = 0;
  virtual void on_search_index_refreshed(search::SearchRebuildResult) {}
};

// Local API composition backed by the canonical torrent dataset. Search is a
// derived snapshot: metadata commits only enqueue a refresh, while a dedicated
// owner thread performs the potentially expensive rebuild. That owner is a
// narrow replacement seam for a future stdexec scheduler.
class LocalApiService final {
public:
  static core::Result<std::unique_ptr<LocalApiService>>
  create(const config::ApiConfig &configuration,
         storage::TorrentDataset &torrents, ApiServiceObserver &observer,
         std::optional<std::filesystem::path> search_state_directory = {},
         index::DuplicateIndexView *duplicates = nullptr);

  ~LocalApiService();

  LocalApiService(const LocalApiService &) = delete;
  LocalApiService &operator=(const LocalApiService &) = delete;

  core::Result<void> start();
  void stop() noexcept;
  bool running() const noexcept;
  runtime::DatagramEndpoint local_endpoint() const noexcept;

  // Safe from metadata transport callbacks; this does not rebuild inline.
  void request_search_refresh(std::uint64_t committed_generation) noexcept;
  core::Result<search::SearchRebuildResult> refresh_search();
  core::Result<void> reload_credentials();

private:
  struct Impl;
  explicit LocalApiService(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> impl_;
};

} // namespace sakuin::service

namespace sakuin::service {
namespace {

class ReloadableAuthenticator final : public api::ApiKeyAuthenticator {
public:
  explicit ReloadableAuthenticator(
      std::unique_ptr<api::InMemoryApiKeyAuthenticator> initial)
      : current_(std::move(initial)) {}

  core::Result<std::optional<api::ApiPrincipal>>
  authenticate(std::string_view key_id, core::ByteView secret) const override {
    std::shared_lock lock{mutex_};
    return current_->authenticate(key_id, secret);
  }

  void replace(std::unique_ptr<api::InMemoryApiKeyAuthenticator> replacement) {
    std::unique_lock lock{mutex_};
    current_ = std::move(replacement);
  }

private:
  mutable std::shared_mutex mutex_;
  std::unique_ptr<api::InMemoryApiKeyAuthenticator> current_;
};

core::Result<runtime::DatagramEndpoint>
resolve_bind_endpoint(const config::ApiConfig &configuration) {
  runtime::AsioDatagramEndpointResolver resolver;
  auto ipv4 =
      resolver.resolve(configuration.listen_address, configuration.listen_port,
                       runtime::AddressFamily::IPv4);
  if (ipv4 && !ipv4->empty())
    return ipv4->front();
  auto ipv6 =
      resolver.resolve(configuration.listen_address, configuration.listen_port,
                       runtime::AddressFamily::IPv6);
  if (ipv6 && !ipv6->empty())
    return ipv6->front();
  std::string message =
      "Could not resolve API listen address " + configuration.listen_address;
  if (!ipv4)
    message += ": " + ipv4.error().message;
  if (!ipv6)
    message += "; " + ipv6.error().message;
  return std::unexpected(
      core::Error{core::ErrorCode::InvalidArgument, std::move(message)});
}

runtime::HttpServerOptions
server_options(const config::ApiConfig &configuration,
               runtime::DatagramEndpoint bind_to) {
  runtime::HttpServerOptions result{
      .bind_to = bind_to,
      .maximum_connections = configuration.maximum_connections,
      .read_buffer_bytes = configuration.read_buffer_bytes,
      .request_timeout = std::chrono::duration_cast<std::chrono::milliseconds>(
          configuration.request_timeout),
      .parser_limits = {
          .maximum_target_bytes = configuration.maximum_target_bytes,
          .maximum_header_bytes = configuration.maximum_header_bytes,
          .maximum_header_count = configuration.maximum_header_count,
          .maximum_body_bytes = configuration.maximum_body_bytes}};
  if (configuration.tls_certificate_chain_file) {
    result.tls = runtime::HttpTlsOptions{
        .certificate_chain_file = *configuration.tls_certificate_chain_file,
        .private_key_file = *configuration.tls_private_key_file};
  }
  return result;
}

} // namespace

struct LocalApiService::Impl final : runtime::HttpServerEvents {
  storage::TorrentDataset *torrents{};
  ApiServiceObserver *observer{};
  std::unique_ptr<api::LocalApiCredentialStore> credential_store;
  std::unique_ptr<ReloadableAuthenticator> authenticator;
  std::unique_ptr<search::SearchIndex> index;
  std::unique_ptr<search::LocalSearchCursorStore> cursor_store;
  std::unique_ptr<api::FixedWindowRequestGovernor> governor;
  std::unique_ptr<api::SearchHttpHandler> handler;
  std::unique_ptr<runtime::AsioHttpServer> server;

  mutable std::mutex refresh_mutex;
  std::mutex requests_mutex;
  std::condition_variable_any requests_changed;
  std::uint64_t requested_generation{};
  std::atomic<std::uint64_t> indexed_generation{};
  storage::TorrentChangeCursor search_cursor;
  std::jthread refresh_worker;
  std::atomic<bool> active{};

  void on_http_server_error(core::Error error) override {
    report(std::move(error));
  }

  void report(core::Error error) noexcept {
    try {
      observer->on_api_error(std::move(error));
    } catch (...) {
      // Runtime observers cannot unwind through API-owned threads.
    }
  }

  void report(search::SearchRebuildResult result) noexcept {
    try {
      observer->on_search_index_refreshed(result);
    } catch (...) {
      report({core::ErrorCode::Internal,
              "Search-index refresh observer threw an exception"});
    }
  }

  core::Result<search::SearchRebuildResult> refresh() {
    search::SearchRebuildResult result;
    {
      std::lock_guard lock{refresh_mutex};
      auto synchronized = search::synchronize(*torrents, *index, search_cursor);
      if (!synchronized)
        return std::unexpected(synchronized.error());
      if (cursor_store) {
        auto saved = cursor_store->save(synchronized->cursor);
        if (!saved)
          return std::unexpected(saved.error());
      }
      search_cursor = synchronized->cursor;
      result = {.source_generation = synchronized->source_generation,
                .records_indexed = synchronized->records_indexed};
      indexed_generation.store(result.source_generation,
                               std::memory_order_release);
    }
    report(result);
    return result;
  }

  void run(std::stop_token stop) noexcept {
    while (!stop.stop_requested()) {
      std::uint64_t requested{};
      {
        std::unique_lock lock{requests_mutex};
        requests_changed.wait(lock, stop, [&] {
          return requested_generation >
                 indexed_generation.load(std::memory_order_acquire);
        });
        if (stop.stop_requested())
          break;
        requested = requested_generation;
      }
      auto refreshed = refresh();
      if (!refreshed) {
        report(refreshed.error());
        // Do not spin on a persistent storage failure. A later commit or an
        // explicit refresh can retry the snapshot.
        std::lock_guard lock{requests_mutex};
        requested_generation =
            indexed_generation.load(std::memory_order_acquire);
      } else if (refreshed->source_generation < requested) {
        report({core::ErrorCode::Conflict,
                "Search snapshot generation lagged a committed torrent"});
        std::lock_guard lock{requests_mutex};
        requested_generation =
            indexed_generation.load(std::memory_order_acquire);
      }
    }
  }
};

LocalApiService::LocalApiService(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

core::Result<std::unique_ptr<LocalApiService>> LocalApiService::create(
    const config::ApiConfig &configuration, storage::TorrentDataset &torrents,
    ApiServiceObserver &observer,
    std::optional<std::filesystem::path> search_state_directory,
    index::DuplicateIndexView *duplicates) {
  if (!configuration.enabled)
    return std::unexpected(
        core::Error{core::ErrorCode::InvalidArgument,
                    "Local API service requires enabled API configuration"});
  if (configuration.tls_certificate_chain_file.has_value() !=
      configuration.tls_private_key_file.has_value())
    return std::unexpected(core::Error{core::ErrorCode::InvalidArgument,
                                       "API TLS certificate chain and private "
                                       "key must be configured together"});

  auto endpoint = resolve_bind_endpoint(configuration);
  if (!endpoint)
    return std::unexpected(endpoint.error());
  auto store = api::LocalApiCredentialStore::open(
      configuration.credential_store_directory);
  if (!store)
    return std::unexpected(store.error());
  auto loaded = api::load_api_key_authenticator(**store);
  if (!loaded)
    return std::unexpected(loaded.error());

  auto result = std::make_unique<Impl>();
  result->torrents = &torrents;
  result->observer = &observer;
  result->credential_store = std::move(*store);
  result->authenticator =
      std::make_unique<ReloadableAuthenticator>(std::move(*loaded));
  if (search_state_directory) {
    const auto index_path = *search_state_directory / "index.v1";
    auto local_index = search::LocalSearchIndex::open(index_path);
    if (!local_index) {
      switch (local_index.error().code) {
      case core::ErrorCode::CorruptSegment:
      case core::ErrorCode::ChecksumMismatch:
      case core::ErrorCode::UnsupportedFormat:
      case core::ErrorCode::InvalidManifest:
        result->index = search::LocalSearchIndex::create_empty(index_path);
        break;
      default:
        return std::unexpected(local_index.error());
      }
    } else {
      result->index = std::move(*local_index);
    }
    result->cursor_store = std::make_unique<search::LocalSearchCursorStore>(
        *search_state_directory / "cursor.v1");
    auto cursor = result->cursor_store->load();
    if (cursor && cursor->initialized &&
        cursor->source_generation == result->index->source_generation())
      result->search_cursor = *cursor;
  } else {
    result->index = std::make_unique<search::InMemorySearchIndex>();
  }
  if (configuration.rate_limit.enabled) {
    auto governor = api::FixedWindowRequestGovernor::create(
        {.maximum_requests = configuration.rate_limit.requests_per_window,
         .period = configuration.rate_limit.window});
    if (!governor)
      return std::unexpected(governor.error());
    result->governor = std::move(*governor);
  }
  result->handler = std::make_unique<api::SearchHttpHandler>(
      *result->authenticator, *result->index, result->governor.get(),
      duplicates);
  auto server =
      runtime::AsioHttpServer::create(server_options(configuration, *endpoint));
  if (!server)
    return std::unexpected(server.error());
  result->server = std::move(*server);

  auto initial = result->refresh();
  if (!initial)
    return std::unexpected(initial.error());
  return std::unique_ptr<LocalApiService>{
      new LocalApiService{std::move(result)}};
}

LocalApiService::~LocalApiService() { stop(); }

core::Result<void> LocalApiService::start() {
  bool expected = false;
  if (!impl_->active.compare_exchange_strong(expected, true,
                                             std::memory_order_acq_rel))
    return std::unexpected(
        core::Error{core::ErrorCode::Conflict, "API service already started"});
  try {
    impl_->refresh_worker = std::jthread{
        [impl = impl_.get()](std::stop_token stop) { impl->run(stop); }};
  } catch (const std::exception &exception) {
    impl_->active.store(false, std::memory_order_release);
    return std::unexpected(
        core::Error{core::ErrorCode::Internal,
                    std::string{"Could not start search refresh worker: "} +
                        exception.what()});
  }
  auto started = impl_->server->start(*impl_->handler, impl_.get());
  if (!started) {
    impl_->refresh_worker.request_stop();
    impl_->requests_changed.notify_all();
    impl_->refresh_worker.join();
    impl_->active.store(false, std::memory_order_release);
    return std::unexpected(started.error());
  }
  return {};
}

void LocalApiService::stop() noexcept {
  if (!impl_)
    return;
  impl_->active.store(false, std::memory_order_release);
  if (impl_->server)
    impl_->server->stop();
  if (impl_->refresh_worker.joinable()) {
    impl_->refresh_worker.request_stop();
    impl_->requests_changed.notify_all();
    if (impl_->refresh_worker.get_id() != std::this_thread::get_id())
      impl_->refresh_worker.join();
  }
}

bool LocalApiService::running() const noexcept {
  return impl_->active.load(std::memory_order_acquire);
}

runtime::DatagramEndpoint LocalApiService::local_endpoint() const noexcept {
  return impl_->server->local_endpoint();
}

void LocalApiService::request_search_refresh(
    std::uint64_t committed_generation) noexcept {
  if (!impl_->active.load(std::memory_order_acquire) ||
      committed_generation <=
          impl_->indexed_generation.load(std::memory_order_acquire))
    return;
  {
    std::lock_guard lock{impl_->requests_mutex};
    impl_->requested_generation =
        std::max(impl_->requested_generation, committed_generation);
  }
  impl_->requests_changed.notify_all();
}

core::Result<search::SearchRebuildResult> LocalApiService::refresh_search() {
  return impl_->refresh();
}

core::Result<void> LocalApiService::reload_credentials() {
  auto reopened =
      api::LocalApiCredentialStore::open(impl_->credential_store->root());
  if (!reopened)
    return std::unexpected(reopened.error());
  auto loaded = api::load_api_key_authenticator(**reopened);
  if (!loaded)
    return std::unexpected(loaded.error());
  impl_->authenticator->replace(std::move(*loaded));
  impl_->credential_store = std::move(*reopened);
  return {};
}

} // namespace sakuin::service

module;

#include <asio.hpp>
#include <asio/ssl.hpp>

export module sakuin.runtime.asio_http;

import std;

import sakuin.api.http;
import sakuin.core.bytes;
import sakuin.core.result;
import sakuin.http.llhttp;
import sakuin.runtime.datagram;
import sakuin.runtime.http;

export namespace sakuin::runtime {

// Standalone Asio implementation of the Sakuin HTTP server contract. No Asio
// types, completion tokens, executors, or awaitables cross this boundary.
class AsioHttpServer final : public HttpServer {
public:
  static core::Result<std::unique_ptr<AsioHttpServer>>
  create(const HttpServerOptions &options = {});

  ~AsioHttpServer() override;

  core::Result<void> start(api::HttpHandler &handler,
                           HttpServerEvents *events = nullptr) override;
  DatagramEndpoint local_endpoint() const noexcept override;
  void stop() noexcept override;

private:
  struct Impl;
  explicit AsioHttpServer(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> impl_;
};

} // namespace sakuin::runtime

namespace sakuin::runtime {
namespace {

using Tcp = asio::ip::tcp;
using TlsStream = asio::ssl::stream<Tcp::socket>;

core::Error network_error(std::string action, const asio::error_code &error) {
  return {core::ErrorCode::IoError, std::move(action) + ": " + error.message()};
}

core::Result<Tcp::endpoint> to_asio(DatagramEndpoint endpoint) {
  if (endpoint.address.family == AddressFamily::IPv4) {
    asio::ip::address_v4::bytes_type bytes{};
    std::copy_n(endpoint.address.bytes.begin(), bytes.size(), bytes.begin());
    return Tcp::endpoint{asio::ip::address_v4{bytes}, endpoint.port};
  }
  asio::ip::address_v6::bytes_type bytes{};
  std::copy(endpoint.address.bytes.begin(), endpoint.address.bytes.end(),
            bytes.begin());
  return Tcp::endpoint{asio::ip::address_v6{bytes}, endpoint.port};
}

DatagramEndpoint from_asio(const Tcp::endpoint &endpoint) {
  DatagramEndpoint result{.port = endpoint.port()};
  if (endpoint.address().is_v4()) {
    result.address.family = AddressFamily::IPv4;
    const auto bytes = endpoint.address().to_v4().to_bytes();
    std::copy(bytes.begin(), bytes.end(), result.address.bytes.begin());
  } else {
    result.address.family = AddressFamily::IPv6;
    const auto bytes = endpoint.address().to_v6().to_bytes();
    std::copy(bytes.begin(), bytes.end(), result.address.bytes.begin());
  }
  return result;
}

api::HttpResponse plain_response(unsigned status, std::string_view message) {
  const auto view = std::as_bytes(std::span{message});
  return {.status = status,
          .headers = {{"content-type", "text/plain; charset=utf-8"},
                      {"cache-control", "no-store"}},
          .body = {view.begin(), view.end()}};
}

template <typename Stream>
class HttpSession final
    : public std::enable_shared_from_this<HttpSession<Stream>> {
public:
  HttpSession(Stream stream, api::HttpHandler *handler,
              const HttpServerOptions &options, bool admitted,
              std::function<void()> on_done)
      : stream_(std::move(stream)), timer_(stream_.get_executor()),
        handler_(handler), parser_(options.parser_limits),
        buffer_(options.read_buffer_bytes), timeout_(options.request_timeout),
        admitted_(admitted), on_done_(std::move(on_done)) {}

  void start() {
    arm_request_timeout();
    if constexpr (std::same_as<Stream, TlsStream>) {
      auto self = this->shared_from_this();
      stream_.async_handshake(asio::ssl::stream_base::server,
                              [self](const asio::error_code &error) {
                                if (error) {
                                  self->close_transport();
                                  return;
                                }
                                self->ready_ = true;
                                self->after_ready();
                              });
    } else {
      ready_ = true;
      after_ready();
    }
  }

private:
  void after_ready() {
    if (!admitted_) {
      respond(plain_response(503, "Service unavailable\n"));
      return;
    }
    read();
  }

  void arm_request_timeout() {
    timer_.expires_after(timeout_);
    auto self = this->shared_from_this();
    timer_.async_wait([self](const asio::error_code &error) {
      if (error || self->closed_ || self->responding_)
        return;
      if (!self->ready_)
        self->close_transport();
      else
        self->respond(plain_response(408, "Request timeout\n"));
    });
  }

  void read() {
    auto self = this->shared_from_this();
    stream_.async_read_some(
        asio::buffer(buffer_),
        [self](const asio::error_code &error, std::size_t transferred) {
          if (self->closed_ || self->responding_)
            return;
          if (error) {
            if (error != asio::error::operation_aborted)
              self->respond(plain_response(400, "Incomplete HTTP request\n"));
            return;
          }
          const core::ByteView input{self->buffer_.data(), transferred};
          auto request = self->parser_.feed(input);
          if (!request) {
            self->respond(plain_response(400, "Malformed HTTP request\n"));
            return;
          }
          if (!*request) {
            self->read();
            return;
          }
          self->dispatch(std::move(**request));
        });
  }

  void dispatch(api::HttpRequest request) {
    try {
      auto response = handler_->handle(std::move(request));
      if (!response) {
        respond(plain_response(500, "Internal server error\n"));
        return;
      }
      respond(std::move(*response));
    } catch (...) {
      respond(plain_response(500, "Internal server error\n"));
    }
  }

  void respond(api::HttpResponse response) {
    if (responding_ || closed_)
      return;
    responding_ = true;
    cancel_timer();
    auto encoded = http::serialize_response(std::move(response));
    if (!encoded) {
      encoded = http::serialize_response(
          plain_response(500, "Internal server error\n"));
    }
    if (!encoded) {
      close_transport();
      return;
    }
    output_ = std::make_shared<core::ByteBuffer>(std::move(*encoded));
    auto self = this->shared_from_this();
    asio::async_write(
        stream_, asio::buffer(*output_),
        [self](const asio::error_code &, std::size_t) { self->after_write(); });
  }

  void after_write() {
    if constexpr (std::same_as<Stream, TlsStream>) {
      timer_.expires_after(std::chrono::seconds{2});
      auto self = this->shared_from_this();
      timer_.async_wait([self](const asio::error_code &error) {
        if (!error)
          self->close_transport();
      });
      stream_.async_shutdown(
          [self](const asio::error_code &) { self->close_transport(); });
    } else {
      close_transport();
    }
  }

  void close_transport() {
    if (closed_)
      return;
    closed_ = true;
    cancel_timer();
    asio::error_code ignored;
    auto &socket = stream_.lowest_layer();
    socket.shutdown(Tcp::socket::shutdown_both, ignored);
    socket.close(ignored);
    if (admitted_ && on_done_)
      on_done_();
  }

  void cancel_timer() noexcept {
    try {
      timer_.cancel();
    } catch (...) {
    }
  }

  Stream stream_;
  asio::steady_timer timer_;
  api::HttpHandler *handler_;
  http::HttpRequestParser parser_;
  std::vector<std::byte> buffer_;
  std::chrono::milliseconds timeout_;
  std::shared_ptr<core::ByteBuffer> output_;
  bool admitted_{};
  bool ready_{};
  bool responding_{};
  bool closed_{};
  std::function<void()> on_done_;
};

core::Result<void> validate(const HttpServerOptions &options) {
  if (options.maximum_connections == 0)
    return std::unexpected(
        core::Error{core::ErrorCode::InvalidArgument,
                    "HTTP maximum connections must be positive"});
  if (options.read_buffer_bytes == 0 || options.read_buffer_bytes > 1024 * 1024)
    return std::unexpected(
        core::Error{core::ErrorCode::InvalidArgument,
                    "HTTP read buffer must be between 1 byte and 1 MiB"});
  if (options.request_timeout <= std::chrono::milliseconds::zero())
    return std::unexpected(
        core::Error{core::ErrorCode::InvalidArgument,
                    "HTTP request timeout must be positive"});
  if (options.parser_limits.maximum_target_bytes == 0 ||
      options.parser_limits.maximum_header_bytes == 0 ||
      options.parser_limits.maximum_header_count == 0)
    return std::unexpected(
        core::Error{core::ErrorCode::InvalidArgument,
                    "HTTP target and header parser limits must be positive"});
  if (options.tls && (options.tls->certificate_chain_file.empty() ||
                      options.tls->private_key_file.empty()))
    return std::unexpected(core::Error{
        core::ErrorCode::InvalidArgument,
        "TLS certificate chain and private key files are both required"});
  return {};
}

} // namespace

struct AsioHttpServer::Impl {
  explicit Impl(HttpServerOptions configured)
      : options(std::move(configured)), acceptor(context) {}

  void report(core::Error error) noexcept {
    if (!events)
      return;
    try {
      events->on_http_server_error(std::move(error));
    } catch (...) {
      // User callbacks may not unwind through the runtime thread.
    }
  }

  bool admit() {
    const auto previous = connection_count.fetch_add(1);
    if (previous < options.maximum_connections)
      return true;
    connection_count.fetch_sub(1);
    return false;
  }

  void accept() {
    if (!active.load(std::memory_order_acquire))
      return;
    acceptor.async_accept([this](const asio::error_code &error,
                                 Tcp::socket socket) {
      if (!error) {
        const bool admitted = admit();
        auto done = [this] { connection_count.fetch_sub(1); };
        if (tls) {
          using Session = HttpSession<TlsStream>;
          std::make_shared<Session>(TlsStream{std::move(socket), *tls}, handler,
                                    options, admitted, std::move(done))
              ->start();
        } else {
          using Session = HttpSession<Tcp::socket>;
          std::make_shared<Session>(std::move(socket), handler, options,
                                    admitted, std::move(done))
              ->start();
        }
      } else if (error != asio::error::operation_aborted) {
        report(network_error("HTTP accept failed", error));
      }
      accept();
    });
  }

  HttpServerOptions options;
  asio::io_context context;
  Tcp::acceptor acceptor;
  std::optional<asio::ssl::context> tls;
  DatagramEndpoint local;
  api::HttpHandler *handler{};
  HttpServerEvents *events{};
  std::thread thread;
  std::atomic<bool> active{};
  std::atomic<std::size_t> connection_count{};
  bool started{};
  std::mutex mutex;
};

AsioHttpServer::AsioHttpServer(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

AsioHttpServer::~AsioHttpServer() { stop(); }

core::Result<std::unique_ptr<AsioHttpServer>>
AsioHttpServer::create(const HttpServerOptions &options) {
  auto valid = validate(options);
  if (!valid)
    return std::unexpected(valid.error());
  auto endpoint = to_asio(options.bind_to);
  if (!endpoint)
    return std::unexpected(endpoint.error());
  auto impl = std::make_unique<Impl>(options);
  asio::error_code error;
  impl->acceptor.open(endpoint->protocol(), error);
  if (error)
    return std::unexpected(network_error("Could not open HTTP socket", error));
  impl->acceptor.set_option(Tcp::acceptor::reuse_address(true), error);
  if (error)
    return std::unexpected(
        network_error("Could not configure HTTP socket", error));
  impl->acceptor.bind(*endpoint, error);
  if (error)
    return std::unexpected(network_error("Could not bind HTTP socket", error));
  impl->acceptor.listen(asio::socket_base::max_listen_connections, error);
  if (error)
    return std::unexpected(
        network_error("Could not listen on HTTP socket", error));
  const auto local = impl->acceptor.local_endpoint(error);
  if (error)
    return std::unexpected(
        network_error("Could not inspect HTTP endpoint", error));
  impl->local = from_asio(local);

  if (options.tls) {
    try {
      impl->tls.emplace(asio::ssl::context::tls_server);
      impl->tls->set_options(
          asio::ssl::context::default_workarounds |
          asio::ssl::context::no_sslv2 | asio::ssl::context::no_sslv3 |
          asio::ssl::context::no_tlsv1 | asio::ssl::context::no_tlsv1_1);
      impl->tls->use_certificate_chain_file(
          options.tls->certificate_chain_file.string());
      impl->tls->use_private_key_file(options.tls->private_key_file.string(),
                                      asio::ssl::context::pem);
    } catch (const std::exception &exception) {
      return std::unexpected(core::Error{
          core::ErrorCode::InvalidArgument,
          std::string{"Could not configure HTTP TLS: "} + exception.what()});
    }
  }
  return std::unique_ptr<AsioHttpServer>{new AsioHttpServer{std::move(impl)}};
}

core::Result<void> AsioHttpServer::start(api::HttpHandler &handler,
                                         HttpServerEvents *events) {
  std::lock_guard lock{impl_->mutex};
  if (impl_->started)
    return std::unexpected(
        core::Error{core::ErrorCode::Conflict, "HTTP server already started"});
  impl_->started = true;
  impl_->handler = &handler;
  impl_->events = events;
  impl_->active.store(true, std::memory_order_release);
  impl_->accept();
  try {
    impl_->thread = std::thread{[impl = impl_.get()] { impl->context.run(); }};
  } catch (const std::exception &exception) {
    impl_->active.store(false, std::memory_order_release);
    asio::error_code ignored;
    impl_->acceptor.cancel(ignored);
    return std::unexpected(
        core::Error{core::ErrorCode::Internal,
                    std::string{"Could not start HTTP runtime thread: "} +
                        exception.what()});
  }
  return {};
}

DatagramEndpoint AsioHttpServer::local_endpoint() const noexcept {
  return impl_->local;
}

void AsioHttpServer::stop() noexcept {
  if (!impl_)
    return;
  if (impl_->active.exchange(false, std::memory_order_acq_rel)) {
    asio::error_code ignored;
    impl_->acceptor.cancel(ignored);
    impl_->acceptor.close(ignored);
    impl_->context.stop();
  }
  if (impl_->thread.joinable() &&
      impl_->thread.get_id() != std::this_thread::get_id())
    impl_->thread.join();
}

} // namespace sakuin::runtime

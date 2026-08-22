module;

#include <asio.hpp>
#include <asio/ssl.hpp>
#include <openssl/evp.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>

export module sakuin.runtime.asio_tls_server;

import std;

import sakuin.core.bytes;
import sakuin.core.result;
import sakuin.core.time;
import sakuin.runtime.datagram;
import sakuin.runtime.stream;

export namespace sakuin::runtime {

// Standalone Asio/OpenSSL mTLS server behind Sakuin's stream/session boundary.
class AsioTlsStreamServer final : public StreamServer {
public:
  static core::Result<std::unique_ptr<AsioTlsStreamServer>>
  create(StreamServerOptions transport, StreamTlsServerOptions tls);

  ~AsioTlsStreamServer() override;

  core::Result<void> start(StreamServerHandler &handler,
                           StreamServerEvents *events = nullptr) override;
  StreamEndpoint local_endpoint() const noexcept override;
  void stop() noexcept override;

private:
  struct Impl;
  explicit AsioTlsStreamServer(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> impl_;
};

} // namespace sakuin::runtime

namespace sakuin::runtime {
namespace {

using Tcp = asio::ip::tcp;
using TlsStream = asio::ssl::stream<Tcp::socket>;

core::Error tls_error(std::string action, const asio::error_code &error) {
  return {core::ErrorCode::IoError, std::move(action) + ": " + error.message()};
}

core::Result<Tcp::endpoint> to_asio(StreamEndpoint endpoint) {
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

StreamEndpoint from_asio(const Tcp::endpoint &endpoint) {
  StreamEndpoint result{.port = endpoint.port()};
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

core::Result<StreamPeerIdentity> certificate_identity(SSL *ssl) {
  std::unique_ptr<X509, decltype(&X509_free)> certificate{
      SSL_get1_peer_certificate(ssl), X509_free};
  if (!certificate)
    return std::unexpected(
        core::Error{core::ErrorCode::PermissionDenied,
                    "TLS peer did not present a certificate"});

  std::array<unsigned char, EVP_MAX_MD_SIZE> digest{};
  unsigned int digest_size{};
  if (X509_digest(certificate.get(), EVP_sha256(), digest.data(),
                  &digest_size) != 1)
    return std::unexpected(
        core::Error{core::ErrorCode::Internal,
                    "Could not fingerprint TLS peer certificate"});
  static constexpr char hexadecimal[] = "0123456789abcdef";
  std::string fingerprint;
  fingerprint.reserve(digest_size * 2);
  for (unsigned int index = 0; index < digest_size; ++index) {
    fingerprint.push_back(hexadecimal[digest[index] >> 4U]);
    fingerprint.push_back(hexadecimal[digest[index] & 0x0fU]);
  }

  std::array<char, 257> common_name{};
  const auto length = X509_NAME_get_text_by_NID(
      X509_get_subject_name(certificate.get()), NID_commonName,
      common_name.data(), static_cast<int>(common_name.size()));
  if (length <= 0 || static_cast<std::size_t>(length) >= common_name.size())
    return std::unexpected(core::Error{
        core::ErrorCode::PermissionDenied,
        "TLS peer certificate requires a bounded subject common name"});
  return StreamPeerIdentity{
      .principal =
          std::string{common_name.data(), static_cast<std::size_t>(length)},
      .certificate_sha256 = std::move(fingerprint)};
}

core::Result<void> validate(const StreamServerOptions &transport,
                            const StreamTlsServerOptions &tls) {
  if (transport.idle_timeout <= core::Duration::zero() ||
      transport.read_buffer_bytes == 0 ||
      transport.read_buffer_bytes > 1024U * 1024U ||
      transport.maximum_queued_write_bytes == 0 ||
      transport.maximum_queued_write_bytes > 64U * 1024U * 1024U ||
      transport.maximum_connections == 0 ||
      transport.maximum_connections > 1'000'000 ||
      tls.trust_anchor_file.empty() || tls.certificate_chain_file.empty() ||
      tls.private_key_file.empty())
    return std::unexpected(core::Error{core::ErrorCode::InvalidArgument,
                                       "Invalid TLS stream server options"});
  return {};
}

class AsioTlsStreamSession final
    : public StreamServerSession,
      public std::enable_shared_from_this<AsioTlsStreamSession> {
public:
  AsioTlsStreamSession(StreamSessionId id, TlsStream stream,
                       const StreamServerOptions &options,
                       StreamServerHandler *handler,
                       std::function<void(StreamSessionId)> on_done)
      : id_(id), stream_(std::move(stream)), timer_(stream_.get_executor()),
        remote_(remote_endpoint(stream_)),
        read_buffer_(options.read_buffer_bytes),
        idle_timeout_(options.idle_timeout),
        maximum_queued_write_bytes_(options.maximum_queued_write_bytes),
        handler_(handler), on_done_(std::move(on_done)) {}

  StreamSessionId id() const noexcept override { return id_; }
  StreamEndpoint remote_endpoint() const noexcept override { return remote_; }
  std::optional<StreamPeerIdentity> peer_identity() const override {
    return identity_;
  }

  core::Result<void> send(core::ByteBuffer bytes) override {
    if (!active_.load(std::memory_order_acquire) || !opened_)
      return std::unexpected(core::Error{core::ErrorCode::Conflict,
                                         "TLS server session is not open"});
    if (bytes.empty())
      return std::unexpected(core::Error{core::ErrorCode::InvalidArgument,
                                         "TLS stream write must not be empty"});
    auto queued = queued_bytes_.load(std::memory_order_acquire);
    while (true) {
      const auto available = maximum_queued_write_bytes_ -
                             std::min(queued, maximum_queued_write_bytes_);
      if (bytes.size() > available)
        return std::unexpected(
            core::Error{core::ErrorCode::QuotaExceeded,
                        "TLS server queued-write limit would be exceeded"});
      if (queued_bytes_.compare_exchange_weak(queued, queued + bytes.size(),
                                              std::memory_order_acq_rel))
        break;
    }
    auto owned = std::make_shared<core::ByteBuffer>(std::move(bytes));
    auto self = shared_from_this();
    asio::post(stream_.get_executor(), [self, owned] {
      if (!self->active_.load(std::memory_order_acquire))
        return;
      self->writes_.push_back(owned);
      self->write_next();
    });
    return {};
  }

  void close() noexcept override {
    if (!active_.load(std::memory_order_acquire))
      return;
    try {
      auto self = shared_from_this();
      asio::post(stream_.get_executor(),
                 [self] { self->close_now(std::nullopt); });
    } catch (...) {
    }
  }

  void start() {
    active_.store(true, std::memory_order_release);
    arm_idle_timeout();
    auto self = shared_from_this();
    stream_.async_handshake(
        asio::ssl::stream_base::server, [self](const asio::error_code &error) {
          if (!self->active_.load(std::memory_order_acquire))
            return;
          if (error) {
            self->close_now(tls_error("TLS client handshake failed", error));
            return;
          }
          auto identity = certificate_identity(self->stream_.native_handle());
          if (!identity) {
            self->close_now(identity.error());
            return;
          }
          self->identity_ = std::move(*identity);
          self->opened_ = true;
          try {
            self->handler_->on_stream_session_opened(*self);
          } catch (const std::exception &exception) {
            self->close_now(core::Error{
                core::ErrorCode::Internal,
                std::string{"TLS session handler threw: "} + exception.what()});
            return;
          } catch (...) {
            self->close_now(
                core::Error{core::ErrorCode::Internal,
                            "TLS session handler threw an unknown exception"});
            return;
          }
          if (self->active_.load(std::memory_order_acquire)) {
            self->arm_idle_timeout();
            self->read();
          }
        });
  }

  void close_now(std::optional<core::Error> error) noexcept {
    if (!active_.exchange(false, std::memory_order_acq_rel))
      return;
    asio::error_code ignored;
    try {
      timer_.cancel();
    } catch (...) {
    }
    stream_.lowest_layer().shutdown(Tcp::socket::shutdown_both, ignored);
    stream_.lowest_layer().close(ignored);
    writes_.clear();
    queued_bytes_.store(0, std::memory_order_release);
    try {
      // Closed is delivered even when authentication failed before opened;
      // handlers can use the error for audit logging without retaining state.
      handler_->on_stream_session_closed(id_, std::move(error));
    } catch (...) {
    }
    if (on_done_)
      on_done_(id_);
  }

private:
  static StreamEndpoint remote_endpoint(TlsStream &stream) noexcept {
    asio::error_code error;
    const auto endpoint = stream.lowest_layer().remote_endpoint(error);
    return error ? StreamEndpoint{} : from_asio(endpoint);
  }

  void arm_idle_timeout() {
    timer_.expires_after(idle_timeout_);
    auto self = shared_from_this();
    timer_.async_wait([self](const asio::error_code &error) {
      if (!error && self->active_.load(std::memory_order_acquire))
        self->close_now(core::Error{core::ErrorCode::Timeout,
                                    "TLS server session was idle too long"});
    });
  }

  void read() {
    if (!active_.load(std::memory_order_acquire))
      return;
    auto self = shared_from_this();
    stream_.async_read_some(
        asio::buffer(read_buffer_),
        [self](const asio::error_code &error, std::size_t transferred) {
          if (!self->active_.load(std::memory_order_acquire))
            return;
          if (error) {
            if (error == asio::error::eof ||
                error == asio::ssl::error::stream_truncated)
              self->close_now(std::nullopt);
            else if (error != asio::error::operation_aborted)
              self->close_now(tls_error("TLS server read failed", error));
            return;
          }
          self->arm_idle_timeout();
          auto bytes =
              core::ByteBuffer{self->read_buffer_.begin(),
                               self->read_buffer_.begin() + transferred};
          try {
            self->handler_->on_stream_session_data(*self, std::move(bytes));
          } catch (const std::exception &exception) {
            self->close_now(core::Error{
                core::ErrorCode::Internal,
                std::string{"TLS session handler threw: "} + exception.what()});
            return;
          } catch (...) {
            self->close_now(
                core::Error{core::ErrorCode::Internal,
                            "TLS session handler threw an unknown exception"});
            return;
          }
          self->read();
        });
  }

  void write_next() {
    if (!active_.load(std::memory_order_acquire) || writing_ || writes_.empty())
      return;
    writing_ = true;
    const auto bytes = writes_.front();
    auto self = shared_from_this();
    asio::async_write(
        stream_, asio::buffer(*bytes),
        [self, bytes](const asio::error_code &error, std::size_t) {
          if (!self->active_.load(std::memory_order_acquire)) {
            self->writing_ = false;
            return;
          }
          self->queued_bytes_.fetch_sub(bytes->size(),
                                        std::memory_order_acq_rel);
          if (!self->writes_.empty())
            self->writes_.pop_front();
          self->writing_ = false;
          if (error) {
            if (error != asio::error::operation_aborted)
              self->close_now(tls_error("TLS server write failed", error));
            return;
          }
          self->arm_idle_timeout();
          self->write_next();
        });
  }

  StreamSessionId id_{};
  TlsStream stream_;
  asio::steady_timer timer_;
  StreamEndpoint remote_;
  std::optional<StreamPeerIdentity> identity_;
  std::vector<std::byte> read_buffer_;
  core::Duration idle_timeout_;
  std::size_t maximum_queued_write_bytes_{};
  StreamServerHandler *handler_{};
  std::function<void(StreamSessionId)> on_done_;
  std::deque<std::shared_ptr<core::ByteBuffer>> writes_;
  std::atomic<bool> active_{};
  std::atomic<std::size_t> queued_bytes_{};
  bool opened_{};
  bool writing_{};
};

} // namespace

struct AsioTlsStreamServer::Impl {
  Impl(StreamServerOptions configured, StreamTlsServerOptions secure)
      : options(std::move(configured)), tls_options(std::move(secure)),
        acceptor(context), tls(asio::ssl::context::tls_server) {}

  void report(core::Error error) noexcept {
    if (!events)
      return;
    try {
      events->on_stream_server_error(std::move(error));
    } catch (...) {
    }
  }

  void accept() {
    if (!active.load(std::memory_order_acquire))
      return;
    acceptor.async_accept([this](const asio::error_code &error,
                                 Tcp::socket socket) {
      if (!error) {
        if (sessions.size() >= options.maximum_connections) {
          asio::error_code ignored;
          socket.close(ignored);
        } else {
          auto id = ++next_session;
          if (id == 0)
            id = ++next_session;
          auto session = std::make_shared<AsioTlsStreamSession>(
              id, TlsStream{std::move(socket), tls}, options, handler,
              [this](StreamSessionId finished) { sessions.erase(finished); });
          sessions.emplace(id, session);
          session->start();
        }
      } else if (error != asio::error::operation_aborted) {
        report(tls_error("TLS server accept failed", error));
      }
      accept();
    });
  }

  void shutdown() noexcept {
    asio::error_code ignored;
    acceptor.cancel(ignored);
    acceptor.close(ignored);
    std::vector<std::shared_ptr<AsioTlsStreamSession>> closing;
    closing.reserve(sessions.size());
    for (const auto &[_, session] : sessions)
      closing.push_back(session);
    for (const auto &session : closing)
      session->close_now(std::nullopt);
    sessions.clear();
  }

  StreamServerOptions options;
  StreamTlsServerOptions tls_options;
  asio::io_context context;
  Tcp::acceptor acceptor;
  asio::ssl::context tls;
  StreamEndpoint local;
  StreamServerHandler *handler{};
  StreamServerEvents *events{};
  std::map<StreamSessionId, std::shared_ptr<AsioTlsStreamSession>> sessions;
  std::thread thread;
  std::atomic<bool> active{};
  StreamSessionId next_session{};
  bool started{};
  std::mutex mutex;
};

AsioTlsStreamServer::AsioTlsStreamServer(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

AsioTlsStreamServer::~AsioTlsStreamServer() { stop(); }

core::Result<std::unique_ptr<AsioTlsStreamServer>>
AsioTlsStreamServer::create(StreamServerOptions transport,
                            StreamTlsServerOptions tls) {
  if (auto valid = validate(transport, tls); !valid)
    return std::unexpected(valid.error());
  auto endpoint = to_asio(transport.bind_to);
  if (!endpoint)
    return std::unexpected(endpoint.error());
  try {
    auto impl = std::make_unique<Impl>(std::move(transport), std::move(tls));
    impl->tls.set_options(
        asio::ssl::context::default_workarounds | asio::ssl::context::no_sslv2 |
        asio::ssl::context::no_sslv3 | asio::ssl::context::no_tlsv1 |
        asio::ssl::context::no_tlsv1_1);
    impl->tls.load_verify_file(impl->tls_options.trust_anchor_file.string());
    impl->tls.use_certificate_chain_file(
        impl->tls_options.certificate_chain_file.string());
    impl->tls.use_private_key_file(impl->tls_options.private_key_file.string(),
                                   asio::ssl::context::pem);
    if (SSL_CTX_check_private_key(impl->tls.native_handle()) != 1)
      return std::unexpected(
          core::Error{core::ErrorCode::InvalidArgument,
                      "TLS server private key does not match its certificate"});
    impl->tls.set_verify_mode(asio::ssl::verify_peer |
                              asio::ssl::verify_fail_if_no_peer_cert);

    asio::error_code error;
    impl->acceptor.open(endpoint->protocol(), error);
    if (error)
      return std::unexpected(
          tls_error("Could not open TLS server socket", error));
    impl->acceptor.set_option(Tcp::acceptor::reuse_address(true), error);
    if (error)
      return std::unexpected(
          tls_error("Could not configure TLS server socket", error));
    impl->acceptor.bind(*endpoint, error);
    if (error)
      return std::unexpected(
          tls_error("Could not bind TLS server socket", error));
    impl->acceptor.listen(asio::socket_base::max_listen_connections, error);
    if (error)
      return std::unexpected(
          tls_error("Could not listen on TLS server socket", error));
    const auto local = impl->acceptor.local_endpoint(error);
    if (error)
      return std::unexpected(
          tls_error("Could not inspect TLS server endpoint", error));
    impl->local = from_asio(local);
    return std::unique_ptr<AsioTlsStreamServer>{
        new AsioTlsStreamServer{std::move(impl)}};
  } catch (const std::exception &exception) {
    return std::unexpected(core::Error{
        core::ErrorCode::InvalidArgument,
        std::string{"Could not configure TLS server: "} + exception.what()});
  }
}

core::Result<void> AsioTlsStreamServer::start(StreamServerHandler &handler,
                                              StreamServerEvents *events) {
  std::lock_guard lock{impl_->mutex};
  if (impl_->started)
    return std::unexpected(core::Error{core::ErrorCode::Conflict,
                                       "TLS stream server already started"});
  impl_->started = true;
  impl_->handler = &handler;
  impl_->events = events;
  impl_->active.store(true, std::memory_order_release);
  impl_->accept();
  try {
    impl_->thread = std::thread{[impl = impl_.get()] { impl->context.run(); }};
  } catch (const std::exception &exception) {
    impl_->active.store(false, std::memory_order_release);
    impl_->shutdown();
    return std::unexpected(
        core::Error{core::ErrorCode::Internal,
                    std::string{"Could not start TLS server runtime thread: "} +
                        exception.what()});
  }
  return {};
}

StreamEndpoint AsioTlsStreamServer::local_endpoint() const noexcept {
  return impl_->local;
}

void AsioTlsStreamServer::stop() noexcept {
  if (!impl_)
    return;
  if (impl_->active.exchange(false, std::memory_order_acq_rel)) {
    if (impl_->thread.joinable() &&
        impl_->thread.get_id() == std::this_thread::get_id()) {
      impl_->shutdown();
      impl_->context.stop();
      return;
    }
    auto stopped = std::make_shared<std::promise<void>>();
    auto ready = stopped->get_future();
    try {
      asio::post(impl_->context, [impl = impl_.get(), stopped] {
        impl->shutdown();
        stopped->set_value();
      });
      ready.wait();
    } catch (...) {
      impl_->shutdown();
    }
    impl_->context.stop();
  }
  if (impl_->thread.joinable() &&
      impl_->thread.get_id() != std::this_thread::get_id())
    impl_->thread.join();
}

} // namespace sakuin::runtime

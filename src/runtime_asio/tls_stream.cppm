module;

#include <asio.hpp>
#include <asio/ssl.hpp>
#include <openssl/evp.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>

export module sakuin.runtime.asio_tls_stream;

import std;

import sakuin.core.bytes;
import sakuin.core.result;
import sakuin.core.time;
import sakuin.runtime.datagram;
import sakuin.runtime.stream;

export namespace sakuin::runtime {

// Standalone Asio/OpenSSL mTLS client behind Sakuin's owned-byte stream
// contract. No Asio or OpenSSL type crosses this boundary.
class AsioTlsStreamTransport final : public StreamTransport {
public:
  static core::Result<std::unique_ptr<AsioTlsStreamTransport>>
  create(StreamTransportOptions transport, StreamTlsClientOptions tls);

  ~AsioTlsStreamTransport() override;

  core::Result<void> start(StreamReceiver &receiver) override;
  core::Result<void> send(core::ByteBuffer bytes) override;
  StreamEndpoint remote_endpoint() const noexcept override;
  std::optional<StreamPeerIdentity> peer_identity() const override;
  void stop() noexcept override;

private:
  struct Impl;
  explicit AsioTlsStreamTransport(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> impl_;
};

class AsioTlsStreamTransportFactory final : public StreamTransportFactory {
public:
  explicit AsioTlsStreamTransportFactory(StreamTlsClientOptions tls)
      : tls_(std::move(tls)) {}

  core::Result<std::shared_ptr<StreamTransport>>
  create(StreamTransportOptions options) override;

private:
  StreamTlsClientOptions tls_;
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
  if (endpoint.port == 0)
    return std::unexpected(core::Error{core::ErrorCode::InvalidArgument,
                                       "TLS peer port must be nonzero"});
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

bool valid_tls(const StreamTlsClientOptions &tls) {
  return !tls.trust_anchor_file.empty() &&
         !tls.certificate_chain_file.empty() && !tls.private_key_file.empty() &&
         !tls.server_name.empty() && tls.server_name.size() <= 253;
}

} // namespace

struct AsioTlsStreamTransport::Impl {
  Impl(StreamTransportOptions configured, StreamTlsClientOptions secure,
       Tcp::endpoint endpoint)
      : options(std::move(configured)), tls_options(std::move(secure)),
        remote(std::move(endpoint)), tls(asio::ssl::context::tls_client),
        timer(context) {}

  StreamTransportOptions options;
  StreamTlsClientOptions tls_options;
  Tcp::endpoint remote;
  asio::io_context context;
  asio::ssl::context tls;
  std::unique_ptr<TlsStream> stream;
  asio::steady_timer timer;
  std::vector<std::byte> read_buffer;
  std::deque<std::shared_ptr<core::ByteBuffer>> writes;
  StreamReceiver *receiver{};
  std::thread thread;
  std::atomic<bool> active{};
  std::atomic<bool> connected{};
  std::atomic<std::size_t> queued_bytes{};
  std::mutex start_mutex;
  std::mutex identity_mutex;
  std::optional<StreamPeerIdentity> identity;
  bool started{};
  bool writing{};
  bool close_notified{};

  void notify_connected() {
    try {
      receiver->on_stream_connected();
    } catch (const std::exception &exception) {
      close(core::Error{core::ErrorCode::Internal,
                        std::string{"Stream receiver threw: "} +
                            exception.what()});
    } catch (...) {
      close(core::Error{core::ErrorCode::Internal,
                        "Stream receiver threw an unknown exception"});
    }
  }

  void notify_data(core::ByteBuffer bytes) {
    try {
      receiver->on_stream_data(std::move(bytes));
    } catch (const std::exception &exception) {
      close(core::Error{core::ErrorCode::Internal,
                        std::string{"Stream receiver threw: "} +
                            exception.what()});
    } catch (...) {
      close(core::Error{core::ErrorCode::Internal,
                        "Stream receiver threw an unknown exception"});
    }
  }

  void notify_closed(std::optional<core::Error> error) noexcept {
    if (close_notified || !receiver)
      return;
    close_notified = true;
    try {
      receiver->on_stream_closed(std::move(error));
    } catch (...) {
    }
  }

  void close(std::optional<core::Error> error) noexcept {
    if (!active.exchange(false, std::memory_order_acq_rel))
      return;
    connected.store(false, std::memory_order_release);
    asio::error_code ignored;
    try {
      timer.cancel();
    } catch (...) {
    }
    stream->lowest_layer().shutdown(Tcp::socket::shutdown_both, ignored);
    stream->lowest_layer().close(ignored);
    writes.clear();
    queued_bytes.store(0, std::memory_order_release);
    notify_closed(std::move(error));
  }

  void arm_idle_timeout() {
    timer.expires_after(options.idle_timeout);
    timer.async_wait([this](const asio::error_code &error) {
      if (!error && active.load(std::memory_order_acquire))
        close(core::Error{core::ErrorCode::Timeout,
                          "TLS peer connection was idle too long"});
    });
  }

  void read() {
    if (!active.load(std::memory_order_acquire))
      return;
    stream->async_read_some(
        asio::buffer(read_buffer),
        [this](const asio::error_code &error, std::size_t transferred) {
          if (!active.load(std::memory_order_acquire))
            return;
          if (error) {
            if (error == asio::error::eof ||
                error == asio::ssl::error::stream_truncated)
              close(std::nullopt);
            else if (error != asio::error::operation_aborted)
              close(tls_error("TLS peer read failed", error));
            return;
          }
          arm_idle_timeout();
          notify_data(core::ByteBuffer{read_buffer.begin(),
                                       read_buffer.begin() + transferred});
          read();
        });
  }

  void write_next() {
    if (!active.load(std::memory_order_acquire) || writing || writes.empty())
      return;
    writing = true;
    const auto bytes = writes.front();
    asio::async_write(
        *stream, asio::buffer(*bytes),
        [this, bytes](const asio::error_code &error, std::size_t) {
          if (!active.load(std::memory_order_acquire)) {
            writing = false;
            return;
          }
          queued_bytes.fetch_sub(bytes->size(), std::memory_order_acq_rel);
          if (!writes.empty())
            writes.pop_front();
          writing = false;
          if (error) {
            if (error != asio::error::operation_aborted)
              close(tls_error("TLS peer write failed", error));
            return;
          }
          arm_idle_timeout();
          write_next();
        });
  }
};

AsioTlsStreamTransport::AsioTlsStreamTransport(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

AsioTlsStreamTransport::~AsioTlsStreamTransport() { stop(); }

core::Result<std::unique_ptr<AsioTlsStreamTransport>>
AsioTlsStreamTransport::create(StreamTransportOptions transport,
                               StreamTlsClientOptions tls) {
  if (transport.connect_timeout <= core::Duration::zero() ||
      transport.idle_timeout <= core::Duration::zero() ||
      transport.read_buffer_bytes == 0 ||
      transport.read_buffer_bytes > 1024U * 1024U ||
      transport.maximum_queued_write_bytes == 0 || !valid_tls(tls))
    return std::unexpected(core::Error{core::ErrorCode::InvalidArgument,
                                       "Invalid TLS stream transport options"});
  auto remote = to_asio(transport.remote);
  if (!remote)
    return std::unexpected(remote.error());
  try {
    auto impl =
        std::make_unique<Impl>(std::move(transport), std::move(tls), *remote);
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
                      "TLS client private key does not match its certificate"});
    impl->stream = std::make_unique<TlsStream>(impl->context, impl->tls);
    impl->stream->set_verify_mode(asio::ssl::verify_peer);
    impl->stream->set_verify_callback(
        asio::ssl::host_name_verification(impl->tls_options.server_name));
    impl->read_buffer.resize(impl->options.read_buffer_bytes);
    return std::unique_ptr<AsioTlsStreamTransport>{
        new AsioTlsStreamTransport{std::move(impl)}};
  } catch (const std::exception &exception) {
    return std::unexpected(core::Error{
        core::ErrorCode::InvalidArgument,
        std::string{"Could not configure TLS client: "} + exception.what()});
  }
}

core::Result<std::shared_ptr<StreamTransport>>
AsioTlsStreamTransportFactory::create(StreamTransportOptions options) {
  auto transport = AsioTlsStreamTransport::create(std::move(options), tls_);
  if (!transport)
    return std::unexpected(transport.error());
  return std::shared_ptr<StreamTransport>{std::move(*transport)};
}

core::Result<void> AsioTlsStreamTransport::start(StreamReceiver &receiver) {
  std::lock_guard lock{impl_->start_mutex};
  if (impl_->started)
    return std::unexpected(core::Error{core::ErrorCode::Conflict,
                                       "TLS stream transport already started"});
  impl_->started = true;
  impl_->receiver = &receiver;
  impl_->active.store(true, std::memory_order_release);
  if (SSL_set_tlsext_host_name(impl_->stream->native_handle(),
                               impl_->tls_options.server_name.c_str()) != 1) {
    impl_->active.store(false, std::memory_order_release);
    return std::unexpected(core::Error{core::ErrorCode::InvalidArgument,
                                       "Could not configure TLS server name"});
  }
  impl_->timer.expires_after(impl_->options.connect_timeout);
  impl_->timer.async_wait([impl = impl_.get()](const asio::error_code &error) {
    if (!error && impl->active.load(std::memory_order_acquire) &&
        !impl->connected.load(std::memory_order_acquire))
      impl->close(core::Error{core::ErrorCode::Timeout,
                              "TLS peer connection timed out"});
  });
  impl_->stream->lowest_layer().async_connect(
      impl_->remote, [impl = impl_.get()](const asio::error_code &error) {
        if (!impl->active.load(std::memory_order_acquire))
          return;
        if (error) {
          impl->close(tls_error("TLS peer TCP connection failed", error));
          return;
        }
        impl->stream->async_handshake(
            asio::ssl::stream_base::client,
            [impl](const asio::error_code &handshake_error) {
              if (!impl->active.load(std::memory_order_acquire))
                return;
              if (handshake_error) {
                impl->close(
                    tls_error("TLS peer handshake failed", handshake_error));
                return;
              }
              auto identity =
                  certificate_identity(impl->stream->native_handle());
              if (!identity) {
                impl->close(identity.error());
                return;
              }
              {
                std::lock_guard identity_lock{impl->identity_mutex};
                impl->identity = std::move(*identity);
              }
              impl->connected.store(true, std::memory_order_release);
              impl->arm_idle_timeout();
              impl->notify_connected();
              impl->read();
            });
      });
  try {
    impl_->thread = std::thread{[impl = impl_.get()] { impl->context.run(); }};
  } catch (const std::exception &exception) {
    impl_->active.store(false, std::memory_order_release);
    return std::unexpected(
        core::Error{core::ErrorCode::Internal,
                    std::string{"Could not start TLS client runtime thread: "} +
                        exception.what()});
  }
  return {};
}

core::Result<void> AsioTlsStreamTransport::send(core::ByteBuffer bytes) {
  if (!impl_->active.load(std::memory_order_acquire) ||
      !impl_->connected.load(std::memory_order_acquire))
    return std::unexpected(core::Error{
        core::ErrorCode::Conflict, "TLS stream transport is not connected"});
  if (bytes.empty())
    return std::unexpected(core::Error{core::ErrorCode::InvalidArgument,
                                       "TLS stream write must not be empty"});
  auto queued = impl_->queued_bytes.load(std::memory_order_acquire);
  while (true) {
    const auto available =
        impl_->options.maximum_queued_write_bytes -
        std::min(queued, impl_->options.maximum_queued_write_bytes);
    if (bytes.size() > available)
      return std::unexpected(
          core::Error{core::ErrorCode::QuotaExceeded,
                      "TLS stream queued-write limit would be exceeded"});
    if (impl_->queued_bytes.compare_exchange_weak(queued, queued + bytes.size(),
                                                  std::memory_order_acq_rel))
      break;
  }
  auto owned = std::make_shared<core::ByteBuffer>(std::move(bytes));
  asio::post(impl_->context, [impl = impl_.get(), owned] {
    if (!impl->active.load(std::memory_order_acquire))
      return;
    impl->writes.push_back(owned);
    impl->write_next();
  });
  return {};
}

StreamEndpoint AsioTlsStreamTransport::remote_endpoint() const noexcept {
  return impl_->options.remote;
}

std::optional<StreamPeerIdentity>
AsioTlsStreamTransport::peer_identity() const {
  std::lock_guard lock{impl_->identity_mutex};
  return impl_->identity;
}

void AsioTlsStreamTransport::stop() noexcept {
  if (!impl_)
    return;
  impl_->active.store(false, std::memory_order_release);
  impl_->connected.store(false, std::memory_order_release);
  impl_->context.stop();
  if (impl_->thread.joinable()) {
    if (impl_->thread.get_id() == std::this_thread::get_id())
      return;
    impl_->thread.join();
  }
  asio::error_code ignored;
  try {
    impl_->timer.cancel();
  } catch (...) {
  }
  if (impl_->stream)
    impl_->stream->lowest_layer().close(ignored);
}

} // namespace sakuin::runtime

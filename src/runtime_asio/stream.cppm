module;

#include <asio.hpp>

export module sakuin.runtime.asio_stream;

import std;

import sakuin.core.bytes;
import sakuin.core.result;
import sakuin.core.time;
import sakuin.runtime.datagram;
import sakuin.runtime.stream;

export namespace sakuin::runtime {

// Standalone Asio TCP client behind the owned-byte stream contract. No Asio
// executor, token, awaitable, or error type crosses this module boundary.
class AsioTcpStreamTransport final : public StreamTransport {
public:
  static core::Result<std::unique_ptr<AsioTcpStreamTransport>>
  create(StreamTransportOptions options);

  ~AsioTcpStreamTransport() override;

  core::Result<void> start(StreamReceiver &receiver) override;
  core::Result<void> send(core::ByteBuffer bytes) override;
  StreamEndpoint remote_endpoint() const noexcept override;
  void stop() noexcept override;

private:
  struct Impl;
  explicit AsioTcpStreamTransport(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> impl_;
};

class AsioTcpStreamTransportFactory final : public StreamTransportFactory {
public:
  core::Result<std::shared_ptr<StreamTransport>>
  create(StreamTransportOptions options) override;
};

} // namespace sakuin::runtime

namespace sakuin::runtime {
namespace {

using Tcp = asio::ip::tcp;

core::Error network_error(std::string action, const asio::error_code &error) {
  return {core::ErrorCode::IoError, std::move(action) + ": " + error.message()};
}

core::Result<Tcp::endpoint> to_asio(StreamEndpoint endpoint) {
  if (endpoint.port == 0)
    return std::unexpected(core::Error{core::ErrorCode::InvalidArgument,
                                       "TCP peer port must be nonzero"});
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

} // namespace

struct AsioTcpStreamTransport::Impl {
  StreamTransportOptions options;
  Tcp::endpoint remote;
  asio::io_context context;
  Tcp::socket socket{context};
  asio::steady_timer timer{context};
  std::vector<std::byte> read_buffer;
  std::deque<std::shared_ptr<core::ByteBuffer>> writes;
  StreamReceiver *receiver{};
  std::thread thread;
  std::atomic<bool> active{};
  std::atomic<std::size_t> queued_bytes{};
  std::mutex start_mutex;
  bool started{};
  std::atomic<bool> connected{};
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
      // User callbacks may not unwind through the runtime thread.
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
    socket.shutdown(Tcp::socket::shutdown_both, ignored);
    socket.close(ignored);
    writes.clear();
    queued_bytes.store(0, std::memory_order_release);
    notify_closed(std::move(error));
  }

  void arm_idle_timeout() {
    timer.expires_after(options.idle_timeout);
    timer.async_wait([this](const asio::error_code &error) {
      if (!error && active.load(std::memory_order_acquire))
        close(core::Error{core::ErrorCode::Timeout,
                          "TCP peer connection was idle too long"});
    });
  }

  void read() {
    if (!active.load(std::memory_order_acquire))
      return;
    socket.async_read_some(
        asio::buffer(read_buffer),
        [this](const asio::error_code &error, std::size_t transferred) {
          if (!active.load(std::memory_order_acquire))
            return;
          if (error) {
            if (error == asio::error::eof)
              close(std::nullopt);
            else if (error != asio::error::operation_aborted)
              close(network_error("TCP peer read failed", error));
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
        socket, asio::buffer(*bytes),
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
              close(network_error("TCP peer write failed", error));
            return;
          }
          arm_idle_timeout();
          write_next();
        });
  }
};

AsioTcpStreamTransport::AsioTcpStreamTransport(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

AsioTcpStreamTransport::~AsioTcpStreamTransport() { stop(); }

core::Result<std::unique_ptr<AsioTcpStreamTransport>>
AsioTcpStreamTransport::create(StreamTransportOptions options) {
  if (options.connect_timeout <= core::Duration::zero() ||
      options.idle_timeout <= core::Duration::zero() ||
      options.read_buffer_bytes == 0 ||
      options.read_buffer_bytes > 1024U * 1024U ||
      options.maximum_queued_write_bytes == 0)
    return std::unexpected(core::Error{core::ErrorCode::InvalidArgument,
                                       "Invalid TCP stream transport limits"});
  auto remote = to_asio(options.remote);
  if (!remote)
    return std::unexpected(remote.error());
  auto impl = std::make_unique<Impl>();
  impl->options = std::move(options);
  impl->remote = *remote;
  impl->read_buffer.resize(impl->options.read_buffer_bytes);
  return std::unique_ptr<AsioTcpStreamTransport>{
      new AsioTcpStreamTransport{std::move(impl)}};
}

core::Result<std::shared_ptr<StreamTransport>>
AsioTcpStreamTransportFactory::create(StreamTransportOptions options) {
  auto transport = AsioTcpStreamTransport::create(std::move(options));
  if (!transport)
    return std::unexpected(transport.error());
  return std::shared_ptr<StreamTransport>{std::move(*transport)};
}

core::Result<void> AsioTcpStreamTransport::start(StreamReceiver &receiver) {
  std::lock_guard lock{impl_->start_mutex};
  if (impl_->started)
    return std::unexpected(core::Error{core::ErrorCode::Conflict,
                                       "TCP stream transport already started"});
  impl_->started = true;
  impl_->receiver = &receiver;
  impl_->active.store(true, std::memory_order_release);
  impl_->timer.expires_after(impl_->options.connect_timeout);
  impl_->timer.async_wait([impl = impl_.get()](const asio::error_code &error) {
    if (!error && impl->active.load(std::memory_order_acquire) &&
        !impl->connected.load(std::memory_order_acquire))
      impl->close(core::Error{core::ErrorCode::Timeout,
                              "TCP peer connection timed out"});
  });
  impl_->socket.async_connect(
      impl_->remote, [impl = impl_.get()](const asio::error_code &error) {
        if (!impl->active.load(std::memory_order_acquire))
          return;
        if (error) {
          impl->close(network_error("TCP peer connection failed", error));
          return;
        }
        impl->connected.store(true, std::memory_order_release);
        impl->arm_idle_timeout();
        impl->notify_connected();
        impl->read();
      });
  impl_->thread = std::thread{[impl = impl_.get()] { impl->context.run(); }};
  return {};
}

core::Result<void> AsioTcpStreamTransport::send(core::ByteBuffer bytes) {
  if (!impl_->active.load(std::memory_order_acquire) ||
      !impl_->connected.load(std::memory_order_acquire))
    return std::unexpected(core::Error{
        core::ErrorCode::Conflict, "TCP stream transport is not connected"});
  if (bytes.empty())
    return std::unexpected(core::Error{core::ErrorCode::InvalidArgument,
                                       "TCP stream write must not be empty"});
  auto queued = impl_->queued_bytes.load(std::memory_order_acquire);
  while (true) {
    if (bytes.size() >
        impl_->options.maximum_queued_write_bytes -
            std::min(queued, impl_->options.maximum_queued_write_bytes))
      return std::unexpected(
          core::Error{core::ErrorCode::QuotaExceeded,
                      "TCP stream queued-write limit would be exceeded"});
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

StreamEndpoint AsioTcpStreamTransport::remote_endpoint() const noexcept {
  return impl_->options.remote;
}

void AsioTcpStreamTransport::stop() noexcept {
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
  impl_->socket.close(ignored);
}

} // namespace sakuin::runtime

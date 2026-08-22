module;

#include <asio.hpp>

export module sakuin.runtime.asio_server;

import std;

import sakuin.core.bytes;
import sakuin.core.result;
import sakuin.core.time;
import sakuin.runtime.datagram;
import sakuin.runtime.stream;

export namespace sakuin::runtime {

// Standalone Asio TCP server behind Sakuin's owned-byte session contract. No
// Asio executor, completion token, awaitable, or error type crosses this API.
class AsioTcpStreamServer final : public StreamServer {
public:
  static core::Result<std::unique_ptr<AsioTcpStreamServer>>
  create(StreamServerOptions options = {});

  ~AsioTcpStreamServer() override;

  core::Result<void> start(StreamServerHandler &handler,
                           StreamServerEvents *events = nullptr) override;
  StreamEndpoint local_endpoint() const noexcept override;
  void stop() noexcept override;

private:
  struct Impl;
  explicit AsioTcpStreamServer(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> impl_;
};

} // namespace sakuin::runtime

namespace sakuin::runtime {
namespace {

using Tcp = asio::ip::tcp;

core::Error network_error(std::string action, const asio::error_code &error) {
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

core::Result<void> validate(const StreamServerOptions &options) {
  if (options.idle_timeout <= core::Duration::zero())
    return std::unexpected(
        core::Error{core::ErrorCode::InvalidArgument,
                    "TCP server idle timeout must be positive"});
  if (options.read_buffer_bytes == 0 ||
      options.read_buffer_bytes > 1024U * 1024U)
    return std::unexpected(
        core::Error{core::ErrorCode::InvalidArgument,
                    "TCP server read buffer must be between 1 byte and 1 MiB"});
  if (options.maximum_queued_write_bytes == 0 ||
      options.maximum_queued_write_bytes > 64U * 1024U * 1024U)
    return std::unexpected(core::Error{
        core::ErrorCode::InvalidArgument,
        "TCP server queued-write limit must be between 1 byte and 64 MiB"});
  if (options.maximum_connections == 0 ||
      options.maximum_connections > 1'000'000)
    return std::unexpected(core::Error{
        core::ErrorCode::InvalidArgument,
        "TCP server connection limit must be between 1 and 1000000"});
  return {};
}

} // namespace

class AsioTcpStreamSession final
    : public StreamServerSession,
      public std::enable_shared_from_this<AsioTcpStreamSession> {
public:
  AsioTcpStreamSession(StreamSessionId id, Tcp::socket socket,
                       const StreamServerOptions &options,
                       StreamServerHandler *handler,
                       std::function<void(StreamSessionId)> on_done)
      : id_(id), socket_(std::move(socket)), timer_(socket_.get_executor()),
        remote_(remote_endpoint(socket_)),
        read_buffer_(options.read_buffer_bytes),
        idle_timeout_(options.idle_timeout),
        maximum_queued_write_bytes_(options.maximum_queued_write_bytes),
        handler_(handler), on_done_(std::move(on_done)) {}

  StreamSessionId id() const noexcept override { return id_; }
  StreamEndpoint remote_endpoint() const noexcept override { return remote_; }

  core::Result<void> send(core::ByteBuffer bytes) override {
    if (!active_.load(std::memory_order_acquire))
      return std::unexpected(core::Error{core::ErrorCode::Conflict,
                                         "TCP server session is closed"});
    if (bytes.empty())
      return std::unexpected(core::Error{core::ErrorCode::InvalidArgument,
                                         "TCP stream write must not be empty"});
    auto queued = queued_bytes_.load(std::memory_order_acquire);
    while (true) {
      const auto available = maximum_queued_write_bytes_ -
                             std::min(queued, maximum_queued_write_bytes_);
      if (bytes.size() > available)
        return std::unexpected(core::Error{
            core::ErrorCode::QuotaExceeded,
            "TCP server session queued-write limit would be exceeded"});
      if (queued_bytes_.compare_exchange_weak(queued, queued + bytes.size(),
                                              std::memory_order_acq_rel))
        break;
    }
    auto owned = std::make_shared<core::ByteBuffer>(std::move(bytes));
    auto self = shared_from_this();
    asio::post(socket_.get_executor(), [self, owned] {
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
      asio::post(socket_.get_executor(),
                 [self] { self->close_now(std::nullopt); });
    } catch (...) {
      // close_now() is also called by server shutdown and destruction.
    }
  }

  void start() {
    active_.store(true, std::memory_order_release);
    try {
      handler_->on_stream_session_opened(*this);
    } catch (const std::exception &exception) {
      close_now(core::Error{core::ErrorCode::Internal,
                            std::string{"TCP session handler threw: "} +
                                exception.what()});
      return;
    } catch (...) {
      close_now(core::Error{core::ErrorCode::Internal,
                            "TCP session handler threw an unknown exception"});
      return;
    }
    if (!active_.load(std::memory_order_acquire))
      return;
    arm_idle_timeout();
    read();
  }

  void close_now(std::optional<core::Error> error) noexcept {
    if (!active_.exchange(false, std::memory_order_acq_rel))
      return;
    asio::error_code ignored;
    try {
      timer_.cancel();
    } catch (...) {
    }
    socket_.shutdown(Tcp::socket::shutdown_both, ignored);
    socket_.close(ignored);
    writes_.clear();
    queued_bytes_.store(0, std::memory_order_release);
    try {
      handler_->on_stream_session_closed(id_, std::move(error));
    } catch (...) {
      // User callbacks may not unwind through the runtime thread.
    }
    if (on_done_)
      on_done_(id_);
  }

private:
  static StreamEndpoint remote_endpoint(Tcp::socket &socket) noexcept {
    asio::error_code error;
    const auto endpoint = socket.remote_endpoint(error);
    return error ? StreamEndpoint{} : from_asio(endpoint);
  }

  void arm_idle_timeout() {
    timer_.expires_after(idle_timeout_);
    auto self = shared_from_this();
    timer_.async_wait([self](const asio::error_code &error) {
      if (!error && self->active_.load(std::memory_order_acquire))
        self->close_now(core::Error{core::ErrorCode::Timeout,
                                    "TCP server session was idle too long"});
    });
  }

  void read() {
    if (!active_.load(std::memory_order_acquire))
      return;
    auto self = shared_from_this();
    socket_.async_read_some(
        asio::buffer(read_buffer_),
        [self](const asio::error_code &error, std::size_t transferred) {
          if (!self->active_.load(std::memory_order_acquire))
            return;
          if (error) {
            if (error == asio::error::eof ||
                error == asio::error::connection_reset)
              self->close_now(std::nullopt);
            else if (error != asio::error::operation_aborted)
              self->close_now(network_error("TCP server read failed", error));
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
                std::string{"TCP session handler threw: "} + exception.what()});
            return;
          } catch (...) {
            self->close_now(
                core::Error{core::ErrorCode::Internal,
                            "TCP session handler threw an unknown exception"});
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
        socket_, asio::buffer(*bytes),
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
              self->close_now(network_error("TCP server write failed", error));
            return;
          }
          self->arm_idle_timeout();
          self->write_next();
        });
  }

  StreamSessionId id_{};
  Tcp::socket socket_;
  asio::steady_timer timer_;
  StreamEndpoint remote_;
  std::vector<std::byte> read_buffer_;
  core::Duration idle_timeout_;
  std::size_t maximum_queued_write_bytes_{};
  StreamServerHandler *handler_{};
  std::function<void(StreamSessionId)> on_done_;
  std::deque<std::shared_ptr<core::ByteBuffer>> writes_;
  std::atomic<bool> active_{};
  std::atomic<std::size_t> queued_bytes_{};
  bool writing_{};
};

struct AsioTcpStreamServer::Impl {
  explicit Impl(StreamServerOptions configured)
      : options(std::move(configured)), acceptor(context) {}

  void report(core::Error error) noexcept {
    if (!events)
      return;
    try {
      events->on_stream_server_error(std::move(error));
    } catch (...) {
      // User callbacks may not unwind through the runtime thread.
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
          auto session = std::make_shared<AsioTcpStreamSession>(
              id, std::move(socket), options, handler,
              [this](StreamSessionId finished) { sessions.erase(finished); });
          sessions.emplace(id, session);
          session->start();
        }
      } else if (error != asio::error::operation_aborted) {
        report(network_error("TCP server accept failed", error));
      }
      accept();
    });
  }

  void shutdown() noexcept {
    asio::error_code ignored;
    acceptor.cancel(ignored);
    acceptor.close(ignored);
    std::vector<std::shared_ptr<AsioTcpStreamSession>> closing;
    closing.reserve(sessions.size());
    for (const auto &[_, session] : sessions)
      closing.push_back(session);
    for (const auto &session : closing)
      session->close_now(std::nullopt);
    sessions.clear();
  }

  StreamServerOptions options;
  asio::io_context context;
  Tcp::acceptor acceptor;
  StreamEndpoint local;
  StreamServerHandler *handler{};
  StreamServerEvents *events{};
  std::map<StreamSessionId, std::shared_ptr<AsioTcpStreamSession>> sessions;
  std::thread thread;
  std::atomic<bool> active{};
  StreamSessionId next_session{};
  bool started{};
  std::mutex mutex;
};

AsioTcpStreamServer::AsioTcpStreamServer(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

AsioTcpStreamServer::~AsioTcpStreamServer() { stop(); }

core::Result<std::unique_ptr<AsioTcpStreamServer>>
AsioTcpStreamServer::create(StreamServerOptions options) {
  if (auto valid = validate(options); !valid)
    return std::unexpected(valid.error());
  auto endpoint = to_asio(options.bind_to);
  if (!endpoint)
    return std::unexpected(endpoint.error());
  auto impl = std::make_unique<Impl>(std::move(options));
  asio::error_code error;
  impl->acceptor.open(endpoint->protocol(), error);
  if (error)
    return std::unexpected(
        network_error("Could not open TCP server socket", error));
  impl->acceptor.set_option(Tcp::acceptor::reuse_address(true), error);
  if (error)
    return std::unexpected(
        network_error("Could not configure TCP server socket", error));
  impl->acceptor.bind(*endpoint, error);
  if (error)
    return std::unexpected(
        network_error("Could not bind TCP server socket", error));
  impl->acceptor.listen(asio::socket_base::max_listen_connections, error);
  if (error)
    return std::unexpected(
        network_error("Could not listen on TCP server socket", error));
  const auto local = impl->acceptor.local_endpoint(error);
  if (error)
    return std::unexpected(
        network_error("Could not inspect TCP server endpoint", error));
  impl->local = from_asio(local);
  return std::unique_ptr<AsioTcpStreamServer>{
      new AsioTcpStreamServer{std::move(impl)}};
}

core::Result<void> AsioTcpStreamServer::start(StreamServerHandler &handler,
                                              StreamServerEvents *events) {
  std::lock_guard lock{impl_->mutex};
  if (impl_->started)
    return std::unexpected(core::Error{core::ErrorCode::Conflict,
                                       "TCP stream server already started"});
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
                    std::string{"Could not start TCP server runtime thread: "} +
                        exception.what()});
  }
  return {};
}

StreamEndpoint AsioTcpStreamServer::local_endpoint() const noexcept {
  return impl_->local;
}

void AsioTcpStreamServer::stop() noexcept {
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

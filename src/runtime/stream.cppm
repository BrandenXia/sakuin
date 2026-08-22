export module sakuin.runtime.stream;

import std;

import sakuin.core.bytes;
import sakuin.core.result;
import sakuin.core.time;
import sakuin.runtime.datagram;

export namespace sakuin::runtime {

struct StreamEndpoint {
  IpAddress address;
  std::uint16_t port{};

  friend bool operator==(const StreamEndpoint &,
                         const StreamEndpoint &) = default;
};

struct StreamTransportOptions {
  StreamEndpoint remote;
  core::Duration connect_timeout{std::chrono::seconds{10}};
  core::Duration idle_timeout{std::chrono::seconds{30}};
  std::size_t read_buffer_bytes{16U * 1024U};
  std::size_t maximum_queued_write_bytes{1024U * 1024U};
};

class StreamReceiver {
public:
  virtual ~StreamReceiver() = default;

  // Callbacks are serialized on a runtime-owned execution thread. The
  // receiver must outlive the transport until stop() returns.
  virtual void on_stream_connected() = 0;
  virtual void on_stream_data(core::ByteBuffer bytes) = 0;
  virtual void on_stream_closed(std::optional<core::Error> error) = 0;
};

class StreamTransport {
public:
  virtual ~StreamTransport() = default;

  StreamTransport(const StreamTransport &) = delete;
  StreamTransport &operator=(const StreamTransport &) = delete;

  virtual core::Result<void> start(StreamReceiver &receiver) = 0;
  virtual core::Result<void> send(core::ByteBuffer bytes) = 0;
  virtual StreamEndpoint remote_endpoint() const noexcept = 0;
  virtual void stop() noexcept = 0;

  // This contract exposes owned byte chunks and lifecycle events rather than
  // Asio tokens/awaitables. A future sender/receiver adapter can add scheduler
  // affinity and cancellation propagation without changing peer protocols.

protected:
  StreamTransport() = default;
};

class StreamTransportFactory {
public:
  virtual ~StreamTransportFactory() = default;
  virtual core::Result<std::shared_ptr<StreamTransport>>
  create(StreamTransportOptions options) = 0;

protected:
  StreamTransportFactory() = default;
};

using StreamSessionId = std::uint64_t;

struct StreamServerOptions {
  StreamEndpoint bind_to{};
  core::Duration idle_timeout{std::chrono::seconds{30}};
  std::size_t read_buffer_bytes{16U * 1024U};
  std::size_t maximum_queued_write_bytes{1024U * 1024U};
  std::size_t maximum_connections{256};
};

class StreamServerSession {
public:
  virtual ~StreamServerSession() = default;

  StreamServerSession(const StreamServerSession &) = delete;
  StreamServerSession &operator=(const StreamServerSession &) = delete;

  virtual StreamSessionId id() const noexcept = 0;
  virtual StreamEndpoint remote_endpoint() const noexcept = 0;
  virtual core::Result<void> send(core::ByteBuffer bytes) = 0;
  virtual void close() noexcept = 0;

protected:
  StreamServerSession() = default;
};

class StreamServerHandler {
public:
  virtual ~StreamServerHandler() = default;

  // Callbacks for all sessions are serialized on one runtime-owned execution
  // thread. Session references remain valid until their closed callback
  // returns.
  virtual void on_stream_session_opened(StreamServerSession &session) = 0;
  virtual void on_stream_session_data(StreamServerSession &session,
                                      core::ByteBuffer bytes) = 0;
  virtual void on_stream_session_closed(StreamSessionId session,
                                        std::optional<core::Error> error) = 0;
};

class StreamServerEvents {
public:
  virtual ~StreamServerEvents() = default;
  virtual void on_stream_server_error(core::Error error) = 0;
};

class StreamServer {
public:
  virtual ~StreamServer() = default;

  StreamServer(const StreamServer &) = delete;
  StreamServer &operator=(const StreamServer &) = delete;

  // The handler and optional event sink must outlive the server until stop()
  // returns. This boundary deliberately exposes no Asio execution types.
  virtual core::Result<void> start(StreamServerHandler &handler,
                                   StreamServerEvents *events = nullptr) = 0;
  virtual StreamEndpoint local_endpoint() const noexcept = 0;
  virtual void stop() noexcept = 0;

protected:
  StreamServer() = default;
};

} // namespace sakuin::runtime

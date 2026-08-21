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

} // namespace sakuin::runtime

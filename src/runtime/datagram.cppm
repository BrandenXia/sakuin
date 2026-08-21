export module sakuin.runtime.datagram;

import std;

import sakuin.core.bytes;
import sakuin.core.result;

export namespace sakuin::runtime {

enum class AddressFamily : std::uint8_t { IPv4, IPv6 };

struct IpAddress {
  AddressFamily family{AddressFamily::IPv4};
  std::array<std::uint8_t, 16> bytes{};

  static IpAddress any_v4() noexcept { return {}; }
  static IpAddress loopback_v4() noexcept {
    IpAddress result;
    result.bytes[0] = 127;
    result.bytes[3] = 1;
    return result;
  }

  friend bool operator==(const IpAddress &, const IpAddress &) = default;
};

struct DatagramEndpoint {
  IpAddress address;
  std::uint16_t port{};

  friend bool operator==(const DatagramEndpoint &,
                         const DatagramEndpoint &) = default;
};

struct Datagram {
  DatagramEndpoint source;
  core::ByteBuffer payload;
};

struct DatagramTransportOptions {
  DatagramEndpoint bind_to{};
  std::size_t maximum_datagram_size{65'507};
};

class DatagramReceiver {
public:
  virtual ~DatagramReceiver() = default;

  // Invoked on a runtime-owned execution thread. The receiver must outlive the
  // transport until stop() returns.
  virtual void on_datagram(Datagram datagram) = 0;
  virtual void on_transport_error(core::Error error) = 0;
};

class DatagramTransport {
public:
  virtual ~DatagramTransport() = default;

  DatagramTransport(const DatagramTransport &) = delete;
  DatagramTransport &operator=(const DatagramTransport &) = delete;

  virtual core::Result<void> start(DatagramReceiver &receiver) = 0;

  // Ownership of the payload transfers to the transport, so asynchronous
  // implementations never depend on caller buffer lifetime.
  virtual core::Result<void> send(DatagramEndpoint destination,
                                  core::ByteBuffer payload) = 0;

  virtual DatagramEndpoint local_endpoint() const noexcept = 0;
  virtual void stop() noexcept = 0;

  // This deliberately models transport operations rather than an execution
  // framework. A future stdexec sender/receiver adapter can layer cancellation
  // and scheduler affinity over it without changing DHT protocol APIs.

protected:
  DatagramTransport() = default;
};

} // namespace sakuin::runtime

module;

#include <asio.hpp>

export module sakuin.runtime.asio;

import std;

import sakuin.core.bytes;
import sakuin.core.result;
import sakuin.runtime.datagram;

export namespace sakuin::runtime {

// Standalone Asio implementation of the Sakuin datagram contract. No Asio
// executor, token, awaitable, or error type crosses this module boundary.
class AsioDatagramTransport final : public DatagramTransport {
public:
  static core::Result<std::unique_ptr<AsioDatagramTransport>>
  create(const DatagramTransportOptions &options = {});

  ~AsioDatagramTransport() override;

  core::Result<void> start(DatagramReceiver &receiver) override;
  core::Result<void> send(DatagramEndpoint destination,
                          core::ByteBuffer payload) override;
  DatagramEndpoint local_endpoint() const noexcept override;
  void stop() noexcept override;

private:
  struct Impl;
  explicit AsioDatagramTransport(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> impl_;
};

} // namespace sakuin::runtime

namespace sakuin::runtime {
namespace {

core::Error network_error(std::string action, const asio::error_code &error) {
  return {core::ErrorCode::IoError, std::move(action) + ": " + error.message()};
}

core::Result<asio::ip::udp::endpoint> to_asio(DatagramEndpoint endpoint) {
  if (endpoint.address.family == AddressFamily::IPv4) {
    asio::ip::address_v4::bytes_type bytes{};
    std::copy_n(endpoint.address.bytes.begin(), bytes.size(), bytes.begin());
    return asio::ip::udp::endpoint{asio::ip::address_v4{bytes}, endpoint.port};
  }
  asio::ip::address_v6::bytes_type bytes{};
  std::copy(endpoint.address.bytes.begin(), endpoint.address.bytes.end(),
            bytes.begin());
  return asio::ip::udp::endpoint{asio::ip::address_v6{bytes}, endpoint.port};
}

DatagramEndpoint from_asio(const asio::ip::udp::endpoint &endpoint) {
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

} // namespace

struct AsioDatagramTransport::Impl {
  asio::io_context context;
  asio::ip::udp::socket socket{context};
  DatagramEndpoint local;
  std::vector<std::byte> receive_buffer;
  std::size_t maximum_datagram_size{};
  asio::ip::udp::endpoint sender;
  DatagramReceiver *receiver{};
  std::thread thread;
  std::atomic<bool> active{};
  bool started{};
  mutable std::mutex mutex;

  void report(core::Error error) noexcept {
    if (!receiver)
      return;
    try {
      receiver->on_transport_error(std::move(error));
    } catch (...) {
      // User callbacks may not unwind through the runtime thread.
    }
  }

  void report_delivery_failure(DatagramDeliveryFailure failure) noexcept {
    if (!receiver)
      return;
    try {
      receiver->on_datagram_delivery_failure(std::move(failure));
    } catch (...) {
      // User callbacks may not unwind through the runtime thread.
    }
  }

  void receive() {
    if (!active.load(std::memory_order_acquire))
      return;
    socket.async_receive_from(
        asio::buffer(receive_buffer), sender,
        [this](const asio::error_code &error, std::size_t transferred) {
          if (!error) {
            Datagram datagram{
                .source = from_asio(sender),
                .payload = {receive_buffer.begin(),
                            receive_buffer.begin() + transferred}};
            try {
              receiver->on_datagram(std::move(datagram));
            } catch (const std::exception &exception) {
              report({core::ErrorCode::Internal,
                      std::string{"Datagram receiver threw: "} +
                          exception.what()});
            } catch (...) {
              report({core::ErrorCode::Internal,
                      "Datagram receiver threw an unknown exception"});
            }
          } else if (error != asio::error::operation_aborted) {
            report(network_error("UDP receive failed", error));
          }
          receive();
        });
  }
};

AsioDatagramTransport::AsioDatagramTransport(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

AsioDatagramTransport::~AsioDatagramTransport() { stop(); }

core::Result<std::unique_ptr<AsioDatagramTransport>>
AsioDatagramTransport::create(const DatagramTransportOptions &options) {
  if (options.maximum_datagram_size == 0 ||
      options.maximum_datagram_size > 65'507)
    return std::unexpected(core::Error{
        core::ErrorCode::InvalidArgument,
        "UDP maximum datagram size must be between 1 and 65507 bytes"});
  auto endpoint = to_asio(options.bind_to);
  if (!endpoint)
    return std::unexpected(endpoint.error());
  auto impl = std::make_unique<Impl>();
  impl->receive_buffer.resize(options.maximum_datagram_size);
  impl->maximum_datagram_size = options.maximum_datagram_size;
  asio::error_code error;
  impl->socket.open(endpoint->protocol(), error);
  if (error)
    return std::unexpected(network_error("Could not open UDP socket", error));
  if (endpoint->protocol() == asio::ip::udp::v6()) {
    impl->socket.set_option(asio::ip::v6_only{true}, error);
    if (error)
      return std::unexpected(
          network_error("Could not configure IPv6-only UDP socket", error));
  }
  impl->socket.bind(*endpoint, error);
  if (error)
    return std::unexpected(network_error("Could not bind UDP socket", error));
  const auto local = impl->socket.local_endpoint(error);
  if (error)
    return std::unexpected(
        network_error("Could not inspect UDP endpoint", error));
  impl->local = from_asio(local);
  return std::unique_ptr<AsioDatagramTransport>{
      new AsioDatagramTransport{std::move(impl)}};
}

core::Result<void> AsioDatagramTransport::start(DatagramReceiver &receiver) {
  std::lock_guard lock{impl_->mutex};
  if (impl_->started)
    return std::unexpected(core::Error{core::ErrorCode::Conflict,
                                       "Datagram transport already started"});
  impl_->started = true;
  impl_->receiver = &receiver;
  impl_->active.store(true, std::memory_order_release);
  impl_->receive();
  impl_->thread = std::thread{[impl = impl_.get()] { impl->context.run(); }};
  return {};
}

core::Result<void> AsioDatagramTransport::send(DatagramEndpoint destination,
                                               core::ByteBuffer payload) {
  if (!impl_->active.load(std::memory_order_acquire))
    return std::unexpected(core::Error{core::ErrorCode::Conflict,
                                       "Datagram transport is not running"});
  if (payload.size() > impl_->maximum_datagram_size)
    return std::unexpected(core::Error{core::ErrorCode::InvalidArgument,
                                       "UDP payload exceeds transport limit"});
  auto endpoint = to_asio(destination);
  if (!endpoint)
    return std::unexpected(endpoint.error());
  auto owned = std::make_shared<core::ByteBuffer>(std::move(payload));
  asio::post(impl_->context, [impl = impl_.get(), destination,
                              endpoint = *endpoint, owned] {
    if (!impl->active.load(std::memory_order_acquire))
      return;
    impl->socket.async_send_to(
        asio::buffer(*owned), endpoint,
        [impl, destination, owned](const asio::error_code &error, std::size_t) {
          if (error && error != asio::error::operation_aborted)
            impl->report_delivery_failure(
                {.destination = destination,
                 .error = network_error("UDP send failed", error)});
        });
  });
  return {};
}

DatagramEndpoint AsioDatagramTransport::local_endpoint() const noexcept {
  return impl_->local;
}

void AsioDatagramTransport::stop() noexcept {
  if (!impl_)
    return;
  if (impl_->active.exchange(false, std::memory_order_acq_rel))
    impl_->context.stop();
  if (impl_->thread.joinable()) {
    if (impl_->thread.get_id() == std::this_thread::get_id())
      return;
    impl_->thread.join();
  }
  asio::error_code ignored;
  impl_->socket.close(ignored);
}

} // namespace sakuin::runtime

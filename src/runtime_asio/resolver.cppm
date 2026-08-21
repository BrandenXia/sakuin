module;

#include <asio.hpp>

export module sakuin.runtime.asio_resolver;

import std;

import sakuin.core.result;
import sakuin.runtime.datagram;

export namespace sakuin::runtime {

// Synchronous startup-time DNS resolution behind the Sakuin endpoint
// contract. No Asio resolver, executor, or error type crosses the boundary.
class AsioDatagramEndpointResolver final : public DatagramEndpointResolver {
public:
  core::Result<std::vector<DatagramEndpoint>>
  resolve(std::string_view host, std::uint16_t port,
          AddressFamily address_family) override;
};

} // namespace sakuin::runtime

namespace sakuin::runtime {
namespace {

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

core::Result<std::vector<DatagramEndpoint>>
AsioDatagramEndpointResolver::resolve(std::string_view host, std::uint16_t port,
                                      AddressFamily address_family) {
  if (host.empty() || port == 0)
    return std::unexpected(core::Error{
        core::ErrorCode::InvalidArgument,
        "Datagram endpoint resolution requires a host and nonzero port"});
  asio::io_context context;
  asio::ip::udp::resolver resolver{context};
  asio::error_code error;
  const auto protocol = address_family == AddressFamily::IPv4
                            ? asio::ip::udp::v4()
                            : asio::ip::udp::v6();
  const auto resolved =
      resolver.resolve(protocol, std::string{host}, std::to_string(port),
                       asio::ip::resolver_base::numeric_service, error);
  if (error)
    return std::unexpected(
        core::Error{core::ErrorCode::IoError, "Could not resolve UDP host " +
                                                  std::string{host} + ": " +
                                                  error.message()});

  std::vector<DatagramEndpoint> result;
  for (const auto &entry : resolved) {
    auto endpoint = from_asio(entry.endpoint());
    if (endpoint.address.family == address_family &&
        std::ranges::find(result, endpoint) == result.end())
      result.push_back(endpoint);
  }
  if (result.empty())
    return std::unexpected(core::Error{
        core::ErrorCode::NotFound,
        "UDP host did not resolve for the requested address family"});
  return result;
}

} // namespace sakuin::runtime

#include <asio.hpp>

import std;

import sakuin.api;
import sakuin.core;
import sakuin.runtime.asio_http;
import sakuin.runtime.datagram;
import sakuin.runtime.http;

namespace {

class Handler final : public sakuin::api::HttpHandler {
public:
  sakuin::core::Result<sakuin::api::HttpResponse>
  handle(sakuin::api::HttpRequest request) override {
    if (request.target != "/health")
      return sakuin::api::HttpResponse{.status = 404};
    constexpr std::string_view body{"healthy"};
    const auto bytes = std::as_bytes(std::span{body});
    return sakuin::api::HttpResponse{
        .status = 200,
        .headers = {{"content-type", "text/plain"}},
        .body = {bytes.begin(), bytes.end()}};
  }
};

std::string request(std::uint16_t port, std::string_view wire) {
  asio::io_context context;
  asio::ip::tcp::socket socket{context};
  socket.connect({asio::ip::address_v4::loopback(), port});
  asio::write(socket, asio::buffer(wire));
  std::string response;
  std::array<char, 1024> buffer{};
  asio::error_code error;
  while (true) {
    const auto count = socket.read_some(asio::buffer(buffer), error);
    response.append(buffer.data(), count);
    if (error == asio::error::eof)
      break;
    if (error)
      return {};
  }
  return response;
}

} // namespace

int main() {
  using namespace sakuin;
  runtime::HttpServerOptions options{
      .bind_to = {.address = runtime::IpAddress::loopback_v4(), .port = 0},
      .request_timeout = std::chrono::milliseconds{500}};
  auto server = runtime::AsioHttpServer::create(options);
  if (!server || (*server)->local_endpoint().port == 0)
    return 1;
  Handler handler;
  if (!(*server)->start(handler) || (*server)->start(handler))
    return 2;

  const auto valid = request(
      (*server)->local_endpoint().port,
      "GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
  if (!valid.starts_with("HTTP/1.1 200 OK\r\n") ||
      !valid.contains("content-length: 7\r\n") ||
      !valid.ends_with("\r\n\r\nhealthy"))
    return 3;

  const auto malformed =
      request((*server)->local_endpoint().port,
              "GET / HTTP/1.1\r\nHost: one\r\nHost: two\r\n\r\n");
  if (!malformed.starts_with("HTTP/1.1 400 Bad Request\r\n"))
    return 4;

  (*server)->stop();
  return 0;
}

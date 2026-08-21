export module sakuin.runtime.http;

import std;

import sakuin.api.http;
import sakuin.core.result;
import sakuin.http.llhttp;
import sakuin.runtime.datagram;

export namespace sakuin::runtime {

struct HttpTlsOptions {
  std::filesystem::path certificate_chain_file;
  std::filesystem::path private_key_file;
};

struct HttpServerOptions {
  DatagramEndpoint bind_to{};
  std::size_t maximum_connections{256};
  std::size_t read_buffer_bytes{16 * 1024};
  std::chrono::milliseconds request_timeout{15'000};
  http::HttpParserLimits parser_limits{};
  std::optional<HttpTlsOptions> tls;
};

class HttpServerEvents {
public:
  virtual ~HttpServerEvents() = default;
  virtual void on_http_server_error(core::Error error) = 0;
};

class HttpServer {
public:
  virtual ~HttpServer() = default;

  HttpServer(const HttpServer &) = delete;
  HttpServer &operator=(const HttpServer &) = delete;

  // The handler and optional events sink must outlive the server until stop()
  // returns. Handler calls run on a runtime-owned execution thread.
  virtual core::Result<void> start(api::HttpHandler &handler,
                                   HttpServerEvents *events = nullptr) = 0;
  virtual DatagramEndpoint local_endpoint() const noexcept = 0;
  virtual void stop() noexcept = 0;

protected:
  HttpServer() = default;
};

} // namespace sakuin::runtime

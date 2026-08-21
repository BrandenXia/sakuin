export module sakuin.api.http;

import std;

import sakuin.core.bytes;
import sakuin.core.result;

export namespace sakuin::api {

enum class HttpMethod { Get, Head, Post, Put, Delete, Options, Unknown };

struct HttpRequest {
  HttpMethod method{HttpMethod::Unknown};
  std::string target;
  std::map<std::string, std::string, std::less<>> headers;
  core::ByteBuffer body;
  unsigned version_major{1};
  unsigned version_minor{1};
};

struct HttpResponse {
  unsigned status{200};
  std::map<std::string, std::string, std::less<>> headers;
  core::ByteBuffer body;
};

class HttpHandler {
public:
  virtual ~HttpHandler() = default;
  virtual core::Result<HttpResponse> handle(HttpRequest request) = 0;
};

} // namespace sakuin::api

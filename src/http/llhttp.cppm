module;

#include <llhttp.h>

export module sakuin.http.llhttp;

import std;

import sakuin.api.http;
import sakuin.core.bytes;
import sakuin.core.result;

export namespace sakuin::http {

struct HttpParserLimits {
  std::size_t maximum_target_bytes{8 * 1024};
  std::size_t maximum_header_bytes{32 * 1024};
  std::size_t maximum_header_count{100};
  std::size_t maximum_body_bytes{1024 * 1024};
};

class HttpRequestParser {
public:
  explicit HttpRequestParser(HttpParserLimits limits = {});
  ~HttpRequestParser();

  HttpRequestParser(HttpRequestParser &&) noexcept;
  HttpRequestParser &operator=(HttpRequestParser &&) noexcept;
  HttpRequestParser(const HttpRequestParser &) = delete;
  HttpRequestParser &operator=(const HttpRequestParser &) = delete;

  core::Result<std::optional<api::HttpRequest>> feed(core::ByteView bytes);
  [[nodiscard]] bool complete() const noexcept;

private:
  struct State;
  std::unique_ptr<State> state_;
};

core::Result<core::ByteBuffer> serialize_response(api::HttpResponse response);

} // namespace sakuin::http

namespace sakuin::http {
namespace {

core::Error parse_error(std::string message) {
  return {core::ErrorCode::InvalidArgument, std::move(message)};
}

api::HttpMethod method(std::uint8_t value) {
  switch (static_cast<llhttp_method_t>(value)) {
  case HTTP_GET:
    return api::HttpMethod::Get;
  case HTTP_HEAD:
    return api::HttpMethod::Head;
  case HTTP_POST:
    return api::HttpMethod::Post;
  case HTTP_PUT:
    return api::HttpMethod::Put;
  case HTTP_DELETE:
    return api::HttpMethod::Delete;
  case HTTP_OPTIONS:
    return api::HttpMethod::Options;
  default:
    return api::HttpMethod::Unknown;
  }
}

bool token_character(unsigned char value) {
  return std::isalnum(value) || value == '!' || value == '#' || value == '$' ||
         value == '%' || value == '&' || value == '\'' || value == '*' ||
         value == '+' || value == '-' || value == '.' || value == '^' ||
         value == '_' || value == '`' || value == '|' || value == '~';
}

bool valid_header_name(std::string_view value) {
  return !value.empty() && std::ranges::all_of(value, [](unsigned char byte) {
    return token_character(byte);
  });
}

bool valid_header_value(std::string_view value) {
  return std::ranges::all_of(value, [](unsigned char byte) {
    return byte == '\t' || (byte >= 0x20 && byte != 0x7f);
  });
}

std::string lowercase(std::string_view value) {
  std::string result{value};
  std::ranges::transform(result, result.begin(), [](unsigned char byte) {
    return static_cast<char>(std::tolower(byte));
  });
  return result;
}

std::string_view reason_phrase(unsigned status) {
  switch (status) {
  case 200:
    return "OK";
  case 201:
    return "Created";
  case 202:
    return "Accepted";
  case 204:
    return "No Content";
  case 400:
    return "Bad Request";
  case 401:
    return "Unauthorized";
  case 403:
    return "Forbidden";
  case 404:
    return "Not Found";
  case 405:
    return "Method Not Allowed";
  case 408:
    return "Request Timeout";
  case 413:
    return "Content Too Large";
  case 429:
    return "Too Many Requests";
  case 431:
    return "Request Header Fields Too Large";
  case 500:
    return "Internal Server Error";
  case 503:
    return "Service Unavailable";
  default:
    return "Status";
  }
}

} // namespace

struct HttpRequestParser::State {
  explicit State(HttpParserLimits configured_limits)
      : limits(configured_limits) {
    llhttp_settings_init(&settings);
    settings.on_url = [](llhttp_t *parser, const char *at,
                         std::size_t length) -> int {
      auto &self = *static_cast<State *>(parser->data);
      if (length > self.limits.maximum_target_bytes -
                       std::min(self.request.target.size(),
                                self.limits.maximum_target_bytes))
        return self.fail("HTTP request target exceeds its configured limit");
      self.request.target.append(at, length);
      return 0;
    };
    settings.on_header_field = [](llhttp_t *parser, const char *at,
                                  std::size_t length) -> int {
      auto &self = *static_cast<State *>(parser->data);
      if (!self.add_header_bytes(length))
        return HPE_USER;
      self.header_name.append(at, length);
      return 0;
    };
    settings.on_header_value = [](llhttp_t *parser, const char *at,
                                  std::size_t length) -> int {
      auto &self = *static_cast<State *>(parser->data);
      if (!self.add_header_bytes(length))
        return HPE_USER;
      self.header_value.append(at, length);
      return 0;
    };
    settings.on_header_value_complete = [](llhttp_t *parser) -> int {
      auto &self = *static_cast<State *>(parser->data);
      if (self.request.headers.size() >= self.limits.maximum_header_count)
        return self.fail("HTTP request contains too many headers");
      auto name = lowercase(self.header_name);
      if (!self.request.headers.emplace(std::move(name), self.header_value)
               .second)
        return self.fail("Duplicate HTTP request headers are not accepted");
      self.header_name.clear();
      self.header_value.clear();
      return 0;
    };
    settings.on_headers_complete = [](llhttp_t *parser) -> int {
      auto &self = *static_cast<State *>(parser->data);
      self.request.method = method(llhttp_get_method(parser));
      self.request.version_major = llhttp_get_http_major(parser);
      self.request.version_minor = llhttp_get_http_minor(parser);
      if (self.request.method == api::HttpMethod::Unknown)
        return self.fail("Unsupported HTTP method");
      if (self.request.version_major != 1 || self.request.version_minor > 1)
        return self.fail("Only HTTP/1.0 and HTTP/1.1 are supported");
      if (llhttp_get_upgrade(parser) != 0)
        return self.fail("HTTP protocol upgrades are not supported");
      if (self.request.version_minor == 1) {
        const auto host = self.request.headers.find("host");
        if (host == self.request.headers.end() || host->second.empty())
          return self.fail("HTTP/1.1 requests require a Host header");
      }
      if (self.request.headers.contains("expect"))
        return self.fail("HTTP Expect semantics are not supported");
      return 0;
    };
    settings.on_body = [](llhttp_t *parser, const char *at,
                          std::size_t length) -> int {
      auto &self = *static_cast<State *>(parser->data);
      if (length > self.limits.maximum_body_bytes -
                       std::min(self.request.body.size(),
                                self.limits.maximum_body_bytes))
        return self.fail("HTTP request body exceeds its configured limit");
      const auto bytes = std::as_bytes(std::span{at, length});
      self.request.body.insert(self.request.body.end(), bytes.begin(),
                               bytes.end());
      return 0;
    };
    settings.on_message_complete = [](llhttp_t *parser) -> int {
      auto &self = *static_cast<State *>(parser->data);
      self.is_complete = true;
      return HPE_PAUSED;
    };
    llhttp_init(&parser, HTTP_REQUEST, &settings);
    parser.data = this;
  }

  int fail(std::string message) {
    failure = parse_error(std::move(message));
    return HPE_USER;
  }

  bool add_header_bytes(std::size_t length) {
    if (length > limits.maximum_header_bytes -
                     std::min(header_bytes, limits.maximum_header_bytes)) {
      fail("HTTP request headers exceed their configured limit");
      return false;
    }
    header_bytes += length;
    return true;
  }

  HttpParserLimits limits;
  llhttp_settings_t settings{};
  llhttp_t parser{};
  api::HttpRequest request;
  std::string header_name;
  std::string header_value;
  std::size_t header_bytes{};
  std::optional<core::Error> failure;
  bool is_complete{};
  bool delivered{};
};

HttpRequestParser::HttpRequestParser(HttpParserLimits limits)
    : state_(std::make_unique<State>(limits)) {}

HttpRequestParser::~HttpRequestParser() = default;
HttpRequestParser::HttpRequestParser(HttpRequestParser &&) noexcept = default;
HttpRequestParser &
HttpRequestParser::operator=(HttpRequestParser &&) noexcept = default;

core::Result<std::optional<api::HttpRequest>>
HttpRequestParser::feed(core::ByteView bytes) {
  if (state_->delivered)
    return std::unexpected(
        parse_error("HTTP parser has already delivered a request"));
  if (bytes.empty())
    return std::optional<api::HttpRequest>{};
  const auto *data = reinterpret_cast<const char *>(bytes.data());
  const auto error = llhttp_execute(&state_->parser, data, bytes.size());
  if (state_->failure)
    return std::unexpected(*state_->failure);
  if (error != HPE_OK && error != HPE_PAUSED)
    return std::unexpected(parse_error(
        std::string{"Malformed HTTP request: "} + llhttp_errno_name(error) +
        " (" + llhttp_get_error_reason(&state_->parser) + ")"));
  if (!state_->is_complete)
    return std::optional<api::HttpRequest>{};
  const auto *position = llhttp_get_error_pos(&state_->parser);
  if (position != nullptr && position != data + bytes.size())
    return std::unexpected(
        parse_error("Trailing bytes after an HTTP request are not accepted"));
  state_->delivered = true;
  return std::optional<api::HttpRequest>{std::move(state_->request)};
}

bool HttpRequestParser::complete() const noexcept {
  return state_->is_complete;
}

core::Result<core::ByteBuffer> serialize_response(api::HttpResponse response) {
  if (response.status < 100 || response.status > 599)
    return std::unexpected(parse_error("HTTP response status is out of range"));
  std::string output = "HTTP/1.1 " + std::to_string(response.status) + " " +
                       std::string{reason_phrase(response.status)} + "\r\n";
  for (const auto &[name, value] : response.headers) {
    if (!valid_header_name(name) || !valid_header_value(value))
      return std::unexpected(
          parse_error("HTTP response contains an invalid header"));
    const auto normalized = lowercase(name);
    if (normalized == "content-length" || normalized == "connection")
      return std::unexpected(parse_error(
          "HTTP response framing headers are owned by the transport"));
    output.append(name).append(": ").append(value).append("\r\n");
  }
  output.append("content-length: ")
      .append(std::to_string(response.body.size()))
      .append("\r\nconnection: close\r\n\r\n");
  const auto header = std::as_bytes(std::span{output});
  core::ByteBuffer encoded;
  encoded.reserve(header.size() + response.body.size());
  encoded.insert(encoded.end(), header.begin(), header.end());
  encoded.insert(encoded.end(), response.body.begin(), response.body.end());
  return encoded;
}

} // namespace sakuin::http

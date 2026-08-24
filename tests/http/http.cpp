import std;

import sakuin.api;
import sakuin.core;
import sakuin.http;

namespace {

void require(bool condition) {
  if (!condition)
    std::terminate();
}

sakuin::core::ByteView bytes(std::string_view text) {
  return std::as_bytes(std::span{text});
}

std::string text(sakuin::core::ByteView bytes) {
  return {reinterpret_cast<const char *>(bytes.data()), bytes.size()};
}

void parses_fragmented_request() {
  sakuin::http::HttpRequestParser parser;
  const auto first = parser.feed(bytes("GET /v1/se"));
  require(first && !*first);
  const auto second =
      parser.feed(bytes("arch?q=linux HTTP/1.1\r\nHost: "
                        "localhost\r\nAuthorization: token\r\n\r\n"));
  require(second && *second);
  const auto &request = **second;
  require(request.method == sakuin::api::HttpMethod::Get);
  require(request.target == "/v1/search?q=linux");
  require(request.headers.at("host") == "localhost");
  require(request.headers.at("authorization") == "token");
  require(parser.complete());
}

void parses_bounded_body() {
  sakuin::http::HttpRequestParser parser({.maximum_target_bytes = 64,
                                          .maximum_header_bytes = 128,
                                          .maximum_header_count = 8,
                                          .maximum_body_bytes = 4});
  const auto result = parser.feed(bytes(
      "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 4\r\n\r\ndata"));
  require(result && *result);
  require(text((**result).body) == "data");

  sakuin::http::HttpRequestParser too_large({.maximum_target_bytes = 64,
                                             .maximum_header_bytes = 128,
                                             .maximum_header_count = 8,
                                             .maximum_body_bytes = 3});
  require(!too_large.feed(bytes(
      "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 4\r\n\r\ndata")));
}

void rejects_ambiguous_or_trailing_requests() {
  sakuin::http::HttpRequestParser duplicate;
  require(!duplicate.feed(
      bytes("GET / HTTP/1.1\r\nHost: first\r\nHost: second\r\n\r\n")));

  sakuin::http::HttpRequestParser trailing;
  require(!trailing.feed(bytes(
      "GET / HTTP/1.1\r\nHost: localhost\r\n\r\nGET /two HTTP/1.1\r\n\r\n")));

  sakuin::http::HttpRequestParser missing_host;
  require(!missing_host.feed(bytes("GET / HTTP/1.1\r\n\r\n")));

  sakuin::http::HttpRequestParser expecting;
  require(!expecting.feed(
      bytes("POST / HTTP/1.1\r\nHost: localhost\r\nExpect: 100-continue\r\n"
            "Content-Length: 1\r\n\r\nx")));
}

void serializes_owned_framing() {
  sakuin::api::HttpResponse response{
      .status = 200,
      .headers = {{"content-type", "application/json"}},
      .body = {std::byte{'{'}, std::byte{'}'}}};
  const auto encoded = sakuin::http::serialize_response(std::move(response));
  require(static_cast<bool>(encoded));
  const auto output = text(*encoded);
  require(output.starts_with("HTTP/1.1 200 OK\r\n"));
  require(output.contains("content-length: 2\r\n"));
  require(output.ends_with("\r\n\r\n{}"));

  sakuin::api::HttpResponse injected{
      .status = 200, .headers = {{"x-test", "ok\r\ninjected: yes"}}};
  require(!sakuin::http::serialize_response(std::move(injected)));

  sakuin::api::HttpResponse framing{.status = 200,
                                    .headers = {{"Content-Length", "0"}}};
  require(!sakuin::http::serialize_response(std::move(framing)));

  const auto accepted_body = bytes("accepted");
  auto accepted = sakuin::http::serialize_response(
      {.status = 202, .body = {accepted_body.begin(), accepted_body.end()}});
  require(static_cast<bool>(accepted));
  require(text(*accepted).starts_with("HTTP/1.1 202 Accepted\r\n"));
}

} // namespace

int main() {
  parses_fragmented_request();
  parses_bounded_body();
  rejects_ambiguous_or_trailing_requests();
  serializes_owned_framing();
}

import std;

import sakuin.api;
import sakuin.core;
import sakuin.model.torrent;
import sakuin.search;

namespace {

std::string body(const sakuin::api::HttpResponse &response) {
  return {reinterpret_cast<const char *>(response.body.data()),
          response.body.size()};
}

std::string credential(std::string_view key_id,
                       const sakuin::api::ApiKeySecret &secret) {
  constexpr std::string_view digits{"0123456789abcdef"};
  std::string result{"Bearer sakuin_"};
  result.append(key_id).push_back('_');
  for (const auto byte : secret.bytes) {
    result.push_back(digits[byte >> 4]);
    result.push_back(digits[byte & 0x0f]);
  }
  return result;
}

} // namespace

int main() {
  using namespace sakuin;
  api::ApiKeyPepper pepper;
  std::ranges::iota(pepper.bytes, std::uint8_t{1});
  auto authenticator = api::InMemoryApiKeyAuthenticator::create(pepper);
  if (!authenticator)
    return 1;
  api::ApiKeySecret secret;
  std::ranges::iota(secret.bytes, std::uint8_t{41});
  if (!(*authenticator)->put("reader", secret, {api::Permission::Search}))
    return 2;

  search::InMemorySearchIndex index;
  auto rebuild = index.begin_rebuild(7);
  if (!rebuild)
    return 3;
  model::TorrentRecord record;
  record.info_hash.bytes.fill(0x11);
  record.name = "Linux Distribution";
  record.total_size = 4096;
  record.first_seen = core::Timestamp{core::Timestamp::duration{10}};
  record.last_seen = core::Timestamp{core::Timestamp::duration{20}};
  record.files.push_back({.path = "linux.iso", .size = 4096});
  if (!(*rebuild)->append(record) || !(*rebuild)->commit())
    return 4;

  api::SearchHttpHandler handler{**authenticator, index};
  auto health =
      handler.handle({.method = api::HttpMethod::Get, .target = "/v1/health"});
  if (!health || health->status != 200 ||
      body(*health) != "{\"status\":\"ok\"}")
    return 5;

  auto unauthorized = handler.handle(
      {.method = api::HttpMethod::Get, .target = "/v1/search?q=linux"});
  if (!unauthorized || unauthorized->status != 401)
    return 6;

  api::HttpRequest query{
      .method = api::HttpMethod::Get,
      .target = "/v1/search?q=linux&min_size=1000&limit=10",
      .headers = {{"authorization", credential("reader", secret)}}};
  auto found = handler.handle(std::move(query));
  if (!found || found->status != 200 ||
      !body(*found).contains("\"source_generation\":7") ||
      !body(*found).contains("\"total_matches\":1") ||
      !body(*found).contains("Linux Distribution") ||
      !body(*found).contains(std::string(40, '1')))
    return 7;

  api::HttpRequest invalid{
      .method = api::HttpMethod::Get,
      .target = "/v1/search?unknown=yes",
      .headers = {{"authorization", credential("reader", secret)}}};
  auto rejected = handler.handle(std::move(invalid));
  if (!rejected || rejected->status != 400 ||
      !body(*rejected).contains("invalid_query"))
    return 8;
  auto governor = api::FixedWindowRequestGovernor::create(
      {.maximum_requests = 1, .period = std::chrono::hours{24}});
  if (!governor)
    return 9;
  api::SearchHttpHandler limited_handler{**authenticator, index,
                                         governor->get()};
  const auto authenticated = [&] {
    return api::HttpRequest{
        .method = api::HttpMethod::Get,
        .target = "/v1/search?q=linux",
        .headers = {{"authorization", credential("reader", secret)}}};
  };
  auto allowed = limited_handler.handle(authenticated());
  auto limited = limited_handler.handle(authenticated());
  if (!allowed || allowed->status != 200 || !limited ||
      limited->status != 429 || !limited->headers.contains("retry-after"))
    return 10;
  return 0;
}

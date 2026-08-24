import std;

import sakuin.api;
import sakuin.core;
import sakuin.index.duplicates;
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

class Duplicates final : public sakuin::index::DuplicateIndexView {
public:
  sakuin::index::DuplicateIndexState snapshot() const override { return state; }

  sakuin::index::DuplicateIndexState state;
};

class Status final : public sakuin::api::StatusProvider {
public:
  sakuin::api::ServiceStatus status() const override { return snapshot; }

  sakuin::api::ServiceStatus snapshot;
};

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
  api::ApiKeySecret admin_secret;
  std::ranges::iota(admin_secret.bytes, std::uint8_t{81});
  if (!(*authenticator)
           ->put("operator", admin_secret, {api::Permission::Admin}))
    return 15;

  search::InMemorySearchIndex index;
  auto rebuild = index.begin_rebuild(7);
  if (!rebuild)
    return 3;
  model::TorrentRecord record;
  record.info_hash.bytes.fill(0x11);
  record.name = "Linux Distribution";
  record.total_size = 4096;
  record.first_seen = core::Timestamp{std::chrono::milliseconds{10}};
  record.last_seen = core::Timestamp{std::chrono::milliseconds{20}};
  record.files.push_back({.path = "linux.iso", .size = 4096});
  if (!(*rebuild)->append(record) || !(*rebuild)->commit())
    return 4;

  Duplicates duplicates;
  index::DuplicateGroup duplicate_group{
      .fingerprint =
          {.algorithm =
               index::DuplicateFingerprintAlgorithm::ExactFileLayoutV1},
      .torrents = {record.info_hash, core::InfoHash{}}};
  duplicate_group.fingerprint.digest.bytes.fill(0xab);
  duplicate_group.torrents[1].bytes.fill(0x22);
  duplicates.state.stats.source_generation = 7;
  duplicates.state.entries.push_back(std::move(duplicate_group));

  Status status;
  status.snapshot.state = "running";
  status.snapshot.ipv4.enabled = true;
  status.snapshot.ipv4.running = true;
  status.snapshot.ipv4.cycles = 42;
  status.snapshot.ipv4.routing_nodes = 13;
  status.snapshot.ipv4.metadata_in_flight = 2;
  status.snapshot.ipv4.observations_stored = 99;
  status.snapshot.ipv4.bootstrap_complete = true;
  status.snapshot.search_source_generation = 7;
  status.snapshot.search_records_indexed = 21;
  std::size_t refreshes{};
  api::SearchHttpHandler handler{**authenticator,
                                 index,
                                 nullptr,
                                 &duplicates,
                                 &status,
                                 [&refreshes]() -> core::Result<void> {
                                   ++refreshes;
                                   return {};
                                 }};
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
      .target =
          "/v1/search?q=linux&min_size=1000&min_files=1&max_files=1&limit=10",
      .headers = {{"authorization", credential("reader", secret)}}};
  auto found = handler.handle(std::move(query));
  if (!found || found->status != 200 ||
      !body(*found).contains("\"source_generation\":7") ||
      !body(*found).contains("\"total_matches\":1") ||
      !body(*found).contains("Linux Distribution") ||
      !body(*found).contains(std::string(40, '1')))
    return 7;

  api::HttpRequest time_query{
      .method = api::HttpMethod::Get,
      .target = "/v1/search?q=linux&first_seen_at_or_after_ms=10&"
                "last_seen_at_or_before_ms=20",
      .headers = {{"authorization", credential("reader", secret)}}};
  auto time_filtered = handler.handle(std::move(time_query));
  if (!time_filtered || time_filtered->status != 200 ||
      !body(*time_filtered).contains("\"total_matches\":1"))
    return 14;

  api::HttpRequest invalid{
      .method = api::HttpMethod::Get,
      .target = "/v1/search?unknown=yes",
      .headers = {{"authorization", credential("reader", secret)}}};
  auto rejected = handler.handle(std::move(invalid));
  if (!rejected || rejected->status != 400 ||
      !body(*rejected).contains("invalid_query"))
    return 8;

  api::HttpRequest duplicate_query{
      .method = api::HttpMethod::Get,
      .target = "/v1/duplicates?algorithm=exact_file_layout_v1&limit=10",
      .headers = {{"authorization", credential("reader", secret)}}};
  auto duplicate_groups = handler.handle(std::move(duplicate_query));
  if (!duplicate_groups || duplicate_groups->status != 200 ||
      !body(*duplicate_groups).contains("\"source_generation\":7") ||
      !body(*duplicate_groups).contains("\"total_groups\":1") ||
      !body(*duplicate_groups).contains("abababababababab"))
    return 9;

  api::HttpRequest duplicate_match{
      .method = api::HttpMethod::Get,
      .target = "/v1/duplicates/" + std::string(40, '1'),
      .headers = {{"authorization", credential("reader", secret)}}};
  auto matched = handler.handle(std::move(duplicate_match));
  if (!matched || matched->status != 200 ||
      !body(*matched).contains("exact_file_layout_v1") ||
      !body(*matched).contains(std::string(40, '2')))
    return 10;

  api::HttpRequest invalid_duplicate_query{
      .method = api::HttpMethod::Get,
      .target = "/v1/duplicates?algorithm=exact_file_layout_v1&min_members=1",
      .headers = {{"authorization", credential("reader", secret)}}};
  auto invalid_duplicate = handler.handle(std::move(invalid_duplicate_query));
  if (!invalid_duplicate || invalid_duplicate->status != 400)
    return 11;

  auto caps =
      handler.handle({.method = api::HttpMethod::Get, .target = "/api?t=caps"});
  if (!caps || caps->status != 200 ||
      !body(*caps).contains("<server version=\"1.3\" title=\"Sakuin\"") ||
      !body(*caps).contains("<search available=\"yes\""))
    return 16;

  auto token = credential("reader", secret);
  token.erase(0, std::string_view{"Bearer "}.size());
  auto torznab = handler.handle(
      {.method = api::HttpMethod::Get,
       .target = "/api?t=search&q=linux&cat=&limit=10&apikey=" + token});
  if (!torznab || torznab->status != 200 ||
      !body(*torznab).contains("<rss version=\"2.0\"") ||
      !body(*torznab).contains("name=\"infohash\"") ||
      !body(*torznab).contains("magnet:?xt=urn:btih:") ||
      !body(*torznab).contains(std::string(40, '1')))
    return 17;

  auto torznab_unauthorized = handler.handle(
      {.method = api::HttpMethod::Get, .target = "/api?t=search&q=linux"});
  if (!torznab_unauthorized ||
      !body(*torznab_unauthorized).contains("code=\"100\""))
    return 18;

  auto reader_status = handler.handle(
      {.method = api::HttpMethod::Get,
       .target = "/v1/status",
       .headers = {{"authorization", credential("reader", secret)}}});
  if (!reader_status || reader_status->status != 403)
    return 19;
  auto operator_status = handler.handle(
      {.method = api::HttpMethod::Get,
       .target = "/v1/status",
       .headers = {{"authorization", credential("operator", admin_secret)}}});
  if (!operator_status || operator_status->status != 200 ||
      !body(*operator_status).contains("\"cycles\":42") ||
      !body(*operator_status).contains("\"routing_nodes\":13") ||
      !body(*operator_status).contains("\"metadata_in_flight\":2") ||
      !body(*operator_status).contains("\"last_error\":null") ||
      body(*operator_status).contains("\"last_error\":[null]") ||
      !body(*operator_status).contains("\"state\":\"running\""))
    return 20;

  auto reader_metrics = handler.handle(
      {.method = api::HttpMethod::Get,
       .target = "/metrics",
       .headers = {{"authorization", credential("reader", secret)}}});
  if (!reader_metrics || reader_metrics->status != 403)
    return 23;
  auto operator_metrics = handler.handle(
      {.method = api::HttpMethod::Get,
       .target = "/metrics",
       .headers = {{"authorization", credential("operator", admin_secret)}}});
  if (!operator_metrics || operator_metrics->status != 200 ||
      operator_metrics->headers["content-type"] !=
          "text/plain; version=0.0.4; charset=utf-8" ||
      !body(*operator_metrics)
           .contains("# TYPE sakuin_dht_cycles_total counter\n") ||
      !body(*operator_metrics)
           .contains("sakuin_dht_cycles_total{family=\"ipv4\"} 42\n") ||
      !body(*operator_metrics)
           .contains(
               "sakuin_dht_observations_stored_total{family=\"ipv4\"} 99\n") ||
      !body(*operator_metrics)
           .contains("sakuin_dht_bootstrap_complete{family=\"ipv4\"} 1\n") ||
      !body(*operator_metrics)
           .contains("sakuin_search_source_generation 7\n") ||
      !body(*operator_metrics)
           .contains("sakuin_search_records_indexed_total 21\n"))
    return 24;
  auto metrics_with_post = handler.handle(
      {.method = api::HttpMethod::Post,
       .target = "/v1/metrics",
       .headers = {{"authorization", credential("operator", admin_secret)}}});
  if (!metrics_with_post || metrics_with_post->status != 405 ||
      metrics_with_post->headers["allow"] != "GET")
    return 25;

  auto refreshed = handler.handle(
      {.method = api::HttpMethod::Post,
       .target = "/v1/operations/search-refresh",
       .headers = {{"authorization", credential("operator", admin_secret)}}});
  if (!refreshed || refreshed->status != 200 || refreshes != 1 ||
      !body(*refreshed).contains("search_refresh"))
    return 21;
  auto refresh_with_get = handler.handle(
      {.method = api::HttpMethod::Get,
       .target = "/v1/operations/search-refresh",
       .headers = {{"authorization", credential("operator", admin_secret)}}});
  if (!refresh_with_get || refresh_with_get->status != 405 ||
      refresh_with_get->headers["allow"] != "POST")
    return 22;

  auto governor = api::FixedWindowRequestGovernor::create(
      {.maximum_requests = 1, .period = std::chrono::hours{24}});
  if (!governor)
    return 12;
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
    return 13;
  return 0;
}

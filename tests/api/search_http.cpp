import std;

import sakuin.api;
import sakuin.classification;
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
  model::TorrentRecord movie;
  movie.info_hash.bytes.fill(0x33);
  movie.name = "Example Anime Movie 2024 1080p";
  movie.total_size = 1'000'000;
  movie.first_seen = core::Timestamp{std::chrono::milliseconds{30}};
  movie.last_seen = core::Timestamp{std::chrono::milliseconds{40}};
  movie.files.push_back(
      {.path = "Example.Anime.Movie.2024.1080p.mkv", .size = 1'000'000});
  if (!(*rebuild)->append(record) || !(*rebuild)->append(movie) ||
      !(*rebuild)->commit())
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
  status.snapshot.ipv4.discovery_queries_started = 17;
  status.snapshot.ipv4.discovery_in_flight = 1;
  status.snapshot.ipv4.inbound_queries = 12;
  status.snapshot.ipv4.inbound_get_peers_queries = 7;
  status.snapshot.ipv4.last_inbound_query_ms = 12'000;
  status.snapshot.ipv4.bootstrap_complete = true;
  status.snapshot.search_source_generation = 7;
  status.snapshot.search_records_indexed = 21;
  std::size_t refreshes{};
  std::vector<bool> maintenance_requests;
  api::SearchHttpHandler handler{
      **authenticator,
      index,
      nullptr,
      &duplicates,
      &status,
      [&refreshes]() -> core::Result<void> {
        ++refreshes;
        return {};
      },
      [&maintenance_requests](bool verify) -> core::Result<void> {
        maintenance_requests.push_back(verify);
        return {};
      }};
  auto health =
      handler.handle({.method = api::HttpMethod::Get, .target = "/v1/health"});
  if (!health || health->status != 200 ||
      body(*health) != "{\"status\":\"ok\"}")
    return 5;
  auto ready =
      handler.handle({.method = api::HttpMethod::Get, .target = "/v1/ready"});
  if (!ready || ready->status != 200 ||
      body(*ready) != "{\"status\":\"ready\"}")
    return 29;
  status.snapshot.state = "starting";
  auto not_ready =
      handler.handle({.method = api::HttpMethod::Get, .target = "/v1/ready"});
  if (!not_ready || not_ready->status != 503 ||
      body(*not_ready) != "{\"status\":\"not_ready\"}")
    return 30;
  status.snapshot.state = "running";

  auto openapi = handler.handle(
      {.method = api::HttpMethod::Get, .target = "/openapi.json"});
  if (!openapi || openapi->status != 200 ||
      openapi->headers["content-type"] != "application/json; charset=utf-8" ||
      !body(*openapi).contains("\"openapi\":\"3.1.2\"") ||
      !body(*openapi).contains(
          "\"jsonSchemaDialect\":\"https://spec.openapis.org/oas/3.1/"
          "dialect/base\"") ||
      !body(*openapi).contains("\"version\":\"" + std::string{core::version} +
                               "\"") ||
      !body(*openapi).contains("\"/v1/search\"") ||
      !body(*openapi).contains("\"classification_state\"") ||
      !body(*openapi).contains("\"minimum_label_confidence\"") ||
      !body(*openapi).contains("\"bearerAuth\"") ||
      body(*openapi).contains("sakuin_reader_"))
    return 33;
  auto openapi_alias = handler.handle(
      {.method = api::HttpMethod::Get, .target = "/v1/openapi.json"});
  if (!openapi_alias || body(*openapi_alias) != body(*openapi))
    return 34;
  auto openapi_post = handler.handle(
      {.method = api::HttpMethod::Post, .target = "/openapi.json"});
  if (!openapi_post || openapi_post->status != 405 ||
      openapi_post->headers["allow"] != "GET")
    return 35;

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
      !body(*found).contains("\"classification\"") ||
      !body(*found).contains("\"categories\":[\"other\"]") ||
      !body(*found).contains("\"evidence\":[") ||
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

  api::HttpRequest category_query{
      .method = api::HttpMethod::Get,
      .target = "/v1/search?category=movie&limit=10",
      .headers = {{"authorization", credential("reader", secret)}}};
  auto category_filtered = handler.handle(std::move(category_query));
  if (!category_filtered || category_filtered->status != 200 ||
      !body(*category_filtered).contains("\"total_matches\":1") ||
      !body(*category_filtered).contains("Example Anime Movie 2024 1080p") ||
      !body(*category_filtered).contains("\"code\":\"release_year_token\"") ||
      !body(*category_filtered).contains("\"subject\":\"movie\"") ||
      !body(*category_filtered).contains("\"weight\":"))
    return 38;
  api::HttpRequest game_category_query{
      .method = api::HttpMethod::Get,
      .target = "/v1/search?category=game&limit=10",
      .headers = {{"authorization", credential("reader", secret)}}};
  auto game_category_filtered = handler.handle(std::move(game_category_query));
  if (!game_category_filtered || game_category_filtered->status != 200 ||
      !body(*game_category_filtered).contains("\"total_matches\":0"))
    return 48;

  api::HttpRequest classification_query{
      .method = api::HttpMethod::Get,
      .target = "/v1/search?classification_state=classified&content_kind="
                "movie&minimum_kind_confidence=high&limit=10",
      .headers = {{"authorization", credential("reader", secret)}}};
  auto classification_filtered =
      handler.handle(std::move(classification_query));
  if (!classification_filtered || classification_filtered->status != 200 ||
      !body(*classification_filtered).contains("\"total_matches\":1") ||
      !body(*classification_filtered)
           .contains("Example Anime Movie 2024 1080p"))
    return 40;

  api::HttpRequest unknown_classification_query{
      .method = api::HttpMethod::Get,
      .target = "/v1/search?classification_state=unknown&content_kind=unknown",
      .headers = {{"authorization", credential("reader", secret)}}};
  auto unknown_classification =
      handler.handle(std::move(unknown_classification_query));
  if (!unknown_classification || unknown_classification->status != 200 ||
      !body(*unknown_classification).contains("\"total_matches\":1") ||
      !body(*unknown_classification).contains("Linux Distribution"))
    return 41;

  api::HttpRequest label_query{
      .method = api::HttpMethod::Get,
      .target = "/v1/search?label=anime&minimum_label_confidence=medium",
      .headers = {{"authorization", credential("reader", secret)}}};
  auto label_filtered = handler.handle(std::move(label_query));
  if (!label_filtered || label_filtered->status != 200 ||
      !body(*label_filtered).contains("\"total_matches\":1") ||
      !body(*label_filtered).contains("Example Anime Movie 2024 1080p"))
    return 42;

  api::HttpRequest invalid_classification{
      .method = api::HttpMethod::Get,
      .target = "/v1/search?content_kind=video",
      .headers = {{"authorization", credential("reader", secret)}}};
  auto invalid_classification_result =
      handler.handle(std::move(invalid_classification));
  if (!invalid_classification_result ||
      invalid_classification_result->status != 400 ||
      !body(*invalid_classification_result).contains("invalid_query"))
    return 43;

  api::HttpRequest orphan_label_confidence{
      .method = api::HttpMethod::Get,
      .target = "/v1/search?minimum_label_confidence=high",
      .headers = {{"authorization", credential("reader", secret)}}};
  auto orphan_label_result = handler.handle(std::move(orphan_label_confidence));
  if (!orphan_label_result || orphan_label_result->status != 400 ||
      !body(*orphan_label_result)
           .contains("minimum_label_confidence requires at least one label"))
    return 44;

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
      !body(*caps).contains("<search available=\"yes\"") ||
      !body(*caps).contains("<movie-search available=\"yes\"") ||
      !body(*caps).contains("<audio-search available=\"yes\"") ||
      !body(*caps).contains("<category id=\"2000\" name=\"Movies\"") ||
      !body(*caps).contains("<subcat id=\"4050\" name=\"Games\""))
    return 16;
  auto game_request = api::parse_torznab_request("t=search&cat=4050");
  if (!game_request || game_request->category_filter_matches_nothing ||
      game_request->query.categories !=
          std::vector{classification::MediaCategory::Game})
    return 45;
  auto pc_request = api::parse_torznab_request("t=search&cat=4000");
  if (!pc_request ||
      !std::ranges::contains(pc_request->query.categories,
                             classification::MediaCategory::Application) ||
      !std::ranges::contains(pc_request->query.categories,
                             classification::MediaCategory::Game))
    return 47;
  search::SearchHit game_hit{
      .name = "Example Game",
      .total_size = 1024,
      .file_count = 1,
      .categories = {classification::MediaCategory::Game}};
  auto game_feed = api::torznab_search_response(
      {.hits = {std::move(game_hit)}, .total_matches = 1}, 0);
  if (!body(game_feed).contains("<category>4050</category>") ||
      !body(game_feed).contains("name=\"category\" value=\"4050\""))
    return 46;

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

  auto torznab_movie = handler.handle(
      {.method = api::HttpMethod::Get,
       .target = "/api?t=movie&q=example&limit=10&apikey=" + token});
  if (!torznab_movie ||
      !body(*torznab_movie).contains("<category>2000</category>") ||
      !body(*torznab_movie).contains("<category>2040</category>") ||
      body(*torznab_movie).contains("<category>Movies</category>") ||
      body(*torznab_movie).contains("<category>Movies/HD</category>") ||
      !body(*torznab_movie).contains("name=\"category\" value=\"2000\"") ||
      !body(*torznab_movie).contains("name=\"category\" value=\"2040\"") ||
      !body(*torznab_movie).contains("total=\"1\""))
    return 36;
  auto unknown_category = handler.handle(
      {.method = api::HttpMethod::Get,
       .target = "/api?t=search&cat=9999&limit=10&apikey=" + token});
  if (!unknown_category || !body(*unknown_category).contains("total=\"0\""))
    return 37;

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
      !body(*operator_status)
           .contains("\"version\":\"" + std::string{core::version} + "\"") ||
      !body(*operator_status).contains("\"cycles\":42") ||
      !body(*operator_status).contains("\"routing_nodes\":13") ||
      !body(*operator_status).contains("\"inbound_queries\":12") ||
      !body(*operator_status).contains("\"inbound_get_peers_queries\":7") ||
      !body(*operator_status).contains("\"discovery_in_flight\":1") ||
      !body(*operator_status).contains("\"metadata_in_flight\":2") ||
      !body(*operator_status).contains("\"total_records\":2") ||
      !body(*operator_status).contains("\"adult_labeled\":0") ||
      !body(*operator_status).contains("\"movie\":1") ||
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
           .contains("sakuin_service_info{state=\"running\",version=\"" +
                     std::string{core::version} + "\"} 1\n") ||
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
           .contains("sakuin_dht_discovery_queries_started_total{family="
                     "\"ipv4\"} 17\n") ||
      !body(*operator_metrics)
           .contains("sakuin_dht_inbound_query_methods_total{family=\"ipv4\","
                     "method=\"get_peers\"} 7\n") ||
      !body(*operator_metrics)
           .contains("sakuin_dht_last_inbound_query_timestamp_seconds{family="
                     "\"ipv4\"} 12.000000\n") ||
      !body(*operator_metrics)
           .contains("sakuin_search_source_generation 7\n") ||
      !body(*operator_metrics)
           .contains("sakuin_search_records_indexed_total 21\n"))
    return 24;
  if (!body(*operator_metrics).contains("sakuin_classification_enabled 1\n") ||
      !body(*operator_metrics)
           .contains("sakuin_classification_adult_labeled_records 0\n") ||
      !body(*operator_metrics)
           .contains(
               "sakuin_classification_category_records{category=\"movie\"} "
               "1\n"))
    return 39;
  if (!body(*operator_metrics).contains("sakuin_service_ready 1\n"))
    return 31;
  auto metrics_with_post = handler.handle(
      {.method = api::HttpMethod::Post,
       .target = "/v1/metrics",
       .headers = {{"authorization", credential("operator", admin_secret)}}});
  if (!metrics_with_post || metrics_with_post->status != 405 ||
      metrics_with_post->headers["allow"] != "GET")
    return 25;

  auto maintenance = handler.handle(
      {.method = api::HttpMethod::Post,
       .target = "/v1/operations/storage-maintenance?verify=true",
       .headers = {{"authorization", credential("operator", admin_secret)}}});
  if (!maintenance || maintenance->status != 202 ||
      maintenance->headers["location"] != "/v1/status" ||
      maintenance_requests != std::vector{true} ||
      !body(*maintenance).contains("\"verification\":true"))
    return 26;
  auto invalid_maintenance = handler.handle(
      {.method = api::HttpMethod::Post,
       .target = "/v1/operations/storage-maintenance?verify=1",
       .headers = {{"authorization", credential("operator", admin_secret)}}});
  if (!invalid_maintenance || invalid_maintenance->status != 400 ||
      maintenance_requests.size() != 1)
    return 27;
  auto maintenance_with_get = handler.handle(
      {.method = api::HttpMethod::Get,
       .target = "/v1/operations/storage-maintenance",
       .headers = {{"authorization", credential("operator", admin_secret)}}});
  if (!maintenance_with_get || maintenance_with_get->status != 405 ||
      maintenance_with_get->headers["allow"] != "POST")
    return 28;

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
  auto unavailable_readiness = limited_handler.handle(
      {.method = api::HttpMethod::Get, .target = "/v1/ready"});
  if (!unavailable_readiness || unavailable_readiness->status != 503)
    return 32;
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

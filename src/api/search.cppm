export module sakuin.api.search;

import std;

import sakuin.api.auth;
import sakuin.api.http;
import sakuin.api.json;
import sakuin.api.rate_limit;
import sakuin.core.bytes;
import sakuin.core.ids;
import sakuin.core.random;
import sakuin.core.result;
import sakuin.core.time;
import sakuin.index.duplicates;
import sakuin.search.index;

export namespace sakuin::api {

class SearchHttpHandler final : public HttpHandler {
public:
  SearchHttpHandler(ApiKeyAuthenticator &authenticator,
                    search::SearchIndex &index,
                    ApiRequestGovernor *governor = nullptr,
                    index::DuplicateIndexView *duplicates = nullptr)
      : authenticator_(&authenticator), index_(&index), governor_(governor),
        duplicates_(duplicates) {}

  core::Result<HttpResponse> handle(HttpRequest request) override;

private:
  ApiKeyAuthenticator *authenticator_;
  search::SearchIndex *index_;
  ApiRequestGovernor *governor_;
  index::DuplicateIndexView *duplicates_;
};

} // namespace sakuin::api

namespace sakuin::api {
namespace {

HttpResponse json_response(unsigned status, core::ByteBuffer body) {
  return {.status = status,
          .headers = {{"content-type", "application/json; charset=utf-8"},
                      {"cache-control", "no-store"}},
          .body = std::move(body)};
}

core::Result<HttpResponse> error_response(unsigned status,
                                          std::string_view code,
                                          std::string_view message) {
  auto body = json_error(code, message);
  if (!body)
    return std::unexpected(body.error());
  return json_response(status, std::move(*body));
}

core::Result<HttpResponse> method_not_allowed() {
  auto response =
      error_response(405, "method_not_allowed", "Only GET is supported");
  if (response)
    response->headers["allow"] = "GET";
  return response;
}

core::Result<HttpResponse> unauthorized() {
  auto response =
      error_response(401, "unauthorized", "A valid API key is required");
  if (response)
    response->headers["www-authenticate"] = "Bearer realm=\"sakuin\"";
  return response;
}

core::Result<HttpResponse> rate_limited(core::Duration retry_after) {
  auto response =
      error_response(429, "rate_limited", "API key request limit exceeded");
  if (response) {
    const auto seconds = std::max<std::int64_t>(
        1, std::chrono::ceil<std::chrono::seconds>(retry_after).count());
    response->headers["retry-after"] = std::to_string(seconds);
  }
  return response;
}

std::optional<std::uint8_t> hex_digit(char value) {
  if (value >= '0' && value <= '9')
    return static_cast<std::uint8_t>(value - '0');
  if (value >= 'a' && value <= 'f')
    return static_cast<std::uint8_t>(value - 'a' + 10);
  if (value >= 'A' && value <= 'F')
    return static_cast<std::uint8_t>(value - 'A' + 10);
  return std::nullopt;
}

struct Credential {
  std::string key_id;
  ApiKeySecret secret;
};

class CredentialCleaner {
public:
  explicit CredentialCleaner(ApiKeySecret &secret) : secret_(&secret) {}
  ~CredentialCleaner() {
    core::secure_clear({reinterpret_cast<std::byte *>(secret_->bytes.data()),
                        secret_->bytes.size()});
  }
  CredentialCleaner(const CredentialCleaner &) = delete;
  CredentialCleaner &operator=(const CredentialCleaner &) = delete;

private:
  ApiKeySecret *secret_;
};

std::optional<Credential> credential(std::string_view header) {
  constexpr std::string_view prefix{"Bearer sakuin_"};
  if (!header.starts_with(prefix))
    return std::nullopt;
  header.remove_prefix(prefix.size());
  const auto separator = header.rfind('_');
  if (separator == std::string_view::npos || separator == 0 ||
      header.size() - separator - 1 != 64)
    return std::nullopt;
  Credential result{.key_id = std::string{header.substr(0, separator)}};
  const auto encoded = header.substr(separator + 1);
  for (std::size_t index = 0; index < result.secret.bytes.size(); ++index) {
    const auto high = hex_digit(encoded[index * 2]);
    const auto low = hex_digit(encoded[index * 2 + 1]);
    if (!high || !low)
      return std::nullopt;
    result.secret.bytes[index] = static_cast<std::uint8_t>((*high << 4) | *low);
  }
  return result;
}

core::ByteView secret_view(const ApiKeySecret &secret) {
  return {reinterpret_cast<const std::byte *>(secret.bytes.data()),
          secret.bytes.size()};
}

core::Result<std::string> percent_decode(std::string_view value) {
  std::string result;
  result.reserve(value.size());
  for (std::size_t index = 0; index < value.size(); ++index) {
    if (value[index] == '+') {
      result.push_back(' ');
    } else if (value[index] == '%') {
      if (index + 2 >= value.size())
        return std::unexpected(core::Error{core::ErrorCode::InvalidQuery,
                                           "Incomplete percent escape"});
      const auto high = hex_digit(value[index + 1]);
      const auto low = hex_digit(value[index + 2]);
      if (!high || !low)
        return std::unexpected(core::Error{core::ErrorCode::InvalidQuery,
                                           "Invalid percent escape"});
      const auto decoded = static_cast<char>((*high << 4) | *low);
      if (decoded == '\0')
        return std::unexpected(core::Error{core::ErrorCode::InvalidQuery,
                                           "NUL is not allowed in a URL"});
      result.push_back(decoded);
      index += 2;
    } else {
      result.push_back(value[index]);
    }
  }
  return result;
}

core::Result<std::map<std::string, std::string, std::less<>>>
query_parameters(std::string_view query) {
  std::map<std::string, std::string, std::less<>> result;
  while (!query.empty()) {
    const auto separator = query.find('&');
    const auto pair = query.substr(0, separator);
    const auto equals = pair.find('=');
    auto name = percent_decode(pair.substr(0, equals));
    auto value = percent_decode(equals == std::string_view::npos
                                    ? std::string_view{}
                                    : pair.substr(equals + 1));
    if (!name || !value)
      return std::unexpected(name ? value.error() : name.error());
    if (name->empty() ||
        !result.emplace(std::move(*name), std::move(*value)).second)
      return std::unexpected(
          core::Error{core::ErrorCode::InvalidQuery,
                      "Query parameter names must be nonempty and unique"});
    if (separator == std::string_view::npos)
      break;
    query.remove_prefix(separator + 1);
  }
  return result;
}

template <std::unsigned_integral T>
core::Result<std::optional<T>> optional_number(
    const std::map<std::string, std::string, std::less<>> &parameters,
    std::string_view name) {
  const auto found = parameters.find(name);
  if (found == parameters.end())
    return std::optional<T>{};
  T value{};
  const auto [end, error] = std::from_chars(
      found->second.data(), found->second.data() + found->second.size(), value);
  if (error != std::errc{} ||
      end != found->second.data() + found->second.size())
    return std::unexpected(
        core::Error{core::ErrorCode::InvalidQuery,
                    std::string{name} + " must be an unsigned integer"});
  return std::optional<T>{value};
}

core::Result<std::optional<std::int64_t>> optional_integer(
    const std::map<std::string, std::string, std::less<>> &parameters,
    std::string_view name) {
  const auto found = parameters.find(name);
  if (found == parameters.end())
    return std::optional<std::int64_t>{};
  std::int64_t value{};
  const auto [end, error] = std::from_chars(
      found->second.data(), found->second.data() + found->second.size(), value);
  if (error != std::errc{} ||
      end != found->second.data() + found->second.size())
    return std::unexpected(
        core::Error{core::ErrorCode::InvalidQuery,
                    std::string{name} + " must be an integer"});
  return std::optional<std::int64_t>{value};
}

core::Result<std::optional<core::Timestamp>> optional_timestamp(
    const std::map<std::string, std::string, std::less<>> &parameters,
    std::string_view name) {
  auto value = optional_integer(parameters, name);
  if (!value)
    return std::unexpected(value.error());
  if (!*value)
    return std::optional<core::Timestamp>{};
  const auto minimum = std::chrono::duration_cast<std::chrono::milliseconds>(
                           core::Timestamp::duration::min())
                           .count();
  const auto maximum = std::chrono::duration_cast<std::chrono::milliseconds>(
                           core::Timestamp::duration::max())
                           .count();
  if (**value < minimum || **value > maximum)
    return std::unexpected(
        core::Error{core::ErrorCode::InvalidQuery,
                    std::string{name} + " is outside the timestamp range"});
  return std::optional<core::Timestamp>{
      core::Timestamp{std::chrono::duration_cast<core::Timestamp::duration>(
          std::chrono::milliseconds{**value})}};
}

core::Result<search::SearchQuery> search_query(std::string_view encoded) {
  auto parameters = query_parameters(encoded);
  if (!parameters)
    return std::unexpected(parameters.error());
  for (const auto &[name, _] : *parameters) {
    if (name != "q" && name != "min_size" && name != "max_size" &&
        name != "first_seen_at_or_after_ms" &&
        name != "last_seen_at_or_before_ms" && name != "offset" &&
        name != "limit")
      return std::unexpected(core::Error{core::ErrorCode::InvalidQuery,
                                         "Unknown query parameter: " + name});
  }
  auto minimum = optional_number<std::uint64_t>(*parameters, "min_size");
  auto maximum = optional_number<std::uint64_t>(*parameters, "max_size");
  auto first_seen =
      optional_timestamp(*parameters, "first_seen_at_or_after_ms");
  auto last_seen = optional_timestamp(*parameters, "last_seen_at_or_before_ms");
  auto offset = optional_number<std::size_t>(*parameters, "offset");
  auto limit = optional_number<std::size_t>(*parameters, "limit");
  if (!minimum || !maximum || !first_seen || !last_seen || !offset || !limit)
    return std::unexpected(!minimum      ? minimum.error()
                           : !maximum    ? maximum.error()
                           : !first_seen ? first_seen.error()
                           : !last_seen  ? last_seen.error()
                           : !offset     ? offset.error()
                                         : limit.error());
  const auto text = parameters->find("q");
  return search::SearchQuery{.text = text == parameters->end() ? std::string{}
                                                               : text->second,
                             .minimum_size = *minimum,
                             .maximum_size = *maximum,
                             .first_seen_at_or_after = *first_seen,
                             .last_seen_at_or_before = *last_seen,
                             .offset = offset->value_or(0),
                             .limit = limit->value_or(50)};
}

struct DuplicateGroupsQuery {
  index::DuplicateFingerprintAlgorithm algorithm;
  std::size_t minimum_members{2};
  std::size_t offset{};
  std::size_t limit{50};
};

core::Result<DuplicateGroupsQuery>
duplicate_groups_query(std::string_view encoded) {
  auto parameters = query_parameters(encoded);
  if (!parameters)
    return std::unexpected(parameters.error());
  for (const auto &[name, _] : *parameters) {
    if (name != "algorithm" && name != "min_members" && name != "offset" &&
        name != "limit")
      return std::unexpected(core::Error{core::ErrorCode::InvalidQuery,
                                         "Unknown query parameter: " + name});
  }
  const auto algorithm = parameters->find("algorithm");
  if (algorithm == parameters->end())
    return std::unexpected(
        core::Error{core::ErrorCode::InvalidQuery, "algorithm is required"});
  DuplicateGroupsQuery result;
  if (algorithm->second == "exact_file_layout_v1")
    result.algorithm = index::DuplicateFingerprintAlgorithm::ExactFileLayoutV1;
  else if (algorithm->second == "normalized_metadata_v1")
    result.algorithm =
        index::DuplicateFingerprintAlgorithm::NormalizedMetadataV1;
  else
    return std::unexpected(core::Error{
        core::ErrorCode::InvalidQuery,
        "algorithm must be exact_file_layout_v1 or normalized_metadata_v1"});

  auto minimum = optional_number<std::size_t>(*parameters, "min_members");
  auto offset = optional_number<std::size_t>(*parameters, "offset");
  auto limit = optional_number<std::size_t>(*parameters, "limit");
  if (!minimum || !offset || !limit)
    return std::unexpected(!minimum  ? minimum.error()
                           : !offset ? offset.error()
                                     : limit.error());
  result.minimum_members = minimum->value_or(2);
  result.offset = offset->value_or(0);
  result.limit = limit->value_or(50);
  if (result.minimum_members < 2)
    return std::unexpected(core::Error{core::ErrorCode::InvalidQuery,
                                       "min_members must be at least 2"});
  if (result.limit == 0 || result.limit > 200)
    return std::unexpected(core::Error{core::ErrorCode::InvalidQuery,
                                       "limit must be between 1 and 200"});
  return result;
}

core::Result<core::InfoHash> info_hash(std::string_view encoded) {
  if (encoded.size() != core::InfoHash{}.bytes.size() * 2)
    return std::unexpected(core::Error{core::ErrorCode::InvalidQuery,
                                       "infohash must contain 40 hex digits"});
  core::InfoHash result;
  for (std::size_t index = 0; index < result.bytes.size(); ++index) {
    const auto high = hex_digit(encoded[index * 2]);
    const auto low = hex_digit(encoded[index * 2 + 1]);
    if (!high || !low)
      return std::unexpected(
          core::Error{core::ErrorCode::InvalidQuery,
                      "infohash must contain 40 hex digits"});
    result.bytes[index] = static_cast<std::uint8_t>((*high << 4) | *low);
  }
  return result;
}

} // namespace

core::Result<HttpResponse> SearchHttpHandler::handle(HttpRequest request) {
  const auto query_start = request.target.find('?');
  const auto path = std::string_view{request.target}.substr(0, query_start);
  const auto query =
      query_start == std::string::npos
          ? std::string_view{}
          : std::string_view{request.target}.substr(query_start + 1);
  if (request.method != HttpMethod::Get)
    return method_not_allowed();
  if (!request.body.empty())
    return error_response(400, "invalid_request",
                          "GET requests must not contain a body");
  if (path == "/v1/health") {
    auto body = json_health();
    if (!body)
      return std::unexpected(body.error());
    return json_response(200, std::move(*body));
  }
  constexpr std::string_view duplicates_path{"/v1/duplicates"};
  const bool duplicate_route =
      path == duplicates_path || path.starts_with("/v1/duplicates/");
  if (path != "/v1/search" && !duplicate_route)
    return error_response(404, "not_found", "Route not found");
  if (duplicate_route && !duplicates_)
    return error_response(404, "not_found", "Duplicate index is disabled");

  const auto authorization = request.headers.find("authorization");
  auto parsed = authorization == request.headers.end()
                    ? std::optional<Credential>{}
                    : credential(authorization->second);
  if (!parsed)
    return unauthorized();
  CredentialCleaner credential_cleaner{parsed->secret};
  auto principal =
      authenticator_->authenticate(parsed->key_id, secret_view(parsed->secret));
  if (!principal)
    return std::unexpected(principal.error());
  if (!*principal)
    return unauthorized();
  if (!(*principal)->allows(Permission::Search))
    return error_response(403, "forbidden", "Search permission is required");
  if (governor_) {
    auto admission = governor_->admit((*principal)->key_id,
                                      std::chrono::system_clock::now());
    if (!admission)
      return std::unexpected(admission.error());
    if (!admission->allowed)
      return rate_limited(
          admission->retry_after.value_or(core::Duration::zero()));
  }

  if (path == "/v1/search") {
    auto parsed_query = search_query(query);
    if (!parsed_query)
      return error_response(400, "invalid_query", parsed_query.error().message);
    auto result = index_->search(*parsed_query);
    if (!result) {
      if (result.error().code == core::ErrorCode::InvalidQuery)
        return error_response(400, "invalid_query", result.error().message);
      return std::unexpected(result.error());
    }
    auto body = json_search_result(*result);
    if (!body)
      return std::unexpected(body.error());
    return json_response(200, std::move(*body));
  }

  auto snapshot = duplicates_->snapshot();
  if (path == duplicates_path) {
    auto parsed_query = duplicate_groups_query(query);
    if (!parsed_query)
      return error_response(400, "invalid_query", parsed_query.error().message);
    std::vector<index::DuplicateGroup> matching;
    for (const auto &group : snapshot.entries) {
      if (group.fingerprint.algorithm == parsed_query->algorithm &&
          group.torrents.size() >= parsed_query->minimum_members)
        matching.push_back(group);
    }
    const auto total = matching.size();
    const auto start = std::min(parsed_query->offset, matching.size());
    const auto count = std::min(parsed_query->limit, matching.size() - start);
    auto body = json_duplicate_groups(
        snapshot.stats.source_generation, total,
        std::span<const index::DuplicateGroup>{matching}.subspan(start, count));
    if (!body)
      return std::unexpected(body.error());
    return json_response(200, std::move(*body));
  }

  if (!query.empty())
    return error_response(400, "invalid_query",
                          "Duplicate match lookup has no query parameters");
  const auto encoded_hash = path.substr(duplicates_path.size() + 1);
  if (encoded_hash.empty() || encoded_hash.contains('/'))
    return error_response(404, "not_found", "Route not found");
  auto torrent = info_hash(encoded_hash);
  if (!torrent)
    return error_response(400, "invalid_query", torrent.error().message);
  std::vector<index::DuplicateGroup> matching;
  for (const auto &group : snapshot.entries) {
    if (std::ranges::binary_search(group.torrents, torrent->bytes, {},
                                   &core::InfoHash::bytes))
      matching.push_back(group);
  }
  auto body = json_duplicate_matches(snapshot.stats.source_generation, *torrent,
                                     matching);
  if (!body)
    return std::unexpected(body.error());
  return json_response(200, std::move(*body));
}

} // namespace sakuin::api

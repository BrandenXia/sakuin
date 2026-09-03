export module sakuin.api.search;

import std;

import sakuin.api.auth;
import sakuin.api.http;
import sakuin.api.json;
import sakuin.api.metrics;
import sakuin.api.openapi;
import sakuin.api.rate_limit;
import sakuin.api.status;
import sakuin.api.torznab;
import sakuin.classification;
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
  SearchHttpHandler(
      ApiKeyAuthenticator &authenticator, search::SearchIndex &index,
      ApiRequestGovernor *governor = nullptr,
      index::DuplicateIndexView *duplicates = nullptr,
      StatusProvider *status = nullptr,
      std::function<core::Result<void>()> refresh_search = {},
      std::function<core::Result<void>(bool)> request_maintenance = {},
      search::AdultContentMode adult_content =
          search::AdultContentMode::Include)
      : authenticator_(&authenticator), index_(&index), governor_(governor),
        duplicates_(duplicates), status_(status),
        refresh_search_(std::move(refresh_search)),
        request_maintenance_(std::move(request_maintenance)),
        adult_content_(adult_content) {}

  // Composition hook used before the HTTP server starts. Maintenance is
  // constructed after the API because its commits notify search refreshes.
  void
  set_maintenance_requester(std::function<core::Result<void>(bool)> requester) {
    request_maintenance_ = std::move(requester);
  }

  core::Result<HttpResponse> handle(HttpRequest request) override;

private:
  ApiKeyAuthenticator *authenticator_;
  search::SearchIndex *index_;
  ApiRequestGovernor *governor_;
  index::DuplicateIndexView *duplicates_;
  StatusProvider *status_;
  std::function<core::Result<void>()> refresh_search_;
  std::function<core::Result<void>(bool)> request_maintenance_;
  search::AdultContentMode adult_content_;
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

core::Result<HttpResponse>
method_not_allowed(std::string_view allowed_method = "GET") {
  auto response =
      error_response(405, "method_not_allowed",
                     "Only " + std::string{allowed_method} + " is supported");
  if (response)
    response->headers["allow"] = std::string{allowed_method};
  return response;
}

core::Result<HttpResponse> unauthorized() {
  auto response =
      error_response(401, "unauthorized", "A valid API key is required");
  if (response)
    response->headers["www-authenticate"] = "Bearer realm=\"sakuin\"";
  return response;
}

core::Result<HttpResponse> forbidden(std::string_view message) {
  return error_response(403, "forbidden", message);
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

std::optional<Credential> credential_token(std::string_view token) {
  constexpr std::string_view prefix{"sakuin_"};
  if (!token.starts_with(prefix))
    return std::nullopt;
  token.remove_prefix(prefix.size());
  const auto separator = token.rfind('_');
  if (separator == std::string_view::npos || separator == 0 ||
      token.size() - separator - 1 != 64)
    return std::nullopt;
  Credential result{.key_id = std::string{token.substr(0, separator)}};
  const auto encoded = token.substr(separator + 1);
  for (std::size_t index = 0; index < result.secret.bytes.size(); ++index) {
    const auto high = hex_digit(encoded[index * 2]);
    const auto low = hex_digit(encoded[index * 2 + 1]);
    if (!high || !low)
      return std::nullopt;
    result.secret.bytes[index] = static_cast<std::uint8_t>((*high << 4) | *low);
  }
  return result;
}

std::optional<Credential> credential(std::string_view header) {
  constexpr std::string_view prefix{"Bearer "};
  if (!header.starts_with(prefix))
    return std::nullopt;
  header.remove_prefix(prefix.size());
  return credential_token(header);
}

core::ByteView secret_view(const ApiKeySecret &secret) {
  return {reinterpret_cast<const std::byte *>(secret.bytes.data()),
          secret.bytes.size()};
}

core::Result<std::optional<ApiPrincipal>>
authenticate(ApiKeyAuthenticator &authenticator,
             std::optional<Credential> parsed) {
  if (!parsed)
    return std::optional<ApiPrincipal>{};
  CredentialCleaner cleaner{parsed->secret};
  return authenticator.authenticate(parsed->key_id,
                                    secret_view(parsed->secret));
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

core::Result<std::vector<classification::MediaCategory>>
categories(const std::map<std::string, std::string, std::less<>> &parameters) {
  const auto found = parameters.find("category");
  if (found == parameters.end())
    return {};
  std::vector<classification::MediaCategory> result;
  std::string_view remaining{found->second};
  if (remaining.empty())
    return std::unexpected(core::Error{core::ErrorCode::InvalidQuery,
                                       "category must not be empty"});
  while (!remaining.empty()) {
    const auto separator = remaining.find(',');
    const auto value = remaining.substr(0, separator);
    using enum classification::MediaCategory;
    const auto category =
        [&]() -> std::optional<classification::MediaCategory> {
      if (value == "movie")
        return Movie;
      if (value == "movie_sd")
        return MovieSd;
      if (value == "movie_hd")
        return MovieHd;
      if (value == "movie_uhd")
        return MovieUhd;
      if (value == "series")
        return Series;
      if (value == "series_sd")
        return SeriesSd;
      if (value == "series_hd")
        return SeriesHd;
      if (value == "series_uhd")
        return SeriesUhd;
      if (value == "series_anime")
        return SeriesAnime;
      if (value == "audio")
        return Audio;
      if (value == "audiobook")
        return Audiobook;
      if (value == "application")
        return Application;
      if (value == "game")
        return Game;
      if (value == "books")
        return Books;
      if (value == "ebook")
        return Ebook;
      if (value == "adult")
        return Adult;
      if (value == "other")
        return Other;
      return std::nullopt;
    }();
    if (!category)
      return std::unexpected(
          core::Error{core::ErrorCode::InvalidQuery,
                      "Unknown search category: " + std::string{value}});
    if (!std::ranges::contains(result, *category))
      result.push_back(*category);
    if (separator == std::string_view::npos)
      break;
    remaining.remove_prefix(separator + 1);
  }
  return result;
}

core::Result<std::optional<classification::ClassificationState>>
classification_state(
    const std::map<std::string, std::string, std::less<>> &parameters) {
  const auto found = parameters.find("classification_state");
  if (found == parameters.end())
    return std::optional<classification::ClassificationState>{};
  using enum classification::ClassificationState;
  constexpr std::array values{
      std::pair{"awaiting_metadata", AwaitingMetadata},
      std::pair{"classified", Classified},
      std::pair{"ambiguous", Ambiguous},
      std::pair{"unknown", Unknown},
  };
  const auto value = std::ranges::find(values, found->second,
                                       &decltype(values)::value_type::first);
  if (value == values.end())
    return std::unexpected(core::Error{
        core::ErrorCode::InvalidQuery,
        "classification_state must be awaiting_metadata, classified, "
        "ambiguous, or unknown"});
  return std::optional{value->second};
}

core::Result<std::optional<classification::ContentKind>> content_kind(
    const std::map<std::string, std::string, std::less<>> &parameters) {
  const auto found = parameters.find("content_kind");
  if (found == parameters.end())
    return std::optional<classification::ContentKind>{};
  using enum classification::ContentKind;
  constexpr std::array values{
      std::pair{"unknown", Unknown},     std::pair{"movie", Movie},
      std::pair{"series", Series},       std::pair{"music", Music},
      std::pair{"audiobook", Audiobook}, std::pair{"ebook", Ebook},
      std::pair{"game", Game},           std::pair{"application", Application},
      std::pair{"mixed", Mixed},
  };
  const auto value = std::ranges::find(values, found->second,
                                       &decltype(values)::value_type::first);
  if (value == values.end())
    return std::unexpected(core::Error{
        core::ErrorCode::InvalidQuery,
        "content_kind must be unknown, movie, series, music, audiobook, "
        "ebook, game, application, or mixed"});
  return std::optional{value->second};
}

core::Result<std::optional<classification::Confidence>> optional_confidence(
    const std::map<std::string, std::string, std::less<>> &parameters,
    std::string_view name) {
  const auto found = parameters.find(name);
  if (found == parameters.end())
    return std::optional<classification::Confidence>{};
  using enum classification::Confidence;
  constexpr std::array values{std::pair{"low", Low},
                              std::pair{"medium", Medium},
                              std::pair{"high", High}};
  const auto value = std::ranges::find(values, found->second,
                                       &decltype(values)::value_type::first);
  if (value == values.end())
    return std::unexpected(
        core::Error{core::ErrorCode::InvalidQuery,
                    std::string{name} + " must be low, medium, or high"});
  return std::optional{value->second};
}

core::Result<std::vector<classification::ContentLabel>>
labels(const std::map<std::string, std::string, std::less<>> &parameters) {
  const auto found = parameters.find("label");
  if (found == parameters.end())
    return {};
  if (found->second.empty())
    return std::unexpected(
        core::Error{core::ErrorCode::InvalidQuery, "label must not be empty"});
  std::vector<classification::ContentLabel> result;
  std::string_view remaining{found->second};
  while (!remaining.empty()) {
    const auto separator = remaining.find(',');
    const auto value = remaining.substr(0, separator);
    const auto label = [&]() -> std::optional<classification::ContentLabel> {
      if (value == "adult")
        return classification::ContentLabel::Adult;
      if (value == "anime")
        return classification::ContentLabel::Anime;
      return std::nullopt;
    }();
    if (!label)
      return std::unexpected(
          core::Error{core::ErrorCode::InvalidQuery,
                      "Unknown classification label: " + std::string{value}});
    if (!std::ranges::contains(result, *label))
      result.push_back(*label);
    if (separator == std::string_view::npos)
      break;
    remaining.remove_prefix(separator + 1);
  }
  return result;
}

core::Result<search::SearchQuery> search_query(std::string_view encoded) {
  auto parameters = query_parameters(encoded);
  if (!parameters)
    return std::unexpected(parameters.error());
  for (const auto &[name, _] : *parameters) {
    if (name != "q" && name != "min_size" && name != "max_size" &&
        name != "min_files" && name != "max_files" &&
        name != "first_seen_at_or_after_ms" &&
        name != "last_seen_at_or_before_ms" && name != "offset" &&
        name != "limit" && name != "category" &&
        name != "classification_state" && name != "content_kind" &&
        name != "minimum_kind_confidence" && name != "label" &&
        name != "minimum_label_confidence")
      return std::unexpected(core::Error{core::ErrorCode::InvalidQuery,
                                         "Unknown query parameter: " + name});
  }
  auto minimum = optional_number<std::uint64_t>(*parameters, "min_size");
  auto maximum = optional_number<std::uint64_t>(*parameters, "max_size");
  auto minimum_files = optional_number<std::size_t>(*parameters, "min_files");
  auto maximum_files = optional_number<std::size_t>(*parameters, "max_files");
  auto first_seen =
      optional_timestamp(*parameters, "first_seen_at_or_after_ms");
  auto last_seen = optional_timestamp(*parameters, "last_seen_at_or_before_ms");
  auto offset = optional_number<std::size_t>(*parameters, "offset");
  auto limit = optional_number<std::size_t>(*parameters, "limit");
  auto parsed_categories = categories(*parameters);
  auto parsed_state = classification_state(*parameters);
  auto parsed_kind = content_kind(*parameters);
  auto minimum_kind =
      optional_confidence(*parameters, "minimum_kind_confidence");
  auto parsed_labels = labels(*parameters);
  auto minimum_label =
      optional_confidence(*parameters, "minimum_label_confidence");
  if (!minimum || !maximum || !minimum_files || !maximum_files || !first_seen ||
      !last_seen || !offset || !limit || !parsed_categories || !parsed_state ||
      !parsed_kind || !minimum_kind || !parsed_labels || !minimum_label)
    return std::unexpected(!minimum             ? minimum.error()
                           : !maximum           ? maximum.error()
                           : !minimum_files     ? minimum_files.error()
                           : !maximum_files     ? maximum_files.error()
                           : !first_seen        ? first_seen.error()
                           : !last_seen         ? last_seen.error()
                           : !offset            ? offset.error()
                           : !limit             ? limit.error()
                           : !parsed_categories ? parsed_categories.error()
                           : !parsed_state      ? parsed_state.error()
                           : !parsed_kind       ? parsed_kind.error()
                           : !minimum_kind      ? minimum_kind.error()
                           : !parsed_labels     ? parsed_labels.error()
                                                : minimum_label.error());
  if (*minimum_label && parsed_labels->empty())
    return std::unexpected(
        core::Error{core::ErrorCode::InvalidQuery,
                    "minimum_label_confidence requires at least one label"});
  const auto text = parameters->find("q");
  return search::SearchQuery{
      .text = text == parameters->end() ? std::string{} : text->second,
      .minimum_size = *minimum,
      .maximum_size = *maximum,
      .minimum_file_count = *minimum_files,
      .maximum_file_count = *maximum_files,
      .first_seen_at_or_after = *first_seen,
      .last_seen_at_or_before = *last_seen,
      .classification_state = *parsed_state,
      .content_kind = *parsed_kind,
      .minimum_kind_confidence = *minimum_kind,
      .labels = std::move(*parsed_labels),
      .minimum_label_confidence =
          minimum_label->value_or(classification::Confidence::Low),
      .categories = std::move(*parsed_categories),
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
  else if (algorithm->second == "payload_layout_v1")
    result.algorithm = index::DuplicateFingerprintAlgorithm::PayloadLayoutV1;
  else if (algorithm->second == "release_identity_v1")
    result.algorithm = index::DuplicateFingerprintAlgorithm::ReleaseIdentityV1;
  else
    return std::unexpected(core::Error{
        core::ErrorCode::InvalidQuery,
        "algorithm must be exact_file_layout_v1, normalized_metadata_v1, or "
        "payload_layout_v1, or release_identity_v1"});

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

core::Result<bool> maintenance_verification(std::string_view encoded) {
  if (encoded.empty())
    return false;
  auto parameters = query_parameters(encoded);
  if (!parameters)
    return std::unexpected(parameters.error());
  if (parameters->size() != 1 || !parameters->contains("verify"))
    return std::unexpected(core::Error{
        core::ErrorCode::InvalidQuery,
        "Storage maintenance only accepts the verify query parameter"});
  const auto &value = parameters->at("verify");
  if (value == "true")
    return true;
  if (value == "false")
    return false;
  return std::unexpected(core::Error{core::ErrorCode::InvalidQuery,
                                     "verify must be true or false"});
}

} // namespace

core::Result<HttpResponse> SearchHttpHandler::handle(HttpRequest request) {
  const auto query_start = request.target.find('?');
  const auto path = std::string_view{request.target}.substr(0, query_start);
  const auto query =
      query_start == std::string::npos
          ? std::string_view{}
          : std::string_view{request.target}.substr(query_start + 1);
  if (path == "/v1/health") {
    if (request.method != HttpMethod::Get)
      return method_not_allowed();
    if (!request.body.empty())
      return error_response(400, "invalid_request",
                            "GET requests must not contain a body");
    auto body = json_health();
    if (!body)
      return std::unexpected(body.error());
    return json_response(200, std::move(*body));
  }
  if (path == "/v1/ready") {
    if (request.method != HttpMethod::Get)
      return method_not_allowed();
    if (!request.body.empty())
      return error_response(400, "invalid_request",
                            "GET requests must not contain a body");
    const bool ready = status_ && service_ready(status_->status());
    auto body = json_readiness(ready);
    if (!body)
      return std::unexpected(body.error());
    return json_response(ready ? 200 : 503, std::move(*body));
  }
  if (path == "/openapi.json" || path == "/v1/openapi.json") {
    if (request.method != HttpMethod::Get)
      return method_not_allowed();
    if (!request.body.empty())
      return error_response(400, "invalid_request",
                            "GET requests must not contain a body");
    auto body = openapi_document();
    if (!body)
      return std::unexpected(body.error());
    return json_response(200, std::move(*body));
  }

  const bool torznab_route = path == "/api" || path == "/torznab/api";
  if (torznab_route) {
    if (request.method != HttpMethod::Get)
      return torznab_error("201", "Torznab only supports GET requests");
    if (!request.body.empty())
      return torznab_error("201", "GET requests must not contain a body");
    auto parsed = parse_torznab_request(query);
    if (!parsed)
      return torznab_error("201", parsed.error().message);
    if (parsed->function == TorznabFunction::Capabilities)
      return torznab_capabilities(adult_content_);
    if (parsed->function == TorznabFunction::Unsupported)
      return torznab_error("203", "Function not available");
    auto principal =
        authenticate(*authenticator_, credential_token(parsed->api_key));
    if (!principal)
      return std::unexpected(principal.error());
    if (!*principal)
      return torznab_error("100", "Incorrect user credentials");
    if (!(**principal).allows(Permission::Search))
      return torznab_error("100", "Search permission is required");
    if (governor_) {
      auto admission = governor_->admit((**principal).key_id,
                                        std::chrono::system_clock::now());
      if (!admission)
        return std::unexpected(admission.error());
      if (!admission->allowed)
        return torznab_error("910", "API request limit exceeded");
    }
    parsed->query.adult_content = adult_content_;
    search::SearchResult result;
    if (!parsed->category_filter_matches_nothing) {
      auto found = index_->search(parsed->query);
      if (!found)
        return torznab_error("900", found.error().message);
      result = std::move(*found);
    } else {
      result.source_generation = index_->source_generation();
    }
    return torznab_search_response(result, parsed->query.offset);
  }

  constexpr std::string_view duplicates_path{"/v1/duplicates"};
  const bool duplicate_route =
      path == duplicates_path || path.starts_with("/v1/duplicates/");
  const bool status_route = path == "/v1/status";
  const bool metrics_route = path == "/metrics" || path == "/v1/metrics";
  const bool refresh_route = path == "/v1/operations/search-refresh";
  const bool maintenance_route = path == "/v1/operations/storage-maintenance";
  if (path != "/v1/search" && !duplicate_route && !status_route &&
      !metrics_route && !refresh_route && !maintenance_route)
    return error_response(404, "not_found", "Route not found");
  if (duplicate_route && !duplicates_)
    return error_response(404, "not_found", "Duplicate index is disabled");
  if ((status_route || metrics_route) && !status_)
    return error_response(404, "not_found", "Detailed status is unavailable");
  if (refresh_route && !refresh_search_)
    return error_response(404, "not_found",
                          "Search refresh operation is unavailable");
  if (maintenance_route && !request_maintenance_)
    return error_response(404, "not_found",
                          "Storage maintenance operation is unavailable");
  const bool post_route = refresh_route || maintenance_route;
  if ((post_route && request.method != HttpMethod::Post) ||
      (!post_route && request.method != HttpMethod::Get))
    return method_not_allowed(post_route ? "POST" : "GET");
  if (!request.body.empty())
    return error_response(400, "invalid_request",
                          "This endpoint does not accept a request body");

  const auto authorization = request.headers.find("authorization");
  auto parsed = authorization == request.headers.end()
                    ? std::optional<Credential>{}
                    : credential(authorization->second);
  auto principal = authenticate(*authenticator_, std::move(parsed));
  if (!principal)
    return std::unexpected(principal.error());
  if (!*principal)
    return unauthorized();
  const auto required_permission = status_route || metrics_route || post_route
                                       ? Permission::Admin
                                       : Permission::Search;
  if (!(**principal).allows(required_permission))
    return forbidden(required_permission == Permission::Admin
                         ? "Admin permission is required"
                         : "Search permission is required");
  if (governor_) {
    auto admission = governor_->admit((**principal).key_id,
                                      std::chrono::system_clock::now());
    if (!admission)
      return std::unexpected(admission.error());
    if (!admission->allowed)
      return rate_limited(
          admission->retry_after.value_or(core::Duration::zero()));
  }

  if (status_route) {
    auto status = status_->status();
    status.search_source_generation = index_->source_generation();
    status.search_classification = index_->classification_stats();
    auto body = json_status(status);
    if (!body)
      return std::unexpected(body.error());
    return json_response(200, std::move(*body));
  }
  if (metrics_route) {
    auto status = status_->status();
    status.search_source_generation = index_->source_generation();
    status.search_classification = index_->classification_stats();
    auto body = prometheus_metrics(status);
    if (!body)
      return std::unexpected(body.error());
    return HttpResponse{
        .status = 200,
        .headers = {{"content-type",
                     "text/plain; version=0.0.4; charset=utf-8"},
                    {"cache-control", "no-store"}},
        .body = std::move(*body)};
  }
  if (refresh_route) {
    auto refreshed = refresh_search_();
    if (!refreshed)
      return std::unexpected(refreshed.error());
    constexpr std::string_view completed{
        R"json({"operation":"search_refresh","status":"completed"})json"};
    const auto bytes = std::as_bytes(std::span{completed});
    return json_response(200, {bytes.begin(), bytes.end()});
  }
  if (maintenance_route) {
    auto verify = maintenance_verification(query);
    if (!verify)
      return error_response(400, "invalid_query", verify.error().message);
    auto requested = request_maintenance_(*verify);
    if (!requested)
      return std::unexpected(requested.error());
    const std::string_view response =
        *verify
            ? R"json({"operation":"storage_maintenance","status":"accepted","verification":true})json"
            : R"json({"operation":"storage_maintenance","status":"accepted","verification":false})json";
    const auto bytes = std::as_bytes(std::span{response});
    auto accepted = json_response(202, {bytes.begin(), bytes.end()});
    accepted.headers["location"] = "/v1/status";
    return accepted;
  }

  if (path == "/v1/search") {
    auto parsed_query = search_query(query);
    if (!parsed_query)
      return error_response(400, "invalid_query", parsed_query.error().message);
    parsed_query->adult_content = adult_content_;
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

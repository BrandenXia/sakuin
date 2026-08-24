module;

#include <ctime>

export module sakuin.api.torznab;

import std;

import sakuin.api.http;
import sakuin.core.bytes;
import sakuin.core.ids;
import sakuin.core.result;
import sakuin.core.time;
import sakuin.search.index;

export namespace sakuin::api {

enum class TorznabFunction { Capabilities, Search, Unsupported };

struct TorznabRequest {
  TorznabFunction function{TorznabFunction::Unsupported};
  std::string function_name;
  std::string api_key;
  search::SearchQuery query;
  bool category_matches{true};
};

core::Result<TorznabRequest> parse_torznab_request(std::string_view query);
HttpResponse torznab_capabilities();
HttpResponse torznab_search_response(const search::SearchResult &result,
                                     std::size_t offset);
HttpResponse torznab_error(std::string_view code, std::string_view description);

} // namespace sakuin::api

namespace sakuin::api {
namespace {

constexpr std::size_t DefaultLimit = 50;
constexpr std::size_t MaximumLimit = 100;

std::optional<std::uint8_t> hex_digit(char value) {
  if (value >= '0' && value <= '9')
    return static_cast<std::uint8_t>(value - '0');
  if (value >= 'a' && value <= 'f')
    return static_cast<std::uint8_t>(value - 'a' + 10);
  if (value >= 'A' && value <= 'F')
    return static_cast<std::uint8_t>(value - 'A' + 10);
  return std::nullopt;
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
parameters(std::string_view encoded) {
  std::map<std::string, std::string, std::less<>> result;
  while (!encoded.empty()) {
    const auto separator = encoded.find('&');
    const auto pair = encoded.substr(0, separator);
    const auto equals = pair.find('=');
    auto name = percent_decode(pair.substr(0, equals));
    auto value = percent_decode(equals == std::string_view::npos
                                    ? std::string_view{}
                                    : pair.substr(equals + 1));
    if (!name || !value)
      return std::unexpected(name ? value.error() : name.error());
    std::ranges::transform(*name, name->begin(), [](unsigned char character) {
      return static_cast<char>(std::tolower(character));
    });
    if (name->empty() ||
        !result.emplace(std::move(*name), std::move(*value)).second)
      return std::unexpected(
          core::Error{core::ErrorCode::InvalidQuery,
                      "Torznab parameter names must be nonempty and unique"});
    if (separator == std::string_view::npos)
      break;
    encoded.remove_prefix(separator + 1);
  }
  return result;
}

core::Result<std::size_t>
number(const std::map<std::string, std::string, std::less<>> &values,
       std::string_view name, std::size_t fallback) {
  const auto found = values.find(name);
  if (found == values.end())
    return fallback;
  std::size_t result{};
  const auto [end, error] =
      std::from_chars(found->second.data(),
                      found->second.data() + found->second.size(), result);
  if (error != std::errc{} ||
      end != found->second.data() + found->second.size())
    return std::unexpected(
        core::Error{core::ErrorCode::InvalidQuery,
                    std::string{name} + " must be an unsigned integer"});
  return result;
}

std::string xml_escape(std::string_view value) {
  std::string result;
  result.reserve(value.size());
  for (const auto character : value) {
    switch (character) {
    case '&':
      result += "&amp;";
      break;
    case '<':
      result += "&lt;";
      break;
    case '>':
      result += "&gt;";
      break;
    case '\"':
      result += "&quot;";
      break;
    case '\'':
      result += "&apos;";
      break;
    default:
      result.push_back(character);
      break;
    }
  }
  return result;
}

std::string hexadecimal(const core::InfoHash &hash) {
  constexpr std::string_view digits{"0123456789abcdef"};
  std::string result;
  result.reserve(hash.bytes.size() * 2);
  for (const auto byte : hash.bytes) {
    result.push_back(digits[byte >> 4]);
    result.push_back(digits[byte & 0x0f]);
  }
  return result;
}

std::string percent_encode(std::string_view input) {
  constexpr std::string_view digits{"0123456789ABCDEF"};
  std::string result;
  for (const unsigned char character : input) {
    if (std::isalnum(character) || character == '-' || character == '_' ||
        character == '.' || character == '~') {
      result.push_back(static_cast<char>(character));
    } else {
      result.push_back('%');
      result.push_back(digits[character >> 4]);
      result.push_back(digits[character & 0x0f]);
    }
  }
  return result;
}

std::string rss_date(core::Timestamp timestamp) {
  const auto value = std::chrono::system_clock::to_time_t(timestamp);
  std::tm utc{};
#if defined(_WIN32)
  gmtime_s(&utc, &value);
#else
  gmtime_r(&value, &utc);
#endif
  std::ostringstream output;
  output.imbue(std::locale::classic());
  output << std::put_time(&utc, "%a, %d %b %Y %H:%M:%S +0000");
  return output.str();
}

HttpResponse xml_response(std::string text, std::string_view content_type) {
  const auto view = std::as_bytes(std::span{text});
  return {.status = 200,
          .headers = {{"content-type", std::string{content_type}},
                      {"cache-control", "no-store"}},
          .body = core::ByteBuffer{view.begin(), view.end()}};
}

} // namespace

core::Result<TorznabRequest> parse_torznab_request(std::string_view encoded) {
  auto values = parameters(encoded);
  if (!values)
    return std::unexpected(values.error());
  constexpr std::array allowed{"t",        "apikey", "q",     "cat", "attrs",
                               "extended", "offset", "limit", "o"};
  for (const auto &[name, _] : *values)
    if (std::ranges::find(allowed, name) == allowed.end())
      return std::unexpected(core::Error{core::ErrorCode::InvalidQuery,
                                         "Unknown Torznab parameter: " + name});

  const auto function = values->find("t");
  if (function == values->end() || function->second.empty())
    return std::unexpected(core::Error{core::ErrorCode::InvalidQuery,
                                       "Torznab parameter t is required"});
  TorznabRequest result{.function_name = function->second};
  std::ranges::transform(result.function_name, result.function_name.begin(),
                         [](unsigned char character) {
                           return static_cast<char>(std::tolower(character));
                         });
  if (result.function_name == "caps")
    result.function = TorznabFunction::Capabilities;
  else if (result.function_name == "search")
    result.function = TorznabFunction::Search;
  const auto output = values->find("o");
  if (output != values->end()) {
    auto format = output->second;
    std::ranges::transform(format, format.begin(), [](unsigned char character) {
      return static_cast<char>(std::tolower(character));
    });
    if (format != "xml")
      return std::unexpected(core::Error{
          core::ErrorCode::InvalidQuery,
          "Sakuin's Torznab endpoint currently supports XML output"});
  }
  if (const auto key = values->find("apikey"); key != values->end())
    result.api_key = key->second;
  if (const auto text = values->find("q"); text != values->end())
    result.query.text = text->second;
  auto offset = number(*values, "offset", 0);
  auto limit = number(*values, "limit", DefaultLimit);
  if (!offset || !limit)
    return std::unexpected(offset ? limit.error() : offset.error());
  result.query.offset = *offset;
  result.query.limit = std::min(*limit, MaximumLimit);
  if (const auto category = values->find("cat");
      category != values->end() && !category->second.empty()) {
    result.category_matches = false;
    std::string_view remaining{category->second};
    while (!remaining.empty()) {
      const auto separator = remaining.find(',');
      const auto entry = remaining.substr(0, separator);
      unsigned value{};
      const auto [end, error] =
          std::from_chars(entry.data(), entry.data() + entry.size(), value);
      if (entry.empty() || error != std::errc{} ||
          end != entry.data() + entry.size())
        return std::unexpected(core::Error{core::ErrorCode::InvalidQuery,
                                           "cat must contain integer IDs"});
      if (value == 8000)
        result.category_matches = true;
      if (separator == std::string_view::npos)
        break;
      remaining.remove_prefix(separator + 1);
    }
  }
  return result;
}

HttpResponse torznab_capabilities() {
  return xml_response(
      R"xml(<?xml version="1.0" encoding="UTF-8"?>
<caps>
  <server version="1.3" title="Sakuin" />
  <limits default="50" max="100" />
  <registration available="no" open="no" />
  <searching>
    <search available="yes" supportedParams="q" />
    <tv-search available="no" supportedParams="q" />
    <movie-search available="no" supportedParams="q" />
    <music-search available="no" supportedParams="q" />
    <audio-search available="no" supportedParams="q" />
    <book-search available="no" supportedParams="q" />
  </searching>
  <categories><category id="8000" name="Other" /></categories>
</caps>
)xml",
      "application/xml; charset=utf-8");
}

HttpResponse torznab_search_response(const search::SearchResult &result,
                                     std::size_t offset) {
  std::string xml =
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
      "<rss version=\"2.0\" "
      "xmlns:newznab=\"http://www.newznab.com/DTD/2010/feeds/attributes/\" "
      "xmlns:torznab=\"http://torznab.com/schemas/2015/feed\">\n"
      "<channel><title>Sakuin</title><description>Sakuin BitTorrent DHT "
      "index</description><link>urn:sakuin:torznab</link>\n"
      "<newznab:response offset=\"" +
      std::to_string(offset) + "\" total=\"" +
      std::to_string(result.total_matches) + "\" />\n";
  for (const auto &hit : result.hits) {
    const auto hash = hexadecimal(hit.info_hash);
    const auto title = hit.name.value_or(hash);
    auto magnet = "magnet:?xt=urn:btih:" + hash;
    if (hit.name)
      magnet += "&dn=" + percent_encode(*hit.name);
    const auto escaped_magnet = xml_escape(magnet);
    xml += "<item><title>" + xml_escape(title) +
           "</title><guid isPermaLink=\"false\">" + hash + "</guid><link>" +
           escaped_magnet + "</link><pubDate>" + rss_date(hit.last_seen) +
           "</pubDate><category>Other</category><description>Indexed from "
           "the BitTorrent DHT</description><enclosure url=\"" +
           escaped_magnet + "\" length=\"" + std::to_string(hit.total_size) +
           "\" type=\"application/x-bittorrent\" />"
           "<torznab:attr name=\"category\" value=\"8000\" />"
           "<torznab:attr name=\"size\" value=\"" +
           std::to_string(hit.total_size) +
           "\" /><torznab:attr name=\"files\" value=\"" +
           std::to_string(hit.file_count) +
           "\" /><torznab:attr name=\"infohash\" value=\"" + hash +
           "\" /><torznab:attr name=\"magneturl\" value=\"" + escaped_magnet +
           "\" /></item>\n";
  }
  xml += "</channel></rss>\n";
  return xml_response(std::move(xml), "application/rss+xml; charset=utf-8");
}

HttpResponse torznab_error(std::string_view code,
                           std::string_view description) {
  return xml_response("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<error "
                      "code=\"" +
                          xml_escape(code) + "\" description=\"" +
                          xml_escape(description) + "\" />\n",
                      "application/xml; charset=utf-8");
}

} // namespace sakuin::api

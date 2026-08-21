module;

#include <nlohmann/json.hpp>

export module sakuin.api.json;

import std;

import sakuin.core.bytes;
import sakuin.core.ids;
import sakuin.core.result;
import sakuin.core.time;
import sakuin.search.index;

export namespace sakuin::api {

core::Result<core::ByteBuffer>
json_search_result(const search::SearchResult &result);
core::Result<core::ByteBuffer> json_error(std::string_view code,
                                          std::string_view message);
core::Result<core::ByteBuffer> json_health();

} // namespace sakuin::api

namespace sakuin::api {
namespace {

std::string hex(const core::InfoHash &hash) {
  constexpr std::string_view digits{"0123456789abcdef"};
  std::string result;
  result.reserve(hash.bytes.size() * 2);
  for (const auto byte : hash.bytes) {
    result.push_back(digits[byte >> 4]);
    result.push_back(digits[byte & 0x0f]);
  }
  return result;
}

std::int64_t milliseconds(core::Timestamp timestamp) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             timestamp.time_since_epoch())
      .count();
}

core::Result<core::ByteBuffer> encoded(const nlohmann::json &value) {
  try {
    const auto text =
        value.dump(-1, ' ', false, nlohmann::json::error_handler_t::replace);
    const auto view = std::as_bytes(std::span{text});
    return core::ByteBuffer{view.begin(), view.end()};
  } catch (const std::exception &exception) {
    return std::unexpected(core::Error{
        core::ErrorCode::Internal,
        std::string{"Could not serialize JSON response: "} + exception.what()});
  }
}

} // namespace

core::Result<core::ByteBuffer>
json_search_result(const search::SearchResult &result) {
  nlohmann::json hits = nlohmann::json::array();
  for (const auto &hit : result.hits) {
    nlohmann::json item{{"info_hash", hex(hit.info_hash)},
                        {"total_size", hit.total_size},
                        {"file_count", hit.file_count},
                        {"first_seen_ms", milliseconds(hit.first_seen)},
                        {"last_seen_ms", milliseconds(hit.last_seen)},
                        {"score", hit.score}};
    item["name"] =
        hit.name ? nlohmann::json{*hit.name} : nlohmann::json{nullptr};
    hits.push_back(std::move(item));
  }
  return encoded(nlohmann::json{{"source_generation", result.source_generation},
                                {"total_matches", result.total_matches},
                                {"hits", std::move(hits)}});
}

core::Result<core::ByteBuffer> json_error(std::string_view code,
                                          std::string_view message) {
  return encoded(
      nlohmann::json{{"error", {{"code", code}, {"message", message}}}});
}

core::Result<core::ByteBuffer> json_health() {
  return encoded(nlohmann::json{{"status", "ok"}});
}

} // namespace sakuin::api

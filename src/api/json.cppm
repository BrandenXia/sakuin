module;

#include <nlohmann/json.hpp>

export module sakuin.api.json;

import std;

import sakuin.classification;
import sakuin.core.bytes;
import sakuin.core.hash;
import sakuin.core.ids;
import sakuin.core.result;
import sakuin.core.time;
import sakuin.index.duplicates;
import sakuin.search.index;

export namespace sakuin::api {

core::Result<core::ByteBuffer>
json_search_result(const search::SearchResult &result);
core::Result<core::ByteBuffer>
json_duplicate_groups(std::uint64_t source_generation, std::size_t total_groups,
                      std::span<const index::DuplicateGroup> groups);
core::Result<core::ByteBuffer>
json_duplicate_matches(std::uint64_t source_generation,
                       const core::InfoHash &torrent,
                       std::span<const index::DuplicateGroup> groups);
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

std::string hex(const core::Hash256 &hash) {
  constexpr std::string_view digits{"0123456789abcdef"};
  std::string result;
  result.reserve(hash.bytes.size() * 2);
  for (const auto byte : hash.bytes) {
    result.push_back(digits[byte >> 4]);
    result.push_back(digits[byte & 0x0f]);
  }
  return result;
}

std::string_view
algorithm_name(index::DuplicateFingerprintAlgorithm algorithm) {
  switch (algorithm) {
  case index::DuplicateFingerprintAlgorithm::ExactFileLayoutV1:
    return "exact_file_layout_v1";
  case index::DuplicateFingerprintAlgorithm::NormalizedMetadataV1:
    return "normalized_metadata_v1";
  case index::DuplicateFingerprintAlgorithm::PayloadLayoutV1:
    return "payload_layout_v1";
  case index::DuplicateFingerprintAlgorithm::ReleaseIdentityV1:
    return "release_identity_v1";
  }
  std::unreachable();
}

nlohmann::json duplicate_group(const index::DuplicateGroup &group) {
  nlohmann::json torrents = nlohmann::json::array();
  for (const auto &torrent : group.torrents)
    torrents.push_back(hex(torrent));
  return {{"algorithm", algorithm_name(group.fingerprint.algorithm)},
          {"fingerprint", hex(group.fingerprint.digest)},
          {"torrents", std::move(torrents)}};
}

std::string_view name(classification::ClassificationState value) {
  using enum classification::ClassificationState;
  switch (value) {
  case AwaitingMetadata:
    return "awaiting_metadata";
  case Classified:
    return "classified";
  case Ambiguous:
    return "ambiguous";
  case Unknown:
    return "unknown";
  }
  std::unreachable();
}

std::string_view name(classification::ContentKind value) {
  using enum classification::ContentKind;
  switch (value) {
  case Unknown:
    return "unknown";
  case Movie:
    return "movie";
  case Series:
    return "series";
  case Music:
    return "music";
  case Audiobook:
    return "audiobook";
  case Ebook:
    return "ebook";
  case Game:
    return "game";
  case Application:
    return "application";
  case Mixed:
    return "mixed";
  }
  std::unreachable();
}

std::string_view name(classification::Confidence value) {
  using enum classification::Confidence;
  switch (value) {
  case Unknown:
    return "unknown";
  case Low:
    return "low";
  case Medium:
    return "medium";
  case High:
    return "high";
  }
  std::unreachable();
}

std::string_view name(classification::ContentLabel value) {
  using enum classification::ContentLabel;
  switch (value) {
  case Adult:
    return "adult";
  case Anime:
    return "anime";
  }
  std::unreachable();
}

std::string_view name(classification::VideoResolution value) {
  using enum classification::VideoResolution;
  switch (value) {
  case Sd:
    return "sd";
  case Hd720:
    return "720p";
  case Hd1080:
    return "1080p";
  case Uhd2160:
    return "2160p";
  }
  std::unreachable();
}

std::string_view name(classification::MediaCategory value) {
  using enum classification::MediaCategory;
  switch (value) {
  case Movie:
    return "movie";
  case MovieSd:
    return "movie_sd";
  case MovieHd:
    return "movie_hd";
  case MovieUhd:
    return "movie_uhd";
  case Series:
    return "series";
  case SeriesSd:
    return "series_sd";
  case SeriesHd:
    return "series_hd";
  case SeriesUhd:
    return "series_uhd";
  case SeriesAnime:
    return "series_anime";
  case Audio:
    return "audio";
  case Audiobook:
    return "audiobook";
  case Application:
    return "application";
  case Game:
    return "game";
  case Books:
    return "books";
  case Ebook:
    return "ebook";
  case Adult:
    return "adult";
  case Other:
    return "other";
  }
  std::unreachable();
}

std::string_view name(classification::EvidenceSubject value) {
  using enum classification::EvidenceSubject;
  switch (value) {
  case Movie:
    return "movie";
  case Series:
    return "series";
  case Music:
    return "music";
  case Audiobook:
    return "audiobook";
  case Ebook:
    return "ebook";
  case Game:
    return "game";
  case Application:
    return "application";
  case Mixed:
    return "mixed";
  case Adult:
    return "adult";
  case Anime:
    return "anime";
  case Resolution:
    return "resolution";
  }
  std::unreachable();
}

std::string_view name(classification::EvidenceCode value) {
  using enum classification::EvidenceCode;
  switch (value) {
  case VideoPayloadDominant:
    return "video_payload_dominant";
  case AudioPayloadDominant:
    return "audio_payload_dominant";
  case EbookPayloadDominant:
    return "ebook_payload_dominant";
  case GamePayloadDominant:
    return "game_payload_dominant";
  case ApplicationPayloadDominant:
    return "application_payload_dominant";
  case MultiplePayloadFamilies:
    return "multiple_payload_families";
  case SingleDominantVideo:
    return "single_dominant_video";
  case SeasonEpisodeToken:
    return "season_episode_token";
  case ReleaseYearToken:
    return "release_year_token";
  case MusicReleaseToken:
    return "music_release_token";
  case AudiobookToken:
    return "audiobook_token";
  case EbookToken:
    return "ebook_token";
  case GameToken:
    return "game_token";
  case ApplicationToken:
    return "application_token";
  case FuzzySemanticToken:
    return "fuzzy_semantic_token";
  case LearnedContentModel:
    return "learned_content_model";
  case OperatorRule:
    return "operator_rule";
  case AdultToken:
    return "adult_token";
  case AnimeToken:
    return "anime_token";
  case ResolutionToken:
    return "resolution_token";
  }
  std::unreachable();
}

nlohmann::json classification_json(const search::SearchHit &hit) {
  nlohmann::json labels = nlohmann::json::array();
  for (const auto &label : hit.classification.labels)
    labels.push_back(
        {{"name", name(label.label)}, {"confidence", name(label.confidence)}});
  nlohmann::json categories = nlohmann::json::array();
  for (const auto category : hit.categories)
    categories.push_back(name(category));
  nlohmann::json evidence = nlohmann::json::array();
  for (const auto &item : hit.classification.evidence) {
    nlohmann::json entry{{"code", name(item.code)},
                         {"subject", name(item.subject)},
                         {"weight", item.weight}};
    if (item.rule_id)
      entry["rule_id"] = *item.rule_id;
    evidence.push_back(std::move(entry));
  }
  nlohmann::json result{
      {"state", name(hit.classification.state)},
      {"kind", name(hit.classification.kind)},
      {"confidence", name(hit.classification.kind_confidence)},
      {"labels", std::move(labels)},
      {"categories", std::move(categories)},
      {"evidence", std::move(evidence)},
      {"algorithm_version", hit.classification.algorithm_version},
      {"input_truncated", hit.classification.input_truncated}};
  result["resolution"] =
      hit.classification.resolution
          ? nlohmann::json{name(*hit.classification.resolution)}
          : nlohmann::json{nullptr};
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
                        {"score", hit.score},
                        {"classification", classification_json(hit)}};
    item["name"] =
        hit.name ? nlohmann::json{*hit.name} : nlohmann::json{nullptr};
    hits.push_back(std::move(item));
  }
  return encoded(nlohmann::json{{"source_generation", result.source_generation},
                                {"total_matches", result.total_matches},
                                {"hits", std::move(hits)}});
}

core::Result<core::ByteBuffer>
json_duplicate_groups(std::uint64_t source_generation, std::size_t total_groups,
                      std::span<const index::DuplicateGroup> groups) {
  nlohmann::json encoded_groups = nlohmann::json::array();
  for (const auto &group : groups)
    encoded_groups.push_back(duplicate_group(group));
  return encoded(nlohmann::json{{"source_generation", source_generation},
                                {"total_groups", total_groups},
                                {"groups", std::move(encoded_groups)}});
}

core::Result<core::ByteBuffer>
json_duplicate_matches(std::uint64_t source_generation,
                       const core::InfoHash &torrent,
                       std::span<const index::DuplicateGroup> groups) {
  nlohmann::json encoded_groups = nlohmann::json::array();
  for (const auto &group : groups)
    encoded_groups.push_back(duplicate_group(group));
  return encoded(nlohmann::json{{"source_generation", source_generation},
                                {"info_hash", hex(torrent)},
                                {"groups", std::move(encoded_groups)}});
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

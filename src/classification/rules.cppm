export module sakuin.classification.rules;

import std;

import sakuin.classification.model;
import sakuin.model.torrent;

export namespace sakuin::classification {

Classification classify(const model::TorrentRecord &record,
                        ClassifierOptions options = {});

} // namespace sakuin::classification

namespace sakuin::classification {
namespace {

enum class PayloadFamily {
  Video,
  Audio,
  Ebook,
  Game,
  Application,
  Auxiliary,
  Unknown,
  Count,
};

struct PayloadProfile {
  std::array<std::uint64_t, static_cast<std::size_t>(PayloadFamily::Count)>
      bytes{};
  std::array<std::size_t, static_cast<std::size_t>(PayloadFamily::Count)>
      files{};
  std::uint64_t considered_bytes{};
};

struct TokenProfile {
  std::vector<std::string> values;
  bool truncated{};
};

constexpr std::size_t index(PayloadFamily family) {
  return static_cast<std::size_t>(family);
}

constexpr std::size_t index(ContentKind kind) {
  return static_cast<std::size_t>(kind);
}

std::uint64_t saturated_add(std::uint64_t left, std::uint64_t right) {
  if (right > std::numeric_limits<std::uint64_t>::max() - left)
    return std::numeric_limits<std::uint64_t>::max();
  return left + right;
}

template <std::ranges::input_range Range>
bool contains(std::string_view value, const Range &values) {
  return std::ranges::any_of(values, [&](const auto candidate) {
    return value == std::string_view{candidate};
  });
}

PayloadFamily family(std::string_view path) {
  const auto separator = path.find_last_of("/\\");
  const auto dot = path.find_last_of('.');
  if (dot == std::string_view::npos ||
      (separator != std::string_view::npos && dot < separator))
    return PayloadFamily::Unknown;
  std::string extension{path.substr(dot + 1)};
  std::ranges::transform(extension, extension.begin(), [](unsigned char value) {
    return static_cast<char>(std::tolower(value));
  });

  constexpr std::array video{"mkv",  "mp4", "m4v",  "avi", "mov", "wmv",
                             "webm", "mpg", "mpeg", "ts",  "m2ts"};
  constexpr std::array audio{"flac", "mp3", "m4a",  "aac", "ogg",
                             "opus", "wav", "alac", "ape", "mka"};
  constexpr std::array books{"epub", "mobi", "azw", "azw3",
                             "pdf",  "djvu", "cbz", "cbr"};
  constexpr std::array games{"nes", "sfc", "smc", "gba", "gb",  "gbc",
                             "nds", "3ds", "cia", "nsp", "xci", "wad"};
  constexpr std::array applications{"exe",     "msi", "dmg", "pkg",
                                    "deb",     "rpm", "apk", "appimage",
                                    "flatpak", "snap"};
  constexpr std::array auxiliary{"nfo", "txt",  "md",   "jpg",   "jpeg", "png",
                                 "gif", "webp", "srt",  "ass",   "ssa",  "sub",
                                 "idx", "sfv",  "sha1", "sha256"};

  if (contains(extension, video))
    return PayloadFamily::Video;
  if (contains(extension, audio))
    return PayloadFamily::Audio;
  if (contains(extension, books))
    return PayloadFamily::Ebook;
  if (contains(extension, games))
    return PayloadFamily::Game;
  if (contains(extension, applications))
    return PayloadFamily::Application;
  if (contains(extension, auxiliary))
    return PayloadFamily::Auxiliary;
  return PayloadFamily::Unknown;
}

void add_tokens(std::string_view source, const ClassifierOptions &options,
                TokenProfile &tokens) {
  if (source.size() > options.maximum_path_bytes) {
    source = source.substr(0, options.maximum_path_bytes);
    tokens.truncated = true;
  }
  std::string token;
  token.reserve(32);
  const auto finish = [&]() {
    if (token.empty())
      return true;
    if (tokens.values.size() >= options.maximum_tokens) {
      tokens.truncated = true;
      token.clear();
      return false;
    }
    tokens.values.push_back(std::move(token));
    token.clear();
    return true;
  };
  for (const unsigned char value : source) {
    if (std::isalnum(value)) {
      token.push_back(static_cast<char>(std::tolower(value)));
    } else if (!finish()) {
      return;
    }
  }
  finish();
}

bool has_token(const TokenProfile &tokens, std::string_view value) {
  return std::ranges::find(tokens.values, value) != tokens.values.end();
}

template <std::ranges::input_range Range>
bool has_any_token(const TokenProfile &tokens, const Range &values) {
  return std::ranges::any_of(values, [&](const auto value) {
    return has_token(tokens, std::string_view{value});
  });
}

// Restricted Damerau-Levenshtein distance of exactly one. Length is rejected
// before scanning, so work stays proportional to the small built-in signal
// token rather than an untrusted path length.
bool one_edit_apart(std::string_view left, std::string_view right) {
  if (left == right || left.size() + 1 < right.size() ||
      right.size() + 1 < left.size())
    return false;
  if (left.size() == right.size()) {
    std::array<std::size_t, 2> mismatch{};
    std::size_t count{};
    for (std::size_t index = 0; index < left.size(); ++index) {
      if (left[index] == right[index])
        continue;
      if (count == mismatch.size())
        return false;
      mismatch[count++] = index;
    }
    if (count == 1)
      return true;
    return count == 2 && mismatch[1] == mismatch[0] + 1 &&
           left[mismatch[0]] == right[mismatch[1]] &&
           left[mismatch[1]] == right[mismatch[0]];
  }

  const auto shorter = left.size() < right.size() ? left : right;
  const auto longer = left.size() < right.size() ? right : left;
  std::size_t short_index{};
  std::size_t long_index{};
  bool skipped{};
  while (short_index < shorter.size() && long_index < longer.size()) {
    if (shorter[short_index] == longer[long_index]) {
      ++short_index;
      ++long_index;
      continue;
    }
    if (skipped)
      return false;
    skipped = true;
    ++long_index;
  }
  return true;
}

template <std::ranges::input_range Range>
bool has_fuzzy_semantic_token(const TokenProfile &tokens,
                              const Range &signals) {
  constexpr std::size_t MinimumSignalBytes{8};
  return std::ranges::any_of(signals, [&](const auto candidate) {
    const std::string_view signal{candidate};
    if (signal.size() < MinimumSignalBytes)
      return false;
    return std::ranges::any_of(tokens.values, [&](const auto &token) {
      return one_edit_apart(token, signal);
    });
  });
}

bool season_episode_token(std::string_view token) {
  if (token.size() >= 4 && token.front() == 's') {
    const auto episode = token.find('e', 2);
    if (episode != std::string_view::npos && episode + 1 < token.size() &&
        std::ranges::all_of(
            token.substr(1, episode - 1),
            [](unsigned char value) { return std::isdigit(value); }) &&
        std::ranges::all_of(token.substr(episode + 1), [](unsigned char value) {
          return std::isdigit(value);
        }))
      return true;
  }
  const auto separator = token.find('x');
  return separator != std::string_view::npos && separator > 0 &&
         separator + 1 < token.size() &&
         std::ranges::all_of(
             token.substr(0, separator),
             [](unsigned char value) { return std::isdigit(value); }) &&
         std::ranges::all_of(
             token.substr(separator + 1),
             [](unsigned char value) { return std::isdigit(value); });
}

bool prefixed_number_token(std::string_view token, std::string_view prefix,
                           std::size_t minimum_digits,
                           std::size_t maximum_digits) {
  if (!token.starts_with(prefix))
    return false;
  const auto number = token.substr(prefix.size());
  return number.size() >= minimum_digits && number.size() <= maximum_digits &&
         std::ranges::all_of(
             number, [](unsigned char value) { return std::isdigit(value); });
}

bool season_or_episode_token(std::string_view token) {
  return prefixed_number_token(token, "s", 2, 3) ||
         prefixed_number_token(token, "e", 2, 4) ||
         prefixed_number_token(token, "ep", 1, 4);
}

// Absolute-numbered episode releases commonly use "Title - 01" without an
// SxxExx marker. Requiring a hyphen and two or three digits avoids treating a
// sequel number or four-digit release year as an episode.
bool absolute_episode_notation(std::string_view source,
                               std::size_t maximum_bytes) {
  source = source.substr(0, std::min(source.size(), maximum_bytes));
  for (std::size_t index = 0; index < source.size(); ++index) {
    if (source[index] != '-')
      continue;
    auto cursor = index + 1;
    while (cursor < source.size() &&
           std::isspace(static_cast<unsigned char>(source[cursor])))
      ++cursor;
    const auto first_digit = cursor;
    while (cursor < source.size() &&
           std::isdigit(static_cast<unsigned char>(source[cursor])) &&
           cursor - first_digit < 4)
      ++cursor;
    const auto digits = cursor - first_digit;
    if (digits >= 2 && digits <= 3 &&
        (cursor == source.size() ||
         !std::isalnum(static_cast<unsigned char>(source[cursor]))))
      return true;
  }
  return false;
}

bool release_year_token(std::string_view token) {
  if (token.size() != 4 || !std::ranges::all_of(token, [](unsigned char value) {
        return std::isdigit(value);
      }))
    return false;
  unsigned value{};
  const auto [end, error] =
      std::from_chars(token.data(), token.data() + token.size(), value);
  return error == std::errc{} && end == token.data() + token.size() &&
         value >= 1900 && value <= 2099;
}

bool at_least_percent(std::uint64_t part, std::uint64_t whole,
                      unsigned percent) {
  if (whole == 0)
    return false;
  return static_cast<long double>(part) / static_cast<long double>(whole) >=
         static_cast<long double>(percent) / 100.0L;
}

Confidence confidence_for(int score, int margin) {
  if (score >= 85 && margin >= 35)
    return Confidence::High;
  if (score >= 55 && margin >= 20)
    return Confidence::Medium;
  if (score > 0)
    return Confidence::Low;
  return Confidence::Unknown;
}

Confidence label_confidence_for(int score) {
  if (score >= 60)
    return Confidence::High;
  if (score >= 35)
    return Confidence::Medium;
  if (score > 0)
    return Confidence::Low;
  return Confidence::Unknown;
}

EvidenceSubject subject(ContentKind kind) {
  switch (kind) {
  case ContentKind::Movie:
    return EvidenceSubject::Movie;
  case ContentKind::Series:
    return EvidenceSubject::Series;
  case ContentKind::Music:
    return EvidenceSubject::Music;
  case ContentKind::Audiobook:
    return EvidenceSubject::Audiobook;
  case ContentKind::Ebook:
    return EvidenceSubject::Ebook;
  case ContentKind::Game:
    return EvidenceSubject::Game;
  case ContentKind::Application:
    return EvidenceSubject::Application;
  case ContentKind::Mixed:
    return EvidenceSubject::Mixed;
  case ContentKind::Unknown:
    break;
  }
  return EvidenceSubject::Mixed;
}

} // namespace

Classification classify(const model::TorrentRecord &record,
                        ClassifierOptions options) {
  Classification result{.info_hash = record.info_hash};
  if (record.files.empty()) {
    result.state = ClassificationState::AwaitingMetadata;
    return result;
  }

  PayloadProfile payload;
  TokenProfile tokens;
  bool absolute_episode{};
  if (record.name) {
    add_tokens(*record.name, options, tokens);
    absolute_episode =
        absolute_episode_notation(*record.name, options.maximum_path_bytes);
  }
  const auto inspected =
      std::min(record.files.size(), options.maximum_files_to_inspect);
  result.input_truncated = inspected != record.files.size();
  for (std::size_t file_index = 0; file_index < inspected; ++file_index) {
    const auto &file = record.files[file_index];
    const auto payload_family = family(file.path);
    payload.bytes[index(payload_family)] =
        saturated_add(payload.bytes[index(payload_family)], file.size);
    ++payload.files[index(payload_family)];
    if (payload_family != PayloadFamily::Auxiliary)
      payload.considered_bytes =
          saturated_add(payload.considered_bytes, file.size);
    add_tokens(file.path, options, tokens);
    absolute_episode =
        absolute_episode ||
        absolute_episode_notation(file.path, options.maximum_path_bytes);
  }
  result.input_truncated = result.input_truncated || tokens.truncated;

  constexpr auto kinds = static_cast<std::size_t>(ContentKind::Mixed) + 1;
  std::array<int, kinds> scores{};
  const auto add = [&](ContentKind kind, int weight, EvidenceCode code) {
    scores[index(kind)] += weight;
    result.evidence.push_back(
        {.code = code, .subject = subject(kind), .weight = weight});
  };

  const auto bytes = [&](PayloadFamily family) {
    return payload.bytes[index(family)];
  };
  const bool video_dominant = at_least_percent(bytes(PayloadFamily::Video),
                                               payload.considered_bytes, 65);
  const bool audio_dominant = at_least_percent(bytes(PayloadFamily::Audio),
                                               payload.considered_bytes, 70);
  const bool ebook_dominant = at_least_percent(bytes(PayloadFamily::Ebook),
                                               payload.considered_bytes, 60);
  const bool game_dominant = at_least_percent(bytes(PayloadFamily::Game),
                                              payload.considered_bytes, 60);
  const bool application_dominant = at_least_percent(
      bytes(PayloadFamily::Application), payload.considered_bytes, 60);

  if (video_dominant) {
    add(ContentKind::Movie, 30, EvidenceCode::VideoPayloadDominant);
    if (payload.files[index(PayloadFamily::Video)] == 1)
      add(ContentKind::Movie, 30, EvidenceCode::SingleDominantVideo);
  }
  if (audio_dominant)
    add(ContentKind::Music, 65, EvidenceCode::AudioPayloadDominant);
  if (ebook_dominant)
    add(ContentKind::Ebook, 85, EvidenceCode::EbookPayloadDominant);
  if (game_dominant)
    add(ContentKind::Game, 90, EvidenceCode::GamePayloadDominant);
  if (application_dominant)
    add(ContentKind::Application, 90, EvidenceCode::ApplicationPayloadDominant);

  constexpr std::array series_tokens{
      "tv", "television", "series", "season", "seasons", "episode", "episodes"};
  const bool structured_episode =
      std::ranges::any_of(tokens.values, season_episode_token) ||
      std::ranges::any_of(tokens.values, season_or_episode_token);
  const bool explicit_series = has_any_token(tokens, series_tokens);
  const bool episodic_release = has_token(tokens, "subsplease");
  if (structured_episode)
    add(ContentKind::Series, 120, EvidenceCode::SeasonEpisodeToken);
  else if (video_dominant &&
           (explicit_series || episodic_release || absolute_episode))
    add(ContentKind::Series, 120, EvidenceCode::SeasonEpisodeToken);
  if (video_dominant && std::ranges::any_of(tokens.values, release_year_token))
    add(ContentKind::Movie, 25, EvidenceCode::ReleaseYearToken);

  constexpr std::array music_tokens{"album", "discography", "soundtrack"};
  constexpr std::array audiobook_tokens{"audiobook", "unabridged"};
  constexpr std::array ebook_tokens{"ebook", "ebooks"};
  constexpr std::array game_tokens{"game", "games", "rom", "roms"};
  constexpr std::array application_tokens{"application", "applications",
                                          "software", "installer", "setup"};
  if (audio_dominant && has_any_token(tokens, music_tokens)) {
    add(ContentKind::Music, 30, EvidenceCode::MusicReleaseToken);
  } else if (audio_dominant && has_fuzzy_semantic_token(tokens, music_tokens)) {
    add(ContentKind::Music, 20, EvidenceCode::FuzzySemanticToken);
  }
  if (has_any_token(tokens, audiobook_tokens)) {
    add(ContentKind::Audiobook, 130, EvidenceCode::AudiobookToken);
  } else if (has_fuzzy_semantic_token(tokens, audiobook_tokens)) {
    add(ContentKind::Audiobook, 100, EvidenceCode::FuzzySemanticToken);
  }
  if (has_any_token(tokens, ebook_tokens))
    add(ContentKind::Ebook, 55, EvidenceCode::EbookToken);
  if (has_any_token(tokens, game_tokens))
    add(ContentKind::Game, 60, EvidenceCode::GameToken);
  if (has_any_token(tokens, application_tokens)) {
    add(ContentKind::Application, 55, EvidenceCode::ApplicationToken);
  } else if (has_fuzzy_semantic_token(tokens, application_tokens)) {
    add(ContentKind::Application, 45, EvidenceCode::FuzzySemanticToken);
  }

  constexpr std::size_t MaximumOperatorRules = 128;
  constexpr std::size_t MaximumOperatorTokensPerRule = 32;
  const auto operator_rule_count =
      std::min(options.operator_rules.size(), MaximumOperatorRules);
  for (std::size_t rule_index = 0; rule_index < operator_rule_count;
       ++rule_index) {
    const auto &rule = options.operator_rules[rule_index];
    if (rule.id.empty() || rule.id.size() > 64 || rule.tokens.empty() ||
        rule.tokens.size() > MaximumOperatorTokensPerRule || rule.weight <= 0 ||
        rule.weight > 200 || rule.kind == ContentKind::Unknown ||
        rule.kind == ContentKind::Mixed)
      continue;
    const auto matches = [&](const auto &token) {
      return !token.empty() && has_token(tokens, token);
    };
    const bool matched = rule.match == OperatorRuleMatch::Any
                             ? std::ranges::any_of(rule.tokens, matches)
                             : std::ranges::all_of(rule.tokens, matches);
    if (!matched)
      continue;
    scores[index(rule.kind)] += rule.weight;
    result.evidence.push_back({.code = EvidenceCode::OperatorRule,
                               .subject = subject(rule.kind),
                               .weight = rule.weight,
                               .rule_id = rule.id});
  }

  constexpr std::array anime_tokens{"anime", "hentai", "subsplease"};
  if (has_any_token(tokens, anime_tokens)) {
    const int anime_score = has_token(tokens, "subsplease") ? 45 : 60;
    result.labels.push_back({.label = ContentLabel::Anime,
                             .confidence = label_confidence_for(anime_score)});
    result.evidence.push_back({.code = EvidenceCode::AnimeToken,
                               .subject = EvidenceSubject::Anime,
                               .weight = anime_score});
  }

  if (options.adult_detection_enabled) {
    constexpr std::array strong_adult_tokens{"porn", "pornography", "hentai",
                                             "nsfw"};
    constexpr std::array contextual_adult_tokens{"adult", "xxx"};
    int adult_score{};
    if (has_any_token(tokens, strong_adult_tokens))
      adult_score += 60;
    if (has_any_token(tokens, contextual_adult_tokens))
      adult_score += 35;
    if (adult_score > 0) {
      result.labels.push_back(
          {.label = ContentLabel::Adult,
           .confidence = label_confidence_for(adult_score)});
      result.evidence.push_back({.code = EvidenceCode::AdultToken,
                                 .subject = EvidenceSubject::Adult,
                                 .weight = adult_score});
    }
  }

  const auto resolution = [&](VideoResolution value, int weight) {
    result.resolution = value;
    result.evidence.push_back({.code = EvidenceCode::ResolutionToken,
                               .subject = EvidenceSubject::Resolution,
                               .weight = weight});
  };
  if (has_token(tokens, "2160p") || has_token(tokens, "4k"))
    resolution(VideoResolution::Uhd2160, 60);
  else if (has_token(tokens, "1080p") || has_token(tokens, "1080i"))
    resolution(VideoResolution::Hd1080, 60);
  else if (has_token(tokens, "720p"))
    resolution(VideoResolution::Hd720, 60);
  else if (has_token(tokens, "480p") || has_token(tokens, "576p"))
    resolution(VideoResolution::Sd, 60);

  std::size_t substantial_families{};
  for (const auto family :
       {PayloadFamily::Video, PayloadFamily::Audio, PayloadFamily::Ebook,
        PayloadFamily::Game, PayloadFamily::Application})
    if (at_least_percent(bytes(family), payload.considered_bytes, 25))
      ++substantial_families;

  auto best_kind = ContentKind::Unknown;
  int best_score{};
  int second_score{};
  for (const auto kind :
       {ContentKind::Movie, ContentKind::Series, ContentKind::Music,
        ContentKind::Audiobook, ContentKind::Ebook, ContentKind::Game,
        ContentKind::Application}) {
    const auto score = scores[index(kind)];
    if (score > best_score) {
      second_score = best_score;
      best_score = score;
      best_kind = kind;
    } else if (score > second_score) {
      second_score = score;
    }
  }

  if (substantial_families >= 2 && best_score < 100) {
    result.kind = ContentKind::Mixed;
    result.kind_confidence = Confidence::Medium;
    result.state = ClassificationState::Classified;
    result.evidence.push_back({.code = EvidenceCode::MultiplePayloadFamilies,
                               .subject = EvidenceSubject::Mixed,
                               .weight = 65});
    return result;
  }
  if (best_score == 0) {
    result.state = ClassificationState::Unknown;
    return result;
  }

  result.kind = best_kind;
  result.kind_confidence =
      confidence_for(best_score, best_score - second_score);
  result.state = best_score - second_score < 20
                     ? ClassificationState::Ambiguous
                     : ClassificationState::Classified;
  return result;
}

} // namespace sakuin::classification

export module sakuin.classification.model;

import std;

import sakuin.core.ids;

export namespace sakuin::classification {

inline constexpr std::uint32_t AlgorithmVersion{1};

enum class ContentKind {
  Unknown,
  Movie,
  Series,
  Music,
  Audiobook,
  Ebook,
  Game,
  Application,
  Mixed,
};

enum class ClassificationState {
  AwaitingMetadata,
  Classified,
  Ambiguous,
  Unknown,
};

enum class Confidence { Unknown, Low, Medium, High };

enum class ContentLabel { Adult, Anime };

enum class VideoResolution { Sd, Hd720, Hd1080, Uhd2160 };

enum class EvidenceSubject {
  Movie,
  Series,
  Music,
  Audiobook,
  Ebook,
  Game,
  Application,
  Mixed,
  Adult,
  Anime,
  Resolution,
};

enum class EvidenceCode {
  VideoPayloadDominant,
  AudioPayloadDominant,
  EbookPayloadDominant,
  GamePayloadDominant,
  ApplicationPayloadDominant,
  MultiplePayloadFamilies,
  SingleDominantVideo,
  SeasonEpisodeToken,
  ReleaseYearToken,
  MusicReleaseToken,
  AudiobookToken,
  EbookToken,
  GameToken,
  ApplicationToken,
  AdultToken,
  AnimeToken,
  ResolutionToken,
};

struct Evidence {
  EvidenceCode code;
  EvidenceSubject subject;
  int weight;

  friend bool operator==(const Evidence &, const Evidence &) = default;
};

struct LabelAssessment {
  ContentLabel label;
  Confidence confidence;

  friend bool operator==(const LabelAssessment &,
                         const LabelAssessment &) = default;
};

struct ClassifierOptions {
  // Detection and visibility are separate policies. Turning this off only
  // suppresses the Adult label; it must never remove a torrent from search.
  bool adult_detection_enabled{true};
  std::size_t maximum_files_to_inspect{100'000};
  std::size_t maximum_path_bytes{4'096};
  std::size_t maximum_tokens{16'384};
};

struct Classification {
  core::InfoHash info_hash;
  ClassificationState state{ClassificationState::Unknown};
  ContentKind kind{ContentKind::Unknown};
  Confidence kind_confidence{Confidence::Unknown};
  std::vector<LabelAssessment> labels;
  std::optional<VideoResolution> resolution;
  std::vector<Evidence> evidence;
  bool input_truncated{};
  std::uint32_t algorithm_version{AlgorithmVersion};

  friend bool operator==(const Classification &,
                         const Classification &) = default;
};

std::optional<Confidence> label_confidence(const Classification &result,
                                           ContentLabel label) noexcept {
  const auto found =
      std::ranges::find(result.labels, label, &LabelAssessment::label);
  if (found == result.labels.end())
    return std::nullopt;
  return found->confidence;
}

} // namespace sakuin::classification

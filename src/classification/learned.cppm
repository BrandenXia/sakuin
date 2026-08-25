export module sakuin.classification.learned;

import std;

import sakuin.classification.model;
import sakuin.model.torrent;

export namespace sakuin::classification {

struct LearnedClassifierOptions {
  bool enabled{true};
  std::size_t minimum_examples_per_kind{8};
  std::size_t minimum_feature_occurrences{3};
  std::size_t maximum_candidate_features{65'536};
  std::size_t maximum_vocabulary{4'096};
  std::size_t maximum_features_per_record{256};
  double minimum_probability{0.75};
  double minimum_margin{0.25};
};

struct LearnedTrainingExample {
  const model::TorrentRecord *record{};
  const Classification *classification{};
};

struct LearnedClassifierStats {
  bool enabled{};
  bool ready{};
  std::size_t training_records{};
  std::size_t eligible_kinds{};
  std::size_t vocabulary_size{};
};

struct LearnedPrediction {
  ContentKind kind{ContentKind::Unknown};
  double probability{};
  double margin{};
};

// A bounded, rebuildable multinomial Naive Bayes fallback. Training labels are
// supplied only by high-confidence deterministic classifications. The model
// predicts content kind; sensitive and descriptive labels remain exclusively
// deterministic and operator-controlled.
class LearnedContentClassifier {
public:
  static LearnedContentClassifier
  train(std::span<const LearnedTrainingExample> examples,
        ClassifierOptions limits = {}, LearnedClassifierOptions options = {});

  std::optional<LearnedPrediction>
  predict(const model::TorrentRecord &record) const;
  Classification apply(const model::TorrentRecord &record,
                       Classification baseline) const;
  const LearnedClassifierStats &stats() const noexcept { return stats_; }

private:
  static constexpr std::size_t KindCount =
      std::to_underlying(ContentKind::Mixed) + 1U;

  ClassifierOptions limits_;
  LearnedClassifierOptions options_;
  LearnedClassifierStats stats_;
  std::vector<std::string> vocabulary_;
  std::vector<ContentKind> eligible_;
  std::array<std::map<std::string, std::size_t, std::less<>>, KindCount>
      counts_;
  std::array<std::size_t, KindCount> feature_totals_{};
};

} // namespace sakuin::classification

namespace sakuin::classification {
namespace {

constexpr std::size_t index(ContentKind kind) {
  return std::to_underlying(kind);
}

bool learnable(ContentKind kind) {
  return kind != ContentKind::Unknown && kind != ContentKind::Mixed;
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
  case ContentKind::Unknown:
    return EvidenceSubject::Mixed;
  }
  std::unreachable();
}

bool numeric(std::string_view value) {
  return !value.empty() && std::ranges::all_of(value, [](unsigned char byte) {
    return std::isdigit(byte);
  });
}

void append_feature(std::string feature, std::size_t maximum,
                    std::vector<std::string> &features) {
  if (features.size() < maximum && !std::ranges::contains(features, feature))
    features.push_back(std::move(feature));
}

void add_text_features(std::string_view source, std::size_t maximum_bytes,
                       std::size_t maximum_features,
                       std::vector<std::string> &features) {
  source = source.substr(0, std::min(source.size(), maximum_bytes));
  std::string token;
  const auto finish = [&] {
    if (token.size() >= 3 && !numeric(token))
      append_feature("token:" + token, maximum_features, features);
    token.clear();
  };
  for (const unsigned char byte : source) {
    if (std::isalnum(byte))
      token.push_back(static_cast<char>(std::tolower(byte)));
    else
      finish();
  }
  finish();
}

void add_extension_feature(std::string_view path, std::size_t maximum_features,
                           std::vector<std::string> &features) {
  const auto separator = path.find_last_of("/\\");
  const auto dot = path.find_last_of('.');
  if (dot == std::string_view::npos || dot + 1 == path.size() ||
      (separator != std::string_view::npos && dot < separator))
    return;
  auto extension = path.substr(dot + 1);
  if (extension.size() > 12 ||
      !std::ranges::all_of(
          extension, [](unsigned char byte) { return std::isalnum(byte); }))
    return;
  std::string feature{"extension:"};
  for (const unsigned char byte : extension)
    feature.push_back(static_cast<char>(std::tolower(byte)));
  append_feature(std::move(feature), maximum_features, features);
}

std::vector<std::string> features(const model::TorrentRecord &record,
                                  const ClassifierOptions &limits,
                                  const LearnedClassifierOptions &options) {
  std::vector<std::string> result;
  result.reserve(std::min(options.maximum_features_per_record,
                          static_cast<std::size_t>(256)));
  if (record.files.size() == 1)
    append_feature("layout:single", options.maximum_features_per_record,
                   result);
  else if (record.files.size() <= 5)
    append_feature("layout:few", options.maximum_features_per_record, result);
  else
    append_feature("layout:many", options.maximum_features_per_record, result);
  if (record.total_size < 10U * 1024U * 1024U)
    append_feature("size:tiny", options.maximum_features_per_record, result);
  else if (record.total_size < 1024ULL * 1024ULL * 1024ULL)
    append_feature("size:medium", options.maximum_features_per_record, result);
  else
    append_feature("size:large", options.maximum_features_per_record, result);
  if (record.name)
    add_text_features(*record.name, limits.maximum_path_bytes,
                      options.maximum_features_per_record, result);
  const auto inspected =
      std::min(record.files.size(), limits.maximum_files_to_inspect);
  for (std::size_t file_index = 0; file_index < inspected; ++file_index) {
    if (result.size() >= options.maximum_features_per_record)
      break;
    const auto &file = record.files[file_index];
    add_text_features(file.path, limits.maximum_path_bytes,
                      options.maximum_features_per_record, result);
    add_extension_feature(file.path, options.maximum_features_per_record,
                          result);
  }

  std::ranges::sort(result);
  return result;
}

} // namespace

LearnedContentClassifier LearnedContentClassifier::train(
    std::span<const LearnedTrainingExample> examples, ClassifierOptions limits,
    LearnedClassifierOptions options) {
  LearnedContentClassifier result;
  result.limits_ = limits;
  result.options_ = options;
  result.stats_.enabled = options.enabled;
  if (!options.enabled || options.minimum_examples_per_kind == 0 ||
      options.minimum_feature_occurrences == 0 ||
      options.maximum_candidate_features == 0 ||
      options.maximum_vocabulary == 0 ||
      options.maximum_features_per_record == 0 ||
      options.minimum_probability < 0.0 || options.minimum_probability > 1.0 ||
      options.minimum_margin < 0.0 || options.minimum_margin > 1.0)
    return result;

  std::array<std::size_t, KindCount> documents{};
  std::map<std::string, std::size_t, std::less<>> global_counts;
  for (const auto &example : examples) {
    if (!example.record || !example.classification ||
        example.classification->state != ClassificationState::Classified ||
        example.classification->kind_confidence != Confidence::High ||
        !learnable(example.classification->kind))
      continue;
    ++result.stats_.training_records;
    const auto kind_index = index(example.classification->kind);
    ++documents[kind_index];
    for (const auto &feature : features(*example.record, limits, options)) {
      const auto existing = global_counts.find(feature);
      if (existing == global_counts.end()) {
        if (global_counts.size() >= options.maximum_candidate_features)
          continue;
        global_counts.emplace(feature, 1);
      } else {
        ++existing->second;
      }
      ++result.counts_[kind_index][feature];
    }
  }

  for (const auto kind :
       {ContentKind::Movie, ContentKind::Series, ContentKind::Music,
        ContentKind::Audiobook, ContentKind::Ebook, ContentKind::Game,
        ContentKind::Application})
    if (documents[index(kind)] >= options.minimum_examples_per_kind)
      result.eligible_.push_back(kind);

  std::vector<std::pair<std::string, std::size_t>> vocabulary;
  for (const auto &[feature, count] : global_counts)
    if (count >= options.minimum_feature_occurrences)
      vocabulary.emplace_back(feature, count);
  std::ranges::sort(vocabulary, [](const auto &left, const auto &right) {
    return left.second != right.second ? left.second > right.second
                                       : left.first < right.first;
  });
  if (vocabulary.size() > options.maximum_vocabulary)
    vocabulary.resize(options.maximum_vocabulary);
  result.vocabulary_.reserve(vocabulary.size());
  for (auto &[feature, _] : vocabulary)
    result.vocabulary_.push_back(std::move(feature));
  std::ranges::sort(result.vocabulary_);

  for (const auto kind : result.eligible_) {
    auto &counts = result.counts_[index(kind)];
    for (auto iterator = counts.begin(); iterator != counts.end();) {
      if (!std::ranges::binary_search(result.vocabulary_, iterator->first)) {
        iterator = counts.erase(iterator);
      } else {
        result.feature_totals_[index(kind)] += iterator->second;
        ++iterator;
      }
    }
  }
  result.stats_.eligible_kinds = result.eligible_.size();
  result.stats_.vocabulary_size = result.vocabulary_.size();
  result.stats_.ready =
      result.eligible_.size() >= 2 && !result.vocabulary_.empty();
  return result;
}

std::optional<LearnedPrediction>
LearnedContentClassifier::predict(const model::TorrentRecord &record) const {
  if (!stats_.ready || record.files.empty())
    return std::nullopt;
  const auto record_features = features(record, limits_, options_);
  std::vector<double> scores;
  scores.reserve(eligible_.size());
  for (const auto kind : eligible_) {
    const auto kind_index = index(kind);
    const auto denominator =
        static_cast<double>(feature_totals_[kind_index] + vocabulary_.size());
    double score = -std::log(static_cast<double>(eligible_.size()));
    for (const auto &feature : record_features) {
      if (!std::ranges::binary_search(vocabulary_, feature))
        continue;
      const auto found = counts_[kind_index].find(feature);
      const auto count = found == counts_[kind_index].end() ? 0 : found->second;
      score += std::log((static_cast<double>(count) + 1.0) / denominator);
    }
    scores.push_back(score);
  }
  const auto maximum = *std::ranges::max_element(scores);
  double total{};
  std::vector<double> probabilities;
  probabilities.reserve(scores.size());
  for (const auto score : scores) {
    probabilities.push_back(std::exp(score - maximum));
    total += probabilities.back();
  }
  for (auto &probability : probabilities)
    probability /= total;

  const auto best = std::ranges::max_element(probabilities);
  const auto best_index =
      static_cast<std::size_t>(best - probabilities.begin());
  double second{};
  for (std::size_t candidate = 0; candidate < probabilities.size(); ++candidate)
    if (candidate != best_index)
      second = std::max(second, probabilities[candidate]);
  const auto margin = *best - second;
  if (*best < options_.minimum_probability || margin < options_.minimum_margin)
    return std::nullopt;
  return LearnedPrediction{
      .kind = eligible_[best_index], .probability = *best, .margin = margin};
}

Classification
LearnedContentClassifier::apply(const model::TorrentRecord &record,
                                Classification baseline) const {
  if (!options_.enabled || record.files.empty() ||
      (baseline.state != ClassificationState::Unknown &&
       baseline.state != ClassificationState::Ambiguous))
    return baseline;
  const auto prediction = predict(record);
  if (!prediction)
    return baseline;
  baseline.state = ClassificationState::Classified;
  baseline.kind = prediction->kind;
  // Weak supervision can improve coverage, but it must not claim the same
  // authority as a high-confidence deterministic result.
  baseline.kind_confidence = Confidence::Medium;
  baseline.evidence.push_back({.code = EvidenceCode::LearnedContentModel,
                               .subject = subject(prediction->kind),
                               .weight = static_cast<int>(std::lround(
                                   prediction->probability * 100.0))});
  return baseline;
}

} // namespace sakuin::classification

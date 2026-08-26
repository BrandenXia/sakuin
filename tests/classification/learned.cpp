import std;

import sakuin.classification;
import sakuin.core;
import sakuin.model.torrent;

namespace {

sakuin::model::TorrentRecord record(std::uint8_t seed, std::string name,
                                    std::string path, std::uint64_t size) {
  sakuin::core::InfoHash hash;
  hash.bytes.fill(seed);
  return {.info_hash = hash,
          .name = std::move(name),
          .total_size = size,
          .files = {{.path = std::move(path), .size = size}}};
}

} // namespace

int main() {
  using namespace sakuin;
  using namespace classification;

  std::vector<model::TorrentRecord> training{
      record(1, "Workbench Tool Suite", "workbench.rpm", 120'000'000),
      record(2, "Workbench Tool Package", "workbench.exe", 130'000'000),
      record(3, "Workbench Tool Release", "workbench.apk", 140'000'000),
      record(4, "Cinema Feature 2021", "cinema-2021.mkv", 2'000'000'000),
      record(5, "Cinema Feature 2022", "cinema-2022.mp4", 2'100'000'000),
      record(6, "Cinema Feature 2023", "cinema-2023.webm", 2'200'000'000)};
  std::vector<Classification> deterministic;
  std::vector<LearnedTrainingExample> examples;
  deterministic.reserve(training.size());
  examples.reserve(training.size());
  for (const auto &item : training)
    deterministic.push_back(classify(item));
  for (std::size_t index = 0; index < training.size(); ++index)
    examples.push_back(
        {.record = &training[index], .classification = &deterministic[index]});

  const auto model =
      LearnedContentClassifier::train(examples, {},
                                      {.minimum_examples_per_kind = 3,
                                       .minimum_feature_occurrences = 2,
                                       .minimum_probability = 0.65,
                                       .minimum_margin = 0.20});
  if (!model.stats().ready || model.stats().training_records != 6 ||
      model.stats().eligible_kinds != 2 || model.stats().vocabulary_size == 0)
    return 1;

  const auto unknown_record =
      record(7, "Workbench Tool Archive", "payload.bin", 125'000'000);
  const auto unknown = classify(unknown_record);
  if (unknown.state != ClassificationState::Unknown)
    return 2;
  const auto learned = model.apply(unknown_record, unknown);
  if (learned.state != ClassificationState::Classified ||
      learned.kind != ContentKind::Application ||
      learned.kind_confidence != Confidence::Medium ||
      !std::ranges::contains(
          learned.evidence, EvidenceCode::LearnedContentModel, &Evidence::code))
    return 3;

  const auto labeled_record =
      record(8, "Workbench Tool Porn Archive", "payload.bin", 125'000'000);
  const auto labeled_baseline = classify(labeled_record);
  const auto labeled = model.apply(labeled_record, labeled_baseline);
  if (labeled.kind != ContentKind::Application ||
      label_confidence(labeled, ContentLabel::Adult) != Confidence::High)
    return 4;

  const auto authoritative_record =
      record(9, "Workbench Tool Movie 2024", "feature.mkv", 2'000'000'000);
  const auto authoritative = classify(authoritative_record);
  const auto unchanged = model.apply(authoritative_record, authoritative);
  if (authoritative.state != ClassificationState::Classified ||
      authoritative.kind != ContentKind::Movie || unchanged != authoritative)
    return 5;

  const auto awaiting_record = record(10, "", "", 0);
  auto metadata_missing = awaiting_record;
  metadata_missing.files.clear();
  if (const auto awaiting = classify(metadata_missing);
      model.apply(metadata_missing, awaiting) != awaiting)
    return 6;

  const auto disabled = LearnedContentClassifier::train(
      examples, {}, {.enabled = false, .minimum_examples_per_kind = 3});
  if (disabled.stats().ready ||
      disabled.apply(unknown_record, unknown) != unknown)
    return 7;
  return 0;
}

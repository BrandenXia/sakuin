import std;

import sakuin.classification;
import sakuin.core;
import sakuin.model.torrent;

namespace {

using sakuin::classification::Classification;
using sakuin::classification::ClassificationState;
using sakuin::classification::ClassifierOptions;
using sakuin::classification::Confidence;
using sakuin::classification::ContentKind;
using sakuin::classification::ContentLabel;

struct ExpectedLabel {
  ContentLabel label;
  Confidence minimum_confidence;
};

struct CorpusCase {
  std::string id;
  ClassificationState state;
  ContentKind kind;
  Confidence minimum_kind_confidence;
  bool adult_detection_enabled;
  std::vector<ExpectedLabel> required_labels;
  std::vector<ContentLabel> forbidden_labels;
  std::optional<std::string> name;
  std::vector<sakuin::model::FileRecord> files;
};

std::string_view trim(std::string_view value) {
  while (!value.empty() &&
         std::isspace(static_cast<unsigned char>(value.front())))
    value.remove_prefix(1);
  while (!value.empty() &&
         std::isspace(static_cast<unsigned char>(value.back())))
    value.remove_suffix(1);
  return value;
}

std::vector<std::string_view> split(std::string_view value, char delimiter) {
  std::vector<std::string_view> fields;
  while (true) {
    const auto separator = value.find(delimiter);
    fields.push_back(value.substr(0, separator));
    if (separator == std::string_view::npos)
      return fields;
    value.remove_prefix(separator + 1);
  }
}

[[noreturn]] void invalid(std::size_t line, std::string_view message) {
  throw std::runtime_error("line " + std::to_string(line) + ": " +
                           std::string{message});
}

ClassificationState parse_state(std::string_view value, std::size_t line) {
  if (value == "awaiting_metadata")
    return ClassificationState::AwaitingMetadata;
  if (value == "classified")
    return ClassificationState::Classified;
  if (value == "ambiguous")
    return ClassificationState::Ambiguous;
  if (value == "unknown")
    return ClassificationState::Unknown;
  invalid(line, "invalid classification state");
}

ContentKind parse_kind(std::string_view value, std::size_t line) {
  constexpr std::array values{
      std::pair{"unknown", ContentKind::Unknown},
      std::pair{"movie", ContentKind::Movie},
      std::pair{"series", ContentKind::Series},
      std::pair{"music", ContentKind::Music},
      std::pair{"audiobook", ContentKind::Audiobook},
      std::pair{"ebook", ContentKind::Ebook},
      std::pair{"game", ContentKind::Game},
      std::pair{"application", ContentKind::Application},
      std::pair{"mixed", ContentKind::Mixed},
  };
  for (const auto &[name, kind] : values)
    if (value == name)
      return kind;
  invalid(line, "invalid content kind");
}

Confidence parse_confidence(std::string_view value, std::size_t line) {
  if (value == "unknown")
    return Confidence::Unknown;
  if (value == "low")
    return Confidence::Low;
  if (value == "medium")
    return Confidence::Medium;
  if (value == "high")
    return Confidence::High;
  invalid(line, "invalid confidence");
}

ContentLabel parse_label(std::string_view value, std::size_t line) {
  if (value == "adult")
    return ContentLabel::Adult;
  if (value == "anime")
    return ContentLabel::Anime;
  invalid(line, "invalid content label");
}

bool parse_bool(std::string_view value, std::size_t line) {
  if (value == "true")
    return true;
  if (value == "false")
    return false;
  invalid(line, "expected true or false");
}

std::uint64_t parse_size(std::string_view value, std::size_t line) {
  std::uint64_t result{};
  const auto [end, error] =
      std::from_chars(value.data(), value.data() + value.size(), result);
  if (error != std::errc{} || end != value.data() + value.size())
    invalid(line, "invalid file size");
  return result;
}

std::vector<ExpectedLabel> parse_required_labels(std::string_view value,
                                                 std::size_t line) {
  std::vector<ExpectedLabel> labels;
  if (value == "-")
    return labels;
  for (const auto entry : split(value, ',')) {
    const auto fields = split(entry, ':');
    if (fields.size() != 2)
      invalid(line, "required labels must use label:confidence");
    labels.push_back(
        {.label = parse_label(trim(fields[0]), line),
         .minimum_confidence = parse_confidence(trim(fields[1]), line)});
  }
  return labels;
}

std::vector<ContentLabel> parse_forbidden_labels(std::string_view value,
                                                 std::size_t line) {
  std::vector<ContentLabel> labels;
  if (value == "-")
    return labels;
  for (const auto entry : split(value, ','))
    labels.push_back(parse_label(trim(entry), line));
  return labels;
}

std::vector<sakuin::model::FileRecord> parse_files(std::string_view value,
                                                   std::size_t line) {
  std::vector<sakuin::model::FileRecord> files;
  if (value == "-")
    return files;
  for (const auto entry : split(value, ';')) {
    const auto separator = entry.find('|');
    if (separator == std::string_view::npos || separator == 0 ||
        separator + 1 == entry.size())
      invalid(line, "files must use size|path");
    files.push_back({.path = std::string{entry.substr(separator + 1)},
                     .size = parse_size(entry.substr(0, separator), line)});
  }
  return files;
}

CorpusCase parse_case(std::string_view row, std::size_t line) {
  const auto fields = split(row, '\t');
  if (fields.size() != 9)
    invalid(line, "expected 9 tab-separated fields");
  if (fields[0].empty())
    invalid(line, "case id may not be empty");
  return {
      .id = std::string{fields[0]},
      .state = parse_state(fields[1], line),
      .kind = parse_kind(fields[2], line),
      .minimum_kind_confidence = parse_confidence(fields[3], line),
      .adult_detection_enabled = parse_bool(fields[4], line),
      .required_labels = parse_required_labels(fields[5], line),
      .forbidden_labels = parse_forbidden_labels(fields[6], line),
      .name = fields[7] == "-" ? std::nullopt
                               : std::optional{std::string{fields[7]}},
      .files = parse_files(fields[8], line),
  };
}

std::vector<CorpusCase> load_corpus(const std::filesystem::path &path) {
  std::ifstream input{path};
  if (!input)
    throw std::runtime_error("could not open corpus: " + path.string());

  std::vector<CorpusCase> cases;
  std::set<std::string> ids;
  std::string row;
  std::size_t line{};
  while (std::getline(input, row)) {
    ++line;
    if (!row.empty() && row.back() == '\r')
      row.pop_back();
    const auto value = trim(row);
    if (value.empty() || value.starts_with('#'))
      continue;
    auto test_case = parse_case(value, line);
    if (!ids.insert(test_case.id).second)
      invalid(line, "duplicate case id");
    cases.push_back(std::move(test_case));
  }
  if (!input.eof())
    throw std::runtime_error("could not read corpus: " + path.string());
  if (cases.empty())
    throw std::runtime_error("corpus contains no cases");
  return cases;
}

constexpr std::string_view name(ClassificationState value) {
  switch (value) {
  case ClassificationState::AwaitingMetadata:
    return "awaiting_metadata";
  case ClassificationState::Classified:
    return "classified";
  case ClassificationState::Ambiguous:
    return "ambiguous";
  case ClassificationState::Unknown:
    return "unknown";
  }
  return "invalid";
}

constexpr std::string_view name(ContentKind value) {
  switch (value) {
  case ContentKind::Unknown:
    return "unknown";
  case ContentKind::Movie:
    return "movie";
  case ContentKind::Series:
    return "series";
  case ContentKind::Music:
    return "music";
  case ContentKind::Audiobook:
    return "audiobook";
  case ContentKind::Ebook:
    return "ebook";
  case ContentKind::Game:
    return "game";
  case ContentKind::Application:
    return "application";
  case ContentKind::Mixed:
    return "mixed";
  }
  return "invalid";
}

constexpr std::string_view name(Confidence value) {
  switch (value) {
  case Confidence::Unknown:
    return "unknown";
  case Confidence::Low:
    return "low";
  case Confidence::Medium:
    return "medium";
  case Confidence::High:
    return "high";
  }
  return "invalid";
}

constexpr std::string_view name(ContentLabel value) {
  switch (value) {
  case ContentLabel::Adult:
    return "adult";
  case ContentLabel::Anime:
    return "anime";
  }
  return "invalid";
}

sakuin::model::TorrentRecord record(const CorpusCase &test_case,
                                    std::size_t ordinal) {
  sakuin::core::InfoHash hash{};
  hash.bytes[0] = static_cast<unsigned char>(ordinal & 0xff);
  std::uint64_t total_size{};
  for (const auto &file : test_case.files)
    total_size += file.size;
  return {.info_hash = hash,
          .first_seen = {},
          .last_seen = {},
          .name = test_case.name,
          .total_size = total_size,
          .files = test_case.files};
}

std::vector<std::string> evaluate(const CorpusCase &expected,
                                  const Classification &actual) {
  std::vector<std::string> failures;
  if (actual.state != expected.state)
    failures.push_back("state expected=" + std::string{name(expected.state)} +
                       " actual=" + std::string{name(actual.state)});
  if (actual.kind != expected.kind)
    failures.push_back("kind expected=" + std::string{name(expected.kind)} +
                       " actual=" + std::string{name(actual.kind)});
  if (!sakuin::classification::at_least(actual.kind_confidence,
                                        expected.minimum_kind_confidence))
    failures.push_back("kind_confidence minimum=" +
                       std::string{name(expected.minimum_kind_confidence)} +
                       " actual=" + std::string{name(actual.kind_confidence)});
  for (const auto required : expected.required_labels) {
    const auto actual_confidence =
        sakuin::classification::label_confidence(actual, required.label);
    if (!actual_confidence ||
        !sakuin::classification::at_least(*actual_confidence,
                                          required.minimum_confidence))
      failures.push_back(
          "label " + std::string{name(required.label)} + " minimum=" +
          std::string{name(required.minimum_confidence)} + " actual=" +
          (actual_confidence ? std::string{name(*actual_confidence)}
                             : "absent"));
  }
  for (const auto forbidden : expected.forbidden_labels)
    if (sakuin::classification::label_confidence(actual, forbidden))
      failures.push_back("forbidden label present: " +
                         std::string{name(forbidden)});
  return failures;
}

} // namespace

int main(int argc, char **argv) {
  if (argc != 2) {
    std::cerr << "usage: sakuin-classifier-eval <corpus.tsv>\n";
    return 2;
  }

  try {
    const auto cases = load_corpus(argv[1]);
    std::size_t passed{};
    std::array<std::size_t, 4> states{};
    std::array<std::size_t, 9> kinds{};
    std::array<std::size_t, 2> labels{};
    for (std::size_t index = 0; index < cases.size(); ++index) {
      const auto &test_case = cases[index];
      const auto actual = sakuin::classification::classify(
          record(test_case, index + 1),
          ClassifierOptions{.adult_detection_enabled =
                                test_case.adult_detection_enabled});
      ++states[std::to_underlying(actual.state)];
      ++kinds[std::to_underlying(actual.kind)];
      for (const auto assessment : actual.labels)
        ++labels[std::to_underlying(assessment.label)];
      const auto failures = evaluate(test_case, actual);
      if (failures.empty()) {
        ++passed;
        continue;
      }
      for (const auto &failure : failures)
        std::cerr << test_case.id << ": " << failure << '\n';
    }

    std::cout
        << "classification corpus: passed=" << passed
        << " cases=" << cases.size() << " states[classified="
        << states[std::to_underlying(ClassificationState::Classified)]
        << ",ambiguous="
        << states[std::to_underlying(ClassificationState::Ambiguous)]
        << ",unknown="
        << states[std::to_underlying(ClassificationState::Unknown)]
        << ",awaiting_metadata="
        << states[std::to_underlying(ClassificationState::AwaitingMetadata)]
        << "] kinds[movie=" << kinds[std::to_underlying(ContentKind::Movie)]
        << ",series=" << kinds[std::to_underlying(ContentKind::Series)]
        << ",music=" << kinds[std::to_underlying(ContentKind::Music)]
        << ",audiobook=" << kinds[std::to_underlying(ContentKind::Audiobook)]
        << ",ebook=" << kinds[std::to_underlying(ContentKind::Ebook)]
        << ",game=" << kinds[std::to_underlying(ContentKind::Game)]
        << ",application="
        << kinds[std::to_underlying(ContentKind::Application)]
        << ",mixed=" << kinds[std::to_underlying(ContentKind::Mixed)]
        << ",unknown=" << kinds[std::to_underlying(ContentKind::Unknown)]
        << "] labels[adult=" << labels[std::to_underlying(ContentLabel::Adult)]
        << ",anime=" << labels[std::to_underlying(ContentLabel::Anime)]
        << "]\n";
    return passed == cases.size() ? 0 : 1;
  } catch (const std::exception &error) {
    std::cerr << "classifier evaluation error: " << error.what() << '\n';
    return 2;
  }
}

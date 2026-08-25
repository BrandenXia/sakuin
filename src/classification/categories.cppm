export module sakuin.classification.categories;

import std;

import sakuin.classification.model;

export namespace sakuin::classification {

std::vector<MediaCategory>
media_categories(const Classification &classification,
                 Confidence kind_minimum = Confidence::Medium,
                 Confidence adult_minimum = Confidence::High);

} // namespace sakuin::classification

namespace sakuin::classification {
namespace {

void add_resolution(std::vector<MediaCategory> &result, bool series,
                    std::optional<VideoResolution> resolution) {
  if (!resolution)
    return;
  switch (*resolution) {
  case VideoResolution::Sd:
    result.push_back(series ? MediaCategory::SeriesSd : MediaCategory::MovieSd);
    break;
  case VideoResolution::Hd720:
  case VideoResolution::Hd1080:
    result.push_back(series ? MediaCategory::SeriesHd : MediaCategory::MovieHd);
    break;
  case VideoResolution::Uhd2160:
    result.push_back(series ? MediaCategory::SeriesUhd
                            : MediaCategory::MovieUhd);
    break;
  }
}

} // namespace

std::vector<MediaCategory> media_categories(const Classification &value,
                                            Confidence kind_minimum,
                                            Confidence adult_minimum) {
  std::vector<MediaCategory> result;
  if (at_least(value.kind_confidence, kind_minimum)) {
    switch (value.kind) {
    case ContentKind::Movie:
      result.push_back(MediaCategory::Movie);
      add_resolution(result, false, value.resolution);
      break;
    case ContentKind::Series:
      result.push_back(MediaCategory::Series);
      add_resolution(result, true, value.resolution);
      if (const auto anime = label_confidence(value, ContentLabel::Anime);
          anime && at_least(*anime, Confidence::High))
        result.push_back(MediaCategory::SeriesAnime);
      break;
    case ContentKind::Music:
      result.push_back(MediaCategory::Audio);
      break;
    case ContentKind::Audiobook:
      result.push_back(MediaCategory::Audio);
      result.push_back(MediaCategory::Audiobook);
      break;
    case ContentKind::Application:
      result.push_back(MediaCategory::Application);
      break;
    case ContentKind::Game:
      result.push_back(MediaCategory::Game);
      break;
    case ContentKind::Ebook:
      result.push_back(MediaCategory::Books);
      result.push_back(MediaCategory::Ebook);
      break;
    case ContentKind::Mixed:
    case ContentKind::Unknown:
      result.push_back(MediaCategory::Other);
      break;
    }
  } else {
    result.push_back(MediaCategory::Other);
  }
  if (const auto adult = label_confidence(value, ContentLabel::Adult);
      adult && at_least(*adult, adult_minimum))
    result.push_back(MediaCategory::Adult);
  return result;
}

} // namespace sakuin::classification

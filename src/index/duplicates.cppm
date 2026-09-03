export module sakuin.index.duplicates;

import std;

import sakuin.core.bytes;
import sakuin.core.hash;
import sakuin.core.ids;
import sakuin.core.result;
import sakuin.model.torrent;
import sakuin.storage.dataset.snapshot;
import sakuin.storage.dataset.stream;
import sakuin.storage.dataset.torrents;

export namespace sakuin::index {

// These are explicitly versioned derived identities, not canonical ContentId
// semantics. New normalization rules must use a new enum value so indexes can
// be rebuilt or compared without silently changing identity.
enum class DuplicateFingerprintAlgorithm : std::uint8_t {
  ExactFileLayoutV1,
  NormalizedMetadataV1,
  PayloadLayoutV1,
  ReleaseIdentityV1
};

struct DuplicateFingerprint {
  DuplicateFingerprintAlgorithm algorithm{};
  core::Hash256 digest{};

  friend bool operator==(const DuplicateFingerprint &,
                         const DuplicateFingerprint &) = default;
  friend bool operator<(const DuplicateFingerprint &left,
                        const DuplicateFingerprint &right) noexcept {
    if (left.algorithm != right.algorithm)
      return left.algorithm < right.algorithm;
    return left.digest.bytes < right.digest.bytes;
  }
};

struct DuplicateGroup {
  DuplicateFingerprint fingerprint;
  std::vector<core::InfoHash> torrents;
};

struct DuplicateIndexStats {
  std::uint64_t source_generation{};
  std::uint64_t records_read{};
  std::uint64_t records_indexed{};
  std::uint64_t fingerprints{};
  std::uint64_t groups{};
};

struct DuplicateIndexState {
  DuplicateIndexStats stats;
  std::vector<DuplicateGroup> entries;
};

class DuplicateIndexView {
public:
  virtual ~DuplicateIndexView() = default;
  // A self-consistent copy used at service/API boundaries. Implementations
  // may update concurrently, so publishing stats and memberships separately
  // would let callers accidentally label groups with the wrong generation.
  virtual DuplicateIndexState snapshot() const = 0;
};

core::Result<std::optional<DuplicateFingerprint>>
duplicate_fingerprint(const model::TorrentRecord &record,
                      DuplicateFingerprintAlgorithm algorithm);

// In-memory reference derived index. Deleting it loses no canonical data; a
// future persistent or distributed implementation can preserve this query
// contract and rebuild from the same snapshot.
class DuplicateIndex final {
public:
  static core::Result<std::unique_ptr<DuplicateIndex>>
  rebuild(const storage::Snapshot<model::TorrentRecord, storage::AllTorrents>
              &source);
  static core::Result<std::unique_ptr<DuplicateIndex>>
  rebuild(storage::RecordStream<model::TorrentRecord> &source,
          std::uint64_t source_generation);
  static core::Result<std::unique_ptr<DuplicateIndex>>
  from_state(DuplicateIndexState state);

  core::Result<void> apply(std::span<const model::TorrentRecord> updates,
                           std::uint64_t source_generation);

  core::Result<std::vector<DuplicateGroup>>
  groups(DuplicateFingerprintAlgorithm algorithm,
         std::size_t minimum_members = 2) const;
  std::vector<DuplicateGroup> matches(const core::InfoHash &torrent) const;
  const DuplicateIndexStats &stats() const noexcept { return stats_; }
  DuplicateIndexState state() const;

private:
  struct InfoHashHash {
    std::size_t operator()(const core::InfoHash &value) const noexcept;
  };

  std::map<DuplicateFingerprint, std::vector<core::InfoHash>> groups_;
  std::unordered_map<core::InfoHash, std::vector<DuplicateFingerprint>,
                     InfoHashHash>
      by_torrent_;
  DuplicateIndexStats stats_;
};

} // namespace sakuin::index

namespace sakuin::index {
namespace {

constexpr std::array DuplicateFingerprintAlgorithms{
    DuplicateFingerprintAlgorithm::ExactFileLayoutV1,
    DuplicateFingerprintAlgorithm::NormalizedMetadataV1,
    DuplicateFingerprintAlgorithm::PayloadLayoutV1,
    DuplicateFingerprintAlgorithm::ReleaseIdentityV1};

// A tiny single file does not carry enough structure for name-independent
// matching. Large files retain enough entropy in their exact byte length to
// make the payload-layout signal useful while still remaining explicitly a
// likely-duplicate fingerprint rather than a content hash.
constexpr std::uint64_t MinimumSingleFilePayloadBytes = 1024U * 1024U;

void append_u64(core::ByteBuffer &output, std::uint64_t value) {
  for (unsigned shift = 0; shift < 64; shift += 8)
    output.push_back(static_cast<std::byte>(value >> shift));
}

void append_string(core::ByteBuffer &output, std::string_view value) {
  append_u64(output, value.size());
  const auto bytes = std::as_bytes(std::span{value});
  output.insert(output.end(), bytes.begin(), bytes.end());
}

std::string normalized_text(std::string_view value, bool path) {
  std::string result;
  result.reserve(value.size());
  bool whitespace{};
  for (const auto character : value) {
    const auto byte = static_cast<unsigned char>(character);
    if (std::isspace(byte)) {
      whitespace = !result.empty();
      continue;
    }
    if (whitespace) {
      result.push_back(' ');
      whitespace = false;
    }
    if (path && character == '\\')
      result.push_back('/');
    else if (byte < 128)
      result.push_back(static_cast<char>(std::tolower(byte)));
    else
      result.push_back(character);
  }
  return result;
}

std::string normalized_extension(std::string_view path) {
  const auto separator = path.find_last_of("/\\");
  const auto basename = separator == std::string_view::npos ? 0 : separator + 1;
  const auto dot = path.find_last_of('.');
  if (dot == std::string_view::npos || dot <= basename ||
      dot + 1 == path.size())
    return {};
  return normalized_text(path.substr(dot + 1), false);
}

bool release_noise(std::string_view token) {
  static constexpr std::array values{
      std::string_view{"480p"},    std::string_view{"576p"},
      std::string_view{"720p"},    std::string_view{"1080p"},
      std::string_view{"1080i"},   std::string_view{"2160p"},
      std::string_view{"4320p"},   std::string_view{"4k"},
      std::string_view{"8k"},      std::string_view{"bluray"},
      std::string_view{"bdrip"},   std::string_view{"brrip"},
      std::string_view{"webrip"},  std::string_view{"webdl"},
      std::string_view{"web"},     std::string_view{"dl"},
      std::string_view{"hdtv"},    std::string_view{"dvdrip"},
      std::string_view{"remux"},   std::string_view{"x264"},
      std::string_view{"x265"},    std::string_view{"h264"},
      std::string_view{"h265"},    std::string_view{"avc"},
      std::string_view{"hevc"},    std::string_view{"av1"},
      std::string_view{"xvid"},    std::string_view{"10bit"},
      std::string_view{"8bit"},    std::string_view{"hdr"},
      std::string_view{"hdr10"},   std::string_view{"dv"},
      std::string_view{"dolby"},   std::string_view{"vision"},
      std::string_view{"aac"},     std::string_view{"ac3"},
      std::string_view{"eac3"},    std::string_view{"ddp"},
      std::string_view{"dts"},     std::string_view{"truehd"},
      std::string_view{"atmos"},   std::string_view{"flac"},
      std::string_view{"proper"},  std::string_view{"repack"},
      std::string_view{"internal"}};
  return std::ranges::find(values, token) != values.end();
}

std::optional<std::string> release_identity(std::string_view name) {
  std::string_view identity_source = name;
  const auto group_separator = name.find_last_of('-');
  if (group_separator != std::string_view::npos) {
    const auto suffix = name.substr(group_separator + 1);
    const bool plausible_group =
        suffix.size() >= 2 && suffix.size() <= 20 &&
        std::ranges::all_of(suffix, [](const auto character) {
          return std::isalnum(static_cast<unsigned char>(character));
        });
    bool packaging_marker{};
    std::string candidate;
    const auto inspect = [&] {
      if (!candidate.empty() && release_noise(candidate))
        packaging_marker = true;
      candidate.clear();
    };
    if (plausible_group) {
      for (const auto character : name.substr(0, group_separator)) {
        const auto byte = static_cast<unsigned char>(character);
        if (byte < 128 && std::isalnum(byte))
          candidate.push_back(static_cast<char>(std::tolower(byte)));
        else
          inspect();
      }
      inspect();
    }
    if (packaging_marker)
      identity_source = name.substr(0, group_separator);
  }

  std::vector<std::string> tokens;
  std::string token;
  const auto flush = [&] {
    if (token.empty())
      return;
    if (!release_noise(token))
      tokens.push_back(std::move(token));
    token.clear();
  };
  for (const auto character : identity_source) {
    const auto byte = static_cast<unsigned char>(character);
    if (byte >= 128)
      token.push_back(character);
    else if (std::isalnum(byte))
      token.push_back(static_cast<char>(std::tolower(byte)));
    else
      flush();
  }
  flush();
  if (tokens.size() < 2 || std::ranges::none_of(tokens, [](const auto &value) {
        return value.size() >= 3 &&
               std::ranges::any_of(value, [](const auto character) {
                 const auto byte = static_cast<unsigned char>(character);
                 return byte >= 128 || std::isalpha(byte);
               });
      }))
    return std::nullopt;
  std::string result;
  for (const auto &value : tokens) {
    if (!result.empty())
      result.push_back(' ');
    result += value;
  }
  return result;
}

core::ByteBuffer fingerprint_material(const model::TorrentRecord &record,
                                      DuplicateFingerprintAlgorithm algorithm) {
  core::ByteBuffer result;
  const auto domain = [&] {
    switch (algorithm) {
    case DuplicateFingerprintAlgorithm::ExactFileLayoutV1:
      return std::string_view{"sakuin.duplicate.exact-file-layout.v1"};
    case DuplicateFingerprintAlgorithm::NormalizedMetadataV1:
      return std::string_view{"sakuin.duplicate.normalized-metadata.v1"};
    case DuplicateFingerprintAlgorithm::PayloadLayoutV1:
      return std::string_view{"sakuin.duplicate.payload-layout.v1"};
    case DuplicateFingerprintAlgorithm::ReleaseIdentityV1:
      return std::string_view{"sakuin.duplicate.release-identity.v1"};
    }
    std::unreachable();
  }();
  append_string(result, domain);
  if (algorithm == DuplicateFingerprintAlgorithm::ReleaseIdentityV1) {
    const auto identity = record.name ? release_identity(*record.name)
                                      : std::optional<std::string>{};
    if (identity)
      append_string(result, *identity);
    return result;
  }
  append_u64(result, record.total_size);

  std::vector<std::pair<std::string, std::uint64_t>> files;
  files.reserve(record.files.size());
  for (const auto &file : record.files) {
    std::string identity;
    switch (algorithm) {
    case DuplicateFingerprintAlgorithm::ExactFileLayoutV1:
      identity = file.path;
      break;
    case DuplicateFingerprintAlgorithm::NormalizedMetadataV1:
      identity = normalized_text(file.path, true);
      break;
    case DuplicateFingerprintAlgorithm::PayloadLayoutV1:
      identity = normalized_extension(file.path);
      break;
    case DuplicateFingerprintAlgorithm::ReleaseIdentityV1:
      std::unreachable();
    }
    files.emplace_back(std::move(identity), file.size);
  }
  std::ranges::sort(files);
  append_u64(result, files.size());
  for (const auto &[path, size] : files) {
    append_string(result, path);
    append_u64(result, size);
  }
  if (algorithm == DuplicateFingerprintAlgorithm::NormalizedMetadataV1)
    append_string(result,
                  record.name ? normalized_text(*record.name, false) : "");
  return result;
}

} // namespace

core::Result<std::optional<DuplicateFingerprint>>
duplicate_fingerprint(const model::TorrentRecord &record,
                      DuplicateFingerprintAlgorithm algorithm) {
  if (algorithm != DuplicateFingerprintAlgorithm::ExactFileLayoutV1 &&
      algorithm != DuplicateFingerprintAlgorithm::NormalizedMetadataV1 &&
      algorithm != DuplicateFingerprintAlgorithm::PayloadLayoutV1 &&
      algorithm != DuplicateFingerprintAlgorithm::ReleaseIdentityV1)
    return std::unexpected(
        core::Error{core::ErrorCode::InvalidArgument,
                    "Unknown duplicate fingerprint algorithm"});
  // Observation-only materializations contain no metadata and must not all
  // collapse into one meaningless empty group.
  if (record.files.empty())
    return std::optional<DuplicateFingerprint>{};
  if (algorithm == DuplicateFingerprintAlgorithm::PayloadLayoutV1 &&
      record.files.size() == 1 &&
      record.files.front().size < MinimumSingleFilePayloadBytes)
    return std::optional<DuplicateFingerprint>{};
  if (algorithm == DuplicateFingerprintAlgorithm::ReleaseIdentityV1 &&
      (!record.name || !release_identity(*record.name)))
    return std::optional<DuplicateFingerprint>{};
  auto material = fingerprint_material(record, algorithm);
  return std::optional<DuplicateFingerprint>{DuplicateFingerprint{
      .algorithm = algorithm, .digest = core::sha256(material)}};
}

std::size_t DuplicateIndex::InfoHashHash::operator()(
    const core::InfoHash &value) const noexcept {
  std::size_t result = 1469598103934665603ULL;
  for (const auto byte : value.bytes) {
    result ^= byte;
    result *= 1099511628211ULL;
  }
  return result;
}

core::Result<std::unique_ptr<DuplicateIndex>> DuplicateIndex::rebuild(
    const storage::Snapshot<model::TorrentRecord, storage::AllTorrents>
        &source) {
  auto stream = source.scan({});
  if (!stream)
    return std::unexpected(stream.error());
  return rebuild(**stream, source.id().generation);
}

core::Result<std::unique_ptr<DuplicateIndex>>
DuplicateIndex::rebuild(storage::RecordStream<model::TorrentRecord> &source,
                        std::uint64_t source_generation) {
  auto result = std::make_unique<DuplicateIndex>();
  result->stats_.source_generation = source_generation;
  while (true) {
    auto next = source.next();
    if (!next)
      return std::unexpected(next.error());
    if (!*next)
      break;
    ++result->stats_.records_read;
    bool indexed{};
    for (const auto algorithm : DuplicateFingerprintAlgorithms) {
      auto fingerprint = duplicate_fingerprint(**next, algorithm);
      if (!fingerprint)
        return std::unexpected(fingerprint.error());
      if (!*fingerprint)
        continue;
      result->groups_[**fingerprint].push_back((*next)->info_hash);
      result->by_torrent_[(*next)->info_hash].push_back(**fingerprint);
      ++result->stats_.fingerprints;
      indexed = true;
    }
    if (indexed)
      ++result->stats_.records_indexed;
  }
  for (auto &[_, members] : result->groups_)
    std::ranges::sort(members, {}, &core::InfoHash::bytes);
  result->stats_.groups =
      std::ranges::count_if(result->groups_, [](const auto &entry) {
        return entry.second.size() > 1;
      });
  return result;
}

core::Result<void>
DuplicateIndex::apply(std::span<const model::TorrentRecord> updates,
                      std::uint64_t source_generation) {
  if (source_generation < stats_.source_generation)
    return std::unexpected(
        core::Error{core::ErrorCode::Conflict,
                    "Duplicate-index generation cannot move backwards"});
  for (const auto &record : updates) {
    ++stats_.records_read;
    const auto existing = by_torrent_.find(record.info_hash);
    const bool was_indexed = existing != by_torrent_.end();
    if (was_indexed) {
      for (const auto &fingerprint : existing->second) {
        auto group = groups_.find(fingerprint);
        if (group == groups_.end())
          return std::unexpected(core::Error{
              core::ErrorCode::Internal,
              "Duplicate-index reverse membership is inconsistent"});
        const bool was_group = group->second.size() > 1;
        const auto member = std::ranges::lower_bound(
            group->second, record.info_hash.bytes, {}, &core::InfoHash::bytes);
        if (member == group->second.end() || *member != record.info_hash)
          return std::unexpected(
              core::Error{core::ErrorCode::Internal,
                          "Duplicate-index member is missing from its group"});
        group->second.erase(member);
        --stats_.fingerprints;
        if (was_group && group->second.size() == 1)
          --stats_.groups;
        if (group->second.empty())
          groups_.erase(group);
      }
      by_torrent_.erase(existing);
    }

    std::vector<DuplicateFingerprint> replacements;
    for (const auto algorithm : DuplicateFingerprintAlgorithms) {
      auto fingerprint = duplicate_fingerprint(record, algorithm);
      if (!fingerprint)
        return std::unexpected(fingerprint.error());
      if (*fingerprint)
        replacements.push_back(**fingerprint);
    }
    for (const auto &fingerprint : replacements) {
      auto &members = groups_[fingerprint];
      const bool was_group = members.size() > 1;
      members.insert(std::ranges::lower_bound(members, record.info_hash.bytes,
                                              {}, &core::InfoHash::bytes),
                     record.info_hash);
      if (!was_group && members.size() > 1)
        ++stats_.groups;
      ++stats_.fingerprints;
    }
    if (!replacements.empty())
      by_torrent_.emplace(record.info_hash, std::move(replacements));
    const bool is_indexed = by_torrent_.contains(record.info_hash);
    if (!was_indexed && is_indexed)
      ++stats_.records_indexed;
    else if (was_indexed && !is_indexed)
      --stats_.records_indexed;
  }
  stats_.source_generation = source_generation;
  return {};
}

core::Result<std::unique_ptr<DuplicateIndex>>
DuplicateIndex::from_state(DuplicateIndexState state) {
  auto result = std::make_unique<DuplicateIndex>();
  std::optional<DuplicateFingerprint> previous;
  std::uint64_t fingerprints{};
  std::uint64_t groups{};
  for (auto &entry : state.entries) {
    if (entry.fingerprint.algorithm !=
            DuplicateFingerprintAlgorithm::ExactFileLayoutV1 &&
        entry.fingerprint.algorithm !=
            DuplicateFingerprintAlgorithm::NormalizedMetadataV1 &&
        entry.fingerprint.algorithm !=
            DuplicateFingerprintAlgorithm::PayloadLayoutV1 &&
        entry.fingerprint.algorithm !=
            DuplicateFingerprintAlgorithm::ReleaseIdentityV1)
      return std::unexpected(
          core::Error{core::ErrorCode::UnsupportedFormat,
                      "Duplicate index uses an unknown algorithm"});
    if (previous && !(previous.value() < entry.fingerprint))
      return std::unexpected(
          core::Error{core::ErrorCode::CorruptSegment,
                      "Duplicate-index entries are not ordered"});
    if (entry.torrents.empty() ||
        !std::ranges::is_sorted(entry.torrents, {}, &core::InfoHash::bytes) ||
        std::ranges::adjacent_find(entry.torrents) != entry.torrents.end())
      return std::unexpected(
          core::Error{core::ErrorCode::CorruptSegment,
                      "Duplicate-index members are not ordered and unique"});
    previous = entry.fingerprint;
    fingerprints += entry.torrents.size();
    if (entry.torrents.size() > 1)
      ++groups;
    for (const auto &torrent : entry.torrents)
      result->by_torrent_[torrent].push_back(entry.fingerprint);
    result->groups_.emplace(entry.fingerprint, std::move(entry.torrents));
  }
  if (fingerprints != state.stats.fingerprints ||
      groups != state.stats.groups ||
      state.stats.records_indexed > state.stats.records_read ||
      result->by_torrent_.size() != state.stats.records_indexed)
    return std::unexpected(
        core::Error{core::ErrorCode::CorruptSegment,
                    "Duplicate-index statistics are inconsistent"});
  result->stats_ = state.stats;
  return result;
}

core::Result<std::vector<DuplicateGroup>>
DuplicateIndex::groups(DuplicateFingerprintAlgorithm algorithm,
                       std::size_t minimum_members) const {
  if (minimum_members < 2)
    return std::unexpected(
        core::Error{core::ErrorCode::InvalidArgument,
                    "Duplicate groups require at least two members"});
  std::vector<DuplicateGroup> result;
  for (const auto &[fingerprint, members] : groups_)
    if (fingerprint.algorithm == algorithm && members.size() >= minimum_members)
      result.push_back({.fingerprint = fingerprint, .torrents = members});
  return result;
}

std::vector<DuplicateGroup>
DuplicateIndex::matches(const core::InfoHash &torrent) const {
  std::vector<DuplicateGroup> result;
  const auto found = by_torrent_.find(torrent);
  if (found == by_torrent_.end())
    return result;
  for (const auto &fingerprint : found->second) {
    const auto group = groups_.find(fingerprint);
    if (group != groups_.end() && group->second.size() > 1)
      result.push_back({.fingerprint = fingerprint, .torrents = group->second});
  }
  return result;
}

DuplicateIndexState DuplicateIndex::state() const {
  DuplicateIndexState result{.stats = stats_};
  result.entries.reserve(groups_.size());
  for (const auto &[fingerprint, torrents] : groups_)
    result.entries.push_back(
        {.fingerprint = fingerprint, .torrents = torrents});
  return result;
}

} // namespace sakuin::index

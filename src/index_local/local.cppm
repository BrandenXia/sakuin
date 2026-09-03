module;

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>

export module sakuin.index.local;

import std;

import sakuin.core.bytes;
import sakuin.core.hash;
import sakuin.core.ids;
import sakuin.core.result;
export import sakuin.index.duplicates;
import sakuin.model.torrent;
import sakuin.storage.dataset.snapshot;
import sakuin.storage.dataset.torrents;

export namespace sakuin::index {

struct DuplicateSynchronizationResult {
  std::uint64_t source_generation{};
  std::uint64_t records_processed{};
  bool full_rebuild{};
};

// Durable local implementation of the derived duplicate projection. The file
// contains versioned fingerprint memberships, never canonical torrent data,
// and can always be replaced by rebuilding from a canonical snapshot.
class LocalDuplicateIndex final : public DuplicateIndexView {
public:
  static core::Result<std::unique_ptr<LocalDuplicateIndex>>
  open(std::filesystem::path path);
  static core::Result<std::unique_ptr<LocalDuplicateIndex>>
  rebuild(std::filesystem::path path,
          const storage::Snapshot<model::TorrentRecord, storage::AllTorrents>
              &source);

  core::Result<DuplicateSynchronizationResult>
  synchronize(const storage::TorrentDataset &source);

  core::Result<std::vector<DuplicateGroup>>
  groups(DuplicateFingerprintAlgorithm algorithm,
         std::size_t minimum_members = 2) const {
    return index_->groups(algorithm, minimum_members);
  }
  std::vector<DuplicateGroup> matches(const core::InfoHash &torrent) const {
    return index_->matches(torrent);
  }
  const DuplicateIndexStats &stats() const noexcept { return index_->stats(); }
  DuplicateIndexState snapshot() const override { return index_->state(); }
  const storage::TorrentChangeCursor &cursor() const noexcept {
    return cursor_;
  }
  const std::filesystem::path &path() const noexcept { return path_; }

private:
  LocalDuplicateIndex(std::filesystem::path path,
                      std::unique_ptr<DuplicateIndex> index,
                      storage::TorrentChangeCursor cursor = {})
      : path_(std::move(path)), index_(std::move(index)), cursor_(cursor) {}

  core::Result<void> persist(const DuplicateIndex &index,
                             const storage::TorrentChangeCursor &cursor) const;

  std::filesystem::path path_;
  std::unique_ptr<DuplicateIndex> index_;
  storage::TorrentChangeCursor cursor_;
};

} // namespace sakuin::index

namespace sakuin::index {
namespace {

// V3 adds the release-identity fingerprint. The projection is disposable, so an
// older file is intentionally rejected and rebuilt instead of being accepted
// with silently incomplete memberships.
constexpr std::string_view Magic{"sakuin-duplicate-index-v3\n"};
constexpr std::uint64_t MinimumEntryBytes = 1U + 32U + 8U + 20U;

core::Error io_error(std::string action, const std::filesystem::path &path,
                     std::string detail = {}) {
  std::string message = std::move(action) + ": " + path.string();
  if (!detail.empty())
    message += ": " + detail;
  return {core::ErrorCode::IoError, std::move(message)};
}

core::Result<void> sync_path(const std::filesystem::path &path,
                             bool directory) {
  const auto descriptor = ::open(
      path.c_str(), O_RDONLY | O_CLOEXEC | (directory ? O_DIRECTORY : 0));
  if (descriptor < 0)
    return std::unexpected(io_error("Could not open duplicate-index path", path,
                                    std::strerror(errno)));
  if (::fsync(descriptor) != 0) {
    const auto detail = std::string{std::strerror(errno)};
    ::close(descriptor);
    return std::unexpected(
        io_error("Could not sync duplicate-index path", path, detail));
  }
  ::close(descriptor);
  return {};
}

core::Result<void> write_bytes(std::ofstream &output, core::Sha256Hasher &hash,
                               core::ByteView bytes) {
  output.write(reinterpret_cast<const char *>(bytes.data()),
               static_cast<std::streamsize>(bytes.size()));
  if (!output)
    return std::unexpected(core::Error{
        core::ErrorCode::IoError, "Could not write local duplicate index"});
  hash.update(bytes);
  return {};
}

template <std::unsigned_integral T>
core::Result<void> write_integer(std::ofstream &output,
                                 core::Sha256Hasher &hash, T value) {
  std::array<core::Byte, sizeof(T)> encoded{};
  for (std::size_t index = 0; index < encoded.size(); ++index)
    encoded[index] = static_cast<core::Byte>(value >> (index * 8U));
  return write_bytes(output, hash, encoded);
}

core::Result<core::ByteBuffer> read_bytes(std::ifstream &input,
                                          std::size_t size,
                                          core::Sha256Hasher *hash = nullptr) {
  core::ByteBuffer result(size);
  input.read(reinterpret_cast<char *>(result.data()),
             static_cast<std::streamsize>(result.size()));
  if (!input)
    return std::unexpected(core::Error{core::ErrorCode::CorruptSegment,
                                       "Local duplicate index is truncated"});
  if (hash)
    hash->update(result);
  return result;
}

template <std::unsigned_integral T>
core::Result<T> read_integer(std::ifstream &input, core::Sha256Hasher &hash) {
  auto encoded = read_bytes(input, sizeof(T), &hash);
  if (!encoded)
    return std::unexpected(encoded.error());
  T result{};
  for (std::size_t index = 0; index < encoded->size(); ++index)
    result |= static_cast<T>(std::to_integer<unsigned>((*encoded)[index]))
              << (index * 8U);
  return result;
}

template <std::size_t Size>
core::Result<std::array<std::uint8_t, Size>>
read_array(std::ifstream &input, core::Sha256Hasher &hash) {
  auto encoded = read_bytes(input, Size, &hash);
  if (!encoded)
    return std::unexpected(encoded.error());
  std::array<std::uint8_t, Size> result{};
  std::ranges::transform(*encoded, result.begin(), [](core::Byte byte) {
    return static_cast<std::uint8_t>(std::to_integer<unsigned>(byte));
  });
  return result;
}

struct StoredIndex {
  std::unique_ptr<DuplicateIndex> index;
  storage::TorrentChangeCursor cursor;
};

core::Result<StoredIndex> load(const std::filesystem::path &path) {
  std::error_code status_error;
  if (!std::filesystem::exists(path, status_error)) {
    if (status_error)
      return std::unexpected(io_error("Could not inspect local duplicate index",
                                      path, status_error.message()));
    return StoredIndex{.index = std::make_unique<DuplicateIndex>()};
  }
  const auto size = std::filesystem::file_size(path, status_error);
  if (status_error)
    return std::unexpected(io_error("Could not size local duplicate index",
                                    path, status_error.message()));
  std::ifstream input{path, std::ios::binary};
  if (!input)
    return std::unexpected(
        io_error("Could not open local duplicate index", path));

  core::Sha256Hasher hash;
  auto magic = read_bytes(input, Magic.size(), &hash);
  if (!magic)
    return std::unexpected(magic.error());
  if (!std::ranges::equal(*magic, std::as_bytes(std::span{Magic})))
    return std::unexpected(
        core::Error{core::ErrorCode::UnsupportedFormat,
                    "Local duplicate-index format is unsupported"});

  std::array<std::uint64_t, 6> header{};
  for (auto &value : header) {
    auto decoded = read_integer<std::uint64_t>(input, hash);
    if (!decoded)
      return std::unexpected(decoded.error());
    value = *decoded;
  }
  const auto &[source_generation, records_read, records_indexed, fingerprints,
               duplicate_groups, entry_count] = header;
  if (entry_count > size / MinimumEntryBytes)
    return std::unexpected(
        core::Error{core::ErrorCode::CorruptSegment,
                    "Local duplicate-index entry count is invalid"});

  auto cursor_initialized = read_integer<std::uint8_t>(input, hash);
  auto cursor_generation = read_integer<std::uint64_t>(input, hash);
  auto cursor_segment_count = read_integer<std::uint64_t>(input, hash);
  auto cursor_digest = read_array<32>(input, hash);
  if (!cursor_initialized)
    return std::unexpected(cursor_initialized.error());
  if (!cursor_generation)
    return std::unexpected(cursor_generation.error());
  if (!cursor_segment_count)
    return std::unexpected(cursor_segment_count.error());
  if (!cursor_digest)
    return std::unexpected(cursor_digest.error());
  if (*cursor_initialized > 1 ||
      (*cursor_initialized == 1 && *cursor_generation != source_generation) ||
      (*cursor_initialized == 0 &&
       (*cursor_generation != 0 || *cursor_segment_count != 0 ||
        std::ranges::any_of(*cursor_digest,
                            [](std::uint8_t value) { return value != 0; }))))
    return std::unexpected(
        core::Error{core::ErrorCode::CorruptSegment,
                    "Local duplicate-index cursor is inconsistent"});
  storage::TorrentChangeCursor cursor{
      .initialized = *cursor_initialized == 1,
      .source_generation = *cursor_generation,
      .segment_count = *cursor_segment_count,
      .segment_prefix_digest = core::Hash256{.bytes = *cursor_digest}};

  DuplicateIndexState state;
  state.stats = {.source_generation = source_generation,
                 .records_read = records_read,
                 .records_indexed = records_indexed,
                 .fingerprints = fingerprints,
                 .groups = duplicate_groups};
  state.entries.reserve(static_cast<std::size_t>(entry_count));
  std::optional<DuplicateFingerprint> previous;
  std::uint64_t decoded_fingerprints{};
  std::uint64_t decoded_groups{};
  for (std::uint64_t entry = 0; entry < entry_count; ++entry) {
    auto algorithm = read_integer<std::uint8_t>(input, hash);
    auto digest = read_array<32>(input, hash);
    auto member_count = read_integer<std::uint64_t>(input, hash);
    if (!algorithm)
      return std::unexpected(algorithm.error());
    if (!digest)
      return std::unexpected(digest.error());
    if (!member_count)
      return std::unexpected(member_count.error());
    if (*algorithm > static_cast<std::uint8_t>(
                         DuplicateFingerprintAlgorithm::ReleaseIdentityV1))
      return std::unexpected(
          core::Error{core::ErrorCode::UnsupportedFormat,
                      "Local duplicate index uses an unknown algorithm"});
    if (*member_count == 0 || *member_count > size / 20U)
      return std::unexpected(
          core::Error{core::ErrorCode::CorruptSegment,
                      "Local duplicate-index member count is invalid"});

    DuplicateFingerprint fingerprint{
        .algorithm = static_cast<DuplicateFingerprintAlgorithm>(*algorithm),
        .digest = core::Hash256{.bytes = *digest}};
    if (previous && !(previous.value() < fingerprint))
      return std::unexpected(
          core::Error{core::ErrorCode::CorruptSegment,
                      "Local duplicate-index entries are not ordered"});
    previous = fingerprint;

    std::vector<core::InfoHash> members;
    members.reserve(static_cast<std::size_t>(*member_count));
    for (std::uint64_t member = 0; member < *member_count; ++member) {
      auto bytes = read_array<20>(input, hash);
      if (!bytes)
        return std::unexpected(bytes.error());
      core::InfoHash info_hash{.bytes = *bytes};
      if (!members.empty() && !std::ranges::lexicographical_compare(
                                  members.back().bytes, info_hash.bytes))
        return std::unexpected(
            core::Error{core::ErrorCode::CorruptSegment,
                        "Local duplicate-index members are not ordered"});
      members.push_back(info_hash);
      ++decoded_fingerprints;
    }
    if (*member_count > 1)
      ++decoded_groups;
    state.entries.push_back(
        {.fingerprint = fingerprint, .torrents = std::move(members)});
  }

  const auto expected = hash.finalize();
  auto actual = read_bytes(input, expected.bytes.size());
  if (!actual)
    return std::unexpected(actual.error());
  if (!core::constant_time_equal(std::as_bytes(std::span{expected.bytes}),
                                 *actual))
    return std::unexpected(
        core::Error{core::ErrorCode::ChecksumMismatch,
                    "Local duplicate-index checksum mismatch"});
  if (input.peek() != std::char_traits<char>::eof())
    return std::unexpected(
        core::Error{core::ErrorCode::CorruptSegment,
                    "Local duplicate index has trailing data"});

  if (decoded_fingerprints != fingerprints ||
      decoded_groups != duplicate_groups)
    return std::unexpected(
        core::Error{core::ErrorCode::CorruptSegment,
                    "Local duplicate-index statistics are inconsistent"});
  auto index = DuplicateIndex::from_state(std::move(state));
  if (!index)
    return std::unexpected(index.error());
  return StoredIndex{.index = std::move(*index), .cursor = cursor};
}

} // namespace

core::Result<std::unique_ptr<LocalDuplicateIndex>>
LocalDuplicateIndex::open(std::filesystem::path path) {
  auto loaded = load(path);
  if (!loaded)
    return std::unexpected(loaded.error());
  return std::unique_ptr<LocalDuplicateIndex>{new LocalDuplicateIndex{
      std::move(path), std::move(loaded->index), loaded->cursor}};
}

core::Result<std::unique_ptr<LocalDuplicateIndex>> LocalDuplicateIndex::rebuild(
    std::filesystem::path path,
    const storage::Snapshot<model::TorrentRecord, storage::AllTorrents>
        &source) {
  auto rebuilt = DuplicateIndex::rebuild(source);
  if (!rebuilt)
    return std::unexpected(rebuilt.error());
  auto result = std::unique_ptr<LocalDuplicateIndex>{
      new LocalDuplicateIndex{std::move(path), std::move(*rebuilt)}};
  if (auto persisted = result->persist(*result->index_, result->cursor_);
      !persisted)
    return std::unexpected(persisted.error());
  return result;
}

core::Result<DuplicateSynchronizationResult>
LocalDuplicateIndex::synchronize(const storage::TorrentDataset &source) {
  auto changes = source.changes_since(cursor_);
  if (!changes)
    return std::unexpected(changes.error());
  DuplicateSynchronizationResult result{
      .source_generation = changes->cursor.source_generation,
      .full_rebuild = changes->full_rebuild_required};
  if (cursor_.initialized && changes->cursor == cursor_)
    return result;
  std::unique_ptr<DuplicateIndex> replacement;
  if (changes->full_rebuild_required) {
    auto rebuilt = DuplicateIndex::rebuild(*changes->records,
                                           changes->cursor.source_generation);
    if (!rebuilt)
      return std::unexpected(rebuilt.error());
    result.records_processed = (*rebuilt)->stats().records_read;
    replacement = std::move(*rebuilt);
  } else {
    auto copied = DuplicateIndex::from_state(index_->state());
    if (!copied)
      return std::unexpected(copied.error());
    std::vector<model::TorrentRecord> updates;
    while (true) {
      auto next = changes->records->next();
      if (!next)
        return std::unexpected(next.error());
      if (!*next)
        break;
      updates.push_back(std::move(**next));
    }
    result.records_processed = updates.size();
    if (auto applied =
            (*copied)->apply(updates, changes->cursor.source_generation);
        !applied)
      return std::unexpected(applied.error());
    replacement = std::move(*copied);
  }
  if (auto persisted = persist(*replacement, changes->cursor); !persisted)
    return std::unexpected(persisted.error());
  index_ = std::move(replacement);
  cursor_ = changes->cursor;
  return result;
}

core::Result<void>
LocalDuplicateIndex::persist(const DuplicateIndex &index,
                             const storage::TorrentChangeCursor &cursor) const {
  std::error_code directory_error;
  std::filesystem::create_directories(path_.parent_path(), directory_error);
  if (directory_error)
    return std::unexpected(
        io_error("Could not create duplicate-index directory",
                 path_.parent_path(), directory_error.message()));
  const auto temporary = path_.string() + ".temporary";
  std::ofstream output{temporary, std::ios::binary | std::ios::trunc};
  if (!output)
    return std::unexpected(
        io_error("Could not create local duplicate index", temporary));
  core::Sha256Hasher hash;
  if (auto written = write_bytes(output, hash, std::as_bytes(std::span{Magic}));
      !written)
    return written;
  const auto state = index.state();
  const auto &stats = state.stats;
  for (const auto value :
       {stats.source_generation, stats.records_read, stats.records_indexed,
        stats.fingerprints, stats.groups,
        static_cast<std::uint64_t>(state.entries.size())})
    if (auto written = write_integer(output, hash, value); !written)
      return written;
  if (auto written = write_integer(
          output, hash, static_cast<std::uint8_t>(cursor.initialized));
      !written)
    return written;
  if (auto written = write_integer(output, hash, cursor.source_generation);
      !written)
    return written;
  if (auto written = write_integer(output, hash, cursor.segment_count);
      !written)
    return written;
  if (auto written = write_bytes(
          output, hash,
          std::as_bytes(std::span{cursor.segment_prefix_digest.bytes}));
      !written)
    return written;
  for (const auto &entry : state.entries) {
    const auto &fingerprint = entry.fingerprint;
    const auto &members = entry.torrents;
    if (auto written = write_integer(
            output, hash, static_cast<std::uint8_t>(fingerprint.algorithm));
        !written)
      return written;
    if (auto written = write_bytes(
            output, hash, std::as_bytes(std::span{fingerprint.digest.bytes}));
        !written)
      return written;
    if (auto written = write_integer(
            output, hash, static_cast<std::uint64_t>(members.size()));
        !written)
      return written;
    for (const auto &member : members)
      if (auto written =
              write_bytes(output, hash, std::as_bytes(std::span{member.bytes}));
          !written)
        return written;
  }
  const auto digest = hash.finalize();
  output.write(reinterpret_cast<const char *>(digest.bytes.data()),
               static_cast<std::streamsize>(digest.bytes.size()));
  output.flush();
  if (!output)
    return std::unexpected(
        io_error("Could not finish local duplicate index", temporary));
  output.close();
  if (auto synced = sync_path(temporary, false); !synced)
    return synced;
  std::error_code rename_error;
  std::filesystem::rename(temporary, path_, rename_error);
  if (rename_error)
    return std::unexpected(io_error("Could not publish local duplicate index",
                                    path_, rename_error.message()));
  return sync_path(path_.parent_path(), true);
}

} // namespace sakuin::index

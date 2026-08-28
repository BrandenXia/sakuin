module;

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>

export module sakuin.search.local;

import std;

import sakuin.core.bytes;
import sakuin.core.hash;
import sakuin.core.result;
import sakuin.model.torrent;
import sakuin.search.index;
import sakuin.search.memory;
import sakuin.storage.codec.model_records;

namespace sakuin::search {
class LocalRebuildSession;
class LocalUpdateSession;
} // namespace sakuin::search

export namespace sakuin::search {

// Durable local implementation for the derived search projection. Its file is
// disposable and versioned; canonical torrent segments remain authoritative.
class LocalSearchIndex final : public SearchIndex {
public:
  static core::Result<std::unique_ptr<LocalSearchIndex>>
  open(std::filesystem::path path,
       SearchClassificationOptions classification = {});
  static std::unique_ptr<LocalSearchIndex>
  create_empty(std::filesystem::path path,
               SearchClassificationOptions classification = {}) {
    return std::unique_ptr<LocalSearchIndex>{
        new LocalSearchIndex{std::move(path), std::move(classification)}};
  }

  core::Result<std::unique_ptr<SearchRebuildSession>>
  begin_rebuild(std::uint64_t source_generation) override;
  core::Result<std::unique_ptr<SearchUpdateSession>>
  begin_update(std::uint64_t source_generation) override;
  core::Result<SearchResult> search(const SearchQuery &query) const override;
  std::uint64_t source_generation() const noexcept override;
  ClassificationIndexStats classification_stats() const noexcept override;

  const std::filesystem::path &path() const noexcept { return path_; }

private:
  friend class LocalRebuildSession;
  friend class LocalUpdateSession;

  explicit LocalSearchIndex(std::filesystem::path path,
                            SearchClassificationOptions classification)
      : path_(std::move(path)), memory_(std::move(classification)) {}

  core::Result<void> replace(std::uint64_t source_generation,
                             std::vector<model::TorrentRecord> records);
  core::Result<void> apply(std::uint64_t source_generation,
                           std::span<const model::TorrentRecord> updates);
  core::Result<void>
  persist(std::uint64_t source_generation,
          std::span<const model::TorrentRecord> records) const;
  core::Result<void>
  persist_updates(std::uint64_t source_generation,
                  std::span<const model::TorrentRecord> updates) const;
  core::Result<void> clear_updates() const;

  std::filesystem::path path_;
  mutable std::mutex mutex_;
  InMemorySearchIndex memory_;
};

} // namespace sakuin::search

namespace sakuin::search {
namespace {

constexpr std::string_view Magic{"sakuin-search-index-v1\n"};
constexpr std::string_view UpdateMagic{"sakuin-search-update-v1\n"};
constexpr std::uint64_t MaximumRecordBytes = 64U * 1024U * 1024U;

struct StoredState {
  std::uint64_t source_generation{};
  std::vector<model::TorrentRecord> records;
};

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
    return std::unexpected(io_error("Could not open search-index path", path,
                                    std::strerror(errno)));
  if (::fsync(descriptor) != 0) {
    const auto detail = std::string{std::strerror(errno)};
    ::close(descriptor);
    return std::unexpected(
        io_error("Could not sync search-index path", path, detail));
  }
  ::close(descriptor);
  return {};
}

core::Result<void> write_bytes(std::ofstream &output, core::Sha256Hasher &hash,
                               core::ByteView bytes) {
  output.write(reinterpret_cast<const char *>(bytes.data()),
               static_cast<std::streamsize>(bytes.size()));
  if (!output)
    return std::unexpected(core::Error{core::ErrorCode::IoError,
                                       "Could not write local search index"});
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
                                       "Local search index is truncated"});
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

core::Result<StoredState> load(const std::filesystem::path &path) {
  std::error_code status_error;
  if (!std::filesystem::exists(path, status_error)) {
    if (status_error)
      return std::unexpected(io_error("Could not inspect local search index",
                                      path, status_error.message()));
    return StoredState{};
  }
  const auto size = std::filesystem::file_size(path, status_error);
  if (status_error)
    return std::unexpected(io_error("Could not size local search index", path,
                                    status_error.message()));
  std::ifstream input{path, std::ios::binary};
  if (!input)
    return std::unexpected(io_error("Could not open local search index", path));

  core::Sha256Hasher hash;
  auto magic = read_bytes(input, Magic.size(), &hash);
  if (!magic)
    return std::unexpected(magic.error());
  if (!std::ranges::equal(*magic, std::as_bytes(std::span{Magic})))
    return std::unexpected(
        core::Error{core::ErrorCode::UnsupportedFormat,
                    "Local search-index format is unsupported"});
  auto generation = read_integer<std::uint64_t>(input, hash);
  auto count = read_integer<std::uint64_t>(input, hash);
  if (!generation)
    return std::unexpected(generation.error());
  if (!count)
    return std::unexpected(count.error());
  if (*count > size / sizeof(std::uint32_t))
    return std::unexpected(
        core::Error{core::ErrorCode::CorruptSegment,
                    "Local search-index record count is invalid"});

  storage::TorrentRecordCodec codec;
  StoredState result{.source_generation = *generation};
  result.records.reserve(static_cast<std::size_t>(*count));
  for (std::uint64_t index = 0; index < *count; ++index) {
    auto record_size = read_integer<std::uint32_t>(input, hash);
    if (!record_size)
      return std::unexpected(record_size.error());
    if (*record_size > MaximumRecordBytes)
      return std::unexpected(
          core::Error{core::ErrorCode::CorruptSegment,
                      "Local search-index record exceeds the safety limit"});
    auto encoded = read_bytes(input, *record_size, &hash);
    if (!encoded)
      return std::unexpected(encoded.error());
    auto decoded = codec.decode(*encoded);
    if (!decoded)
      return std::unexpected(decoded.error());
    result.records.push_back(std::move(*decoded));
  }
  const auto expected = hash.finalize();
  auto actual = read_bytes(input, expected.bytes.size());
  if (!actual)
    return std::unexpected(actual.error());
  if (!core::constant_time_equal(std::as_bytes(std::span{expected.bytes}),
                                 *actual))
    return std::unexpected(core::Error{core::ErrorCode::ChecksumMismatch,
                                       "Local search-index checksum mismatch"});
  if (input.peek() != std::char_traits<char>::eof())
    return std::unexpected(core::Error{core::ErrorCode::CorruptSegment,
                                       "Local search index has trailing data"});
  return result;
}

template <typename Consumer>
core::Result<void> replay_updates(const std::filesystem::path &path,
                                  Consumer &&consume) {
  std::error_code status_error;
  if (!std::filesystem::exists(path, status_error)) {
    if (status_error)
      return std::unexpected(io_error("Could not inspect search updates", path,
                                      status_error.message()));
    return {};
  }
  const auto file_size = std::filesystem::file_size(path, status_error);
  if (status_error)
    return std::unexpected(io_error("Could not size search updates", path,
                                    status_error.message()));
  std::ifstream input{path, std::ios::binary};
  if (!input)
    return std::unexpected(io_error("Could not open search updates", path));
  storage::TorrentRecordCodec codec;
  while (input.peek() != std::char_traits<char>::eof()) {
    core::Sha256Hasher hash;
    auto magic = read_bytes(input, UpdateMagic.size(), &hash);
    if (!magic)
      return std::unexpected(magic.error());
    if (!std::ranges::equal(*magic, std::as_bytes(std::span{UpdateMagic})))
      return std::unexpected(
          core::Error{core::ErrorCode::UnsupportedFormat,
                      "Local search-update format is unsupported"});
    auto generation = read_integer<std::uint64_t>(input, hash);
    auto count = read_integer<std::uint64_t>(input, hash);
    if (!generation)
      return std::unexpected(generation.error());
    if (!count)
      return std::unexpected(count.error());
    if (*count > file_size / sizeof(std::uint32_t))
      return std::unexpected(
          core::Error{core::ErrorCode::CorruptSegment,
                      "Local search-update record count is invalid"});
    std::vector<model::TorrentRecord> records;
    records.reserve(static_cast<std::size_t>(*count));
    for (std::uint64_t index = 0; index < *count; ++index) {
      auto record_size = read_integer<std::uint32_t>(input, hash);
      if (!record_size)
        return std::unexpected(record_size.error());
      if (*record_size > MaximumRecordBytes)
        return std::unexpected(
            core::Error{core::ErrorCode::CorruptSegment,
                        "Local search update exceeds the safety limit"});
      auto encoded = read_bytes(input, *record_size, &hash);
      if (!encoded)
        return std::unexpected(encoded.error());
      auto decoded = codec.decode(*encoded);
      if (!decoded)
        return std::unexpected(decoded.error());
      records.push_back(std::move(*decoded));
    }
    const auto expected = hash.finalize();
    auto actual = read_bytes(input, expected.bytes.size());
    if (!actual)
      return std::unexpected(actual.error());
    if (!core::constant_time_equal(std::as_bytes(std::span{expected.bytes}),
                                   *actual))
      return std::unexpected(
          core::Error{core::ErrorCode::ChecksumMismatch,
                      "Local search-update checksum mismatch"});
    if (auto consumed = consume(*generation, records); !consumed)
      return consumed;
  }
  return {};
}

} // namespace

class LocalRebuildSession final : public SearchRebuildSession {
public:
  LocalRebuildSession(LocalSearchIndex &owner, std::uint64_t generation)
      : owner_(&owner), generation_(generation) {}
  ~LocalRebuildSession() override { abort(); }

  core::Result<void> append(const model::TorrentRecord &record) override {
    if (!active_)
      return std::unexpected(
          core::Error{core::ErrorCode::InvalidArgument,
                      "Local search rebuild session is no longer active"});
    // Keep the durable rebuild buffer bounded to the records which can enter
    // the search projection. The in-memory backend applies the same filter,
    // but waiting until replace() would retain a second placeholder copy.
    if (!record.name || record.files.empty())
      return {};
    records_.push_back(record);
    return {};
  }
  core::Result<void> commit() override {
    if (!active_)
      return std::unexpected(
          core::Error{core::ErrorCode::InvalidArgument,
                      "Local search rebuild session is no longer active"});
    active_ = false;
    return owner_->replace(generation_, std::move(records_));
  }
  void abort() noexcept override {
    active_ = false;
    records_.clear();
  }

private:
  LocalSearchIndex *owner_;
  std::uint64_t generation_;
  std::vector<model::TorrentRecord> records_;
  bool active_{true};
};

class LocalUpdateSession final : public SearchUpdateSession {
public:
  LocalUpdateSession(LocalSearchIndex &owner, std::uint64_t generation)
      : owner_(&owner), generation_(generation) {}
  ~LocalUpdateSession() override { abort(); }

  core::Result<void> upsert(const model::TorrentRecord &record) override {
    if (!active_)
      return std::unexpected(
          core::Error{core::ErrorCode::InvalidArgument,
                      "Local search update session is no longer active"});
    updates_.push_back(record);
    return {};
  }
  core::Result<void> commit() override {
    if (!active_)
      return std::unexpected(
          core::Error{core::ErrorCode::InvalidArgument,
                      "Local search update session is no longer active"});
    active_ = false;
    return owner_->apply(generation_, updates_);
  }
  void abort() noexcept override {
    active_ = false;
    updates_.clear();
  }

private:
  LocalSearchIndex *owner_;
  std::uint64_t generation_;
  std::vector<model::TorrentRecord> updates_;
  bool active_{true};
};

core::Result<std::unique_ptr<LocalSearchIndex>>
LocalSearchIndex::open(std::filesystem::path path,
                       SearchClassificationOptions classification) {
  auto stored = load(path);
  if (!stored)
    return std::unexpected(stored.error());
  auto result = std::unique_ptr<LocalSearchIndex>{
      new LocalSearchIndex{std::move(path), std::move(classification)}};
  auto session = result->memory_.begin_rebuild(stored->source_generation);
  if (!session)
    return std::unexpected(session.error());
  for (const auto &record : stored->records)
    if (auto appended = (*session)->append(record); !appended)
      return std::unexpected(appended.error());
  if (auto committed = (*session)->commit(); !committed)
    return std::unexpected(committed.error());
  const auto updates_path = result->path_.string() + ".updates";
  auto replayed = replay_updates(
      updates_path,
      [&](std::uint64_t generation,
          std::span<const model::TorrentRecord> updates) -> core::Result<void> {
        if (generation <= result->memory_.source_generation())
          return {};
        auto update = result->memory_.begin_update(generation);
        if (!update)
          return std::unexpected(update.error());
        for (const auto &record : updates)
          if (auto applied = (*update)->upsert(record); !applied)
            return std::unexpected(applied.error());
        return (*update)->commit();
      });
  if (!replayed)
    return std::unexpected(replayed.error());
  return result;
}

core::Result<std::unique_ptr<SearchRebuildSession>>
LocalSearchIndex::begin_rebuild(std::uint64_t source_generation) {
  return std::unique_ptr<SearchRebuildSession>{
      std::make_unique<LocalRebuildSession>(*this, source_generation)};
}

core::Result<std::unique_ptr<SearchUpdateSession>>
LocalSearchIndex::begin_update(std::uint64_t source_generation) {
  return std::unique_ptr<SearchUpdateSession>{
      std::make_unique<LocalUpdateSession>(*this, source_generation)};
}

core::Result<SearchResult>
LocalSearchIndex::search(const SearchQuery &query) const {
  return memory_.search(query);
}

std::uint64_t LocalSearchIndex::source_generation() const noexcept {
  return memory_.source_generation();
}

ClassificationIndexStats
LocalSearchIndex::classification_stats() const noexcept {
  return memory_.classification_stats();
}

core::Result<void>
LocalSearchIndex::replace(std::uint64_t source_generation,
                          std::vector<model::TorrentRecord> records) {
  std::lock_guard lock{mutex_};
  auto session = memory_.begin_rebuild(source_generation);
  if (!session)
    return std::unexpected(session.error());
  for (const auto &record : records)
    if (auto appended = (*session)->append(record); !appended)
      return std::unexpected(appended.error());
  if (auto committed = (*session)->commit(); !committed)
    return std::unexpected(committed.error());
  if (auto saved = persist(source_generation, records); !saved)
    return saved;
  return clear_updates();
}

core::Result<void>
LocalSearchIndex::apply(std::uint64_t source_generation,
                        std::span<const model::TorrentRecord> updates) {
  std::lock_guard lock{mutex_};
  auto session = memory_.begin_update(source_generation);
  if (!session)
    return std::unexpected(session.error());
  for (const auto &update : updates)
    if (auto applied = (*session)->upsert(update); !applied)
      return std::unexpected(applied.error());
  if (auto saved = persist_updates(source_generation, updates); !saved)
    return saved;
  return (*session)->commit();
}

core::Result<void>
LocalSearchIndex::persist(std::uint64_t source_generation,
                          std::span<const model::TorrentRecord> records) const {
  std::error_code directory_error;
  std::filesystem::create_directories(path_.parent_path(), directory_error);
  if (directory_error)
    return std::unexpected(io_error("Could not create search-index directory",
                                    path_.parent_path(),
                                    directory_error.message()));
  const auto temporary = path_.string() + ".temporary";
  std::ofstream output{temporary, std::ios::binary | std::ios::trunc};
  if (!output)
    return std::unexpected(
        io_error("Could not create local search index", temporary));
  core::Sha256Hasher hash;
  if (auto written = write_bytes(output, hash, std::as_bytes(std::span{Magic}));
      !written)
    return written;
  if (auto written = write_integer(output, hash, source_generation); !written)
    return written;
  const auto searchable_records =
      std::ranges::count_if(records, [](const auto &record) {
        return record.name.has_value() && !record.files.empty();
      });
  if (auto written = write_integer(
          output, hash, static_cast<std::uint64_t>(searchable_records));
      !written)
    return written;
  storage::TorrentRecordCodec codec;
  for (const auto &record : records) {
    if (!record.name || record.files.empty())
      continue;
    core::ByteBuffer encoded;
    if (auto encoded_result = codec.encode(record, encoded); !encoded_result)
      return encoded_result;
    if (encoded.size() > std::numeric_limits<std::uint32_t>::max())
      return std::unexpected(core::Error{
          core::ErrorCode::InvalidArgument,
          "Torrent record is too large for the local search index"});
    if (auto written = write_integer(
            output, hash, static_cast<std::uint32_t>(encoded.size()));
        !written)
      return written;
    if (auto written = write_bytes(output, hash, encoded); !written)
      return written;
  }
  const auto digest = hash.finalize();
  output.write(reinterpret_cast<const char *>(digest.bytes.data()),
               static_cast<std::streamsize>(digest.bytes.size()));
  output.flush();
  if (!output)
    return std::unexpected(
        io_error("Could not finish local search index", temporary));
  output.close();
  if (auto synced = sync_path(temporary, false); !synced)
    return synced;
  std::error_code rename_error;
  std::filesystem::rename(temporary, path_, rename_error);
  if (rename_error)
    return std::unexpected(io_error("Could not publish local search index",
                                    path_, rename_error.message()));
  return sync_path(path_.parent_path(), true);
}

core::Result<void> LocalSearchIndex::persist_updates(
    std::uint64_t source_generation,
    std::span<const model::TorrentRecord> updates) const {
  if (updates.empty())
    return {};
  const auto searchable_updates =
      std::ranges::count_if(updates, [](const auto &record) {
        return record.name.has_value() && !record.files.empty();
      });
  if (searchable_updates == 0)
    return {};
  std::error_code directory_error;
  std::filesystem::create_directories(path_.parent_path(), directory_error);
  if (directory_error)
    return std::unexpected(io_error("Could not create search-index directory",
                                    path_.parent_path(),
                                    directory_error.message()));
  const auto updates_path = path_.string() + ".updates";
  std::ofstream output{updates_path, std::ios::binary | std::ios::app};
  if (!output)
    return std::unexpected(
        io_error("Could not append local search updates", updates_path));
  core::Sha256Hasher hash;
  if (auto written =
          write_bytes(output, hash, std::as_bytes(std::span{UpdateMagic}));
      !written)
    return written;
  if (auto written = write_integer(output, hash, source_generation); !written)
    return written;
  if (auto written = write_integer(
          output, hash, static_cast<std::uint64_t>(searchable_updates));
      !written)
    return written;
  storage::TorrentRecordCodec codec;
  for (const auto &record : updates) {
    if (!record.name || record.files.empty())
      continue;
    core::ByteBuffer encoded;
    if (auto encoded_result = codec.encode(record, encoded); !encoded_result)
      return encoded_result;
    if (encoded.size() > std::numeric_limits<std::uint32_t>::max())
      return std::unexpected(core::Error{
          core::ErrorCode::InvalidArgument,
          "Torrent record is too large for the local search update log"});
    if (auto written = write_integer(
            output, hash, static_cast<std::uint32_t>(encoded.size()));
        !written)
      return written;
    if (auto written = write_bytes(output, hash, encoded); !written)
      return written;
  }
  const auto digest = hash.finalize();
  output.write(reinterpret_cast<const char *>(digest.bytes.data()),
               static_cast<std::streamsize>(digest.bytes.size()));
  output.flush();
  if (!output)
    return std::unexpected(
        io_error("Could not finish local search updates", updates_path));
  output.close();
  return sync_path(updates_path, false);
}

core::Result<void> LocalSearchIndex::clear_updates() const {
  const auto updates_path = std::filesystem::path{path_.string() + ".updates"};
  std::error_code remove_error;
  const auto removed = std::filesystem::remove(updates_path, remove_error);
  if (remove_error)
    return std::unexpected(io_error("Could not clear local search updates",
                                    updates_path, remove_error.message()));
  return removed ? sync_path(path_.parent_path(), true) : core::Result<void>{};
}

} // namespace sakuin::search

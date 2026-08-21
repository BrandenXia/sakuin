module;

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>

export module sakuin.search.checkpoint;

import std;

import sakuin.core.bytes;
import sakuin.core.hash;
import sakuin.core.result;
import sakuin.storage.dataset.torrents;

export namespace sakuin::search {

class LocalSearchCursorStore final {
public:
  explicit LocalSearchCursorStore(std::filesystem::path path)
      : path_(std::move(path)) {}

  core::Result<storage::TorrentChangeCursor> load() const;
  core::Result<void> save(const storage::TorrentChangeCursor &cursor) const;
  const std::filesystem::path &path() const noexcept { return path_; }

private:
  std::filesystem::path path_;
};

} // namespace sakuin::search

namespace sakuin::search {
namespace {

constexpr std::string_view CursorMagic{"sakuin-search-cursor-v1\n"};

core::Error io_error(std::string action, const std::filesystem::path &path,
                     std::string detail = {}) {
  std::string message = std::move(action) + ": " + path.string();
  if (!detail.empty())
    message += ": " + detail;
  return {core::ErrorCode::IoError, std::move(message)};
}

void append_integer(core::ByteBuffer &output, std::uint64_t value) {
  for (std::size_t index = 0; index < sizeof(value); ++index)
    output.push_back(static_cast<core::Byte>(value >> (index * 8U)));
}

std::uint64_t read_integer(core::ByteView input, std::size_t offset) {
  std::uint64_t result{};
  for (std::size_t index = 0; index < sizeof(result); ++index)
    result |= static_cast<std::uint64_t>(
                  std::to_integer<unsigned>(input[offset + index]))
              << (index * 8U);
  return result;
}

core::Result<void> sync_path(const std::filesystem::path &path,
                             bool directory) {
  const auto descriptor = ::open(
      path.c_str(), O_RDONLY | O_CLOEXEC | (directory ? O_DIRECTORY : 0));
  if (descriptor < 0)
    return std::unexpected(io_error("Could not open search cursor path", path,
                                    std::strerror(errno)));
  if (::fsync(descriptor) != 0) {
    const auto detail = std::string{std::strerror(errno)};
    ::close(descriptor);
    return std::unexpected(
        io_error("Could not sync search cursor path", path, detail));
  }
  ::close(descriptor);
  return {};
}

} // namespace

core::Result<storage::TorrentChangeCursor>
LocalSearchCursorStore::load() const {
  std::error_code status_error;
  if (!std::filesystem::exists(path_, status_error)) {
    if (status_error)
      return std::unexpected(io_error("Could not inspect search cursor", path_,
                                      status_error.message()));
    return storage::TorrentChangeCursor{};
  }
  std::ifstream input{path_, std::ios::binary};
  if (!input)
    return std::unexpected(io_error("Could not open search cursor", path_));
  const std::vector<char> raw{std::istreambuf_iterator<char>{input},
                              std::istreambuf_iterator<char>{}};
  core::ByteBuffer encoded(raw.size());
  std::memcpy(encoded.data(), raw.data(), raw.size());
  constexpr auto fields_size = 1U + sizeof(std::uint64_t) * 2U + 32U;
  const auto payload_size = CursorMagic.size() + fields_size;
  if (encoded.size() != payload_size + 32U)
    return std::unexpected(core::Error{core::ErrorCode::InvalidManifest,
                                       "Search cursor has an invalid size"});
  const core::ByteView payload{encoded.data(), payload_size};
  if (!std::ranges::equal(payload.first(CursorMagic.size()),
                          std::as_bytes(std::span{CursorMagic})))
    return std::unexpected(core::Error{core::ErrorCode::UnsupportedFormat,
                                       "Search cursor version is unsupported"});
  const auto checksum = core::sha256(payload);
  if (!core::constant_time_equal(std::as_bytes(std::span{checksum.bytes}),
                                 core::ByteView{encoded}.subspan(payload_size)))
    return std::unexpected(core::Error{core::ErrorCode::ChecksumMismatch,
                                       "Search cursor checksum mismatch"});
  std::size_t offset = CursorMagic.size();
  storage::TorrentChangeCursor result;
  result.initialized = std::to_integer<unsigned>(encoded[offset++]) != 0;
  result.source_generation = read_integer(payload, offset);
  offset += sizeof(std::uint64_t);
  result.segment_count = read_integer(payload, offset);
  offset += sizeof(std::uint64_t);
  for (std::size_t index = 0; index < result.segment_prefix_digest.bytes.size();
       ++index)
    result.segment_prefix_digest.bytes[index] =
        std::to_integer<std::uint8_t>(payload[offset + index]);
  return result;
}

core::Result<void>
LocalSearchCursorStore::save(const storage::TorrentChangeCursor &cursor) const {
  std::error_code directory_error;
  std::filesystem::create_directories(path_.parent_path(), directory_error);
  if (directory_error)
    return std::unexpected(io_error("Could not create search cursor directory",
                                    path_.parent_path(),
                                    directory_error.message()));
  core::ByteBuffer encoded;
  const auto magic = std::as_bytes(std::span{CursorMagic});
  encoded.insert(encoded.end(), magic.begin(), magic.end());
  encoded.push_back(static_cast<core::Byte>(cursor.initialized));
  append_integer(encoded, cursor.source_generation);
  append_integer(encoded, cursor.segment_count);
  const auto digest =
      std::as_bytes(std::span{cursor.segment_prefix_digest.bytes});
  encoded.insert(encoded.end(), digest.begin(), digest.end());
  const auto checksum = core::sha256(encoded);
  const auto checksum_bytes = std::as_bytes(std::span{checksum.bytes});
  encoded.insert(encoded.end(), checksum_bytes.begin(), checksum_bytes.end());

  const auto temporary = path_.string() + ".temporary";
  {
    std::ofstream output{temporary, std::ios::binary | std::ios::trunc};
    output.write(reinterpret_cast<const char *>(encoded.data()),
                 static_cast<std::streamsize>(encoded.size()));
    output.flush();
    if (!output)
      return std::unexpected(
          io_error("Could not write search cursor", temporary));
  }
  if (auto synced = sync_path(temporary, false); !synced)
    return synced;
  std::error_code rename_error;
  std::filesystem::rename(temporary, path_, rename_error);
  if (rename_error)
    return std::unexpected(io_error("Could not publish search cursor", path_,
                                    rename_error.message()));
  return sync_path(path_.parent_path(), true);
}

} // namespace sakuin::search

module;

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>

export module sakuin.storage.blob.local;

import std;

import sakuin.core.bytes;
import sakuin.core.hash;
import sakuin.core.ids;
import sakuin.core.result;
import sakuin.storage.blob.reader;
import sakuin.storage.blob.store;
import sakuin.storage.blob.writer;

export namespace sakuin::storage {

class LocalBlobStore final : public BlobStore {
public:
  explicit LocalBlobStore(std::filesystem::path root);

  core::Result<std::unique_ptr<BlobWriter>> create() override;
  core::Result<std::unique_ptr<BlobReader>> open(core::ObjectId id) override;
  core::Result<bool> exists(core::ObjectId id) override;
  core::Result<void> remove(core::ObjectId id) override;

private:
  std::filesystem::path object_path(core::ObjectId id) const;
  std::filesystem::path root_;
};

} // namespace sakuin::storage

namespace sakuin::storage {
namespace {

core::Error io_error(std::string operation, const std::filesystem::path &path,
                     std::string detail = {}) {
  std::string message = std::move(operation) + " '" + path.string() + "'";
  if (!detail.empty())
    message += ": " + detail;
  return {core::ErrorCode::IoError, std::move(message)};
}

std::string object_name(const core::ObjectId &id) {
  static constexpr char digits[] = "0123456789abcdef";
  std::string result;
  result.resize(id.bytes.size() * 2);
  for (std::size_t i = 0; i < id.bytes.size(); ++i) {
    result[2 * i] = digits[id.bytes[i] >> 4];
    result[2 * i + 1] = digits[id.bytes[i] & 0x0f];
  }
  return result;
}

core::ObjectId object_id(const core::Hash256 &hash) {
  return {.bytes = hash.bytes};
}

core::Result<void> sync_file(const std::filesystem::path &path) {
  const int descriptor = ::open(path.c_str(), O_RDONLY);
  if (descriptor < 0)
    return std::unexpected(io_error("Could not open", path, std::strerror(errno)));
  if (::fsync(descriptor) != 0) {
    const auto detail = std::string{std::strerror(errno)};
    ::close(descriptor);
    return std::unexpected(io_error("Could not sync", path, detail));
  }
  ::close(descriptor);
  return {};
}

core::Result<void> sync_directory(const std::filesystem::path &path) {
  const int descriptor = ::open(path.c_str(), O_RDONLY | O_DIRECTORY);
  if (descriptor < 0)
    return std::unexpected(io_error("Could not open directory", path,
                                    std::strerror(errno)));
  if (::fsync(descriptor) != 0) {
    const auto detail = std::string{std::strerror(errno)};
    ::close(descriptor);
    return std::unexpected(io_error("Could not sync directory", path, detail));
  }
  ::close(descriptor);
  return {};
}

class LocalBlobReader final : public BlobReader {
public:
  LocalBlobReader(std::filesystem::path path, std::uint64_t size)
      : path_(std::move(path)), stream_(path_, std::ios::binary), size_(size) {}

  bool is_open() const noexcept { return stream_.is_open(); }

  core::Result<std::size_t> read(core::MutableByteView buffer) override {
    stream_.read(reinterpret_cast<char *>(buffer.data()),
                 static_cast<std::streamsize>(buffer.size()));
    const auto count = static_cast<std::size_t>(stream_.gcount());
    if (stream_.bad())
      return std::unexpected(io_error("Could not read", path_));
    return count;
  }

  core::Result<std::size_t>
  read_at(std::uint64_t offset, core::MutableByteView buffer) override {
    std::ifstream stream{path_, std::ios::binary};
    if (!stream)
      return std::unexpected(io_error("Could not open blob", path_));
    stream.seekg(static_cast<std::streamoff>(offset));
    if (!stream)
      return std::unexpected(io_error("Could not seek blob", path_));
    stream.read(reinterpret_cast<char *>(buffer.data()),
                static_cast<std::streamsize>(buffer.size()));
    const auto count = static_cast<std::size_t>(stream.gcount());
    if (stream.bad())
      return std::unexpected(io_error("Could not read blob", path_));
    return count;
  }

  std::uint64_t size() const noexcept override { return size_; }

private:
  std::filesystem::path path_;
  std::ifstream stream_;
  std::uint64_t size_;
};

class LocalBlobWriter final : public BlobWriter {
public:
  LocalBlobWriter(std::filesystem::path temporary,
                  std::filesystem::path objects)
      : temporary_(std::move(temporary)), objects_(std::move(objects)),
        stream_(temporary_, std::ios::binary | std::ios::trunc) {}

  ~LocalBlobWriter() override { abort(); }

  bool is_open() const noexcept { return stream_.is_open(); }

  core::Result<void> write(core::ByteView data) override {
    if (!active_)
      return std::unexpected(core::Error{core::ErrorCode::InvalidArgument,
                                         "Blob writer is no longer active"});
    stream_.write(reinterpret_cast<const char *>(data.data()),
                  static_cast<std::streamsize>(data.size()));
    if (!stream_)
      return std::unexpected(io_error("Could not write", temporary_));
    try {
      hasher_.update(data);
    } catch (const std::exception &error) {
      return std::unexpected(core::Error{core::ErrorCode::Internal, error.what()});
    }
    return {};
  }

  core::Result<core::ObjectId> finalize() override {
    if (!active_)
      return std::unexpected(core::Error{core::ErrorCode::InvalidArgument,
                                         "Blob writer is no longer active"});

    stream_.flush();
    if (!stream_)
      return std::unexpected(io_error("Could not flush", temporary_));
    stream_.close();

    if (auto synced = sync_file(temporary_); !synced)
      return std::unexpected(synced.error());

    core::ObjectId id;
    try {
      id = object_id(hasher_.finalize());
    } catch (const std::exception &error) {
      return std::unexpected(core::Error{core::ErrorCode::Internal, error.what()});
    }

    const auto name = object_name(id);
    const auto directory = objects_ / name.substr(0, 2);
    const auto destination = directory / name;
    std::error_code error;
    std::filesystem::create_directories(directory, error);
    if (error)
      return std::unexpected(io_error("Could not create directory", directory,
                                      error.message()));

    if (std::filesystem::exists(destination, error)) {
      if (error)
        return std::unexpected(io_error("Could not inspect", destination,
                                        error.message()));
      std::filesystem::remove(temporary_, error);
      if (error)
        return std::unexpected(io_error("Could not remove temporary blob",
                                        temporary_, error.message()));
    } else {
      std::filesystem::rename(temporary_, destination, error);
      if (error)
        return std::unexpected(io_error("Could not publish blob", destination,
                                        error.message()));
      if (auto synced = sync_directory(directory); !synced)
        return std::unexpected(synced.error());
    }

    active_ = false;
    return id;
  }

  void abort() noexcept override {
    if (!active_)
      return;
    active_ = false;
    stream_.close();
    std::error_code ignored;
    std::filesystem::remove(temporary_, ignored);
  }

private:
  std::filesystem::path temporary_;
  std::filesystem::path objects_;
  std::ofstream stream_;
  core::Sha256Hasher hasher_;
  bool active_{true};
};

} // namespace

LocalBlobStore::LocalBlobStore(std::filesystem::path root)
    : root_(std::move(root)) {}

std::filesystem::path LocalBlobStore::object_path(core::ObjectId id) const {
  const auto name = object_name(id);
  return root_ / "objects" / name.substr(0, 2) / name;
}

core::Result<std::unique_ptr<BlobWriter>> LocalBlobStore::create() {
  const auto temporary_directory = root_ / "temporary";
  std::error_code error;
  std::filesystem::create_directories(temporary_directory, error);
  if (error)
    return std::unexpected(io_error("Could not create directory",
                                    temporary_directory, error.message()));

  static std::atomic<std::uint64_t> sequence{};
  const auto nonce = std::to_string(::getpid()) + "-" +
                     std::to_string(sequence.fetch_add(1)) + "-" +
                     std::to_string(std::chrono::steady_clock::now()
                                        .time_since_epoch()
                                        .count());
  auto writer = std::make_unique<LocalBlobWriter>(temporary_directory / nonce,
                                                   root_ / "objects");
  if (!writer->is_open())
    return std::unexpected(io_error("Could not create temporary blob",
                                    temporary_directory / nonce));
  return std::unique_ptr<BlobWriter>{std::move(writer)};
}

core::Result<std::unique_ptr<BlobReader>>
LocalBlobStore::open(core::ObjectId id) {
  const auto path = object_path(id);
  std::error_code error;
  const auto size = std::filesystem::file_size(path, error);
  if (error) {
    const auto code = error == std::errc::no_such_file_or_directory
                          ? core::ErrorCode::NotFound
                          : core::ErrorCode::IoError;
    return std::unexpected(core::Error{code, "Could not open blob '" +
                                                path.string() + "': " +
                                                error.message()});
  }
  auto reader = std::make_unique<LocalBlobReader>(path, size);
  if (!reader->is_open())
    return std::unexpected(io_error("Could not open blob", path));
  return std::unique_ptr<BlobReader>{std::move(reader)};
}

core::Result<bool> LocalBlobStore::exists(core::ObjectId id) {
  std::error_code error;
  const bool found = std::filesystem::is_regular_file(object_path(id), error);
  if (error == std::errc::no_such_file_or_directory)
    return false;
  if (error)
    return std::unexpected(io_error("Could not inspect blob", object_path(id),
                                    error.message()));
  return found;
}

core::Result<void> LocalBlobStore::remove(core::ObjectId id) {
  const auto path = object_path(id);
  std::error_code error;
  const bool removed = std::filesystem::remove(path, error);
  if (error)
    return std::unexpected(io_error("Could not remove blob", path,
                                    error.message()));
  if (!removed)
    return std::unexpected(core::Error{core::ErrorCode::NotFound,
                                       "Blob does not exist: '" +
                                           path.string() + "'"});
  return {};
}

} // namespace sakuin::storage

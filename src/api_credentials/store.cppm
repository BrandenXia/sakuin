module;

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <toml++/toml.hpp>
#include <unistd.h>

export module sakuin.api.credentials.store;

import std;

import sakuin.api.auth;
import sakuin.core.bytes;
import sakuin.core.hash;
import sakuin.core.random;
import sakuin.core.result;
import sakuin.core.time;

export namespace sakuin::api {

struct ApiCredentialStoreEntry {
  ApiCredential credential;
  core::Timestamp created_at;
};

struct ApiCredentialStoreSnapshot {
  std::uint64_t generation{};
  std::vector<ApiCredentialStoreEntry> entries;
};

class ApiCredentialStore {
public:
  virtual ~ApiCredentialStore() = default;
  virtual core::Result<ApiKeyPepper> pepper() const = 0;
  virtual core::Result<ApiCredentialStoreSnapshot> snapshot() const = 0;
  virtual core::Result<void> insert(ApiCredential credential) = 0;
  virtual core::Result<bool> disable(std::string_view key_id) = 0;
};

class LocalApiCredentialStore final : public ApiCredentialStore {
public:
  static core::Result<std::unique_ptr<LocalApiCredentialStore>>
  initialize(std::filesystem::path root);
  static core::Result<std::unique_ptr<LocalApiCredentialStore>>
  open(std::filesystem::path root);

  ~LocalApiCredentialStore() override;

  core::Result<ApiKeyPepper> pepper() const override;
  core::Result<ApiCredentialStoreSnapshot> snapshot() const override;
  core::Result<void> insert(ApiCredential credential) override;
  core::Result<bool> disable(std::string_view key_id) override;

  [[nodiscard]] const std::filesystem::path &root() const noexcept;

private:
  struct Impl;
  explicit LocalApiCredentialStore(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> impl_;
};

core::Result<std::unique_ptr<InMemoryApiKeyAuthenticator>>
load_api_key_authenticator(const ApiCredentialStore &store);

} // namespace sakuin::api

namespace sakuin::api {
namespace {

constexpr std::uint64_t format_version = 1;
constexpr std::size_t maximum_store_bytes = 4U * 1024U * 1024U;
constexpr std::size_t maximum_credentials = 10'000;

core::Error io_error(std::string action, const std::filesystem::path &path,
                     std::string detail = {}) {
  std::string message = std::move(action) + ": " + path.string();
  if (!detail.empty())
    message += ": " + detail;
  return {core::ErrorCode::IoError, std::move(message)};
}

core::Error invalid(std::string message) {
  return {core::ErrorCode::InvalidArgument, std::move(message)};
}

class Descriptor {
public:
  explicit Descriptor(int value = -1) : value_(value) {}
  ~Descriptor() {
    if (value_ >= 0)
      ::close(value_);
  }
  Descriptor(Descriptor &&other) noexcept
      : value_(std::exchange(other.value_, -1)) {}
  Descriptor &operator=(Descriptor &&other) noexcept {
    if (this != &other) {
      if (value_ >= 0)
        ::close(value_);
      value_ = std::exchange(other.value_, -1);
    }
    return *this;
  }
  Descriptor(const Descriptor &) = delete;
  Descriptor &operator=(const Descriptor &) = delete;
  [[nodiscard]] int get() const noexcept { return value_; }
  [[nodiscard]] bool valid() const noexcept { return value_ >= 0; }

private:
  int value_;
};

core::Result<void> secure_path(const std::filesystem::path &path,
                               bool directory) {
  struct stat status{};
  if (::lstat(path.c_str(), &status) != 0)
    return std::unexpected(io_error("Could not inspect credential path", path,
                                    std::strerror(errno)));
  if ((directory && !S_ISDIR(status.st_mode)) ||
      (!directory && !S_ISREG(status.st_mode)))
    return std::unexpected(
        invalid("Credential path has the wrong file type: " + path.string()));
  if (status.st_uid != ::geteuid())
    return std::unexpected(invalid(
        "Credential paths must be owned by the current operating-system user"));
  if ((status.st_mode & (S_IRWXG | S_IRWXO)) != 0)
    return std::unexpected(
        invalid("Credential paths must not grant group or other permissions: " +
                path.string()));
  return {};
}

core::Result<void> write_all(int descriptor, core::ByteView bytes,
                             const std::filesystem::path &path) {
  while (!bytes.empty()) {
    const auto written = ::write(descriptor, bytes.data(), bytes.size());
    if (written < 0) {
      if (errno == EINTR)
        continue;
      return std::unexpected(io_error("Could not write credential file", path,
                                      std::strerror(errno)));
    }
    bytes = bytes.subspan(static_cast<std::size_t>(written));
  }
  return {};
}

core::Result<void> sync_directory(const std::filesystem::path &path) {
  Descriptor descriptor{
      ::open(path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC)};
  if (!descriptor.valid())
    return std::unexpected(io_error("Could not open credential directory", path,
                                    std::strerror(errno)));
  if (::fsync(descriptor.get()) != 0)
    return std::unexpected(io_error("Could not sync credential directory", path,
                                    std::strerror(errno)));
  return {};
}

core::Result<void> write_exclusive(const std::filesystem::path &path,
                                   core::ByteView bytes) {
  Descriptor descriptor{
      ::open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW,
             S_IRUSR | S_IWUSR)};
  if (!descriptor.valid())
    return std::unexpected(io_error("Could not create credential file", path,
                                    std::strerror(errno)));
  if (auto written = write_all(descriptor.get(), bytes, path); !written)
    return written;
  if (::fsync(descriptor.get()) != 0)
    return std::unexpected(
        io_error("Could not sync credential file", path, std::strerror(errno)));
  return sync_directory(path.parent_path());
}

core::Result<void> write_atomic(const std::filesystem::path &path,
                                core::ByteView bytes) {
  static std::atomic<std::uint64_t> sequence{};
  const auto temporary = path.string() + ".tmp." +
                         std::to_string(static_cast<long long>(::getpid())) +
                         "." + std::to_string(sequence.fetch_add(1));
  Descriptor descriptor{::open(
      temporary.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW,
      S_IRUSR | S_IWUSR)};
  if (!descriptor.valid())
    return std::unexpected(
        io_error("Could not create temporary credential file", temporary,
                 std::strerror(errno)));
  auto cleanup = [&] {
    std::error_code ignored;
    std::filesystem::remove(temporary, ignored);
  };
  if (auto written = write_all(descriptor.get(), bytes, temporary); !written) {
    cleanup();
    return written;
  }
  if (::fsync(descriptor.get()) != 0) {
    auto error = io_error("Could not sync temporary credential file", temporary,
                          std::strerror(errno));
    cleanup();
    return std::unexpected(std::move(error));
  }
  if (::rename(temporary.c_str(), path.c_str()) != 0) {
    auto error = io_error("Could not publish credential file", path,
                          std::strerror(errno));
    cleanup();
    return std::unexpected(std::move(error));
  }
  return sync_directory(path.parent_path());
}

core::Result<core::ByteBuffer> read_file(const std::filesystem::path &path,
                                         std::size_t maximum_bytes) {
  Descriptor descriptor{
      ::open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW)};
  if (!descriptor.valid())
    return std::unexpected(
        io_error("Could not open credential file", path, std::strerror(errno)));
  struct stat status{};
  if (::fstat(descriptor.get(), &status) != 0)
    return std::unexpected(io_error("Could not inspect credential file", path,
                                    std::strerror(errno)));
  if (!S_ISREG(status.st_mode) || status.st_uid != ::geteuid() ||
      (status.st_mode & (S_IRWXG | S_IRWXO)) != 0)
    return std::unexpected(
        invalid("Credential file must be a current-user-owned regular file "
                "with mode 0600: " +
                path.string()));
  if (status.st_size < 0 ||
      static_cast<std::uint64_t>(status.st_size) > maximum_bytes)
    return std::unexpected(
        invalid("Credential file exceeds its size limit: " + path.string()));
  core::ByteBuffer result(static_cast<std::size_t>(status.st_size));
  std::size_t offset{};
  while (offset < result.size()) {
    const auto count = ::read(descriptor.get(), result.data() + offset,
                              result.size() - offset);
    if (count < 0) {
      if (errno == EINTR)
        continue;
      return std::unexpected(io_error("Could not read credential file", path,
                                      std::strerror(errno)));
    }
    if (count == 0)
      return std::unexpected(
          invalid("Credential file was truncated while read"));
    offset += static_cast<std::size_t>(count);
  }
  return result;
}

class FileLock {
public:
  static core::Result<FileLock> acquire(const std::filesystem::path &path,
                                        bool exclusive) {
    Descriptor descriptor{::open(path.c_str(),
                                 O_RDWR | O_CREAT | O_CLOEXEC | O_NOFOLLOW,
                                 S_IRUSR | S_IWUSR)};
    if (!descriptor.valid())
      return std::unexpected(io_error("Could not open credential lock", path,
                                      std::strerror(errno)));
    if (auto secure = secure_path(path, false); !secure)
      return std::unexpected(secure.error());
    if (::flock(descriptor.get(), exclusive ? LOCK_EX : LOCK_SH) != 0)
      return std::unexpected(io_error("Could not lock credential store", path,
                                      std::strerror(errno)));
    return FileLock{std::move(descriptor)};
  }

  FileLock(FileLock &&) noexcept = default;
  FileLock &operator=(FileLock &&) noexcept = default;
  FileLock(const FileLock &) = delete;
  FileLock &operator=(const FileLock &) = delete;

private:
  explicit FileLock(Descriptor descriptor)
      : descriptor_(std::move(descriptor)) {}
  Descriptor descriptor_;
};

std::optional<std::uint8_t> hex_digit(char value) {
  if (value >= '0' && value <= '9')
    return static_cast<std::uint8_t>(value - '0');
  if (value >= 'a' && value <= 'f')
    return static_cast<std::uint8_t>(value - 'a' + 10);
  if (value >= 'A' && value <= 'F')
    return static_cast<std::uint8_t>(value - 'A' + 10);
  return std::nullopt;
}

std::string hex(const core::Hash256 &hash) {
  constexpr std::string_view digits{"0123456789abcdef"};
  std::string result;
  result.reserve(hash.bytes.size() * 2);
  for (const auto byte : hash.bytes) {
    result.push_back(digits[byte >> 4]);
    result.push_back(digits[byte & 0x0f]);
  }
  return result;
}

core::Result<core::Hash256> hash(std::string_view encoded) {
  if (encoded.size() != core::Hash256{}.bytes.size() * 2)
    return std::unexpected(
        invalid("Credential verifier must contain 64 hex digits"));
  core::Hash256 result{};
  for (std::size_t index = 0; index < result.bytes.size(); ++index) {
    const auto high = hex_digit(encoded[index * 2]);
    const auto low = hex_digit(encoded[index * 2 + 1]);
    if (!high || !low)
      return std::unexpected(invalid("Credential verifier is not hexadecimal"));
    result.bytes[index] = static_cast<std::uint8_t>((*high << 4) | *low);
  }
  if (std::ranges::all_of(result.bytes,
                          [](std::uint8_t byte) { return byte == 0; }))
    return std::unexpected(invalid("Credential verifier must not be all zero"));
  return result;
}

std::string_view permission_name(Permission permission) {
  return permission == Permission::Search ? "search" : "admin";
}

core::Result<Permission> permission(std::string_view name) {
  if (name == "search")
    return Permission::Search;
  if (name == "admin")
    return Permission::Admin;
  return std::unexpected(
      invalid("Unknown API permission: " + std::string{name}));
}

core::Result<void> check_keys(const toml::table &table,
                              std::initializer_list<std::string_view> allowed,
                              std::string_view context) {
  for (const auto &[key, _] : table) {
    if (std::ranges::find(allowed, key.str()) == allowed.end())
      return std::unexpected(invalid("Unknown " + std::string{context} +
                                     " key: " + std::string{key.str()}));
  }
  return {};
}

core::Result<ApiCredentialStoreSnapshot> decode(std::string_view source) {
  try {
    const auto document =
        toml::parse(source, std::string_view{"credentials.toml"});
    if (auto checked =
            check_keys(document, {"format_version", "generation", "credential"},
                       "credential-store");
        !checked)
      return std::unexpected(checked.error());
    const auto version = document["format_version"].value<std::int64_t>();
    const auto generation = document["generation"].value<std::int64_t>();
    if (!version || *version != static_cast<std::int64_t>(format_version))
      return std::unexpected(
          invalid("Unsupported credential-store format version"));
    if (!generation || *generation < 0)
      return std::unexpected(invalid("Credential-store generation is invalid"));

    ApiCredentialStoreSnapshot result{
        .generation = static_cast<std::uint64_t>(*generation)};
    const auto credentials = document["credential"];
    if (!credentials)
      return result;
    const auto *array = credentials.as_array();
    if (!array || array->size() > maximum_credentials)
      return std::unexpected(
          invalid("Credential entries must be a bounded TOML array"));
    std::unordered_set<std::string> ids;
    for (const auto &node : *array) {
      const auto *table = node.as_table();
      if (!table)
        return std::unexpected(
            invalid("Credential entry must be a TOML table"));
      if (auto checked = check_keys(
              *table,
              {"id", "verifier", "permissions", "enabled", "created_at_ms"},
              "credential");
          !checked)
        return std::unexpected(checked.error());
      const auto id = (*table)["id"].value<std::string>();
      const auto encoded = (*table)["verifier"].value<std::string>();
      const auto enabled = (*table)["enabled"].value<bool>();
      const auto created = (*table)["created_at_ms"].value<std::int64_t>();
      if (!id || !valid_api_key_id(*id) || !encoded || !enabled || !created ||
          *created < 0 || !ids.insert(*id).second)
        return std::unexpected(invalid("Credential entry fields are invalid"));
      auto verifier = hash(*encoded);
      if (!verifier)
        return std::unexpected(verifier.error());
      const auto *permissions = (*table)["permissions"].as_array();
      if (!permissions || permissions->empty())
        return std::unexpected(
            invalid("Credential permissions must be a nonempty array"));
      std::set<Permission> parsed_permissions;
      for (const auto &item : *permissions) {
        const auto name = item.value<std::string>();
        if (!name)
          return std::unexpected(
              invalid("Credential permission must be a string"));
        auto parsed = permission(*name);
        if (!parsed || !parsed_permissions.insert(*parsed).second)
          return std::unexpected(
              parsed ? invalid("Duplicate credential permission")
                     : parsed.error());
      }
      result.entries.push_back(ApiCredentialStoreEntry{
          .credential = {.key_id = *id,
                         .verifier = *verifier,
                         .permissions = std::move(parsed_permissions),
                         .enabled = *enabled},
          .created_at = core::Timestamp{
              std::chrono::duration_cast<core::Timestamp::duration>(
                  std::chrono::milliseconds{*created})}});
    }
    std::ranges::sort(result.entries, {}, [](const auto &entry) {
      return entry.credential.key_id;
    });
    return result;
  } catch (const toml::parse_error &error) {
    return std::unexpected(invalid("Invalid credential-store TOML: " +
                                   std::string{error.description()}));
  }
}

core::ByteBuffer encode(const ApiCredentialStoreSnapshot &snapshot) {
  std::string output = "format_version = " + std::to_string(format_version) +
                       "\ngeneration = " + std::to_string(snapshot.generation) +
                       "\n";
  for (const auto &entry : snapshot.entries) {
    output += "\n[[credential]]\n";
    output += "id = \"" + entry.credential.key_id + "\"\n";
    output += "verifier = \"" + hex(entry.credential.verifier) + "\"\n";
    output += "permissions = [";
    bool first = true;
    for (const auto permission : entry.credential.permissions) {
      if (!first)
        output += ", ";
      first = false;
      output += "\"" + std::string{permission_name(permission)} + "\"";
    }
    output += "]\n";
    output += std::string{"enabled = "} +
              (entry.credential.enabled ? "true\n" : "false\n");
    const auto created = std::chrono::duration_cast<std::chrono::milliseconds>(
                             entry.created_at.time_since_epoch())
                             .count();
    output += "created_at_ms = " + std::to_string(created) + "\n";
  }
  const auto bytes = std::as_bytes(std::span{output});
  return {bytes.begin(), bytes.end()};
}

core::Result<ApiCredentialStoreSnapshot>
read_snapshot(const std::filesystem::path &path) {
  auto bytes = read_file(path, maximum_store_bytes);
  if (!bytes)
    return std::unexpected(bytes.error());
  const std::string_view source{reinterpret_cast<const char *>(bytes->data()),
                                bytes->size()};
  return decode(source);
}

core::MutableByteView pepper_view(ApiKeyPepper &pepper) {
  return {reinterpret_cast<std::byte *>(pepper.bytes.data()),
          pepper.bytes.size()};
}

} // namespace

struct LocalApiCredentialStore::Impl {
  std::filesystem::path root;
  std::filesystem::path pepper_path;
  std::filesystem::path credentials_path;
  std::filesystem::path lock_path;
};

LocalApiCredentialStore::LocalApiCredentialStore(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}
LocalApiCredentialStore::~LocalApiCredentialStore() = default;

core::Result<std::unique_ptr<LocalApiCredentialStore>>
LocalApiCredentialStore::initialize(std::filesystem::path root) {
  if (root.empty())
    return std::unexpected(invalid("Credential-store directory is required"));
  std::error_code error;
  std::filesystem::create_directories(root, error);
  if (error)
    return std::unexpected(io_error("Could not create credential directory",
                                    root, error.message()));
  if (::chmod(root.c_str(), S_IRWXU) != 0)
    return std::unexpected(io_error("Could not secure credential directory",
                                    root, std::strerror(errno)));
  if (auto secure = secure_path(root, true); !secure)
    return std::unexpected(secure.error());
  auto impl = std::make_unique<Impl>(Impl{
      .root = std::move(root),
  });
  impl->pepper_path = impl->root / "pepper";
  impl->credentials_path = impl->root / "credentials.toml";
  impl->lock_path = impl->root / "credentials.lock";
  auto lock = FileLock::acquire(impl->lock_path, true);
  if (!lock)
    return std::unexpected(lock.error());
  if (std::filesystem::exists(impl->pepper_path) ||
      std::filesystem::exists(impl->credentials_path))
    return std::unexpected(core::Error{
        core::ErrorCode::Conflict, "Credential store is already initialized"});

  ApiKeyPepper pepper;
  if (auto generated = core::secure_random(pepper_view(pepper)); !generated)
    return std::unexpected(generated.error());
  auto written = write_exclusive(impl->pepper_path, pepper_view(pepper));
  core::secure_clear(pepper_view(pepper));
  if (!written)
    return std::unexpected(written.error());
  const auto encoded = encode({});
  if (auto published = write_atomic(impl->credentials_path, encoded);
      !published) {
    std::error_code ignored;
    std::filesystem::remove(impl->pepper_path, ignored);
    return std::unexpected(published.error());
  }
  return std::unique_ptr<LocalApiCredentialStore>{
      new LocalApiCredentialStore{std::move(impl)}};
}

core::Result<std::unique_ptr<LocalApiCredentialStore>>
LocalApiCredentialStore::open(std::filesystem::path root) {
  if (root.empty())
    return std::unexpected(invalid("Credential-store directory is required"));
  if (auto secure = secure_path(root, true); !secure)
    return std::unexpected(secure.error());
  auto impl = std::make_unique<Impl>(Impl{.root = std::move(root)});
  impl->pepper_path = impl->root / "pepper";
  impl->credentials_path = impl->root / "credentials.toml";
  impl->lock_path = impl->root / "credentials.lock";
  if (auto secure = secure_path(impl->pepper_path, false); !secure)
    return std::unexpected(secure.error());
  if (auto secure = secure_path(impl->credentials_path, false); !secure)
    return std::unexpected(secure.error());
  auto store = std::unique_ptr<LocalApiCredentialStore>{
      new LocalApiCredentialStore{std::move(impl)}};
  if (auto loaded = store->pepper(); !loaded)
    return std::unexpected(loaded.error());
  if (auto loaded = store->snapshot(); !loaded)
    return std::unexpected(loaded.error());
  return store;
}

core::Result<ApiKeyPepper> LocalApiCredentialStore::pepper() const {
  auto lock = FileLock::acquire(impl_->lock_path, false);
  if (!lock)
    return std::unexpected(lock.error());
  auto bytes = read_file(impl_->pepper_path, ApiKeyPepper{}.bytes.size());
  if (!bytes)
    return std::unexpected(bytes.error());
  if (bytes->size() != ApiKeyPepper{}.bytes.size())
    return std::unexpected(
        invalid("API credential pepper must contain 32 bytes"));
  ApiKeyPepper result;
  std::memcpy(result.bytes.data(), bytes->data(), bytes->size());
  core::secure_clear(*bytes);
  return result;
}

core::Result<ApiCredentialStoreSnapshot>
LocalApiCredentialStore::snapshot() const {
  auto lock = FileLock::acquire(impl_->lock_path, false);
  if (!lock)
    return std::unexpected(lock.error());
  return read_snapshot(impl_->credentials_path);
}

core::Result<void> LocalApiCredentialStore::insert(ApiCredential credential) {
  if (!valid_api_key_id(credential.key_id) || credential.permissions.empty() ||
      !credential.enabled ||
      std::ranges::all_of(credential.verifier.bytes,
                          [](std::uint8_t byte) { return byte == 0; }))
    return std::unexpected(invalid(
        "New credential requires a valid id, permissions, and enabled state"));
  auto lock = FileLock::acquire(impl_->lock_path, true);
  if (!lock)
    return std::unexpected(lock.error());
  auto snapshot = read_snapshot(impl_->credentials_path);
  if (!snapshot)
    return std::unexpected(snapshot.error());
  if (std::ranges::any_of(snapshot->entries, [&](const auto &entry) {
        return entry.credential.key_id == credential.key_id;
      }))
    return std::unexpected(
        core::Error{core::ErrorCode::Conflict, "API key id already exists"});
  if (snapshot->entries.size() >= maximum_credentials ||
      snapshot->generation == std::numeric_limits<std::uint64_t>::max())
    return std::unexpected(
        core::Error{core::ErrorCode::QuotaExceeded,
                    "Credential store has reached its configured bounds"});
  snapshot->entries.push_back({.credential = std::move(credential),
                               .created_at = std::chrono::system_clock::now()});
  std::ranges::sort(snapshot->entries, {},
                    [](const auto &entry) { return entry.credential.key_id; });
  ++snapshot->generation;
  const auto encoded = encode(*snapshot);
  return write_atomic(impl_->credentials_path, encoded);
}

core::Result<bool> LocalApiCredentialStore::disable(std::string_view key_id) {
  if (!valid_api_key_id(key_id))
    return std::unexpected(invalid("A valid API key id is required"));
  auto lock = FileLock::acquire(impl_->lock_path, true);
  if (!lock)
    return std::unexpected(lock.error());
  auto snapshot = read_snapshot(impl_->credentials_path);
  if (!snapshot)
    return std::unexpected(snapshot.error());
  const auto found =
      std::ranges::find(snapshot->entries, key_id, [](const auto &entry) {
        return entry.credential.key_id;
      });
  if (found == snapshot->entries.end())
    return false;
  if (!found->credential.enabled)
    return true;
  if (snapshot->generation == std::numeric_limits<std::uint64_t>::max())
    return std::unexpected(core::Error{core::ErrorCode::QuotaExceeded,
                                       "Credential generation overflow"});
  found->credential.enabled = false;
  ++snapshot->generation;
  const auto encoded = encode(*snapshot);
  if (auto written = write_atomic(impl_->credentials_path, encoded); !written)
    return std::unexpected(written.error());
  return true;
}

const std::filesystem::path &LocalApiCredentialStore::root() const noexcept {
  return impl_->root;
}

core::Result<std::unique_ptr<InMemoryApiKeyAuthenticator>>
load_api_key_authenticator(const ApiCredentialStore &store) {
  auto pepper = store.pepper();
  if (!pepper)
    return std::unexpected(pepper.error());
  auto snapshot = store.snapshot();
  if (!snapshot) {
    core::secure_clear(pepper_view(*pepper));
    return std::unexpected(snapshot.error());
  }
  auto authenticator = InMemoryApiKeyAuthenticator::create(*pepper);
  core::secure_clear(pepper_view(*pepper));
  if (!authenticator)
    return std::unexpected(authenticator.error());
  for (const auto &entry : snapshot->entries) {
    if (auto inserted = (*authenticator)->put_verifier(entry.credential);
        !inserted)
      return std::unexpected(inserted.error());
  }
  return authenticator;
}

} // namespace sakuin::api

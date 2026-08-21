export module sakuin.api.auth;

import std;

import sakuin.core.bytes;
import sakuin.core.hash;
import sakuin.core.random;
import sakuin.core.result;

export namespace sakuin::api {

enum class Permission : std::uint8_t { Search, Admin };

struct ApiPrincipal {
  std::string key_id;
  std::set<Permission> permissions;

  bool allows(Permission permission) const noexcept {
    return permissions.contains(permission) ||
           permissions.contains(Permission::Admin);
  }
};

struct ApiKeySecret {
  std::array<std::uint8_t, 32> bytes{};
};

struct ApiKeyPepper {
  std::array<std::uint8_t, 32> bytes{};
};

struct ApiCredential {
  std::string key_id;
  core::Hash256 verifier;
  std::set<Permission> permissions;
  bool enabled{true};
};

bool valid_api_key_id(std::string_view value) noexcept;
core::Result<core::Hash256> derive_api_key_verifier(const ApiKeyPepper &pepper,
                                                    std::string_view key_id,
                                                    core::ByteView secret);

class ApiKeyAuthenticator {
public:
  virtual ~ApiKeyAuthenticator() = default;
  virtual core::Result<std::optional<ApiPrincipal>>
  authenticate(std::string_view key_id, core::ByteView secret) const = 0;
};

class InMemoryApiKeyAuthenticator final : public ApiKeyAuthenticator {
public:
  static core::Result<std::unique_ptr<InMemoryApiKeyAuthenticator>>
  create(ApiKeyPepper pepper);

  ~InMemoryApiKeyAuthenticator() override;

  core::Result<void> put(std::string key_id, const ApiKeySecret &secret,
                         std::set<Permission> permissions);
  core::Result<void> put_verifier(ApiCredential credential);
  bool disable(std::string_view key_id);

  core::Result<std::optional<ApiPrincipal>>
  authenticate(std::string_view key_id, core::ByteView secret) const override;

private:
  explicit InMemoryApiKeyAuthenticator(ApiKeyPepper pepper) : pepper_(pepper) {}

  ApiKeyPepper pepper_;
  mutable std::shared_mutex mutex_;
  std::unordered_map<std::string, ApiCredential> entries_;
};

} // namespace sakuin::api

namespace sakuin::api {
namespace {

core::ByteBuffer bytes(std::span<const std::uint8_t> input) {
  core::ByteBuffer result;
  result.reserve(input.size());
  for (const auto byte : input)
    result.push_back(static_cast<std::byte>(byte));
  return result;
}

bool all_zero(std::span<const std::uint8_t> input) noexcept {
  std::uint8_t combined{};
  for (const auto byte : input)
    combined |= byte;
  return combined == 0;
}

class BufferCleaner {
public:
  explicit BufferCleaner(core::ByteBuffer &buffer) : buffer_(&buffer) {}
  ~BufferCleaner() { core::secure_clear(*buffer_); }
  BufferCleaner(const BufferCleaner &) = delete;
  BufferCleaner &operator=(const BufferCleaner &) = delete;

private:
  core::ByteBuffer *buffer_;
};

bool valid_key_id(std::string_view value) noexcept {
  return !value.empty() && value.size() <= 64 &&
         std::ranges::all_of(value, [](const unsigned char character) {
           return (character >= 'a' && character <= 'z') ||
                  (character >= 'A' && character <= 'Z') ||
                  (character >= '0' && character <= '9') || character == '-';
         });
}

core::ByteView view(const core::Hash256 &hash) {
  return {reinterpret_cast<const std::byte *>(hash.bytes.data()),
          hash.bytes.size()};
}

} // namespace

bool valid_api_key_id(std::string_view value) noexcept {
  return valid_key_id(value);
}

core::Result<core::Hash256> derive_api_key_verifier(const ApiKeyPepper &pepper,
                                                    std::string_view key_id,
                                                    core::ByteView secret) {
  if (all_zero(pepper.bytes) || !valid_key_id(key_id) ||
      secret.size() != ApiKeySecret{}.bytes.size())
    return std::unexpected(core::Error{core::ErrorCode::InvalidArgument,
                                       "API key verifier requires a pepper, "
                                       "valid key id, and 32-byte secret"});
  core::ByteBuffer message;
  BufferCleaner message_cleaner{message};
  constexpr std::string_view domain{"sakuin-api-key-v1"};
  const auto domain_bytes = std::as_bytes(std::span{domain});
  const auto id_bytes = std::as_bytes(std::span{key_id});
  message.reserve(domain_bytes.size() + 1 + 8 + id_bytes.size() +
                  secret.size());
  message.insert(message.end(), domain_bytes.begin(), domain_bytes.end());
  message.push_back(std::byte{0});
  const auto size = static_cast<std::uint64_t>(id_bytes.size());
  for (int shift = 56; shift >= 0; shift -= 8)
    message.push_back(static_cast<std::byte>((size >> shift) & 0xff));
  message.insert(message.end(), id_bytes.begin(), id_bytes.end());
  message.insert(message.end(), secret.begin(), secret.end());
  try {
    auto key = bytes(pepper.bytes);
    BufferCleaner key_cleaner{key};
    return core::hmac_sha256(key, message);
  } catch (const std::exception &exception) {
    return std::unexpected(
        core::Error{core::ErrorCode::Internal,
                    std::string{"Could not compute API key verifier: "} +
                        exception.what()});
  }
}

core::Result<std::unique_ptr<InMemoryApiKeyAuthenticator>>
InMemoryApiKeyAuthenticator::create(ApiKeyPepper pepper) {
  if (all_zero(pepper.bytes))
    return std::unexpected(core::Error{
        core::ErrorCode::InvalidArgument,
        "API key pepper must contain cryptographically random bytes"});
  return std::unique_ptr<InMemoryApiKeyAuthenticator>{
      new InMemoryApiKeyAuthenticator{pepper}};
}

InMemoryApiKeyAuthenticator::~InMemoryApiKeyAuthenticator() {
  core::secure_clear({reinterpret_cast<std::byte *>(pepper_.bytes.data()),
                      pepper_.bytes.size()});
}

core::Result<void>
InMemoryApiKeyAuthenticator::put(std::string key_id, const ApiKeySecret &secret,
                                 std::set<Permission> permissions) {
  if (!valid_key_id(key_id) || all_zero(secret.bytes) || permissions.empty())
    return std::unexpected(
        core::Error{core::ErrorCode::InvalidArgument,
                    "API key id, random secret, and permissions are required"});
  auto digest = derive_api_key_verifier(
      pepper_, key_id,
      {reinterpret_cast<const std::byte *>(secret.bytes.data()),
       secret.bytes.size()});
  if (!digest)
    return std::unexpected(digest.error());
  return put_verifier(ApiCredential{.key_id = std::move(key_id),
                                    .verifier = *digest,
                                    .permissions = std::move(permissions),
                                    .enabled = true});
}

core::Result<void>
InMemoryApiKeyAuthenticator::put_verifier(ApiCredential credential) {
  if (!valid_key_id(credential.key_id) || all_zero(credential.verifier.bytes) ||
      credential.permissions.empty())
    return std::unexpected(core::Error{
        core::ErrorCode::InvalidArgument,
        "Stored API credential requires an id, verifier, and permissions"});
  std::unique_lock lock{mutex_};
  const auto key_id = credential.key_id;
  entries_.insert_or_assign(key_id, std::move(credential));
  return {};
}

bool InMemoryApiKeyAuthenticator::disable(std::string_view key_id) {
  std::unique_lock lock{mutex_};
  const auto found = entries_.find(std::string{key_id});
  if (found == entries_.end())
    return false;
  found->second.enabled = false;
  return true;
}

core::Result<std::optional<ApiPrincipal>>
InMemoryApiKeyAuthenticator::authenticate(std::string_view key_id,
                                          core::ByteView secret) const {
  if (secret.size() != ApiKeySecret{}.bytes.size() || !valid_key_id(key_id))
    return std::optional<ApiPrincipal>{};
  auto presented = derive_api_key_verifier(pepper_, key_id, secret);
  if (!presented)
    return std::unexpected(presented.error());

  std::shared_lock lock{mutex_};
  const auto found = entries_.find(std::string{key_id});
  if (found == entries_.end() || !found->second.enabled ||
      !core::constant_time_equal(view(*presented),
                                 view(found->second.verifier)))
    return std::optional<ApiPrincipal>{};
  return std::optional<ApiPrincipal>{ApiPrincipal{
      .key_id = found->first, .permissions = found->second.permissions}};
}

} // namespace sakuin::api

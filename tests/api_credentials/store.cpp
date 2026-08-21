import std;

import sakuin.api;
import sakuin.api.credentials;
import sakuin.core;

namespace {

struct TemporaryDirectory {
  std::filesystem::path path;
  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::permissions(path, std::filesystem::perms::owner_all,
                                 std::filesystem::perm_options::add, ignored);
    std::filesystem::remove_all(path, ignored);
  }
};

sakuin::core::ByteView view(const sakuin::api::ApiKeySecret &secret) {
  return {reinterpret_cast<const std::byte *>(secret.bytes.data()),
          secret.bytes.size()};
}

bool owner_only(const std::filesystem::path &path) {
  std::error_code error;
  const auto permissions = std::filesystem::status(path, error).permissions();
  const auto exposed =
      std::filesystem::perms::group_all | std::filesystem::perms::others_all;
  return !error && (permissions & exposed) == std::filesystem::perms::none;
}

} // namespace

int main() {
  using namespace sakuin;
  const auto nonce = std::to_string(
      std::chrono::steady_clock::now().time_since_epoch().count());
  TemporaryDirectory temporary{std::filesystem::temp_directory_path() /
                               ("sakuin-credentials-" + nonce)};
  const auto root = temporary.path / "state";
  auto store = api::LocalApiCredentialStore::initialize(root);
  if (!store || api::LocalApiCredentialStore::initialize(root))
    return 1;
  if (!owner_only(root) || !owner_only(root / "pepper") ||
      !owner_only(root / "credentials.toml") ||
      !owner_only(root / "credentials.lock"))
    return 2;

  auto pepper = (*store)->pepper();
  auto initial = (*store)->snapshot();
  if (!pepper || !initial || initial->generation != 0 ||
      !initial->entries.empty())
    return 3;
  api::ApiCredential zero{.key_id = "zero",
                          .permissions = {api::Permission::Search}};
  if ((*store)->insert(std::move(zero)))
    return 20;
  api::ApiKeySecret secret;
  std::ranges::iota(secret.bytes, std::uint8_t{17});
  auto verifier = api::derive_api_key_verifier(*pepper, "reader", view(secret));
  if (!verifier)
    return 4;
  api::ApiCredential credential{.key_id = "reader",
                                .verifier = *verifier,
                                .permissions = {api::Permission::Search}};
  if (!(*store)->insert(credential) || (*store)->insert(credential))
    return 5;
  auto snapshot = (*store)->snapshot();
  if (!snapshot || snapshot->generation != 1 || snapshot->entries.size() != 1 ||
      snapshot->entries.front().credential.key_id != "reader" ||
      !snapshot->entries.front().credential.enabled)
    return 6;

  auto authenticator = api::load_api_key_authenticator(**store);
  if (!authenticator)
    return 7;
  auto principal = (*authenticator)->authenticate("reader", view(secret));
  if (!principal || !*principal ||
      !(*principal)->allows(api::Permission::Search))
    return 8;

  auto disabled = (*store)->disable("reader");
  auto missing = (*store)->disable("missing");
  if (!disabled || !*disabled || !missing || *missing)
    return 9;
  authenticator = api::load_api_key_authenticator(**store);
  if (!authenticator)
    return 10;
  principal = (*authenticator)->authenticate("reader", view(secret));
  if (!principal || *principal)
    return 11;

  auto second_store = api::LocalApiCredentialStore::open(root);
  auto first_verifier =
      api::derive_api_key_verifier(*pepper, "concurrent-a", view(secret));
  auto second_verifier =
      api::derive_api_key_verifier(*pepper, "concurrent-b", view(secret));
  if (!second_store || !first_verifier || !second_verifier)
    return 21;
  api::ApiCredential first{.key_id = "concurrent-a",
                           .verifier = *first_verifier,
                           .permissions = {api::Permission::Search}};
  api::ApiCredential second{.key_id = "concurrent-b",
                            .verifier = *second_verifier,
                            .permissions = {api::Permission::Admin}};
  std::atomic<int> inserted{};
  std::jthread first_thread{[&] {
    if ((*store)->insert(std::move(first)))
      ++inserted;
  }};
  std::jthread second_thread{[&] {
    if ((*second_store)->insert(std::move(second)))
      ++inserted;
  }};
  first_thread.join();
  second_thread.join();
  snapshot = (*store)->snapshot();
  if (inserted != 2 || !snapshot || snapshot->generation != 4 ||
      snapshot->entries.size() != 3)
    return 22;

  for (const auto &entry : std::filesystem::directory_iterator(root))
    if (entry.path().filename().string().contains(".tmp."))
      return 12;

  std::filesystem::permissions(root / "credentials.toml",
                               std::filesystem::perms::group_read,
                               std::filesystem::perm_options::add);
  if (api::LocalApiCredentialStore::open(root))
    return 13;
  std::filesystem::permissions(root / "credentials.toml",
                               std::filesystem::perms::group_all,
                               std::filesystem::perm_options::remove);
  if (!api::LocalApiCredentialStore::open(root))
    return 14;

  {
    std::ofstream output{root / "credentials.toml", std::ios::app};
    output << "unknown = true\n";
  }
  if (api::LocalApiCredentialStore::open(root))
    return 15;
  return 0;
}

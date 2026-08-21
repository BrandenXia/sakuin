import std;

import sakuin.api;
import sakuin.core;

int main() {
  using namespace sakuin;
  api::ApiKeyPepper pepper;
  std::ranges::iota(pepper.bytes, std::uint8_t{1});
  auto authenticator = api::InMemoryApiKeyAuthenticator::create(pepper);
  if (!authenticator ||
      api::InMemoryApiKeyAuthenticator::create(api::ApiKeyPepper{}))
    return 1;

  api::ApiKeySecret secret;
  std::ranges::iota(secret.bytes, std::uint8_t{32});
  if (!(*authenticator)->put("reader", secret, {api::Permission::Search}))
    return 2;
  auto invalid_character =
      (*authenticator)->put("bad_id", secret, {api::Permission::Search});
  auto too_long =
      (*authenticator)
          ->put(std::string(65, 'a'), secret, {api::Permission::Search});
  if (invalid_character)
    return 20;
  if (too_long)
    return 21;
  const core::ByteView secret_view{
      reinterpret_cast<const std::byte *>(secret.bytes.data()),
      secret.bytes.size()};
  auto principal = (*authenticator)->authenticate("reader", secret_view);
  if (!principal || !*principal ||
      !(*principal)->allows(api::Permission::Search) ||
      (*principal)->allows(api::Permission::Admin))
    return 3;

  auto wrong = secret;
  wrong.bytes.front() ^= 1;
  const core::ByteView wrong_view{
      reinterpret_cast<const std::byte *>(wrong.bytes.data()),
      wrong.bytes.size()};
  auto rejected = (*authenticator)->authenticate("reader", wrong_view);
  auto unknown = (*authenticator)->authenticate("unknown", secret_view);
  if (!rejected || *rejected || !unknown || *unknown)
    return 4;
  if (!(*authenticator)->disable("reader") ||
      (*authenticator)->disable("missing"))
    return 5;
  auto disabled = (*authenticator)->authenticate("reader", secret_view);
  if (!disabled || *disabled)
    return 6;
  return 0;
}

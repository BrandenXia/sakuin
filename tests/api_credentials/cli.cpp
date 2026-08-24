import std;

import sakuin.api;
import sakuin.api.credentials;
import sakuin.core;

namespace {

struct TemporaryDirectory {
  std::filesystem::path path;
  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path, ignored);
  }
};

int run(std::initializer_list<std::string_view> arguments,
        std::ostringstream &output, std::ostringstream &error) {
  return sakuin::api::run_api_key_cli(arguments, output, error);
}

std::optional<sakuin::api::ApiKeySecret> parse_secret(std::string_view token,
                                                      std::string_view key_id) {
  const auto prefix = "sakuin_" + std::string{key_id} + '_';
  if (!token.starts_with(prefix) || token.size() != prefix.size() + 65 ||
      token.back() != '\n')
    return std::nullopt;
  token.remove_prefix(prefix.size());
  token.remove_suffix(1);
  auto digit = [](char value) -> std::optional<std::uint8_t> {
    if (value >= '0' && value <= '9')
      return static_cast<std::uint8_t>(value - '0');
    if (value >= 'a' && value <= 'f')
      return static_cast<std::uint8_t>(value - 'a' + 10);
    return std::nullopt;
  };
  sakuin::api::ApiKeySecret result;
  for (std::size_t index = 0; index < result.bytes.size(); ++index) {
    const auto high = digit(token[index * 2]);
    const auto low = digit(token[index * 2 + 1]);
    if (!high || !low)
      return std::nullopt;
    result.bytes[index] = static_cast<std::uint8_t>((*high << 4) | *low);
  }
  return result;
}

} // namespace

int main() {
  using namespace sakuin;
  TemporaryDirectory temporary{
      std::filesystem::temp_directory_path() /
      ("sakuin-api-key-cli-" +
       std::to_string(
           std::chrono::steady_clock::now().time_since_epoch().count()))};
  const auto root = temporary.path / "state";
  const auto root_string = root.string();

  std::ostringstream output;
  std::ostringstream error;
  if (run({"--state-dir", root_string, "init"}, output, error) != 0 ||
      !output.str().empty() || error.str().empty())
    return 1;

  output.str({});
  error.str({});
  if (run({"create", "--state-dir", root_string, "--id", "reader-1",
           "--permissions", "search"},
          output, error) != 0 ||
      !error.str().empty())
    return 2;
  const auto token = output.str();
  const auto secret = parse_secret(token, "reader-1");
  if (!secret)
    return 3;

  auto store = api::LocalApiCredentialStore::open(root);
  auto authenticator =
      store ? api::load_api_key_authenticator(**store)
            : core::Result<std::unique_ptr<api::InMemoryApiKeyAuthenticator>>{
                  std::unexpected(store.error())};
  const core::ByteView secret_view{
      reinterpret_cast<const std::byte *>(secret->bytes.data()),
      secret->bytes.size()};
  auto principal = authenticator
                       ? (*authenticator)->authenticate("reader-1", secret_view)
                       : core::Result<std::optional<api::ApiPrincipal>>{
                             std::unexpected(authenticator.error())};
  if (!principal || !*principal ||
      !(*principal)->allows(api::Permission::Search))
    return 4;

  std::ifstream stored{root / "credentials.toml"};
  const std::string contents{std::istreambuf_iterator<char>{stored}, {}};
  if (contents.contains(token.substr(0, token.size() - 1)) ||
      contents.contains(token.substr(token.size() - 65, 64)))
    return 5;

  output.str({});
  error.str({});
  if (run({"list", "--state-dir", root_string}, output, error) != 0 ||
      !output.str().contains("reader-1\tenabled\tsearch\t") ||
      output.str().contains("verifier") || output.str().contains("sakuin_"))
    return 6;

  output.str({});
  error.str({});
  if (run({"disable", "--id", "reader-1", "--state-dir", root_string}, output,
          error) != 0 ||
      !output.str().empty())
    return 7;
  output.str({});
  error.str({});
  if (run({"list", "--state-dir", root_string}, output, error) != 0 ||
      !output.str().contains("reader-1\tdisabled\tsearch\t"))
    return 8;

  output.str({});
  error.str({});
  if (run({"create", "--state-dir", root_string, "--id", "reader-1",
           "--permissions", "search"},
          output, error) == 0 ||
      !output.str().empty())
    return 9;
  output.str({});
  error.str({});
  if (run({"create", "--state-dir", root_string, "--id", "bad_id",
           "--permissions", "search"},
          output, error) != 2 ||
      !output.str().empty())
    return 10;
  output.str({});
  error.str({});
  if (run({"--version"}, output, error) != 0 ||
      output.str() != "sakuin-api-key " + std::string{core::version} + '\n' ||
      !error.str().empty())
    return 11;
  return 0;
}

export module sakuin.api.credentials.cli;

import std;

import sakuin.api.auth;
import sakuin.api.credentials.store;
import sakuin.core.bytes;
import sakuin.core.random;
import sakuin.core.result;

export namespace sakuin::api {

int run_api_key_cli(std::span<const std::string_view> arguments,
                    std::ostream &output, std::ostream &error);

} // namespace sakuin::api

namespace sakuin::api {
namespace {

constexpr std::string_view usage = R"usage(Usage:
  sakuin-api-key [--state-dir PATH] init
  sakuin-api-key [--state-dir PATH] create --id ID --permissions LIST
  sakuin-api-key [--state-dir PATH] list
  sakuin-api-key [--state-dir PATH] disable --id ID

LIST is a comma-separated subset of: search,admin
)usage";

struct Options {
  std::filesystem::path state_directory{"./data/operational/api"};
  std::string command;
  std::optional<std::string> key_id;
  std::optional<std::string> permissions;
  bool help{};
};

class SecretCleaner {
public:
  explicit SecretCleaner(std::span<std::uint8_t> bytes) : bytes_(bytes) {}
  ~SecretCleaner() {
    core::secure_clear(
        {reinterpret_cast<std::byte *>(bytes_.data()), bytes_.size()});
  }
  SecretCleaner(const SecretCleaner &) = delete;
  SecretCleaner &operator=(const SecretCleaner &) = delete;

private:
  std::span<std::uint8_t> bytes_;
};

core::ByteView view(std::span<const std::uint8_t> bytes) {
  return {reinterpret_cast<const std::byte *>(bytes.data()), bytes.size()};
}

core::MutableByteView mutable_view(std::span<std::uint8_t> bytes) {
  return {reinterpret_cast<std::byte *>(bytes.data()), bytes.size()};
}

int fail(std::ostream &error, std::string_view message, int status = 1) {
  error << "error: " << message << '\n';
  return status;
}

std::optional<Options> parse(std::span<const std::string_view> arguments,
                             std::ostream &error) {
  Options result;
  for (std::size_t index = 0; index < arguments.size(); ++index) {
    const auto argument = arguments[index];
    if (argument == "--help" || argument == "-h") {
      result.help = true;
      continue;
    }
    if (argument == "--state-dir" || argument == "--id" ||
        argument == "--permissions") {
      if (++index == arguments.size()) {
        fail(error, std::string{argument} + " requires a value", 2);
        return std::nullopt;
      }
      const auto value = arguments[index];
      if (value.empty()) {
        fail(error, std::string{argument} + " must not be empty", 2);
        return std::nullopt;
      }
      if (argument == "--state-dir")
        result.state_directory = value;
      else if (argument == "--id")
        result.key_id = value;
      else
        result.permissions = value;
      continue;
    }
    if (argument.starts_with('-')) {
      fail(error, "unknown option: " + std::string{argument}, 2);
      return std::nullopt;
    }
    if (!result.command.empty()) {
      fail(error, "only one command may be specified", 2);
      return std::nullopt;
    }
    result.command = argument;
  }
  return result;
}

core::Result<std::set<Permission>> parse_permissions(std::string_view source) {
  std::set<Permission> result;
  while (!source.empty()) {
    const auto separator = source.find(',');
    const auto name = source.substr(0, separator);
    std::optional<Permission> permission;
    if (name == "search")
      permission = Permission::Search;
    else if (name == "admin")
      permission = Permission::Admin;
    if (!permission)
      return std::unexpected(core::Error{
          core::ErrorCode::InvalidArgument,
          "permissions must be a comma-separated subset of search,admin"});
    if (!result.insert(*permission).second)
      return std::unexpected(core::Error{core::ErrorCode::InvalidArgument,
                                         "permissions must not be repeated"});
    if (separator == std::string_view::npos)
      break;
    source.remove_prefix(separator + 1);
  }
  if (result.empty())
    return std::unexpected(core::Error{core::ErrorCode::InvalidArgument,
                                       "at least one permission is required"});
  return result;
}

std::string permission_list(const std::set<Permission> &permissions) {
  std::string result;
  for (const auto permission : permissions) {
    if (!result.empty())
      result += ',';
    result += permission == Permission::Search ? "search" : "admin";
  }
  return result;
}

std::string secret_hex(const ApiKeySecret &secret) {
  constexpr std::string_view digits{"0123456789abcdef"};
  std::string result;
  result.reserve(secret.bytes.size() * 2);
  for (const auto byte : secret.bytes) {
    result.push_back(digits[byte >> 4]);
    result.push_back(digits[byte & 0x0f]);
  }
  return result;
}

} // namespace

int run_api_key_cli(std::span<const std::string_view> arguments,
                    std::ostream &output, std::ostream &error) {
  auto options = parse(arguments, error);
  if (!options)
    return 2;
  if (options->help) {
    output << usage;
    return 0;
  }
  if (options->command.empty()) {
    error << usage;
    return 2;
  }

  if (options->command == "init") {
    if (options->key_id || options->permissions)
      return fail(error, "init does not accept --id or --permissions", 2);
    auto store = LocalApiCredentialStore::initialize(options->state_directory);
    if (!store)
      return fail(error, store.error().message);
    error << "Initialized API credential store at " << (*store)->root().string()
          << '\n';
    return 0;
  }

  auto store = LocalApiCredentialStore::open(options->state_directory);
  if (!store)
    return fail(error, store.error().message);

  if (options->command == "create") {
    if (!options->key_id || !options->permissions)
      return fail(error, "create requires --id and --permissions", 2);
    if (!valid_api_key_id(*options->key_id))
      return fail(error,
                  "key id must be 1-64 ASCII letters, digits, or hyphens", 2);
    auto permissions = parse_permissions(*options->permissions);
    if (!permissions)
      return fail(error, permissions.error().message, 2);
    auto pepper = (*store)->pepper();
    if (!pepper)
      return fail(error, pepper.error().message);
    SecretCleaner pepper_cleaner{pepper->bytes};
    ApiKeySecret secret;
    SecretCleaner secret_cleaner{secret.bytes};
    if (auto generated = core::secure_random(mutable_view(secret.bytes));
        !generated)
      return fail(error, generated.error().message);
    auto verifier =
        derive_api_key_verifier(*pepper, *options->key_id, view(secret.bytes));
    if (!verifier)
      return fail(error, verifier.error().message);
    if (auto inserted = (*store)->insert(
            ApiCredential{.key_id = *options->key_id,
                          .verifier = *verifier,
                          .permissions = std::move(*permissions)});
        !inserted)
      return fail(error, inserted.error().message);

    // stdout intentionally contains only the one-time bearer token so callers
    // can capture it without mixing it with status output.
    output << "sakuin_" << *options->key_id << '_' << secret_hex(secret)
           << '\n';
    return 0;
  }

  if (options->command == "list") {
    if (options->key_id || options->permissions)
      return fail(error, "list does not accept --id or --permissions", 2);
    auto snapshot = (*store)->snapshot();
    if (!snapshot)
      return fail(error, snapshot.error().message);
    output << "ID\tSTATUS\tPERMISSIONS\tCREATED_AT_MS\n";
    for (const auto &entry : snapshot->entries) {
      const auto created =
          std::chrono::duration_cast<std::chrono::milliseconds>(
              entry.created_at.time_since_epoch())
              .count();
      output << entry.credential.key_id << '\t'
             << (entry.credential.enabled ? "enabled" : "disabled") << '\t'
             << permission_list(entry.credential.permissions) << '\t' << created
             << '\n';
    }
    return 0;
  }

  if (options->command == "disable") {
    if (!options->key_id || options->permissions)
      return fail(error, "disable requires --id and no --permissions", 2);
    auto disabled = (*store)->disable(*options->key_id);
    if (!disabled)
      return fail(error, disabled.error().message);
    if (!*disabled)
      return fail(error, "API key id was not found");
    error << "Disabled API key " << *options->key_id << '\n';
    return 0;
  }

  return fail(error, "unknown command: " + options->command, 2);
}

} // namespace sakuin::api

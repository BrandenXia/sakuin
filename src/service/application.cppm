export module sakuin.service.application;

import std;

import sakuin.core.result;

export namespace sakuin::service {

enum class AdminCommand { Compact, Verify, GarbageCollect };

struct DaemonArguments {
  std::optional<std::filesystem::path> configuration_file;
  std::vector<std::string> configuration_overrides;
  std::optional<AdminCommand> admin_command;
  bool help{};
};

// Separates daemon-owned options from the --key=value configuration overlay
// understood by sakuin.config. Both --config=PATH and --config PATH are
// accepted; every remaining argument is preserved for the config loader.
core::Result<DaemonArguments>
parse_daemon_arguments(std::span<const std::string_view> arguments);

} // namespace sakuin::service

namespace sakuin::service {

core::Result<DaemonArguments>
parse_daemon_arguments(std::span<const std::string_view> arguments) {
  DaemonArguments result;
  for (std::size_t index = 0; index < arguments.size(); ++index) {
    const auto argument = arguments[index];
    if (argument == "admin") {
      if (result.admin_command || ++index == arguments.size())
        return std::unexpected(core::Error{
            core::ErrorCode::InvalidArgument,
            "admin requires exactly one of: compact, verify, gc"});
      const auto command = arguments[index];
      if (command == "compact")
        result.admin_command = AdminCommand::Compact;
      else if (command == "verify")
        result.admin_command = AdminCommand::Verify;
      else if (command == "gc")
        result.admin_command = AdminCommand::GarbageCollect;
      else
        return std::unexpected(core::Error{
            core::ErrorCode::InvalidArgument,
            "Unknown admin command; expected compact, verify, or gc"});
      continue;
    }
    if (argument == "--help" || argument == "-h") {
      result.help = true;
      continue;
    }
    std::optional<std::string_view> configuration_file;
    if (argument == "--config") {
      if (++index == arguments.size())
        return std::unexpected(core::Error{core::ErrorCode::InvalidArgument,
                                           "--config requires a path"});
      configuration_file = arguments[index];
    } else if (argument.starts_with("--config=")) {
      configuration_file =
          argument.substr(std::string_view{"--config="}.size());
    }
    if (configuration_file) {
      if (configuration_file->empty())
        return std::unexpected(core::Error{core::ErrorCode::InvalidArgument,
                                           "--config path must not be empty"});
      if (result.configuration_file)
        return std::unexpected(
            core::Error{core::ErrorCode::InvalidArgument,
                        "--config may only be specified once"});
      result.configuration_file = *configuration_file;
      continue;
    }
    if (!argument.starts_with("--") || !argument.contains('='))
      return std::unexpected(core::Error{
          core::ErrorCode::InvalidArgument,
          "Daemon arguments must be --config PATH, --help, or --key=value"});
    result.configuration_overrides.emplace_back(argument);
  }
  return result;
}

} // namespace sakuin::service

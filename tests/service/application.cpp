import std;

import sakuin.service.application;

int main() {
  using namespace sakuin;
  const std::array arguments{std::string_view{"--config"},
                             std::string_view{"node.toml"},
                             std::string_view{"--network.listen_port=7000"},
                             std::string_view{"--help"}};
  auto parsed = service::parse_daemon_arguments(arguments);
  if (!parsed || !parsed->help ||
      parsed->configuration_file != std::filesystem::path{"node.toml"} ||
      parsed->configuration_overrides !=
          std::vector<std::string>{"--network.listen_port=7000"})
    return 1;

  const std::array equals{std::string_view{"--config=other.toml"}};
  parsed = service::parse_daemon_arguments(equals);
  if (!parsed ||
      parsed->configuration_file != std::filesystem::path{"other.toml"})
    return 2;

  const std::array duplicate{std::string_view{"--config=a.toml"},
                             std::string_view{"--config"},
                             std::string_view{"b.toml"}};
  const std::array missing{std::string_view{"--config"}};
  const std::array positional{std::string_view{"node.toml"}};
  const std::array malformed{std::string_view{"--network.listen_port"}};
  if (service::parse_daemon_arguments(duplicate) ||
      service::parse_daemon_arguments(missing) ||
      service::parse_daemon_arguments(positional) ||
      service::parse_daemon_arguments(malformed))
    return 3;
  return 0;
}

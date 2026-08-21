import std;

import sakuin.api.credentials.cli;

int main(int argc, char **argv) {
  std::vector<std::string_view> arguments;
  arguments.reserve(static_cast<std::size_t>(argc > 0 ? argc - 1 : 0));
  for (int index = 1; index < argc; ++index)
    arguments.emplace_back(argv[index]);
  return sakuin::api::run_api_key_cli(arguments, std::cout, std::cerr);
}

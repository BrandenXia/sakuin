#include <csignal>

import std;

import sakuin.config;
import sakuin.core;
import sakuin.dht.krpc;
import sakuin.integration.dht_worker;
import sakuin.runtime.datagram;
import sakuin.search.rebuild;
import sakuin.service.api;
import sakuin.service.application;
import sakuin.service.local;
import sakuin.service.runtime;

extern char **environ;

namespace {

volatile std::sig_atomic_t pending_signal{};

extern "C" void handle_signal(int signal) { pending_signal = signal; }

constexpr std::string_view usage = R"usage(Usage:
  sakuin [--config PATH] [--key=value ...]

Configuration precedence: defaults, TOML, SAKUIN_* environment, command line.
Send SIGHUP to reload API credentials and rebuild the derived search index.

Before enabling the API for the first time:
  sakuin-api-key --state-dir ./data/operational/api init
  sakuin-api-key --state-dir ./data/operational/api create --id reader --permissions search
)usage";

std::string_view family_name(sakuin::runtime::AddressFamily family) {
  return family == sakuin::runtime::AddressFamily::IPv4 ? "IPv4" : "IPv6";
}

class Observer final : public sakuin::service::DhtRuntimeObserver,
                       public sakuin::service::ApiServiceObserver {
public:
  void on_family_cycle(sakuin::runtime::AddressFamily,
                       sakuin::integration::DhtRuntimeCycle) override {}

  void on_family_error(sakuin::runtime::AddressFamily family,
                       sakuin::core::Error error) override {
    log("DHT " + std::string{family_name(family)} + ": " + error.message);
  }

  void on_identity_reconfigured(sakuin::runtime::AddressFamily family,
                                const sakuin::dht::krpc::NodeId &,
                                const sakuin::dht::krpc::NodeId &,
                                sakuin::runtime::IpAddress) override {
    log("DHT " + std::string{family_name(family)} +
        " identity reconfigured after external-address consensus");
  }

  void on_api_error(sakuin::core::Error error) override {
    log("API: " + error.message);
  }

private:
  void log(std::string message) noexcept {
    try {
      std::lock_guard lock{mutex_};
      std::cerr << "error: " << message << '\n';
    } catch (...) {
    }
  }

  std::mutex mutex_;
};

std::vector<std::pair<std::string, std::string>> environment_values() {
  std::vector<std::pair<std::string, std::string>> result;
  if (!environ)
    return result;
  for (auto entry = environ; *entry; ++entry) {
    const std::string_view value{*entry};
    const auto separator = value.find('=');
    if (separator == std::string_view::npos)
      continue;
    result.emplace_back(value.substr(0, separator),
                        value.substr(separator + 1));
  }
  return result;
}

int fail(std::string_view message, int status = 1) {
  std::cerr << "error: " << message << '\n';
  return status;
}

} // namespace

int main(int argc, char **argv) {
  using namespace sakuin;
  std::vector<std::string_view> raw_arguments;
  raw_arguments.reserve(static_cast<std::size_t>(std::max(0, argc - 1)));
  for (int index = 1; index < argc; ++index)
    raw_arguments.emplace_back(argv[index]);
  auto arguments = service::parse_daemon_arguments(raw_arguments);
  if (!arguments)
    return fail(arguments.error().message, 2);
  if (arguments->help) {
    std::cout << usage;
    return 0;
  }

  const auto environment = environment_values();
  std::vector<std::string_view> overrides;
  overrides.reserve(arguments->configuration_overrides.size());
  for (const auto &argument : arguments->configuration_overrides)
    overrides.push_back(argument);
  auto configuration = config::load({.toml_file = arguments->configuration_file,
                                     .environment = environment,
                                     .command_line = overrides});
  if (!configuration)
    return fail(configuration.error().message, 2);

  Observer observer;
  if (std::signal(SIGINT, handle_signal) == SIG_ERR ||
      std::signal(SIGTERM, handle_signal) == SIG_ERR ||
      std::signal(SIGHUP, handle_signal) == SIG_ERR)
    return fail("Unable to install process signal handlers");
  auto service =
      service::LocalSakuinService::create(*configuration, observer, observer);
  if (!service)
    return fail(service.error().message);
  if (auto started = (*service)->start(); !started)
    return fail(started.error().message);

  std::cout << "Sakuin started";
  if (const auto endpoint = (*service)->api_endpoint())
    std::cout << "; API port " << endpoint->port;
  std::cout << '\n';

  while (true) {
    const auto signal = pending_signal;
    if (signal == SIGINT || signal == SIGTERM)
      break;
    if (signal == SIGHUP) {
      pending_signal = 0;
      if (configuration->api.enabled) {
        if (auto reloaded = (*service)->reload_api_credentials(); !reloaded)
          std::cerr << "error: API credential reload: "
                    << reloaded.error().message << '\n';
        if (auto refreshed = (*service)->refresh_search(); !refreshed)
          std::cerr << "error: search index rebuild: "
                    << refreshed.error().message << '\n';
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds{100});
  }

  if (auto stopped = (*service)->stop(); !stopped)
    return fail(stopped.error().message);
  std::cout << "Sakuin stopped\n";
  return 0;
}

import std;

import sakuin.config;
import sakuin.core;
import sakuin.dht;
import sakuin.integration.dht_worker;
import sakuin.model.observation;
import sakuin.runtime;
import sakuin.service.dht;
import sakuin.service.local;
import sakuin.service.runtime;
import sakuin.service.traffic;

namespace {

class ObservationSink final : public sakuin::dht::ObservationSink {
public:
  sakuin::core::Result<void>
  observe(const sakuin::model::ObservationRecord &) override {
    return {};
  }
};

class Observer final : public sakuin::integration::DhtRuntimeWorkerObserver {
public:
  void on_cycle(sakuin::integration::DhtRuntimeCycle) override {
    {
      std::lock_guard lock{mutex};
      ++cycles;
    }
    condition.notify_all();
  }

  void on_worker_error(sakuin::core::Error) override {
    {
      std::lock_guard lock{mutex};
      ++errors;
    }
    condition.notify_all();
  }

  bool wait_for_cycle() {
    std::unique_lock lock{mutex};
    return condition.wait_for(lock, std::chrono::seconds{2},
                              [&] { return cycles != 0; });
  }

  std::mutex mutex;
  std::condition_variable condition;
  std::size_t cycles{};
  std::size_t errors{};
};

class RuntimeObserver final : public sakuin::service::DhtRuntimeObserver {
public:
  void on_family_cycle(sakuin::runtime::AddressFamily family,
                       sakuin::integration::DhtRuntimeCycle) override {
    {
      std::lock_guard lock{mutex};
      families.insert(family);
    }
    condition.notify_all();
  }

  void on_family_error(sakuin::runtime::AddressFamily,
                       sakuin::core::Error) override {
    std::lock_guard lock{mutex};
    ++errors;
  }

  bool wait_for_count(std::size_t count) {
    std::unique_lock lock{mutex};
    return condition.wait_for(lock, std::chrono::seconds{2},
                              [&] { return families.size() >= count; });
  }

  std::mutex mutex;
  std::condition_variable condition;
  std::set<sakuin::runtime::AddressFamily> families;
  std::size_t errors{};
};

struct TemporaryDirectory {
  TemporaryDirectory() {
    path = std::filesystem::temp_directory_path() /
           ("sakuin-local-dht-service-" +
            std::to_string(
                std::chrono::steady_clock::now().time_since_epoch().count()));
  }
  ~TemporaryDirectory() { std::filesystem::remove_all(path); }
  std::filesystem::path path;
};

} // namespace

int main() {
  using namespace sakuin;

  auto configuration = config::defaults().network.dht;
  configuration.metadata.enabled = false;
  runtime::IpAddress external = runtime::IpAddress::loopback_v4();
  external.bytes = {203, 0, 113, 55};
  auto material = service::secure_dht_family_runtime_material(
      configuration.identity, runtime::AddressFamily::IPv4, external);
  if (!material || !dht::bep42_compliant(material->node_id, external))
    return 1;

  ObservationSink observations;
  Observer observer;
  const runtime::DatagramEndpoint bootstrap{
      .address = runtime::IpAddress::loopback_v4(), .port = 9};
  auto family = service::AsioDhtFamilyRuntime::create(
      configuration, runtime::AddressFamily::IPv4, 0, std::span{&bootstrap, 1},
      std::move(*material),
      {.observations = &observations, .observer = &observer});
  if (!family || (*family)->local_endpoint().port == 0 ||
      (*family)->local_endpoint().address.family !=
          runtime::AddressFamily::IPv4 ||
      !(*family)->identity_policy() ||
      !dht::bep42_compliant((*family)->node_id(), external))
    return 2;
  if (!(*family)->start() || !observer.wait_for_cycle() ||
      !(*family)->running())
    return 3;
  (*family)->stop();
  if ((*family)->running())
    return 4;

  auto missing_observer = service::AsioDhtFamilyRuntime::create(
      configuration, runtime::AddressFamily::IPv4, 0, {},
      service::DhtFamilyRuntimeMaterial{}, {});
  if (missing_observer ||
      missing_observer.error().code != core::ErrorCode::InvalidArgument)
    return 5;
  auto mismatched = service::secure_dht_family_runtime_material(
      configuration.identity, runtime::AddressFamily::IPv4, external);
  if (!mismatched)
    return 6;
  mismatched->node_id.bytes.front() ^= 1;
  auto invalid_identity = service::AsioDhtFamilyRuntime::create(
      configuration, runtime::AddressFamily::IPv4, 0, {},
      std::move(*mismatched),
      {.observations = &observations, .observer = &observer});
  if (invalid_identity ||
      invalid_identity.error().code != core::ErrorCode::InvalidArgument)
    return 7;

  configuration.bootstrap = {"localhost:9", "localhost:9"};
  auto resolved_material = service::secure_dht_family_runtime_material(
      configuration.identity, runtime::AddressFamily::IPv4);
  if (!resolved_material)
    return 8;
  auto resolved_family = service::AsioDhtFamilyRuntime::create(
      configuration, runtime::AddressFamily::IPv4, 0,
      std::move(*resolved_material),
      {.observations = &observations, .observer = &observer});
  if (!resolved_family || (*resolved_family)->local_endpoint().port == 0)
    return 9;

  configuration.bootstrap = {"unbracketed:ipv6:6881"};
  auto invalid_bootstrap_material = service::secure_dht_family_runtime_material(
      configuration.identity, runtime::AddressFamily::IPv4);
  if (!invalid_bootstrap_material)
    return 10;
  auto invalid_bootstrap = service::AsioDhtFamilyRuntime::create(
      configuration, runtime::AddressFamily::IPv4, 0,
      std::move(*invalid_bootstrap_material),
      {.observations = &observations, .observer = &observer});
  if (invalid_bootstrap ||
      invalid_bootstrap.error().code != core::ErrorCode::InvalidArgument)
    return 11;

  auto traffic =
      service::create_traffic_governor({.window = std::chrono::seconds{10},
                                        .inbound_bytes = 100,
                                        .outbound_bytes = 50});
  const auto traffic_now = core::Timestamp{std::chrono::seconds{20}};
  if (!traffic ||
      !(*traffic)
           ->admit(
               {.direction = runtime::TrafficDirection::Inbound, .bytes = 100},
               traffic_now)
           .allowed ||
      (*traffic)
          ->admit({.direction = runtime::TrafficDirection::Inbound, .bytes = 1},
                  traffic_now)
          .allowed ||
      !(*traffic)
           ->admit(
               {.direction = runtime::TrafficDirection::Outbound, .bytes = 50},
               traffic_now)
           .allowed ||
      (*traffic)
          ->admit(
              {.direction = runtime::TrafficDirection::Outbound, .bytes = 1},
              traffic_now)
          .allowed)
    return 12;

  auto dual_configuration = config::defaults().network;
  dual_configuration.listen_port = 0;
  dual_configuration.dht.bootstrap.clear();
  dual_configuration.dht.metadata.enabled = false;
  RuntimeObserver runtime_observer;
  auto dual = service::AsioDhtRuntime::create(
      dual_configuration,
      {.observations = &observations, .observer = &runtime_observer});
  if (!dual || !(*dual)->local_endpoint(runtime::AddressFamily::IPv4) ||
      !(*dual)->local_endpoint(runtime::AddressFamily::IPv6))
    return 13;
  if (!(*dual)->start() || !runtime_observer.wait_for_count(2) ||
      !(*dual)->running())
    return 14;
  const auto local_test_port =
      (*dual)->local_endpoint(runtime::AddressFamily::IPv4)->port;
  (*dual)->stop();
  if ((*dual)->running() || runtime_observer.errors != 0)
    return 15;

  TemporaryDirectory temporary;
  auto app_configuration = config::defaults();
  app_configuration.storage.local_root = temporary.path;
  app_configuration.network.enable_ipv6 = false;
  app_configuration.network.listen_port = local_test_port;
  app_configuration.network.dht.bootstrap.clear();
  RuntimeObserver local_observer;
  auto local =
      service::LocalAsioDhtService::create(app_configuration, local_observer);
  if (!local || !(*local)->start() || !local_observer.wait_for_count(1))
    return 16;
  if (!(*local)->stop() || (*local)->running())
    return 17;
  return 0;
}

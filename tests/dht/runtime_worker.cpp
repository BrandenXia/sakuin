import std;

import sakuin.core;
import sakuin.dht;
import sakuin.integration.dht_runtime;
import sakuin.integration.dht_worker;
import sakuin.model.observation;
import sakuin.runtime;

namespace {

class ObservationSink final : public sakuin::dht::ObservationSink {
public:
  sakuin::core::Result<void>
  observe(const sakuin::model::ObservationRecord &) override {
    return {};
  }
};

class DatagramTransport final : public sakuin::runtime::DatagramTransport {
public:
  sakuin::core::Result<void>
  start(sakuin::runtime::DatagramReceiver &) override {
    running = true;
    return {};
  }
  sakuin::core::Result<void> send(sakuin::runtime::DatagramEndpoint,
                                  sakuin::core::ByteBuffer) override {
    return {};
  }
  sakuin::runtime::DatagramEndpoint local_endpoint() const noexcept override {
    return {};
  }
  void stop() noexcept override { running = false; }

  bool running{};
};

class Observer final : public sakuin::integration::DhtRuntimeWorkerObserver {
public:
  void on_cycle(sakuin::integration::DhtRuntimeCycle cycle) override {
    std::lock_guard lock{mutex};
    ++cycles;
    errors += cycle.poll.errors.size();
    expired += cycle.poll.queries_expired;
    condition.notify_all();
  }

  void on_worker_error(sakuin::core::Error) override {
    std::lock_guard lock{mutex};
    ++worker_errors;
    condition.notify_all();
  }

  bool wait_for(std::size_t wanted_cycles, std::size_t wanted_errors,
                std::size_t wanted_expired = 0) {
    std::unique_lock lock{mutex};
    return condition.wait_for(lock, std::chrono::seconds{2}, [&] {
      return cycles >= wanted_cycles && errors >= wanted_errors &&
             expired >= wanted_expired;
    });
  }

  std::mutex mutex;
  std::condition_variable condition;
  std::size_t cycles{};
  std::size_t errors{};
  std::size_t expired{};
  std::size_t worker_errors{};
};

} // namespace

int main() {
  using namespace sakuin;

  ObservationSink observations;
  integration::DhtRuntimeWakeup wakeup;
  dht::AnnounceTokenSecret secret;
  auto tokens = dht::RotatingAnnounceTokenProvider::create(secret);
  dht::krpc::NodeId local{};
  dht::DhtNode node{
      local, *tokens, {.query_timeout = std::chrono::milliseconds{30}}};
  auto pump = integration::DhtRuntimeActionPump::create(
      observations, {.node = &node}, {.wake_owner = [&] { wakeup.notify(); }});
  if (!pump)
    return 1;
  DatagramTransport transport;
  dht::DhtRuntimeDriver driver{node, transport, **pump};
  Observer observer;
  integration::DhtRuntimeWorker worker{driver, **pump, wakeup, observer};

  if (!worker.start() || !observer.wait_for(1, 0) || !worker.running() ||
      !transport.running)
    return 2;
  auto duplicate = worker.start();
  if (duplicate || duplicate.error().code != core::ErrorCode::Conflict)
    return 3;

  (*pump)->on_error(
      core::Error{core::ErrorCode::IoError, "Synthetic callback error"});
  if (!observer.wait_for(2, 1))
    return 4;

  auto remote_address = runtime::IpAddress::loopback_v4();
  remote_address.bytes[3] = 2;
  const runtime::DatagramEndpoint remote{.address = remote_address,
                                         .port = 6'881};
  auto query = node.ping(remote, std::chrono::system_clock::now());
  if (!query)
    return 6;
  wakeup.notify();
  if (!observer.wait_for(4, 1, 1))
    return 7;

  worker.stop();
  if (worker.running() || transport.running || driver.running())
    return 5;
  return 0;
}

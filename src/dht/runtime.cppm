export module sakuin.dht.runtime;

import std;

import sakuin.core.result;
import sakuin.core.time;
import sakuin.dht.node;
import sakuin.runtime.datagram;
import sakuin.runtime.traffic;

export namespace sakuin::dht {

class DhtRuntimeEvents {
public:
  virtual ~DhtRuntimeEvents() = default;

  // Called on the transport's execution context. Implementations should
  // enqueue work rather than perform blocking storage operations here. A
  // stdexec-backed dispatcher can satisfy this interface without changing the
  // protocol engine or transport.
  virtual void on_actions(DhtActions actions) = 0;
  virtual void on_error(core::Error error) = 0;
};

class DhtRuntimeDriver final : private runtime::DatagramReceiver {
public:
  using Clock = std::function<core::Timestamp()>;

  DhtRuntimeDriver(DhtNode &node, runtime::DatagramTransport &transport,
                   DhtRuntimeEvents &events,
                   Clock clock = [] { return std::chrono::system_clock::now(); })
      : node_(&node), transport_(&transport), events_(&events),
        clock_(std::move(clock)) {}
  DhtRuntimeDriver(DhtNode &node, runtime::DatagramTransport &transport,
                   DhtRuntimeEvents &events,
                   runtime::TrafficGovernor &traffic,
                   Clock clock = [] { return std::chrono::system_clock::now(); })
      : node_(&node), transport_(&transport), events_(&events),
        clock_(std::move(clock)), traffic_(&traffic) {}
  ~DhtRuntimeDriver() override;

  DhtRuntimeDriver(const DhtRuntimeDriver &) = delete;
  DhtRuntimeDriver &operator=(const DhtRuntimeDriver &) = delete;

  core::Result<void> start();
  core::Result<void> send(DatagramSend send);
  void stop() noexcept;
  bool running() const noexcept {
    return running_.load(std::memory_order_acquire);
  }

private:
  void on_datagram(runtime::Datagram datagram) override;
  void on_transport_error(core::Error error) override;
  void report(core::Error error) noexcept;
  void publish(DhtActions actions) noexcept;
  core::Result<core::Timestamp> now() const;
  core::Result<void> send_at(DatagramSend send, core::Timestamp now);

  DhtNode *node_;
  runtime::DatagramTransport *transport_;
  DhtRuntimeEvents *events_;
  Clock clock_;
  runtime::TrafficGovernor *traffic_{};
  std::atomic<bool> running_{};
};

} // namespace sakuin::dht

namespace sakuin::dht {

DhtRuntimeDriver::~DhtRuntimeDriver() { stop(); }

core::Result<void> DhtRuntimeDriver::start() {
  bool expected = false;
  if (!running_.compare_exchange_strong(expected, true,
                                        std::memory_order_acq_rel))
    return std::unexpected(core::Error{core::ErrorCode::Conflict,
                                       "DHT runtime driver already started"});
  auto started = transport_->start(*this);
  if (!started) {
    running_.store(false, std::memory_order_release);
    return std::unexpected(started.error());
  }
  return {};
}

core::Result<void> DhtRuntimeDriver::send(DatagramSend send) {
  if (!running())
    return std::unexpected(core::Error{core::ErrorCode::Conflict,
                                       "DHT runtime driver is not running"});
  auto current = now();
  if (!current)
    return std::unexpected(current.error());
  return send_at(std::move(send), *current);
}

void DhtRuntimeDriver::stop() noexcept {
  running_.store(false, std::memory_order_release);
  // Always delegate: a first stop may have been requested from the runtime
  // thread, in which case a later owner-thread stop still needs to join it.
  transport_->stop();
}

void DhtRuntimeDriver::report(core::Error error) noexcept {
  try {
    events_->on_error(std::move(error));
  } catch (...) {
    // Event handlers may not unwind through a runtime-owned thread.
  }
}

void DhtRuntimeDriver::publish(DhtActions actions) noexcept {
  if (!actions.observation && actions.probes_required.empty() &&
      !actions.metadata_candidate && !actions.query_completion &&
      !actions.observed_address)
    return;
  try {
    events_->on_actions(std::move(actions));
  } catch (const std::exception &exception) {
    report({core::ErrorCode::Internal,
            std::string{"DHT event handler threw: "} + exception.what()});
  } catch (...) {
    report({core::ErrorCode::Internal,
            "DHT event handler threw an unknown exception"});
  }
}

core::Result<core::Timestamp> DhtRuntimeDriver::now() const {
  try {
    return clock_();
  } catch (const std::exception &exception) {
    return std::unexpected(core::Error{
        core::ErrorCode::Internal,
        std::string{"DHT clock failed: "} + exception.what()});
  } catch (...) {
    return std::unexpected(
        core::Error{core::ErrorCode::Internal, "DHT clock failed"});
  }
}

core::Result<void> DhtRuntimeDriver::send_at(DatagramSend send,
                                             core::Timestamp current) {
  if (traffic_) {
    const auto decision = traffic_->admit(
        {.direction = runtime::TrafficDirection::Outbound,
         .traffic_class = send.traffic_class,
         .bytes = send.payload.size()},
        current);
    if (!decision.allowed) {
      if (send.query_transaction)
        node_->cancel_query(*send.query_transaction, send.destination);
      std::string message = "DHT outbound traffic quota denied a datagram";
      if (decision.request_too_large)
        message += " because it can never fit the configured window";
      return std::unexpected(
          core::Error{core::ErrorCode::QuotaExceeded, std::move(message)});
    }
  }
  auto sent = transport_->send(send.destination, std::move(send.payload));
  if (!sent && send.query_transaction)
    node_->cancel_query(*send.query_transaction, send.destination);
  return sent;
}

void DhtRuntimeDriver::on_datagram(runtime::Datagram datagram) {
  auto current = now();
  if (!current) {
    report(current.error());
    return;
  }
  if (traffic_) {
    const auto decision = traffic_->admit(
        {.direction = runtime::TrafficDirection::Inbound,
         .traffic_class = traffic_class::inbound,
         .bytes = datagram.payload.size()},
        *current);
    if (!decision.allowed) {
      report({core::ErrorCode::QuotaExceeded,
              "DHT inbound traffic quota denied a datagram"});
      return;
    }
  }
  auto actions = node_->handle(std::move(datagram), *current);
  if (!actions) {
    report(actions.error());
    return;
  }
  for (auto &outbound : actions->sends) {
    if (auto sent = send_at(std::move(outbound), *current); !sent)
      report(sent.error());
  }
  actions->sends.clear();
  publish(std::move(*actions));
}

void DhtRuntimeDriver::on_transport_error(core::Error error) {
  report(std::move(error));
}

} // namespace sakuin::dht

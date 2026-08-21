export module sakuin.integration.dht_runtime;

import std;

import sakuin.core.result;
import sakuin.core.time;
import sakuin.dht.bootstrap;
import sakuin.dht.metadata_controller;
import sakuin.dht.node;
import sakuin.dht.observation;
import sakuin.dht.routing;
import sakuin.dht.routing_maintenance;
import sakuin.dht.runtime;

export namespace sakuin::integration {

struct DhtRuntimePoll {
  // Actions not consumed by storage or metadata acquisition. Routing probes,
  // query completions, and identity reports stay explicit so a later stdexec
  // owner can schedule them without changing the runtime callback boundary.
  std::vector<dht::DhtActions> actions;
  std::vector<core::Error> errors;
  std::optional<dht::RoutingMaintenanceStep> routing;
  std::optional<dht::BootstrapStep> bootstrap;
  std::vector<dht::QueryTimeout> unhandled_timeouts;
  std::size_t queries_expired{};
  std::size_t observations_stored{};
  std::size_t metadata_candidates_accepted{};
  std::size_t routing_probes_accepted{};
  // Earliest owner-thread deadline after this poll. An empty value means the
  // owner may sleep until a callback explicitly wakes it.
  std::optional<core::Timestamp> next_wakeup;
};

struct DhtRuntimeActionPumpServices {
  dht::MetadataAcquisitionController *metadata{};
  dht::DhtNode *node{};
  dht::BootstrapPlanner *bootstrap{};
  dht::RoutingMaintenancePlanner *routing{};
};

struct DhtRuntimeActionPumpOptions {
  std::size_t maximum_pending_actions{8'192};
  std::size_t maximum_pending_errors{1'024};
  core::Duration deferred_retry_delay{std::chrono::seconds{1}};
  std::function<void()> wake_owner;
};

struct DhtRuntimeDispatch {
  std::vector<core::Error> errors;
  std::size_t attempted{};
  std::size_t accepted{};
  std::size_t failed{};
};

// Thread-safe callback-to-owner bridge. Runtime threads only enqueue values;
// poll() performs storage and acquisition work on its caller's thread.
class DhtRuntimeActionPump final : public dht::DhtRuntimeEvents {
public:
  static core::Result<std::unique_ptr<DhtRuntimeActionPump>>
  create(dht::ObservationSink &observations,
         DhtRuntimeActionPumpServices services = {},
         DhtRuntimeActionPumpOptions options = {});

  void on_actions(dht::DhtActions actions) override;
  void on_error(core::Error error) override;

  DhtRuntimePoll poll(core::Timestamp now);
  core::Result<bool> delivery_failed(const dht::DatagramSend &send,
                                     core::Timestamp now);
  std::size_t pending() const noexcept;

private:
  DhtRuntimeActionPump(dht::ObservationSink &observations,
                       DhtRuntimeActionPumpServices services,
                       DhtRuntimeActionPumpOptions options)
      : observations_(&observations), metadata_(services.metadata),
        node_(services.node), bootstrap_(services.bootstrap),
        routing_(services.routing), options_(options) {}

  static bool has_forward_actions(const dht::DhtActions &actions) noexcept;

  dht::ObservationSink *observations_;
  dht::MetadataAcquisitionController *metadata_;
  dht::DhtNode *node_;
  dht::BootstrapPlanner *bootstrap_;
  dht::RoutingMaintenancePlanner *routing_;
  DhtRuntimeActionPumpOptions options_;
  mutable std::mutex incoming_mutex_;
  std::vector<dht::DhtActions> incoming_actions_;
  std::vector<core::Error> incoming_errors_;
  std::size_t dropped_actions_{};
  std::size_t dropped_errors_{};
  std::atomic<std::size_t> pending_count_{};
  // Owner-thread only. Partially consumed actions remain here until their
  // storage/queue operation succeeds.
  std::deque<dht::DhtActions> deferred_;
  std::optional<core::Timestamp> next_deferred_attempt_;
};

// Drains all owner-produced datagrams in a poll result. Immediate driver
// failures are fed back to their planner so local quota/transport rejection
// schedules a retry without being mistaken for a remote-node timeout.
DhtRuntimeDispatch dispatch_dht_runtime(DhtRuntimePoll &poll,
                                        dht::DhtRuntimeDriver &driver,
                                        DhtRuntimeActionPump &pump,
                                        core::Timestamp now);

} // namespace sakuin::integration

namespace sakuin::integration {
namespace {

core::Error callback_error(std::string_view operation,
                           const std::exception &exception) {
  return {core::ErrorCode::Internal,
          std::string{operation} + " threw: " + exception.what()};
}

core::Error callback_error(std::string_view operation) {
  return {core::ErrorCode::Internal,
          std::string{operation} + " threw an unknown exception"};
}

} // namespace

core::Result<std::unique_ptr<DhtRuntimeActionPump>>
DhtRuntimeActionPump::create(dht::ObservationSink &observations,
                             DhtRuntimeActionPumpServices services,
                             DhtRuntimeActionPumpOptions options) {
  if (options.maximum_pending_actions == 0 ||
      options.maximum_pending_errors == 0 ||
      options.deferred_retry_delay <= core::Duration::zero())
    return std::unexpected(
        core::Error{core::ErrorCode::InvalidArgument,
                    "DHT runtime action-pump pending limits must be nonzero"});
  if ((services.bootstrap || services.routing) && !services.node)
    return std::unexpected(
        core::Error{core::ErrorCode::InvalidArgument,
                    "DHT query planners require an owner-thread node"});
  return std::unique_ptr<DhtRuntimeActionPump>{
      new DhtRuntimeActionPump{observations, services, options}};
}

void DhtRuntimeActionPump::on_actions(dht::DhtActions actions) {
  {
    std::lock_guard lock{incoming_mutex_};
    if (pending_count_.load(std::memory_order_relaxed) >=
        options_.maximum_pending_actions) {
      ++dropped_actions_;
    } else {
      incoming_actions_.push_back(std::move(actions));
      pending_count_.fetch_add(1, std::memory_order_release);
    }
  }
  try {
    if (options_.wake_owner)
      options_.wake_owner();
  } catch (...) {
    // Wake callbacks are advisory and must not unwind through runtime threads.
  }
}

void DhtRuntimeActionPump::on_error(core::Error error) {
  {
    std::lock_guard lock{incoming_mutex_};
    if (incoming_errors_.size() >= options_.maximum_pending_errors) {
      ++dropped_errors_;
    } else {
      incoming_errors_.push_back(std::move(error));
    }
  }
  try {
    if (options_.wake_owner)
      options_.wake_owner();
  } catch (...) {
    // Wake callbacks are advisory and must not unwind through runtime threads.
  }
}

bool DhtRuntimeActionPump::has_forward_actions(
    const dht::DhtActions &actions) noexcept {
  return !actions.sends.empty() || !actions.probes_required.empty() ||
         actions.query_completion.has_value() ||
         actions.observed_address.has_value() ||
         actions.metadata_candidate.has_value();
}

DhtRuntimePoll DhtRuntimeActionPump::poll(core::Timestamp now) {
  std::vector<dht::DhtActions> incoming;
  DhtRuntimePoll result;
  std::size_t dropped_actions{};
  std::size_t dropped_errors{};
  {
    std::lock_guard lock{incoming_mutex_};
    incoming.swap(incoming_actions_);
    result.errors.swap(incoming_errors_);
    std::swap(dropped_actions, dropped_actions_);
    std::swap(dropped_errors, dropped_errors_);
  }
  if (dropped_actions != 0)
    result.errors.push_back(
        core::Error{core::ErrorCode::QuotaExceeded,
                    "DHT runtime action-pump limit dropped " +
                        std::to_string(dropped_actions) + " action batch(es)"});
  if (dropped_errors != 0)
    result.errors.push_back(
        core::Error{core::ErrorCode::QuotaExceeded,
                    "DHT runtime action-pump limit dropped " +
                        std::to_string(dropped_errors) + " error report(s)"});
  for (auto &actions : incoming)
    deferred_.push_back(std::move(actions));

  const bool deferred_ready =
      !next_deferred_attempt_ || *next_deferred_attempt_ <= now;
  const auto count = deferred_ready ? deferred_.size() : 0;
  for (std::size_t index = 0; index < count; ++index) {
    auto actions = std::move(deferred_.front());
    deferred_.pop_front();

    if (actions.observation) {
      try {
        auto stored = observations_->observe(*actions.observation);
        if (stored) {
          actions.observation.reset();
          ++result.observations_stored;
        } else {
          result.errors.push_back(stored.error());
        }
      } catch (const std::exception &exception) {
        result.errors.push_back(
            callback_error("DHT observation sink", exception));
      } catch (...) {
        result.errors.push_back(callback_error("DHT observation sink"));
      }
    }

    if (actions.metadata_candidate && metadata_) {
      try {
        auto accepted = metadata_->offer(*actions.metadata_candidate);
        if (accepted) {
          actions.metadata_candidate.reset();
          if (*accepted)
            ++result.metadata_candidates_accepted;
        } else {
          result.errors.push_back(accepted.error());
        }
      } catch (const std::exception &exception) {
        result.errors.push_back(
            callback_error("Metadata acquisition offer", exception));
      } catch (...) {
        result.errors.push_back(callback_error("Metadata acquisition offer"));
      }
    }

    if (!actions.probes_required.empty() && routing_) {
      std::vector<dht::RoutingProbe> retry;
      for (auto &probe : actions.probes_required) {
        try {
          auto accepted = routing_->offer(probe);
          if (accepted) {
            if (*accepted)
              ++result.routing_probes_accepted;
          } else {
            result.errors.push_back(accepted.error());
            retry.push_back(std::move(probe));
          }
        } catch (const std::exception &exception) {
          result.errors.push_back(
              callback_error("Routing-maintenance offer", exception));
          retry.push_back(std::move(probe));
        } catch (...) {
          result.errors.push_back(callback_error("Routing-maintenance offer"));
          retry.push_back(std::move(probe));
        }
      }
      actions.probes_required = std::move(retry);
    }

    bool completion_consumed = false;
    if (bootstrap_) {
      try {
        completion_consumed = bootstrap_->consume(actions, now);
      } catch (const std::exception &exception) {
        result.errors.push_back(
            callback_error("DHT bootstrap completion", exception));
      } catch (...) {
        result.errors.push_back(callback_error("DHT bootstrap completion"));
      }
    }
    if (actions.query_completion && routing_) {
      try {
        completion_consumed = routing_->consume(actions) || completion_consumed;
      } catch (const std::exception &exception) {
        result.errors.push_back(
            callback_error("Routing-maintenance completion", exception));
      } catch (...) {
        result.errors.push_back(
            callback_error("Routing-maintenance completion"));
      }
    }
    if (completion_consumed)
      actions.query_completion.reset();

    dht::DhtActions forward{
        .sends = std::move(actions.sends),
        .metadata_candidate =
            metadata_ ? std::nullopt : actions.metadata_candidate,
        .probes_required = routing_ ? std::vector<dht::RoutingProbe>{}
                                    : std::move(actions.probes_required),
        .query_completion = std::move(actions.query_completion),
        .observed_address = std::move(actions.observed_address)};
    if (has_forward_actions(forward))
      result.actions.push_back(std::move(forward));

    actions.sends.clear();
    actions.query_completion.reset();
    actions.observed_address.reset();
    if (actions.observation || (actions.metadata_candidate && metadata_) ||
        (!actions.probes_required.empty() && routing_)) {
      deferred_.push_back(std::move(actions));
    } else {
      pending_count_.fetch_sub(1, std::memory_order_release);
    }
  }
  if (deferred_.empty()) {
    next_deferred_attempt_.reset();
  } else if (deferred_ready) {
    next_deferred_attempt_ =
        now + std::chrono::duration_cast<core::Timestamp::duration>(
                  options_.deferred_retry_delay);
  }

  if (metadata_) {
    try {
      auto advanced = metadata_->poll(now);
      if (!advanced)
        result.errors.push_back(advanced.error());
    } catch (const std::exception &exception) {
      result.errors.push_back(
          callback_error("Metadata acquisition poll", exception));
    } catch (...) {
      result.errors.push_back(callback_error("Metadata acquisition poll"));
    }
  }
  if (node_) {
    auto timeouts = node_->expire_queries(now);
    result.queries_expired = timeouts.size();
    for (auto &timeout : timeouts) {
      bool consumed = false;
      if (routing_) {
        try {
          consumed = routing_->consume_timeout(timeout, now);
        } catch (const std::exception &exception) {
          result.errors.push_back(
              callback_error("Routing-maintenance timeout", exception));
        } catch (...) {
          result.errors.push_back(
              callback_error("Routing-maintenance timeout"));
        }
      }
      if (bootstrap_) {
        try {
          consumed = bootstrap_->consume_timeout(timeout, now) || consumed;
        } catch (const std::exception &exception) {
          result.errors.push_back(
              callback_error("DHT bootstrap timeout", exception));
        } catch (...) {
          result.errors.push_back(callback_error("DHT bootstrap timeout"));
        }
      }
      if (!consumed)
        result.unhandled_timeouts.push_back(std::move(timeout));
    }
  }
  if (bootstrap_) {
    try {
      auto step = bootstrap_->poll(now);
      if (step) {
        result.bootstrap = std::move(*step);
      } else {
        result.errors.push_back(step.error());
      }
    } catch (const std::exception &exception) {
      result.errors.push_back(callback_error("DHT bootstrap poll", exception));
    } catch (...) {
      result.errors.push_back(callback_error("DHT bootstrap poll"));
    }
  }
  if (routing_) {
    try {
      auto step = routing_->poll(now);
      if (step) {
        result.routing = std::move(*step);
      } else {
        result.errors.push_back(step.error());
      }
    } catch (const std::exception &exception) {
      result.errors.push_back(
          callback_error("Routing-maintenance poll", exception));
    } catch (...) {
      result.errors.push_back(callback_error("Routing-maintenance poll"));
    }
  }
  const auto include_wakeup = [&](std::optional<core::Timestamp> wakeup) {
    if (wakeup && (!result.next_wakeup || *wakeup < *result.next_wakeup))
      result.next_wakeup = wakeup;
  };
  if (metadata_)
    include_wakeup(metadata_->next_wakeup());
  include_wakeup(next_deferred_attempt_);
  if (node_)
    include_wakeup(node_->next_query_deadline());
  if (result.bootstrap)
    include_wakeup(result.bootstrap->next_wakeup);
  if (result.routing)
    include_wakeup(result.routing->next_wakeup);
  return result;
}

core::Result<bool>
DhtRuntimeActionPump::delivery_failed(const dht::DatagramSend &send,
                                      core::Timestamp now) {
  try {
    bool consumed = false;
    if (routing_)
      consumed = routing_->delivery_failed(send, now);
    if (bootstrap_)
      consumed = bootstrap_->delivery_failed(send, now) || consumed;
    return consumed;
  } catch (const std::exception &exception) {
    return std::unexpected(callback_error("DHT query delivery", exception));
  } catch (...) {
    return std::unexpected(callback_error("DHT query delivery"));
  }
}

std::size_t DhtRuntimeActionPump::pending() const noexcept {
  return pending_count_.load(std::memory_order_acquire);
}

DhtRuntimeDispatch dispatch_dht_runtime(DhtRuntimePoll &poll,
                                        dht::DhtRuntimeDriver &driver,
                                        DhtRuntimeActionPump &pump,
                                        core::Timestamp now) {
  DhtRuntimeDispatch result;
  const auto drain = [&](std::vector<dht::DatagramSend> &sends) {
    for (auto &send : sends) {
      ++result.attempted;
      dht::DatagramSend failure{.destination = send.destination,
                                .traffic_class = send.traffic_class,
                                .query_transaction = send.query_transaction};
      auto delivered = driver.send(std::move(send));
      if (delivered) {
        ++result.accepted;
        continue;
      }
      ++result.failed;
      result.errors.push_back(delivered.error());
      auto consumed = pump.delivery_failed(failure, now);
      if (!consumed)
        result.errors.push_back(consumed.error());
    }
    sends.clear();
  };

  for (auto &actions : poll.actions)
    drain(actions.sends);
  if (poll.bootstrap)
    drain(poll.bootstrap->sends);
  if (poll.routing)
    drain(poll.routing->sends);
  return result;
}

} // namespace sakuin::integration

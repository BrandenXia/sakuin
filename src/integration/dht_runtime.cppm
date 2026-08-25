export module sakuin.integration.dht_runtime;

import std;

import sakuin.core.result;
import sakuin.core.time;
import sakuin.dht.bootstrap;
import sakuin.dht.discovery;
import sakuin.dht.identity;
import sakuin.dht.krpc;
import sakuin.dht.metadata_controller;
import sakuin.dht.node;
import sakuin.dht.observation;
import sakuin.dht.peer_discovery;
import sakuin.dht.routing;
import sakuin.dht.routing_maintenance;
import sakuin.dht.runtime;
import sakuin.integration.metadata_backfill;

export namespace sakuin::integration {

struct DhtRuntimePoll {
  // Actions not consumed by configured owner-thread services. Routing probes
  // and query completions stay explicit so a later stdexec owner can schedule
  // them without changing the runtime callback boundary.
  std::vector<dht::DhtActions> actions;
  std::vector<core::Error> errors;
  std::optional<dht::RoutingMaintenanceStep> routing;
  std::optional<dht::RoutingDiscoveryStep> discovery;
  std::optional<dht::PeerDiscoveryStep> peer_discovery;
  std::optional<MetadataDiscoveryBackfillStep> metadata_backfill;
  std::optional<dht::BootstrapStep> bootstrap;
  // Remains set until the service owner successfully replaces this family's
  // node/runtime and commits the address on the identity policy.
  std::optional<dht::IdentityReconfiguration> identity_reconfiguration;
  std::vector<dht::QueryTimeout> unhandled_timeouts;
  std::size_t queries_expired{};
  std::size_t observations_stored{};
  std::size_t metadata_candidates_accepted{};
  std::size_t routing_probes_accepted{};
  std::size_t discovery_queries_started{};
  std::size_t peer_discovery_queries_started{};
  std::size_t peer_discovery_peers_found{};
  std::size_t peer_discovery_exhausted{};
  std::size_t metadata_backfill_records_scanned{};
  std::size_t metadata_backfill_targets_offered{};
  std::size_t metadata_backfill_records_with_metadata{};
  std::size_t inbound_messages{};
  std::size_t inbound_queries{};
  std::size_t inbound_ping_queries{};
  std::size_t inbound_find_node_queries{};
  std::size_t inbound_get_peers_queries{};
  std::size_t inbound_announce_peer_queries{};
  std::size_t inbound_unknown_queries{};
  std::size_t inbound_responses{};
  std::size_t inbound_protocol_errors{};
  std::optional<core::Timestamp> last_inbound_query;
  // Point-in-time gauges captured on the DHT owner thread.
  std::size_t routing_nodes{};
  std::size_t outstanding_queries{};
  std::size_t pending_actions{};
  std::size_t metadata_queued{};
  std::size_t metadata_in_flight{};
  std::size_t metadata_pending_storage{};
  // Earliest owner-thread deadline after this poll. An empty value means the
  // owner may sleep until a callback explicitly wakes it.
  std::optional<core::Timestamp> next_wakeup;
};

struct DhtRuntimeActionPumpServices {
  dht::MetadataAcquisitionController *metadata{};
  dht::DhtNode *node{};
  dht::BootstrapPlanner *bootstrap{};
  dht::RoutingMaintenancePlanner *routing{};
  dht::RoutingDiscoveryPlanner *discovery{};
  dht::PeerDiscoveryPlanner *peer_discovery{};
  MetadataDiscoveryBackfill *metadata_backfill{};
  dht::Bep42IdentityPolicy *identity{};
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
        routing_(services.routing), discovery_(services.discovery),
        peer_discovery_(services.peer_discovery),
        metadata_backfill_(services.metadata_backfill),
        identity_(services.identity), options_(options) {}

  static bool has_forward_actions(const dht::DhtActions &actions) noexcept;

  dht::ObservationSink *observations_;
  dht::MetadataAcquisitionController *metadata_;
  dht::DhtNode *node_;
  dht::BootstrapPlanner *bootstrap_;
  dht::RoutingMaintenancePlanner *routing_;
  dht::RoutingDiscoveryPlanner *discovery_;
  dht::PeerDiscoveryPlanner *peer_discovery_;
  MetadataDiscoveryBackfill *metadata_backfill_;
  dht::Bep42IdentityPolicy *identity_;
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
  if ((services.bootstrap || services.routing || services.discovery ||
       services.peer_discovery) &&
      !services.node)
    return std::unexpected(
        core::Error{core::ErrorCode::InvalidArgument,
                    "DHT query planners require an owner-thread node"});
  if (services.peer_discovery && !services.metadata)
    return std::unexpected(
        core::Error{core::ErrorCode::InvalidArgument,
                    "Active peer discovery requires metadata acquisition"});
  if (services.metadata_backfill && !services.peer_discovery)
    return std::unexpected(
        core::Error{core::ErrorCode::InvalidArgument,
                    "Metadata backfill requires active peer discovery"});
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

    if (actions.inbound_message) {
      ++result.inbound_messages;
      switch (actions.inbound_message->type) {
      case dht::InboundMessageType::Query:
        ++result.inbound_queries;
        result.last_inbound_query = actions.inbound_message->received_at;
        switch (actions.inbound_message->query_kind) {
        case dht::krpc::QueryKind::Ping:
          ++result.inbound_ping_queries;
          break;
        case dht::krpc::QueryKind::FindNode:
          ++result.inbound_find_node_queries;
          break;
        case dht::krpc::QueryKind::GetPeers:
          ++result.inbound_get_peers_queries;
          break;
        case dht::krpc::QueryKind::AnnouncePeer:
          ++result.inbound_announce_peer_queries;
          break;
        case dht::krpc::QueryKind::Unknown:
          ++result.inbound_unknown_queries;
          break;
        }
        break;
      case dht::InboundMessageType::Response:
        ++result.inbound_responses;
        break;
      case dht::InboundMessageType::ProtocolError:
        ++result.inbound_protocol_errors;
        break;
      }
      actions.inbound_message.reset();
    }

    if (actions.observation) {
      try {
        auto stored = observations_->observe(*actions.observation);
        if (stored) {
          if (peer_discovery_) {
            auto offered =
                peer_discovery_->offer(actions.observation->info_hash,
                                       actions.observation->observed_at);
            if (!offered)
              result.errors.push_back(offered.error());
          }
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

    if (actions.observed_address && identity_) {
      try {
        identity_->observe(*actions.observed_address, now);
        actions.observed_address.reset();
      } catch (const std::exception &exception) {
        result.errors.push_back(
            callback_error("BEP 42 identity observation", exception));
      } catch (...) {
        result.errors.push_back(callback_error("BEP 42 identity observation"));
      }
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
    if (actions.query_completion && discovery_) {
      try {
        completion_consumed =
            discovery_->consume(actions) || completion_consumed;
      } catch (const std::exception &exception) {
        result.errors.push_back(
            callback_error("Routing-discovery completion", exception));
      } catch (...) {
        result.errors.push_back(callback_error("Routing-discovery completion"));
      }
    }
    if (actions.query_completion && peer_discovery_) {
      try {
        completion_consumed =
            peer_discovery_->consume(actions, now) || completion_consumed;
      } catch (const std::exception &exception) {
        result.errors.push_back(
            callback_error("Peer-discovery completion", exception));
      } catch (...) {
        result.errors.push_back(callback_error("Peer-discovery completion"));
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
        .observed_address =
            identity_ ? std::nullopt : std::move(actions.observed_address)};
    if (has_forward_actions(forward))
      result.actions.push_back(std::move(forward));

    actions.sends.clear();
    actions.query_completion.reset();
    if (!identity_)
      actions.observed_address.reset();
    if (actions.observation || (actions.metadata_candidate && metadata_) ||
        (!actions.probes_required.empty() && routing_) ||
        (actions.observed_address && identity_)) {
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
      if (discovery_) {
        try {
          consumed = discovery_->consume_timeout(timeout) || consumed;
        } catch (const std::exception &exception) {
          result.errors.push_back(
              callback_error("Routing-discovery timeout", exception));
        } catch (...) {
          result.errors.push_back(callback_error("Routing-discovery timeout"));
        }
      }
      if (peer_discovery_) {
        try {
          consumed = peer_discovery_->consume_timeout(timeout, now) || consumed;
        } catch (const std::exception &exception) {
          result.errors.push_back(
              callback_error("Peer-discovery timeout", exception));
        } catch (...) {
          result.errors.push_back(callback_error("Peer-discovery timeout"));
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
  if (discovery_) {
    try {
      auto step = discovery_->poll(now);
      if (step) {
        result.discovery_queries_started += step->queries_started;
        result.discovery = std::move(*step);
      } else {
        result.errors.push_back(step.error());
      }
    } catch (const std::exception &exception) {
      result.errors.push_back(
          callback_error("Routing-discovery poll", exception));
    } catch (...) {
      result.errors.push_back(callback_error("Routing-discovery poll"));
    }
  }
  if (metadata_backfill_) {
    try {
      auto step = metadata_backfill_->poll(now);
      if (step) {
        result.metadata_backfill_records_scanned += step->records_scanned;
        result.metadata_backfill_targets_offered += step->targets_offered;
        result.metadata_backfill_records_with_metadata +=
            step->records_with_metadata;
        result.metadata_backfill = std::move(*step);
      } else {
        result.errors.push_back(step.error());
      }
    } catch (const std::exception &exception) {
      result.errors.push_back(
          callback_error("Metadata-discovery backfill poll", exception));
    } catch (...) {
      result.errors.push_back(
          callback_error("Metadata-discovery backfill poll"));
    }
  }
  if (peer_discovery_) {
    try {
      auto step = peer_discovery_->poll(now);
      if (step) {
        result.peer_discovery_queries_started += step->queries_started;
        result.peer_discovery_peers_found += step->peers_found;
        result.peer_discovery_exhausted += step->exhausted;
        if (metadata_) {
          for (const auto &candidate : step->candidates) {
            try {
              auto accepted = metadata_->offer(candidate);
              if (accepted) {
                if (*accepted)
                  ++result.metadata_candidates_accepted;
              } else {
                result.errors.push_back(accepted.error());
                if (auto retried = peer_discovery_->retry(candidate); !retried)
                  result.errors.push_back(retried.error());
              }
            } catch (const std::exception &exception) {
              result.errors.push_back(
                  callback_error("Metadata acquisition offer", exception));
              if (auto retried = peer_discovery_->retry(candidate); !retried)
                result.errors.push_back(retried.error());
            } catch (...) {
              result.errors.push_back(
                  callback_error("Metadata acquisition offer"));
              if (auto retried = peer_discovery_->retry(candidate); !retried)
                result.errors.push_back(retried.error());
            }
          }
        }
        step->candidates.clear();
        result.peer_discovery = std::move(*step);
      } else {
        result.errors.push_back(step.error());
      }
    } catch (const std::exception &exception) {
      result.errors.push_back(callback_error("Peer-discovery poll", exception));
    } catch (...) {
      result.errors.push_back(callback_error("Peer-discovery poll"));
    }
  }
  if (identity_) {
    if (auto proposed = identity_->proposed_external()) {
      result.identity_reconfiguration =
          dht::IdentityReconfiguration{.external_address = *proposed};
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
  if (result.discovery)
    include_wakeup(result.discovery->next_wakeup);
  if (result.metadata_backfill)
    include_wakeup(result.metadata_backfill->next_wakeup);
  result.pending_actions = pending_count_.load(std::memory_order_acquire);
  if (node_) {
    result.routing_nodes = node_->routing_table().size();
    result.outstanding_queries = node_->outstanding_queries();
  }
  if (metadata_) {
    result.metadata_queued = metadata_->queued();
    result.metadata_in_flight = metadata_->in_flight();
    result.metadata_pending_storage = metadata_->pending_storage();
  }
  return result;
}

core::Result<bool>
DhtRuntimeActionPump::delivery_failed(const dht::DatagramSend &send,
                                      core::Timestamp now) {
  try {
    bool consumed = false;
    if (routing_)
      consumed = routing_->delivery_failed(send, now);
    if (discovery_)
      consumed = discovery_->delivery_failed(send) || consumed;
    if (peer_discovery_)
      consumed = peer_discovery_->delivery_failed(send, now) || consumed;
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
  if (poll.discovery)
    drain(poll.discovery->sends);
  if (poll.peer_discovery)
    drain(poll.peer_discovery->sends);
  return result;
}

} // namespace sakuin::integration

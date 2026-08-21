export module sakuin.integration.dht_runtime;

import std;

import sakuin.core.result;
import sakuin.core.time;
import sakuin.dht.metadata_controller;
import sakuin.dht.node;
import sakuin.dht.observation;
import sakuin.dht.runtime;

export namespace sakuin::integration {

struct DhtRuntimePoll {
  // Actions not consumed by storage or metadata acquisition. Routing probes,
  // query completions, and identity reports stay explicit so a later stdexec
  // owner can schedule them without changing the runtime callback boundary.
  std::vector<dht::DhtActions> actions;
  std::vector<core::Error> errors;
  std::size_t observations_stored{};
  std::size_t metadata_candidates_accepted{};
};

struct DhtRuntimeActionPumpOptions {
  std::size_t maximum_pending_actions{8'192};
  std::size_t maximum_pending_errors{1'024};
};

// Thread-safe callback-to-owner bridge. Runtime threads only enqueue values;
// poll() performs storage and acquisition work on its caller's thread.
class DhtRuntimeActionPump final : public dht::DhtRuntimeEvents {
public:
  static core::Result<std::unique_ptr<DhtRuntimeActionPump>>
  create(dht::ObservationSink &observations,
         dht::MetadataAcquisitionController *metadata = nullptr,
         DhtRuntimeActionPumpOptions options = {});

  void on_actions(dht::DhtActions actions) override;
  void on_error(core::Error error) override;

  DhtRuntimePoll poll(core::Timestamp now);
  std::size_t pending() const noexcept;

private:
  DhtRuntimeActionPump(dht::ObservationSink &observations,
                       dht::MetadataAcquisitionController *metadata,
                       DhtRuntimeActionPumpOptions options)
      : observations_(&observations), metadata_(metadata), options_(options) {}

  static bool has_forward_actions(const dht::DhtActions &actions) noexcept;

  dht::ObservationSink *observations_;
  dht::MetadataAcquisitionController *metadata_;
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
};

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
                             dht::MetadataAcquisitionController *metadata,
                             DhtRuntimeActionPumpOptions options) {
  if (options.maximum_pending_actions == 0 ||
      options.maximum_pending_errors == 0)
    return std::unexpected(
        core::Error{core::ErrorCode::InvalidArgument,
                    "DHT runtime action-pump pending limits must be nonzero"});
  return std::unique_ptr<DhtRuntimeActionPump>{
      new DhtRuntimeActionPump{observations, metadata, options}};
}

void DhtRuntimeActionPump::on_actions(dht::DhtActions actions) {
  std::lock_guard lock{incoming_mutex_};
  if (pending_count_.load(std::memory_order_relaxed) >=
      options_.maximum_pending_actions) {
    ++dropped_actions_;
    return;
  }
  incoming_actions_.push_back(std::move(actions));
  pending_count_.fetch_add(1, std::memory_order_release);
}

void DhtRuntimeActionPump::on_error(core::Error error) {
  std::lock_guard lock{incoming_mutex_};
  if (incoming_errors_.size() >= options_.maximum_pending_errors) {
    ++dropped_errors_;
    return;
  }
  incoming_errors_.push_back(std::move(error));
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

  const auto count = deferred_.size();
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

    dht::DhtActions forward{
        .sends = std::move(actions.sends),
        .metadata_candidate =
            metadata_ ? std::nullopt : actions.metadata_candidate,
        .probes_required = std::move(actions.probes_required),
        .query_completion = std::move(actions.query_completion),
        .observed_address = std::move(actions.observed_address)};
    if (has_forward_actions(forward))
      result.actions.push_back(std::move(forward));

    actions.sends.clear();
    actions.probes_required.clear();
    actions.query_completion.reset();
    actions.observed_address.reset();
    if (actions.observation || (actions.metadata_candidate && metadata_)) {
      deferred_.push_back(std::move(actions));
    } else {
      pending_count_.fetch_sub(1, std::memory_order_release);
    }
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
  return result;
}

std::size_t DhtRuntimeActionPump::pending() const noexcept {
  return pending_count_.load(std::memory_order_acquire);
}

} // namespace sakuin::integration

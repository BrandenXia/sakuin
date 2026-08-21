export module sakuin.dht.bootstrap;

import std;

import sakuin.core.result;
import sakuin.core.time;
import sakuin.dht.krpc;
import sakuin.dht.node;
import sakuin.dht.routing;
import sakuin.runtime.datagram;

export namespace sakuin::dht {

struct BootstrapOptions {
  std::size_t maximum_in_flight{8};
  std::size_t maximum_attempts{3};
  core::Duration retry_delay{std::chrono::seconds{5}};
};

struct BootstrapStep {
  std::vector<DatagramSend> sends;
  std::vector<QueryTimeout> timeouts;
  std::size_t known_candidates{};
  bool complete{};
  std::optional<core::Timestamp> next_wakeup;
};

class BootstrapPlanner {
public:
  static core::Result<std::unique_ptr<BootstrapPlanner>>
  create(DhtNode &node,
         std::span<const runtime::DatagramEndpoint> bootstrap_endpoints,
         BootstrapOptions options = {});

  // poll() and consume() are intentionally externally driven. A simple app can
  // call them from a timer, while a future stdexec scheduler can own timing,
  // cancellation, and worker lifetime without changing bootstrap state.
  core::Result<BootstrapStep> poll(core::Timestamp now);
  void consume(const DhtActions &actions, core::Timestamp now);
  void delivery_failed(const DatagramSend &send, core::Timestamp now);

private:
  struct Candidate {
    std::optional<krpc::NodeId> id;
    runtime::DatagramEndpoint endpoint;
    std::size_t attempts{};
    bool outstanding{};
    bool completed{};
    core::Timestamp next_attempt{core::Timestamp::min()};
  };

  BootstrapPlanner(DhtNode &node, BootstrapOptions options)
      : node_(&node), options_(options) {}

  void discover_routing_contacts();
  void failed(runtime::DatagramEndpoint endpoint, core::Timestamp now);
  core::Timestamp retry_at(core::Timestamp now) const;

  DhtNode *node_;
  BootstrapOptions options_;
  std::vector<Candidate> candidates_;
};

} // namespace sakuin::dht

namespace sakuin::dht {

core::Result<std::unique_ptr<BootstrapPlanner>> BootstrapPlanner::create(
    DhtNode &node,
    std::span<const runtime::DatagramEndpoint> bootstrap_endpoints,
    BootstrapOptions options) {
  if (bootstrap_endpoints.empty())
    return std::unexpected(core::Error{
        core::ErrorCode::InvalidArgument,
        "At least one resolved DHT bootstrap endpoint is required"});
  if (options.maximum_in_flight == 0 || options.maximum_attempts == 0 ||
      options.retry_delay < core::Duration::zero())
    return std::unexpected(core::Error{
        core::ErrorCode::InvalidArgument, "Invalid DHT bootstrap options"});

  auto result = std::unique_ptr<BootstrapPlanner>{
      new BootstrapPlanner{node, options}};
  for (const auto &endpoint : bootstrap_endpoints) {
    if (endpoint.port == 0)
      return std::unexpected(core::Error{
          core::ErrorCode::InvalidArgument,
          "DHT bootstrap endpoints must use a nonzero port"});
    if (std::ranges::find(result->candidates_, endpoint,
                          &Candidate::endpoint) == result->candidates_.end())
      result->candidates_.push_back(Candidate{.endpoint = endpoint});
  }
  return result;
}

core::Timestamp BootstrapPlanner::retry_at(core::Timestamp now) const {
  return now + std::chrono::duration_cast<core::Timestamp::duration>(
                   options_.retry_delay);
}

void BootstrapPlanner::discover_routing_contacts() {
  for (const auto &contact : node_->routing_table().all_contacts()) {
    auto found = std::ranges::find_if(candidates_, [&](const auto &entry) {
      return entry.id && *entry.id == contact.id;
    });
    if (found == candidates_.end())
      found = std::ranges::find(candidates_, contact.endpoint,
                                &Candidate::endpoint);
    if (found == candidates_.end()) {
      candidates_.push_back(
          Candidate{.id = contact.id, .endpoint = contact.endpoint});
    } else {
      found->id = contact.id;
      found->endpoint = contact.endpoint;
    }
  }
}

void BootstrapPlanner::failed(runtime::DatagramEndpoint endpoint,
                              core::Timestamp now) {
  const auto found = std::ranges::find(candidates_, endpoint,
                                       &Candidate::endpoint);
  if (found == candidates_.end())
    return;
  found->outstanding = false;
  found->next_attempt = retry_at(now);
}

void BootstrapPlanner::consume(const DhtActions &actions,
                               core::Timestamp now) {
  if (actions.query_completion &&
      actions.query_completion->kind == krpc::QueryKind::FindNode) {
    const auto found =
        std::ranges::find(candidates_, actions.query_completion->remote,
                          &Candidate::endpoint);
    if (found != candidates_.end()) {
      found->outstanding = false;
      if (actions.query_completion->protocol_error)
        found->next_attempt = retry_at(now);
      else
        found->completed = true;
    }
  }
  discover_routing_contacts();
}

void BootstrapPlanner::delivery_failed(const DatagramSend &send,
                                       core::Timestamp now) {
  if (send.query_transaction)
    node_->cancel_query(*send.query_transaction, send.destination);
  failed(send.destination, now);
}

core::Result<BootstrapStep> BootstrapPlanner::poll(core::Timestamp now) {
  BootstrapStep step;
  step.timeouts = node_->expire_queries(now);
  for (const auto &timeout : step.timeouts) {
    if (timeout.kind == krpc::QueryKind::FindNode)
      failed(timeout.remote, now);
  }
  discover_routing_contacts();

  const auto outstanding = node_->outstanding_queries();
  std::size_t capacity = outstanding >= options_.maximum_in_flight
                             ? 0
                             : options_.maximum_in_flight - outstanding;
  for (auto &candidate : candidates_) {
    if (capacity == 0)
      break;
    if (candidate.outstanding || candidate.completed ||
        candidate.attempts >= options_.maximum_attempts ||
        candidate.next_attempt > now)
      continue;
    const auto target = candidate.id.value_or(node_->routing_table().local_id());
    auto query = node_->find_node(candidate.endpoint, target, now);
    if (!query) {
      for (const auto &created : step.sends) {
        if (created.query_transaction) {
          node_->cancel_query(*created.query_transaction, created.destination);
          const auto rollback = std::ranges::find(
              candidates_, created.destination, &Candidate::endpoint);
          if (rollback != candidates_.end()) {
            rollback->outstanding = false;
            --rollback->attempts;
          }
        }
      }
      return std::unexpected(query.error());
    }
    candidate.outstanding = true;
    ++candidate.attempts;
    step.sends.push_back(std::move(*query));
    --capacity;
  }

  step.known_candidates = candidates_.size();
  step.complete = node_->outstanding_queries() == 0 &&
                  std::ranges::all_of(candidates_, [&](const auto &candidate) {
                    return candidate.completed ||
                           candidate.attempts >= options_.maximum_attempts;
                  });
  if (!step.complete) {
    for (const auto &candidate : candidates_) {
      if (candidate.completed || candidate.outstanding ||
          candidate.attempts >= options_.maximum_attempts)
        continue;
      if (!step.next_wakeup || candidate.next_attempt < *step.next_wakeup)
        step.next_wakeup = candidate.next_attempt;
    }
  }
  return step;
}

} // namespace sakuin::dht

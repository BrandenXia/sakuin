export module sakuin.dht.routing_maintenance;

import std;

import sakuin.core.bytes;
import sakuin.core.result;
import sakuin.core.time;
import sakuin.dht.krpc;
import sakuin.dht.node;
import sakuin.dht.routing;
import sakuin.runtime.datagram;

export namespace sakuin::dht {

struct RoutingMaintenanceOptions {
  std::size_t maximum_queued{1'024};
  std::size_t maximum_in_flight{8};
  std::size_t maximum_attempts{2};
  core::Duration retry_delay{std::chrono::seconds{5}};
};

struct RoutingMaintenanceStep {
  std::vector<DatagramSend> sends;
  std::size_t queued{};
  std::size_t in_flight{};
  std::optional<core::Timestamp> next_wakeup;
};

// Owner-thread Kademlia bucket-liveness policy. It probes the incumbent and
// only installs the queued replacement after an actual query timeout. Local
// send failures are retried but never treated as evidence that a peer is dead.
class RoutingMaintenancePlanner {
public:
  static core::Result<std::unique_ptr<RoutingMaintenancePlanner>>
  create(DhtNode &node, RoutingMaintenanceOptions options = {});

  core::Result<bool> offer(RoutingProbe probe);
  core::Result<RoutingMaintenanceStep> poll(core::Timestamp now);
  bool consume(const DhtActions &actions);
  bool consume_timeout(const QueryTimeout &timeout, core::Timestamp now);
  bool delivery_failed(const DatagramSend &send, core::Timestamp now);

  std::size_t queued() const noexcept;
  std::size_t in_flight() const noexcept;

private:
  struct Entry {
    RoutingProbe probe;
    std::size_t attempts{};
    bool outstanding{};
    core::ByteBuffer transaction;
    core::Timestamp next_attempt{core::Timestamp::min()};
  };

  RoutingMaintenancePlanner(DhtNode &node, RoutingMaintenanceOptions options)
      : node_(&node), options_(options) {}

  std::vector<Entry>::iterator find(core::ByteView transaction,
                                    const runtime::DatagramEndpoint &remote);
  bool incumbent_refreshed(const Entry &entry) const;
  core::Timestamp retry_at(core::Timestamp now) const;

  DhtNode *node_;
  RoutingMaintenanceOptions options_;
  std::vector<Entry> entries_;
};

} // namespace sakuin::dht

namespace sakuin::dht {

core::Result<std::unique_ptr<RoutingMaintenancePlanner>>
RoutingMaintenancePlanner::create(DhtNode &node,
                                  RoutingMaintenanceOptions options) {
  if (options.maximum_queued == 0 || options.maximum_in_flight == 0 ||
      options.maximum_attempts == 0 ||
      options.retry_delay <= core::Duration::zero())
    return std::unexpected(
        core::Error{core::ErrorCode::InvalidArgument,
                    "Invalid DHT routing-maintenance limits"});
  return std::unique_ptr<RoutingMaintenancePlanner>{
      new RoutingMaintenancePlanner{node, options}};
}

core::Timestamp RoutingMaintenancePlanner::retry_at(core::Timestamp now) const {
  return now + std::chrono::duration_cast<core::Timestamp::duration>(
                   options_.retry_delay);
}

core::Result<bool> RoutingMaintenancePlanner::offer(RoutingProbe probe) {
  const auto incumbent_bucket =
      bucket_index(node_->routing_table().local_id(), probe.incumbent.id);
  const auto replacement_bucket =
      bucket_index(node_->routing_table().local_id(), probe.replacement.id);
  if (!incumbent_bucket || incumbent_bucket != replacement_bucket ||
      probe.incumbent.id == probe.replacement.id ||
      probe.incumbent.endpoint.port == 0 ||
      probe.replacement.endpoint.port == 0)
    return std::unexpected(core::Error{
        core::ErrorCode::InvalidArgument,
        "Routing probe contacts must be distinct valid peers in one bucket"});

  const auto contacts = node_->routing_table().all_contacts();
  const auto incumbent =
      std::ranges::find(contacts, probe.incumbent.id, &NodeContact::id);
  if (incumbent == contacts.end() ||
      std::ranges::find(contacts, probe.replacement.id, &NodeContact::id) !=
          contacts.end() ||
      incumbent->endpoint != probe.incumbent.endpoint ||
      incumbent->last_seen > probe.incumbent.last_seen)
    return false;
  probe.incumbent = *incumbent;

  const auto existing = std::ranges::find_if(entries_, [&](const auto &entry) {
    return entry.probe.incumbent.id == probe.incumbent.id;
  });
  if (existing != entries_.end()) {
    if (probe.replacement.last_seen > existing->probe.replacement.last_seen)
      existing->probe.replacement = std::move(probe.replacement);
    return false;
  }
  if (entries_.size() >= options_.maximum_queued)
    return std::unexpected(
        core::Error{core::ErrorCode::QuotaExceeded,
                    "Routing-maintenance queue limit would be exceeded"});
  entries_.push_back(Entry{.probe = std::move(probe)});
  return true;
}

std::vector<RoutingMaintenancePlanner::Entry>::iterator
RoutingMaintenancePlanner::find(core::ByteView transaction,
                                const runtime::DatagramEndpoint &remote) {
  return std::ranges::find_if(entries_, [&](const auto &entry) {
    return entry.outstanding && entry.probe.incumbent.endpoint == remote &&
           std::ranges::equal(entry.transaction, transaction);
  });
}

bool RoutingMaintenancePlanner::incumbent_refreshed(const Entry &entry) const {
  const auto contacts = node_->routing_table().all_contacts();
  const auto incumbent =
      std::ranges::find(contacts, entry.probe.incumbent.id, &NodeContact::id);
  return incumbent != contacts.end() &&
         (incumbent->endpoint != entry.probe.incumbent.endpoint ||
          incumbent->last_seen > entry.probe.incumbent.last_seen);
}

core::Result<RoutingMaintenanceStep>
RoutingMaintenancePlanner::poll(core::Timestamp now) {
  RoutingMaintenanceStep step;
  const auto globally_outstanding = node_->outstanding_queries();
  auto capacity = globally_outstanding >= options_.maximum_in_flight
                      ? std::size_t{0}
                      : options_.maximum_in_flight - globally_outstanding;
  for (auto &entry : entries_) {
    if (capacity == 0)
      break;
    if (entry.outstanding || entry.next_attempt > now)
      continue;
    auto query = node_->ping(entry.probe.incumbent.endpoint, now);
    if (!query) {
      for (const auto &created : step.sends) {
        node_->cancel_query(*created.query_transaction, created.destination);
        const auto rollback =
            find(*created.query_transaction, created.destination);
        if (rollback != entries_.end()) {
          rollback->outstanding = false;
          rollback->transaction.clear();
          --rollback->attempts;
        }
      }
      return std::unexpected(query.error());
    }
    entry.outstanding = true;
    entry.transaction = *query->query_transaction;
    ++entry.attempts;
    step.sends.push_back(std::move(*query));
    --capacity;
  }

  step.queued = queued();
  step.in_flight = in_flight();
  for (const auto &entry : entries_) {
    if (entry.outstanding)
      continue;
    if (!step.next_wakeup || entry.next_attempt < *step.next_wakeup)
      step.next_wakeup = entry.next_attempt;
  }
  return step;
}

bool RoutingMaintenancePlanner::consume(const DhtActions &actions) {
  if (!actions.query_completion ||
      actions.query_completion->kind != krpc::QueryKind::Ping)
    return false;
  const auto found = find(actions.query_completion->transaction,
                          actions.query_completion->remote);
  if (found == entries_.end())
    return false;
  // Any matched response, including a protocol error, proves liveness.
  entries_.erase(found);
  return true;
}

bool RoutingMaintenancePlanner::consume_timeout(const QueryTimeout &timeout,
                                                core::Timestamp now) {
  if (timeout.kind != krpc::QueryKind::Ping)
    return false;
  const auto found = find(timeout.transaction, timeout.remote);
  if (found == entries_.end())
    return false;
  if (incumbent_refreshed(*found)) {
    entries_.erase(found);
    return true;
  }
  found->outstanding = false;
  found->transaction.clear();
  if (found->attempts < options_.maximum_attempts) {
    found->next_attempt = retry_at(now);
    return true;
  }
  node_->routing_table().replace_unresponsive(found->probe.incumbent.id,
                                              found->probe.replacement);
  entries_.erase(found);
  return true;
}

bool RoutingMaintenancePlanner::delivery_failed(const DatagramSend &send,
                                                core::Timestamp now) {
  if (!send.query_transaction)
    return false;
  const auto found = find(*send.query_transaction, send.destination);
  if (found == entries_.end())
    return false;
  node_->cancel_query(*send.query_transaction, send.destination);
  found->outstanding = false;
  found->transaction.clear();
  if (found->attempts >= options_.maximum_attempts) {
    entries_.erase(found);
  } else {
    found->next_attempt = retry_at(now);
  }
  return true;
}

std::size_t RoutingMaintenancePlanner::queued() const noexcept {
  return static_cast<std::size_t>(std::ranges::count_if(
      entries_, [](const auto &entry) { return !entry.outstanding; }));
}

std::size_t RoutingMaintenancePlanner::in_flight() const noexcept {
  return static_cast<std::size_t>(std::ranges::count_if(
      entries_, [](const auto &entry) { return entry.outstanding; }));
}

} // namespace sakuin::dht

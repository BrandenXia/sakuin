export module sakuin.dht.peer_discovery;

import std;

import sakuin.core.bytes;
import sakuin.core.ids;
import sakuin.core.result;
import sakuin.core.time;
import sakuin.dht.krpc;
import sakuin.dht.metadata_candidate;
import sakuin.dht.node;
import sakuin.dht.routing;
import sakuin.runtime.datagram;

export namespace sakuin::dht {

struct PeerDiscoveryOptions {
  std::size_t maximum_pending{8'192};
  std::size_t maximum_in_flight{64};
  std::size_t parallelism_per_hash{3};
  std::size_t maximum_queries_per_hash{24};
  core::Duration retry_delay{std::chrono::minutes{5}};
};

struct PeerDiscoveryStep {
  std::vector<DatagramSend> sends;
  std::vector<PeerMetadataCandidate> candidates;
  std::size_t pending{};
  std::size_t in_flight{};
  std::size_t queries_started{};
  std::size_t peers_found{};
  std::size_t exhausted{};
};

// Bounded owner-thread policy for finding live peers for newly observed
// infohashes. The planner owns traversal and retry decisions while DhtNode
// remains a protocol engine and the runtime remains transport-only.
class PeerDiscoveryPlanner {
public:
  static core::Result<std::unique_ptr<PeerDiscoveryPlanner>>
  create(DhtNode &node, PeerDiscoveryOptions options = {});

  core::Result<bool> offer(core::InfoHash info_hash,
                           core::Timestamp observed_at);
  core::Result<PeerDiscoveryStep> poll(core::Timestamp now);
  bool consume(const DhtActions &actions, core::Timestamp now);
  bool consume_timeout(const QueryTimeout &timeout, core::Timestamp now);
  bool delivery_failed(const DatagramSend &send, core::Timestamp now);
  core::Result<void> retry(PeerMetadataCandidate candidate);

  std::size_t pending() const noexcept {
    return entries_.size() + ready_.size();
  }
  std::size_t in_flight() const noexcept;

private:
  struct Outstanding {
    core::ByteBuffer transaction;
    runtime::DatagramEndpoint remote;
  };

  struct Entry {
    core::InfoHash info_hash;
    core::Timestamp observed_at;
    std::size_t attempts{};
    std::vector<NodeContact> frontier;
    std::vector<runtime::DatagramEndpoint> queried;
    std::vector<Outstanding> outstanding;
  };

  struct Cooldown {
    core::InfoHash info_hash;
    core::Timestamp eligible_at;
  };

  PeerDiscoveryPlanner(DhtNode &node, PeerDiscoveryOptions options)
      : node_(&node), options_(options) {}

  std::vector<Entry>::iterator find(core::ByteView transaction,
                                    const runtime::DatagramEndpoint &remote);
  std::vector<Entry>::const_iterator
  find(core::ByteView transaction,
       const runtime::DatagramEndpoint &remote) const;
  void finish(std::vector<Entry>::iterator entry, core::Timestamp now);
  void purge_cooldowns(core::Timestamp now);
  bool cooling_down(const core::InfoHash &info_hash, core::Timestamp now) const;

  DhtNode *node_;
  PeerDiscoveryOptions options_;
  std::vector<Entry> entries_;
  // The next hash to receive a query. Keeping this cursor across polls avoids
  // allowing early queue entries or fast responders to monopolize capacity.
  std::size_t schedule_cursor_{};
  std::deque<Cooldown> cooldowns_;
  std::vector<PeerMetadataCandidate> ready_;
  std::size_t exhausted_since_poll_{};
};

} // namespace sakuin::dht

namespace sakuin::dht {
namespace {

krpc::NodeId target_id(const core::InfoHash &hash) {
  krpc::NodeId result;
  result.bytes = hash.bytes;
  return result;
}

} // namespace

core::Result<std::unique_ptr<PeerDiscoveryPlanner>>
PeerDiscoveryPlanner::create(DhtNode &node, PeerDiscoveryOptions options) {
  if (options.maximum_pending == 0 || options.maximum_in_flight == 0 ||
      options.parallelism_per_hash == 0 ||
      options.parallelism_per_hash > options.maximum_in_flight ||
      options.maximum_queries_per_hash < options.parallelism_per_hash ||
      options.retry_delay <= core::Duration::zero())
    return std::unexpected(core::Error{core::ErrorCode::InvalidArgument,
                                       "Invalid active peer-discovery limits"});
  return std::unique_ptr<PeerDiscoveryPlanner>{
      new PeerDiscoveryPlanner{node, options}};
}

void PeerDiscoveryPlanner::purge_cooldowns(core::Timestamp now) {
  std::erase_if(cooldowns_,
                [&](const auto &entry) { return entry.eligible_at <= now; });
}

bool PeerDiscoveryPlanner::cooling_down(const core::InfoHash &info_hash,
                                        core::Timestamp now) const {
  return std::ranges::any_of(cooldowns_, [&](const auto &entry) {
    return entry.info_hash == info_hash && entry.eligible_at > now;
  });
}

core::Result<bool> PeerDiscoveryPlanner::offer(core::InfoHash info_hash,
                                               core::Timestamp observed_at) {
  purge_cooldowns(observed_at);
  if (cooling_down(info_hash, observed_at))
    return false;
  const auto existing =
      std::ranges::find(entries_, info_hash, &Entry::info_hash);
  if (existing != entries_.end()) {
    existing->observed_at = std::max(existing->observed_at, observed_at);
    return false;
  }
  if (pending() >= options_.maximum_pending)
    return std::unexpected(
        core::Error{core::ErrorCode::QuotaExceeded,
                    "Active peer-discovery queue limit would be exceeded"});
  entries_.push_back(Entry{.info_hash = info_hash, .observed_at = observed_at});
  return true;
}

std::vector<PeerDiscoveryPlanner::Entry>::iterator
PeerDiscoveryPlanner::find(core::ByteView transaction,
                           const runtime::DatagramEndpoint &remote) {
  return std::ranges::find_if(entries_, [&](const auto &entry) {
    return std::ranges::any_of(entry.outstanding, [&](const auto &query) {
      return query.remote == remote &&
             std::ranges::equal(query.transaction, transaction);
    });
  });
}

std::vector<PeerDiscoveryPlanner::Entry>::const_iterator
PeerDiscoveryPlanner::find(core::ByteView transaction,
                           const runtime::DatagramEndpoint &remote) const {
  return std::ranges::find_if(entries_, [&](const auto &entry) {
    return std::ranges::any_of(entry.outstanding, [&](const auto &query) {
      return query.remote == remote &&
             std::ranges::equal(query.transaction, transaction);
    });
  });
}

void PeerDiscoveryPlanner::finish(std::vector<Entry>::iterator entry,
                                  core::Timestamp now) {
  const auto erased =
      static_cast<std::size_t>(std::distance(entries_.begin(), entry));
  for (const auto &query : entry->outstanding)
    node_->cancel_query(query.transaction, query.remote);
  if (cooldowns_.size() >= options_.maximum_pending)
    cooldowns_.pop_front();
  cooldowns_.push_back(
      {.info_hash = entry->info_hash,
       .eligible_at =
           now + std::chrono::duration_cast<core::Timestamp::duration>(
                     options_.retry_delay)});
  entries_.erase(entry);
  if (entries_.empty()) {
    schedule_cursor_ = 0;
  } else {
    if (erased < schedule_cursor_)
      --schedule_cursor_;
    if (schedule_cursor_ >= entries_.size())
      schedule_cursor_ = 0;
  }
}

core::Result<PeerDiscoveryStep>
PeerDiscoveryPlanner::poll(core::Timestamp now) {
  purge_cooldowns(now);
  PeerDiscoveryStep step;
  step.candidates = std::move(ready_);
  ready_.clear();
  step.peers_found = step.candidates.size();
  step.exhausted = std::exchange(exhausted_since_poll_, 0);

  auto capacity = options_.maximum_in_flight -
                  std::min(in_flight(), options_.maximum_in_flight);
  std::size_t visits_without_query{};
  while (capacity != 0 && !entries_.empty() &&
         visits_without_query < entries_.size()) {
    if (schedule_cursor_ >= entries_.size())
      schedule_cursor_ = 0;
    const auto index = schedule_cursor_;
    auto &entry = entries_[index];
    auto routing_contacts = node_->routing_table().closest(
        target_id(entry.info_hash), node_->routing_table().size());
    for (auto &contact : routing_contacts)
      if ((!node_->address_family() ||
           contact.endpoint.address.family == *node_->address_family()) &&
          std::ranges::find(entry.frontier, contact.id, &NodeContact::id) ==
              entry.frontier.end())
        entry.frontier.push_back(std::move(contact));
    const auto target = target_id(entry.info_hash);
    std::ranges::sort(entry.frontier, [&](const auto &left, const auto &right) {
      return closer_to(left.id, right.id, target);
    });
    const auto contact =
        std::ranges::find_if(entry.frontier, [&](const auto &item) {
          return !std::ranges::contains(entry.queried, item.endpoint);
        });
    const bool can_query =
        entry.outstanding.size() < options_.parallelism_per_hash &&
        entry.attempts < options_.maximum_queries_per_hash &&
        contact != entry.frontier.end();
    if (can_query) {
      auto query = node_->get_peers(contact->endpoint, entry.info_hash, now);
      if (!query) {
        for (const auto &created : step.sends) {
          if (!created.query_transaction.empty()) {
            node_->cancel_query(created.query_transaction, created.destination);
            const auto rollback =
                find(created.query_transaction, created.destination);
            if (rollback != entries_.end()) {
              std::erase_if(
                  rollback->outstanding, [&](const auto &outstanding) {
                    return outstanding.remote == created.destination &&
                           std::ranges::equal(outstanding.transaction,
                                              created.query_transaction);
                  });
              std::erase(rollback->queried, created.destination);
              if (rollback->attempts != 0)
                --rollback->attempts;
            }
          }
        }
        return std::unexpected(query.error());
      }
      entry.queried.push_back(contact->endpoint);
      entry.outstanding.push_back({.transaction = query->query_transaction,
                                   .remote = query->destination});
      ++entry.attempts;
      ++step.queries_started;
      step.sends.push_back(std::move(*query));
      --capacity;
      visits_without_query = 0;
      schedule_cursor_ = (index + 1) % entries_.size();
      continue;
    }

    const bool no_more_contacts =
        std::ranges::all_of(entry.frontier, [&](const auto &contact) {
          return std::ranges::contains(entry.queried, contact.endpoint);
        });
    if (entry.outstanding.empty() &&
        (entry.attempts >= options_.maximum_queries_per_hash ||
         no_more_contacts)) {
      ++step.exhausted;
      finish(entries_.begin() + static_cast<std::ptrdiff_t>(index), now);
      visits_without_query = 0;
      continue;
    }
    schedule_cursor_ = (index + 1) % entries_.size();
    ++visits_without_query;
  }
  step.pending = pending();
  step.in_flight = in_flight();
  return step;
}

bool PeerDiscoveryPlanner::consume(const DhtActions &actions,
                                   core::Timestamp now) {
  if (!actions.query_completion ||
      actions.query_completion->kind != krpc::QueryKind::GetPeers)
    return false;
  const auto entry = find(actions.query_completion->transaction,
                          actions.query_completion->remote);
  if (entry == entries_.end())
    return false;
  const auto query =
      std::ranges::find_if(entry->outstanding, [&](const auto &outstanding) {
        return outstanding.remote == actions.query_completion->remote &&
               std::ranges::equal(outstanding.transaction,
                                  actions.query_completion->transaction);
      });
  entry->outstanding.erase(query);
  for (const auto &contact : actions.query_completion->contacts)
    if ((!node_->address_family() ||
         contact.endpoint.address.family == *node_->address_family()) &&
        std::ranges::find(entry->frontier, contact.id, &NodeContact::id) ==
            entry->frontier.end())
      entry->frontier.push_back(contact);
  if (!actions.query_completion->peers.empty()) {
    for (const auto &peer : actions.query_completion->peers) {
      if (ready_.size() >= options_.maximum_pending)
        break;
      ready_.push_back(
          {.info_hash = entry->info_hash, .peer = peer, .observed_at = now});
    }
    finish(entry, now);
  }
  return true;
}

core::Result<void>
PeerDiscoveryPlanner::retry(PeerMetadataCandidate candidate) {
  if (ready_.size() >= options_.maximum_pending)
    return std::unexpected(
        core::Error{core::ErrorCode::QuotaExceeded,
                    "Active peer-discovery result queue limit would be "
                    "exceeded"});
  ready_.push_back(std::move(candidate));
  return {};
}

bool PeerDiscoveryPlanner::consume_timeout(const QueryTimeout &timeout,
                                           core::Timestamp now) {
  if (timeout.kind != krpc::QueryKind::GetPeers)
    return false;
  const auto entry = find(timeout.transaction, timeout.remote);
  if (entry == entries_.end())
    return false;
  const auto query =
      std::ranges::find_if(entry->outstanding, [&](const auto &outstanding) {
        return outstanding.remote == timeout.remote &&
               std::ranges::equal(outstanding.transaction, timeout.transaction);
      });
  entry->outstanding.erase(query);
  if (entry->outstanding.empty() &&
      entry->attempts >= options_.maximum_queries_per_hash) {
    ++exhausted_since_poll_;
    finish(entry, now);
  }
  return true;
}

bool PeerDiscoveryPlanner::delivery_failed(const DatagramSend &send,
                                           core::Timestamp now) {
  if (send.query_transaction.empty())
    return false;
  const auto entry = find(send.query_transaction, send.destination);
  if (entry == entries_.end())
    return false;
  node_->cancel_query(send.query_transaction, send.destination);
  const auto query =
      std::ranges::find_if(entry->outstanding, [&](const auto &outstanding) {
        return outstanding.remote == send.destination &&
               std::ranges::equal(outstanding.transaction,
                                  send.query_transaction);
      });
  entry->outstanding.erase(query);
  if (entry->outstanding.empty() &&
      entry->attempts >= options_.maximum_queries_per_hash) {
    ++exhausted_since_poll_;
    finish(entry, now);
  }
  return true;
}

std::size_t PeerDiscoveryPlanner::in_flight() const noexcept {
  return std::accumulate(entries_.begin(), entries_.end(), std::size_t{},
                         [](std::size_t count, const auto &entry) {
                           return count + entry.outstanding.size();
                         });
}

} // namespace sakuin::dht

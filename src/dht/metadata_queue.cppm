export module sakuin.dht.metadata_queue;

import std;

import sakuin.core.ids;
import sakuin.core.result;
import sakuin.core.time;
import sakuin.dht.metadata_candidate;
import sakuin.runtime.datagram;
import sakuin.runtime.stream;

export namespace sakuin::dht {

struct MetadataQueueOptions {
  std::size_t maximum_queued{4'096};
  std::size_t maximum_in_flight{64};
  std::size_t maximum_attempts_per_peer{3};
  core::Duration initial_retry_delay{std::chrono::seconds{30}};
  core::Duration maximum_retry_delay{std::chrono::minutes{30}};
};

struct MetadataFetchTicket {
  std::uint64_t id{};
  PeerMetadataCandidate candidate;
};

enum class MetadataFetchOutcome { Success, RetryableFailure, PermanentFailure };

// Pure scheduling state. Runtime/sender ownership remains outside this class,
// allowing the same tickets to be driven by callbacks today or stdexec later.
class MetadataCandidateQueue {
public:
  static core::Result<std::unique_ptr<MetadataCandidateQueue>>
  create(MetadataQueueOptions options = {});

  core::Result<bool> offer(PeerMetadataCandidate candidate);
  std::vector<MetadataFetchTicket> ready(core::Timestamp now);
  std::optional<core::Timestamp> next_ready_at() const noexcept;
  core::Result<void> complete(std::uint64_t ticket,
                              MetadataFetchOutcome outcome,
                              core::Timestamp now);

  std::size_t queued() const noexcept { return queued_.size(); }
  std::size_t in_flight() const noexcept { return in_flight_.size(); }

private:
  struct CandidateKey {
    core::InfoHash info_hash;
    runtime::StreamEndpoint peer;
    friend bool operator==(const CandidateKey &,
                           const CandidateKey &) = default;
  };
  struct InfoHashHash {
    std::size_t operator()(const core::InfoHash &hash) const noexcept;
  };
  struct CandidateKeyHash {
    std::size_t operator()(const CandidateKey &key) const noexcept;
  };
  struct Entry {
    PeerMetadataCandidate candidate;
    std::size_t attempts{};
    core::Timestamp ready_at{};
  };

  explicit MetadataCandidateQueue(MetadataQueueOptions options)
      : options_(options) {}

  core::Duration retry_delay(std::size_t attempts) const noexcept;
  void erase_hash(const core::InfoHash &hash);

  MetadataQueueOptions options_;
  std::deque<Entry> queued_;
  std::unordered_map<std::uint64_t, Entry> in_flight_;
  std::unordered_set<CandidateKey, CandidateKeyHash> known_;
  std::unordered_set<core::InfoHash, InfoHashHash> active_hashes_;
  std::uint64_t next_ticket_{};
};

} // namespace sakuin::dht

namespace sakuin::dht {

std::size_t MetadataCandidateQueue::InfoHashHash::operator()(
    const core::InfoHash &hash) const noexcept {
  std::size_t value = 1469598103934665603ULL;
  for (const auto byte : hash.bytes) {
    value ^= byte;
    value *= 1099511628211ULL;
  }
  return value;
}

std::size_t MetadataCandidateQueue::CandidateKeyHash::operator()(
    const CandidateKey &key) const noexcept {
  auto value = InfoHashHash{}(key.info_hash);
  value ^= static_cast<std::size_t>(key.peer.address.family);
  value *= 1099511628211ULL;
  const auto count = key.peer.address.family == runtime::AddressFamily::IPv4
                         ? 4U
                         : key.peer.address.bytes.size();
  for (std::size_t index = 0; index < count; ++index) {
    value ^= key.peer.address.bytes[index];
    value *= 1099511628211ULL;
  }
  value ^= key.peer.port;
  return value;
}

core::Result<std::unique_ptr<MetadataCandidateQueue>>
MetadataCandidateQueue::create(MetadataQueueOptions options) {
  if (options.maximum_queued == 0 || options.maximum_in_flight == 0 ||
      options.maximum_attempts_per_peer == 0 ||
      options.initial_retry_delay <= core::Duration::zero() ||
      options.maximum_retry_delay < options.initial_retry_delay)
    return std::unexpected(
        core::Error{core::ErrorCode::InvalidArgument,
                    "Invalid metadata candidate queue limits"});
  return std::unique_ptr<MetadataCandidateQueue>{
      new MetadataCandidateQueue{options}};
}

core::Result<bool>
MetadataCandidateQueue::offer(PeerMetadataCandidate candidate) {
  if (candidate.peer.port == 0)
    return std::unexpected(core::Error{core::ErrorCode::InvalidArgument,
                                       "Metadata peer port must be nonzero"});
  CandidateKey key{.info_hash = candidate.info_hash, .peer = candidate.peer};
  if (known_.contains(key))
    return false;
  if (queued_.size() >= options_.maximum_queued)
    return std::unexpected(
        core::Error{core::ErrorCode::QuotaExceeded,
                    "Metadata candidate queue limit would be exceeded"});
  known_.insert(std::move(key));
  queued_.push_back(Entry{.candidate = std::move(candidate),
                          .ready_at = core::Timestamp::min()});
  return true;
}

std::vector<MetadataFetchTicket>
MetadataCandidateQueue::ready(core::Timestamp now) {
  std::vector<MetadataFetchTicket> result;
  result.reserve(options_.maximum_in_flight -
                 std::min(in_flight_.size(), options_.maximum_in_flight));
  while (in_flight_.size() < options_.maximum_in_flight) {
    const auto found = std::ranges::find_if(queued_, [&](const Entry &entry) {
      return entry.ready_at <= now &&
             !active_hashes_.contains(entry.candidate.info_hash);
    });
    if (found == queued_.end())
      break;
    Entry entry = std::move(*found);
    queued_.erase(found);
    ++entry.attempts;
    do {
      ++next_ticket_;
    } while (next_ticket_ == 0 || in_flight_.contains(next_ticket_));
    active_hashes_.insert(entry.candidate.info_hash);
    result.push_back(
        MetadataFetchTicket{.id = next_ticket_, .candidate = entry.candidate});
    in_flight_.emplace(next_ticket_, std::move(entry));
  }
  return result;
}

std::optional<core::Timestamp>
MetadataCandidateQueue::next_ready_at() const noexcept {
  if (in_flight_.size() >= options_.maximum_in_flight)
    return std::nullopt;
  std::optional<core::Timestamp> result;
  for (const auto &entry : queued_) {
    if (active_hashes_.contains(entry.candidate.info_hash))
      continue;
    if (!result || entry.ready_at < *result)
      result = entry.ready_at;
  }
  return result;
}

core::Duration
MetadataCandidateQueue::retry_delay(std::size_t attempts) const noexcept {
  auto delay = options_.initial_retry_delay;
  for (std::size_t attempt = 1;
       attempt < attempts && delay < options_.maximum_retry_delay; ++attempt) {
    if (delay > options_.maximum_retry_delay - delay)
      return options_.maximum_retry_delay;
    delay = std::min(delay + delay, options_.maximum_retry_delay);
  }
  return delay;
}

void MetadataCandidateQueue::erase_hash(const core::InfoHash &hash) {
  for (auto current = queued_.begin(); current != queued_.end();) {
    if (current->candidate.info_hash == hash) {
      known_.erase(CandidateKey{.info_hash = current->candidate.info_hash,
                                .peer = current->candidate.peer});
      current = queued_.erase(current);
    } else {
      ++current;
    }
  }
}

core::Result<void> MetadataCandidateQueue::complete(
    std::uint64_t ticket, MetadataFetchOutcome outcome, core::Timestamp now) {
  const auto found = in_flight_.find(ticket);
  if (found == in_flight_.end())
    return std::unexpected(core::Error{core::ErrorCode::NotFound,
                                       "Metadata fetch ticket is not active"});
  Entry entry = std::move(found->second);
  in_flight_.erase(found);
  active_hashes_.erase(entry.candidate.info_hash);
  const CandidateKey key{.info_hash = entry.candidate.info_hash,
                         .peer = entry.candidate.peer};

  if (outcome == MetadataFetchOutcome::Success) {
    known_.erase(key);
    erase_hash(entry.candidate.info_hash);
    return {};
  }
  if (outcome == MetadataFetchOutcome::PermanentFailure ||
      entry.attempts >= options_.maximum_attempts_per_peer) {
    known_.erase(key);
    return {};
  }
  entry.ready_at = now + std::chrono::duration_cast<core::Timestamp::duration>(
                             retry_delay(entry.attempts));
  queued_.push_back(std::move(entry));
  return {};
}

} // namespace sakuin::dht

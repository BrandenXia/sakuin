import std;

import sakuin.core;
import sakuin.dht;
import sakuin.runtime;

namespace {

sakuin::core::InfoHash hash(std::uint8_t value) {
  sakuin::core::InfoHash result;
  result.bytes.fill(value);
  return result;
}

sakuin::dht::PeerMetadataCandidate
candidate(std::uint8_t hash_value, std::uint8_t host, std::uint16_t port) {
  auto address = sakuin::runtime::IpAddress::loopback_v4();
  address.bytes[3] = host;
  return {.info_hash = hash(hash_value),
          .peer = {.address = address, .port = port},
          .observed_at = {}};
}

} // namespace

int main() {
  using namespace sakuin;
  auto queue = dht::MetadataCandidateQueue::create(
      {.maximum_queued = 3,
       .maximum_in_flight = 1,
       .maximum_attempts_per_peer = 2,
       .initial_retry_delay = std::chrono::seconds{10},
       .maximum_retry_delay = std::chrono::seconds{20}});
  if (!queue)
    return 1;
  const auto first = candidate(1, 2, 6001);
  const auto alternate = candidate(1, 3, 6002);
  if (!(*queue)->offer(first).value_or(false) ||
      (*queue)->offer(first).value_or(true) ||
      !(*queue)->offer(alternate).value_or(false) || (*queue)->queued() != 2)
    return 2;

  const auto now = core::Timestamp{std::chrono::seconds{100}};
  auto work = (*queue)->ready(now);
  if (work.size() != 1 || work[0].candidate.peer != first.peer ||
      (*queue)->in_flight() != 1 || (*queue)->next_ready_at())
    return 3;
  if (!(*queue)->complete(work[0].id,
                          dht::MetadataFetchOutcome::RetryableFailure, now))
    return 4;

  work = (*queue)->ready(now);
  if (work.size() != 1 || work[0].candidate.peer != alternate.peer ||
      (*queue)->next_ready_at())
    return 5;
  if (!(*queue)->complete(work[0].id,
                          dht::MetadataFetchOutcome::PermanentFailure, now) ||
      !(*queue)->ready(now).empty() ||
      (*queue)->next_ready_at() != now + std::chrono::seconds{10})
    return 6;
  work = (*queue)->ready(now + std::chrono::seconds{10});
  if (work.size() != 1 || work[0].candidate.peer != first.peer)
    return 7;
  if (!(*queue)->complete(work[0].id, dht::MetadataFetchOutcome::Success,
                          now + std::chrono::seconds{10}) ||
      (*queue)->queued() != 0 || (*queue)->in_flight() != 0)
    return 8;

  auto bounded = dht::MetadataCandidateQueue::create(
      {.maximum_queued = 1, .maximum_in_flight = 1});
  if (!bounded || !(*bounded)->offer(candidate(2, 4, 6003)))
    return 9;
  auto overflow = (*bounded)->offer(candidate(3, 5, 6004));
  if (overflow || overflow.error().code != core::ErrorCode::QuotaExceeded)
    return 10;
  auto bounded_work = (*bounded)->ready(now);
  if (bounded_work.size() != 1 || (*bounded)->ready(now).size() != 0 ||
      (*bounded)->complete(999, dht::MetadataFetchOutcome::Success, now) ||
      (*bounded)
              ->complete(bounded_work[0].id, dht::MetadataFetchOutcome::Success,
                         now)
              .has_value() == false)
    return 11;

  return 0;
}

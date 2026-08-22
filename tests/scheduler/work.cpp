import std;

import sakuin.core;
import sakuin.scheduler;

namespace {

sakuin::core::Timestamp seconds(std::int64_t value) {
  return sakuin::core::Timestamp{
      std::chrono::duration_cast<sakuin::core::Timestamp::duration>(
          std::chrono::seconds{value})};
}

sakuin::core::ByteBuffer bytes(std::string_view value) {
  const auto view = std::as_bytes(std::span{value});
  return {view.begin(), view.end()};
}

sakuin::scheduler::WorkItem item(std::string_view value, std::int32_t priority,
                                 std::int64_t ready = 0,
                                 std::uint32_t attempts = 3) {
  auto payload = bytes(value);
  return {.id = sakuin::scheduler::content_work_id(payload),
          .work_class = sakuin::scheduler::WorkClass::MetadataFetch,
          .payload = std::move(payload),
          .priority = priority,
          .not_before = seconds(ready),
          .maximum_attempts = attempts};
}

} // namespace

int main() {
  using namespace sakuin;
  auto coordinator = scheduler::LocalWorkCoordinator::create(
      {.maximum_work_items = 16,
       .maximum_payload_bytes = 128,
       .worker_timeout = std::chrono::seconds{10},
       .lease_duration = std::chrono::seconds{5}});
  if (!coordinator)
    return 1;

  const auto low = item("low", 1);
  const auto high = item("high", 10);
  const auto delayed = item("delayed", 20, 10);
  if (!(*coordinator)->submit(low).value_or(false) ||
      !(*coordinator)->submit(high).value_or(false) ||
      !(*coordinator)->submit(delayed).value_or(false) ||
      (*coordinator)->submit(low).value_or(true))
    return 2;
  auto conflicting = low;
  conflicting.priority = 99;
  auto conflict = (*coordinator)->submit(conflicting);
  if (conflict || conflict.error().code != core::ErrorCode::Conflict)
    return 3;

  const scheduler::WorkerDescriptor worker{
      .id = "metadata-1",
      .capabilities = {scheduler::WorkClass::MetadataFetch}};
  if (!(*coordinator)->register_worker(worker, seconds(0)).value_or(false) ||
      (*coordinator)->register_worker(worker, seconds(0)).value_or(true))
    return 4;
  auto leased = (*coordinator)->lease(worker.id, 2, seconds(0));
  if (!leased || leased->size() != 2 || leased->front().item.id != high.id ||
      leased->back().item.id != low.id || leased->front().attempt != 1)
    return 5;

  if (!(*coordinator)->heartbeat(worker.id, seconds(4)) ||
      !(*coordinator)->renew(worker.id, leased->front().id, seconds(4)) ||
      !(*coordinator)
           ->fail(worker.id, leased->back().id, true, seconds(4), seconds(7)) ||
      !(*coordinator)->complete(worker.id, leased->front().id, seconds(5)))
    return 6;
  auto early = (*coordinator)->lease(worker.id, 2, seconds(6));
  if (!early || !early->empty())
    return 7;
  auto retry = (*coordinator)->lease(worker.id, 1, seconds(7));
  if (!retry || retry->size() != 1 || retry->front().item.id != low.id ||
      retry->front().attempt != 2)
    return 8;
  if (!(*coordinator)->complete(worker.id, retry->front().id, seconds(8)))
    return 9;
  auto delayed_lease = (*coordinator)->lease(worker.id, 1, seconds(10));
  if (!delayed_lease || delayed_lease->size() != 1 ||
      delayed_lease->front().item.id != delayed.id)
    return 10;
  if (!(*coordinator)->unregister_worker(worker.id, seconds(10)))
    return 11;
  const auto released = (*coordinator)->snapshot(seconds(10));
  if (!released || released->pending != 1 || released->leased != 0 ||
      released->succeeded != 2 || released->workers != 0)
    return 12;

  if (!(*coordinator)
           ->register_worker(
               {.id = "metadata-2",
                .capabilities = {scheduler::WorkClass::MetadataFetch}},
               seconds(10)))
    return 13;
  auto final_attempt = (*coordinator)->lease("metadata-2", 1, seconds(10));
  if (!final_attempt || final_attempt->size() != 1 ||
      final_attempt->front().attempt != 2)
    return 14;
  if (!(*coordinator)
           ->fail("metadata-2", final_attempt->front().id, false, seconds(11),
                  seconds(11)))
    return 15;
  const auto terminal = (*coordinator)->snapshot(seconds(11));
  if (!terminal || terminal->failed != 1 || terminal->succeeded != 2)
    return 16;

  auto concurrent = scheduler::LocalWorkCoordinator::create(
      {.maximum_work_items = 4,
       .maximum_payload_bytes = 128,
       .worker_timeout = std::chrono::minutes{1},
       .lease_duration = std::chrono::seconds{10}});
  if (!concurrent || !(*concurrent)->submit(item("only", 0)))
    return 17;
  for (int index = 0; index < 8; ++index)
    if (!(*concurrent)
             ->register_worker(
                 {.id = "worker-" + std::to_string(index),
                  .capabilities = {scheduler::WorkClass::MetadataFetch}},
                 seconds(0)))
      return 18;
  std::atomic<unsigned> accepted{};
  std::vector<std::jthread> threads;
  for (int index = 0; index < 8; ++index)
    threads.emplace_back([&, index] {
      auto result =
          (*concurrent)
              ->lease("worker-" + std::to_string(index), 1, seconds(0));
      if (result)
        accepted += result->size();
    });
  threads.clear();
  auto concurrent_snapshot = (*concurrent)->snapshot(seconds(0));
  if (accepted != 1 || !concurrent_snapshot || concurrent_snapshot->leased != 1)
    return 19;
  return 0;
}

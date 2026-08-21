import std;

import sakuin.api;
import sakuin.core;

int main() {
  using namespace sakuin;
  auto governor = api::FixedWindowRequestGovernor::create(
      {.maximum_requests = 2, .period = std::chrono::seconds{10}});
  if (!governor ||
      api::FixedWindowRequestGovernor::create(
          {.maximum_requests = 0, .period = std::chrono::seconds{10}}))
    return 1;

  const auto start = core::Timestamp{core::Timestamp::duration{0}};
  auto first = (*governor)->admit("reader", start);
  auto second = (*governor)->admit("reader", start);
  auto denied = (*governor)->admit("reader", start);
  if (!first || !second || !denied || !first->allowed || !second->allowed ||
      denied->allowed || first->remaining != 1 || second->remaining != 0 ||
      denied->retry_after != std::chrono::seconds{10})
    return 2;

  auto independent = (*governor)->admit("admin", start);
  auto next_window =
      (*governor)->admit("reader", start + std::chrono::seconds{10});
  if (!independent || !independent->allowed || !next_window ||
      !next_window->allowed || next_window->remaining != 1)
    return 3;

  const auto before_epoch = core::Timestamp{core::Timestamp::duration{-1}};
  auto prior = (*governor)->admit("prior", before_epoch);
  if (!prior || !prior->allowed)
    return 4;
  return 0;
}

import std;

import sakuin.core;
import sakuin.dht;
import sakuin.runtime;

namespace {

sakuin::dht::krpc::NodeId node(std::uint8_t last) {
  sakuin::dht::krpc::NodeId result;
  result.bytes.front() = 0x80;
  result.bytes.back() = last;
  return result;
}

sakuin::dht::NodeContact contact(std::uint8_t last, std::uint16_t port,
                                 std::int64_t seen) {
  auto address = sakuin::runtime::IpAddress::loopback_v4();
  address.bytes[3] = static_cast<std::uint8_t>(last + 1);
  return {.id = node(last),
          .endpoint = {.address = address, .port = port},
          .last_seen = sakuin::core::Timestamp{std::chrono::seconds{seen}}};
}

sakuin::core::Timestamp seconds(std::int64_t value) {
  return sakuin::core::Timestamp{std::chrono::seconds{value}};
}

} // namespace

int main() {
  using namespace sakuin;

  dht::AnnounceTokenSecret secret;
  auto tokens = dht::RotatingAnnounceTokenProvider::create(secret);
  dht::krpc::NodeId local{};
  dht::DhtNode node_engine{local,
                           *tokens,
                           {.query_timeout = std::chrono::seconds{2},
                            .address_family = runtime::AddressFamily::IPv4}};
  for (std::uint8_t index = 0; index < dht::k_bucket_size; ++index) {
    if (node_engine.routing_table()
            .observe(
                contact(index, static_cast<std::uint16_t>(6'000 + index), 1))
            .mutation != dht::RoutingMutation::Inserted)
      return 1;
  }
  auto update = node_engine.routing_table().observe(contact(20, 7'000, 2));
  if (!update.probe)
    return 2;

  auto planner = dht::RoutingMaintenancePlanner::create(
      node_engine, {.maximum_queued = 2,
                    .maximum_in_flight = 1,
                    .maximum_attempts = 2,
                    .retry_delay = std::chrono::seconds{1}});
  if (!planner || !(*planner)->offer(*update.probe).value_or(false) ||
      (*planner)->offer(*update.probe).value_or(true))
    return 3;

  auto first = (*planner)->poll(seconds(10));
  if (!first || first->sends.size() != 1 || first->queued != 0 ||
      first->in_flight != 1)
    return 4;
  auto query = dht::krpc::decode(first->sends.front().payload);
  const auto *ping = query ? std::get_if<dht::krpc::Query>(&*query) : nullptr;
  if (!ping || ping->kind != dht::krpc::QueryKind::Ping)
    return 5;
  auto response = dht::krpc::encode(dht::krpc::Response{
      .transaction = ping->transaction, .sender = update.probe->incumbent.id});
  auto alive = node_engine.handle(
      {.source = update.probe->incumbent.endpoint, .payload = *response},
      seconds(11));
  if (!alive || !(*planner)->consume(*alive) || (*planner)->queued() != 0 ||
      (*planner)->in_flight() != 0 ||
      !node_engine.routing_table().remove(update.probe->incumbent.id))
    return 6;
  // Restore the incumbent so the timeout path can decide the replacement.
  node_engine.routing_table().observe(update.probe->incumbent);
  if (!(*planner)->offer(*update.probe).value_or(false))
    return 7;

  auto timeout_attempt = (*planner)->poll(seconds(20));
  if (!timeout_attempt || timeout_attempt->sends.size() != 1)
    return 8;
  auto timeouts = node_engine.expire_queries(seconds(22));
  if (timeouts.size() != 1 ||
      !(*planner)->consume_timeout(timeouts.front(), seconds(22)))
    return 9;
  auto waiting = (*planner)->poll(seconds(22));
  if (!waiting || !waiting->sends.empty() ||
      waiting->next_wakeup != seconds(23))
    return 10;
  auto final_attempt = (*planner)->poll(seconds(23));
  if (!final_attempt || final_attempt->sends.size() != 1)
    return 11;
  timeouts = node_engine.expire_queries(seconds(25));
  if (timeouts.size() != 1 ||
      !(*planner)->consume_timeout(timeouts.front(), seconds(25)))
    return 12;
  const auto contacts = node_engine.routing_table().all_contacts();
  if (std::ranges::find(contacts, update.probe->incumbent.id,
                        &dht::NodeContact::id) != contacts.end() ||
      std::ranges::find(contacts, update.probe->replacement.id,
                        &dht::NodeContact::id) == contacts.end())
    return 13;

  auto second_update =
      node_engine.routing_table().observe(contact(21, 7'001, 3));
  if (!second_update.probe ||
      !(*planner)->offer(*second_update.probe).value_or(false))
    return 14;
  auto delivery = (*planner)->poll(seconds(30));
  if (!delivery || delivery->sends.size() != 1 ||
      !(*planner)->delivery_failed(delivery->sends.front(), seconds(30)) ||
      node_engine.outstanding_queries() != 0)
    return 15;
  auto retry = (*planner)->poll(seconds(31));
  if (!retry || retry->sends.size() != 1 ||
      !(*planner)->delivery_failed(retry->sends.front(), seconds(31)))
    return 16;
  const auto after_delivery_failure =
      node_engine.routing_table().all_contacts();
  if (std::ranges::find(after_delivery_failure,
                        second_update.probe->incumbent.id,
                        &dht::NodeContact::id) == after_delivery_failure.end())
    return 17;

  auto refreshed_update =
      node_engine.routing_table().observe(contact(22, 7'002, 4));
  if (!refreshed_update.probe ||
      !(*planner)->offer(*refreshed_update.probe).value_or(false))
    return 18;
  auto stale_probe = (*planner)->poll(seconds(40));
  if (!stale_probe || stale_probe->sends.size() != 1)
    return 19;
  auto refreshed_incumbent = refreshed_update.probe->incumbent;
  refreshed_incumbent.last_seen = seconds(41);
  if (node_engine.routing_table().observe(refreshed_incumbent).mutation !=
      dht::RoutingMutation::Refreshed)
    return 20;
  timeouts = node_engine.expire_queries(seconds(42));
  if (timeouts.size() != 1 ||
      !(*planner)->consume_timeout(timeouts.front(), seconds(42)) ||
      (*planner)->queued() != 0 || (*planner)->in_flight() != 0)
    return 21;
  const auto after_refresh = node_engine.routing_table().all_contacts();
  if (std::ranges::find(after_refresh, refreshed_update.probe->incumbent.id,
                        &dht::NodeContact::id) == after_refresh.end() ||
      std::ranges::find(after_refresh, refreshed_update.probe->replacement.id,
                        &dht::NodeContact::id) != after_refresh.end())
    return 22;
  return 0;
}

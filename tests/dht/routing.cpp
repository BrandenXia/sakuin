import std;

import sakuin.core;
import sakuin.dht;
import sakuin.runtime;

namespace {

sakuin::dht::krpc::NodeId node(std::uint8_t first, std::uint8_t last = 0) {
  sakuin::dht::krpc::NodeId result;
  result.bytes.front() = first;
  result.bytes.back() = last;
  return result;
}

sakuin::dht::NodeContact contact(std::uint8_t first, std::uint8_t last,
                                 std::uint16_t port) {
  return {.id = node(first, last),
          .endpoint = {.address = sakuin::runtime::IpAddress::loopback_v4(),
                       .port = port},
          .last_seen =
              sakuin::core::Timestamp{sakuin::core::Timestamp::duration{last}}};
}

} // namespace

int main() {
  using namespace sakuin;

  const auto local = node(0);
  if (dht::bucket_index(local, local) ||
      dht::bucket_index(local, node(0, 1)) != 0 ||
      dht::bucket_index(local, node(0x80)) != 159)
    return 1;
  if (!dht::closer_to(node(0, 1), node(0, 2), local) ||
      dht::closer_to(node(0, 2), node(0, 1), local))
    return 2;

  dht::RoutingTable table{local};
  for (std::uint8_t index = 0; index < dht::k_bucket_size; ++index) {
    const auto update = table.observe(contact(0x80, index, 6'000 + index));
    if (update.mutation != dht::RoutingMutation::Inserted)
      return 3;
  }
  if (table.size() != dht::k_bucket_size)
    return 4;

  auto overflow = table.observe(contact(0x80, 20, 7'000));
  if (overflow.mutation != dht::RoutingMutation::ProbeRequired ||
      !overflow.probe || overflow.probe->incumbent.id != node(0x80, 0) ||
      overflow.probe->replacement.id != node(0x80, 20) ||
      table.size() != dht::k_bucket_size)
    return 5;

  auto refreshed = table.observe(contact(0x80, 0, 8'000));
  overflow = table.observe(contact(0x80, 21, 7'001));
  if (refreshed.mutation != dht::RoutingMutation::Refreshed ||
      !overflow.probe || overflow.probe->incumbent.id != node(0x80, 1) ||
      overflow.probe->replacement.id != node(0x80, 21))
    return 6;
  if (!table.replace_unresponsive(node(0x80, 1), contact(0x80, 21, 7'001)) ||
      table.replace_unresponsive(node(0x80, 2), contact(0x40, 1, 9'000)))
    return 7;

  const auto nearest = table.closest(node(0x80, 21), 2);
  if (nearest.size() != 2 || nearest.front().id != node(0x80, 21))
    return 8;

  const auto now = core::Timestamp{core::Timestamp::duration{123}};
  const std::array ipv4_contacts{contact(0x80, 30, 6881),
                                 contact(0x80, 31, 51413)};
  auto ipv4 =
      dht::encode_compact_nodes(ipv4_contacts, runtime::AddressFamily::IPv4);
  auto decoded_ipv4 =
      ipv4 ? dht::decode_compact_nodes(*ipv4, runtime::AddressFamily::IPv4, now)
           : core::Result<std::vector<dht::NodeContact>>{
                 std::unexpected(ipv4.error())};
  if (!decoded_ipv4 || decoded_ipv4->size() != 2 ||
      (*decoded_ipv4)[0].id != ipv4_contacts[0].id ||
      (*decoded_ipv4)[1].endpoint.port != 51413 ||
      (*decoded_ipv4)[0].last_seen != now)
    return 9;

  auto ipv6_contact = contact(0x40, 1, 443);
  ipv6_contact.endpoint.address.family = runtime::AddressFamily::IPv6;
  std::ranges::iota(ipv6_contact.endpoint.address.bytes, std::uint8_t{1});
  const std::array ipv6_contacts{ipv6_contact};
  auto ipv6 =
      dht::encode_compact_nodes(ipv6_contacts, runtime::AddressFamily::IPv6);
  auto decoded_ipv6 =
      ipv6 ? dht::decode_compact_nodes(*ipv6, runtime::AddressFamily::IPv6, now)
           : core::Result<std::vector<dht::NodeContact>>{
                 std::unexpected(ipv6.error())};
  if (!decoded_ipv6 || decoded_ipv6->front().id != ipv6_contact.id ||
      decoded_ipv6->front().endpoint != ipv6_contact.endpoint ||
      decoded_ipv6->front().last_seen != now)
    return 10;

  if (dht::decode_compact_nodes(core::ByteView{ipv4->data(), ipv4->size() - 1},
                                runtime::AddressFamily::IPv4, now))
    return 11;
  auto zero_port = ipv4_contacts.front();
  zero_port.endpoint.port = 0;
  const std::array invalid_contacts{zero_port};
  if (dht::encode_compact_nodes(invalid_contacts, runtime::AddressFamily::IPv4))
    return 12;
  if (!table.remove(node(0x80, 21)) || table.size() != 7)
    return 13;
  return 0;
}

import std;

import sakuin.core;
import sakuin.dht;
import sakuin.runtime;

namespace {

sakuin::runtime::IpAddress ipv4(std::array<std::uint8_t, 4> bytes) {
  sakuin::runtime::IpAddress result;
  std::ranges::copy(bytes, result.bytes.begin());
  return result;
}

sakuin::dht::krpc::NodeId node(std::string_view hex) {
  sakuin::dht::krpc::NodeId result;
  const auto digit = [](char value) -> std::uint8_t {
    return static_cast<std::uint8_t>(
        value >= 'a' ? value - 'a' + 10 : value - '0');
  };
  for (std::size_t index = 0; index < result.bytes.size(); ++index)
    result.bytes[index] = static_cast<std::uint8_t>(
        (digit(hex[index * 2]) << 4) | digit(hex[index * 2 + 1]));
  return result;
}

} // namespace

int main() {
  using namespace sakuin;
  struct Vector {
    std::array<std::uint8_t, 4> address;
    std::uint8_t random_byte;
    std::string_view id;
  };
  constexpr std::array vectors{
      Vector{{124, 31, 75, 21}, 1,
             "5fbfbff10c5d6a4ec8a88e4c6ab4c28b95eee401"},
      Vector{{21, 75, 31, 124}, 86,
             "5a3ce9c14e7a08645677bbd1cfe7d8f956d53256"},
      Vector{{65, 23, 51, 170}, 22,
             "a5d43220bc8f112a3d426c84764f8c2a1150e616"},
      Vector{{84, 124, 73, 14}, 65,
             "1b0321dd1bb1fe518101ceef99462b947a01ff41"},
      Vector{{43, 213, 53, 83}, 90,
             "e56f6cbf5b7c4be0237986d5243b87aa6d51305a"},
  };

  for (const auto &vector : vectors) {
    const auto expected = node(vector.id);
    if (dht::bep42_node_id(ipv4(vector.address), vector.random_byte,
                           expected) != expected ||
        !dht::bep42_compliant(expected, ipv4(vector.address)))
      return 1;
  }

  auto invalid = node(vectors.front().id);
  invalid.bytes.front() ^= 1;
  if (dht::bep42_compliant(invalid, ipv4(vectors.front().address)))
    return 2;

  if (!dht::bep42_exempt(ipv4({10, 1, 2, 3})) ||
      !dht::bep42_exempt(ipv4({172, 31, 2, 3})) ||
      dht::bep42_exempt(ipv4({172, 32, 2, 3})) ||
      !dht::bep42_exempt(ipv4({192, 168, 2, 3})) ||
      !dht::bep42_exempt(ipv4({169, 254, 2, 3})) ||
      !dht::bep42_exempt(ipv4({127, 0, 0, 1})))
    return 3;

  runtime::IpAddress ipv6;
  ipv6.family = runtime::AddressFamily::IPv6;
  std::ranges::iota(ipv6.bytes, std::uint8_t{1});
  const auto generated = dht::bep42_node_id(ipv6, 0xa5, {});
  if (!dht::bep42_compliant(generated, ipv6) || dht::bep42_exempt(ipv6))
    return 4;

  auto policy = dht::Bep42IdentityPolicy::create(
      runtime::AddressFamily::IPv4, std::nullopt,
      {.observation_quorum = 3, .vote_window = std::chrono::seconds{30}});
  if (!policy)
    return 5;
  const auto reported = ipv4({203, 0, 113, 8});
  const auto now = core::Timestamp{std::chrono::seconds{100}};
  const auto report = [&](std::uint8_t reporter, core::Timestamp at) {
    return (*policy)->observe(
        {.reporter = {.address = ipv4({198, 51, 100, reporter}), .port = 6881},
         .observed = {.address = reported, .port = 49'999}},
        at);
  };
  if (report(1, now) || report(1, now) || report(2, now))
    return 6;
  const auto proposal = report(3, now);
  if (!proposal || proposal->external_address != reported ||
      report(4, now) || !(*policy)->proposed_external())
    return 7;
  if (!(*policy)->commit(reported) || (*policy)->current_external() != reported ||
      (*policy)->proposed_external())
    return 8;

  const auto changed = ipv4({203, 0, 113, 9});
  const auto later = now + std::chrono::seconds{31};
  const auto changed_report = [&](std::uint8_t reporter) {
    return (*policy)->observe(
        {.reporter = {.address = ipv4({192, 0, 2, reporter}), .port = 6881},
         .observed = {.address = changed, .port = 50'000}},
        later);
  };
  if (changed_report(1) || changed_report(2) || !changed_report(3))
    return 9;
  return 0;
}

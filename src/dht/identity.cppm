export module sakuin.dht.identity;

import std;

import sakuin.core.result;
import sakuin.core.time;
import sakuin.dht.krpc;
import sakuin.runtime.datagram;

export namespace sakuin::dht {

// Constructs the BEP 42 constrained node ID for an externally observed IP.
// entropy supplies the unconstrained bytes; random_byte is retained verbatim
// in the final byte and its low three bits select the IP-derived prefix.
krpc::NodeId bep42_node_id(runtime::IpAddress address,
                           std::uint8_t random_byte,
                           krpc::NodeId entropy) noexcept;

// Checks only the BEP 42 bit constraint. Call bep42_exempt() separately when
// applying the specification's local-address enforcement exemption.
bool bep42_compliant(const krpc::NodeId &id,
                     runtime::IpAddress address) noexcept;

bool bep42_exempt(runtime::IpAddress address) noexcept;

struct ObservedAddressReport {
  runtime::DatagramEndpoint reporter;
  runtime::DatagramEndpoint observed;
};

struct Bep42IdentityPolicyOptions {
  std::size_t observation_quorum{3};
  core::Duration vote_window{std::chrono::minutes{10}};
};

struct IdentityReconfiguration {
  runtime::IpAddress external_address;
};

// One instance governs one address family and therefore one DHT node/routing
// table. Reports are counted once per reporter IP and expire from a bounded
// window. Reconfiguration is proposed but not committed until the owner has
// successfully replaced the runtime node.
class Bep42IdentityPolicy {
public:
  static core::Result<std::unique_ptr<Bep42IdentityPolicy>>
  create(runtime::AddressFamily family,
         std::optional<runtime::IpAddress> current_external = std::nullopt,
         Bep42IdentityPolicyOptions options = {});

  std::optional<IdentityReconfiguration>
  observe(ObservedAddressReport report, core::Timestamp observed_at);
  core::Result<void> commit(runtime::IpAddress external_address);
  void discard_proposal() noexcept { proposed_external_.reset(); }

  runtime::AddressFamily family() const noexcept { return family_; }
  std::optional<runtime::IpAddress> current_external() const noexcept {
    return current_external_;
  }
  std::optional<runtime::IpAddress> proposed_external() const noexcept {
    return proposed_external_;
  }

private:
  struct Vote {
    runtime::IpAddress reporter;
    runtime::IpAddress observed;
    core::Timestamp observed_at;
  };

  Bep42IdentityPolicy(runtime::AddressFamily family,
                      std::optional<runtime::IpAddress> current_external,
                      Bep42IdentityPolicyOptions options)
      : family_(family), current_external_(current_external),
        options_(options) {}

  runtime::AddressFamily family_;
  std::optional<runtime::IpAddress> current_external_;
  std::optional<runtime::IpAddress> proposed_external_;
  Bep42IdentityPolicyOptions options_;
  std::vector<Vote> votes_;
};

} // namespace sakuin::dht

namespace sakuin::dht {
namespace {

std::uint32_t crc32c(std::span<const std::uint8_t> input) noexcept {
  std::uint32_t checksum = 0xffff'ffffU;
  for (const auto byte : input) {
    checksum ^= byte;
    for (int bit = 0; bit < 8; ++bit)
      checksum = (checksum >> 1) ^
                 ((checksum & 1U) != 0 ? 0x82f6'3b78U : 0U);
  }
  return checksum ^ 0xffff'ffffU;
}

std::uint32_t bep42_checksum(runtime::IpAddress address,
                             std::uint8_t random_byte) noexcept {
  constexpr std::array<std::uint8_t, 4> ipv4_mask{0x03, 0x0f, 0x3f,
                                                  0xff};
  constexpr std::array<std::uint8_t, 8> ipv6_mask{
      0x01, 0x03, 0x07, 0x0f, 0x1f, 0x3f, 0x7f, 0xff};
  std::array<std::uint8_t, 8> masked{};
  const auto count = address.family == runtime::AddressFamily::IPv4
                         ? ipv4_mask.size()
                         : ipv6_mask.size();
  for (std::size_t index = 0; index < count; ++index) {
    const auto mask = address.family == runtime::AddressFamily::IPv4
                          ? ipv4_mask[index]
                          : ipv6_mask[index];
    masked[index] = static_cast<std::uint8_t>(address.bytes[index] & mask);
  }
  masked[0] = static_cast<std::uint8_t>(masked[0] |
                                        ((random_byte & 0x07U) << 5));
  return crc32c(std::span{masked}.first(count));
}

} // namespace

krpc::NodeId bep42_node_id(runtime::IpAddress address,
                           std::uint8_t random_byte,
                           krpc::NodeId entropy) noexcept {
  const auto checksum = bep42_checksum(address, random_byte);
  entropy.bytes[0] = static_cast<std::uint8_t>(checksum >> 24);
  entropy.bytes[1] = static_cast<std::uint8_t>(checksum >> 16);
  entropy.bytes[2] = static_cast<std::uint8_t>(
      ((checksum >> 8) & 0xf8U) | (entropy.bytes[2] & 0x07U));
  entropy.bytes[19] = random_byte;
  return entropy;
}

bool bep42_compliant(const krpc::NodeId &id,
                     runtime::IpAddress address) noexcept {
  const auto checksum = bep42_checksum(address, id.bytes[19]);
  return id.bytes[0] == static_cast<std::uint8_t>(checksum >> 24) &&
         id.bytes[1] == static_cast<std::uint8_t>(checksum >> 16) &&
         (id.bytes[2] & 0xf8U) == ((checksum >> 8) & 0xf8U);
}

bool bep42_exempt(runtime::IpAddress address) noexcept {
  if (address.family != runtime::AddressFamily::IPv4)
    return false;
  const auto first = address.bytes[0];
  const auto second = address.bytes[1];
  return first == 10 || first == 127 ||
         (first == 172 && (second & 0xf0U) == 16) ||
         (first == 192 && second == 168) ||
         (first == 169 && second == 254);
}

core::Result<std::unique_ptr<Bep42IdentityPolicy>>
Bep42IdentityPolicy::create(
    runtime::AddressFamily family,
    std::optional<runtime::IpAddress> current_external,
    Bep42IdentityPolicyOptions options) {
  if (options.observation_quorum < 2 ||
      options.vote_window <= core::Duration::zero())
    return std::unexpected(core::Error{
        core::ErrorCode::InvalidArgument,
        "BEP 42 identity policy requires a quorum of at least two and a "
        "positive vote window"});
  if (current_external && current_external->family != family)
    return std::unexpected(core::Error{
        core::ErrorCode::InvalidArgument,
        "BEP 42 current external address has the wrong family"});
  return std::unique_ptr<Bep42IdentityPolicy>{
      new Bep42IdentityPolicy{family, current_external, options}};
}

std::optional<IdentityReconfiguration>
Bep42IdentityPolicy::observe(ObservedAddressReport report,
                             core::Timestamp observed_at) {
  if (report.reporter.address.family != family_ ||
      report.observed.address.family != family_ || report.reporter.port == 0 ||
      report.observed.port == 0)
    return std::nullopt;

  const auto oldest =
      observed_at - std::chrono::duration_cast<core::Timestamp::duration>(
                        options_.vote_window);
  std::erase_if(votes_, [&](const auto &vote) {
    return vote.observed_at < oldest;
  });
  std::erase_if(votes_, [&](const auto &vote) {
    return vote.reporter == report.reporter.address;
  });
  votes_.push_back(Vote{.reporter = report.reporter.address,
                        .observed = report.observed.address,
                        .observed_at = observed_at});

  const auto votes_for_address =
      std::ranges::count(votes_, report.observed.address, &Vote::observed);
  if (votes_for_address < options_.observation_quorum ||
      current_external_ == report.observed.address ||
      proposed_external_ == report.observed.address)
    return std::nullopt;
  proposed_external_ = report.observed.address;
  return IdentityReconfiguration{.external_address = report.observed.address};
}

core::Result<void>
Bep42IdentityPolicy::commit(runtime::IpAddress external_address) {
  if (external_address.family != family_ ||
      proposed_external_ != external_address)
    return std::unexpected(core::Error{
        core::ErrorCode::InvalidArgument,
        "BEP 42 identity reconfiguration does not match the pending proposal"});
  current_external_ = external_address;
  proposed_external_.reset();
  votes_.clear();
  return {};
}

} // namespace sakuin::dht

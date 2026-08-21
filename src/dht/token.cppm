export module sakuin.dht.token;

import std;

import sakuin.core.bytes;
import sakuin.core.hash;
import sakuin.core.result;
import sakuin.core.time;
import sakuin.runtime.datagram;

export namespace sakuin::dht {

class AnnounceTokenProvider {
public:
  virtual ~AnnounceTokenProvider() = default;

  virtual core::Result<core::ByteBuffer>
  issue(const runtime::IpAddress &address, core::Timestamp now) const = 0;
  virtual core::Result<bool> validate(core::ByteView token,
                                      const runtime::IpAddress &address,
                                      core::Timestamp now) const = 0;
};

struct AnnounceTokenSecret {
  std::array<std::uint8_t, 32> bytes{};
};

class RotatingAnnounceTokenProvider final : public AnnounceTokenProvider {
public:
  static core::Result<RotatingAnnounceTokenProvider>
  create(AnnounceTokenSecret secret,
         core::Duration period = std::chrono::minutes{5});

  core::Result<core::ByteBuffer>
  issue(const runtime::IpAddress &address,
        core::Timestamp now) const override;
  core::Result<bool> validate(core::ByteView token,
                              const runtime::IpAddress &address,
                              core::Timestamp now) const override;

private:
  RotatingAnnounceTokenProvider(AnnounceTokenSecret secret,
                                core::Duration period)
      : secret_(secret), period_(period) {}

  std::int64_t slot(core::Timestamp now) const noexcept;
  core::Result<core::ByteBuffer>
  token_for(const runtime::IpAddress &address, std::int64_t slot) const;

  AnnounceTokenSecret secret_;
  core::Duration period_;
};

} // namespace sakuin::dht

namespace sakuin::dht {
core::Result<RotatingAnnounceTokenProvider>
RotatingAnnounceTokenProvider::create(AnnounceTokenSecret secret,
                                      core::Duration period) {
  if (period <= core::Duration::zero())
    return std::unexpected(core::Error{core::ErrorCode::InvalidArgument,
                                       "Announce token period must be positive"});
  return RotatingAnnounceTokenProvider{secret, period};
}

std::int64_t
RotatingAnnounceTokenProvider::slot(core::Timestamp now) const noexcept {
  const auto elapsed = std::chrono::duration_cast<core::Duration>(
      now.time_since_epoch());
  auto quotient = elapsed.count() / period_.count();
  if (elapsed.count() < 0 && elapsed.count() % period_.count() != 0)
    --quotient;
  return quotient;
}

core::Result<core::ByteBuffer> RotatingAnnounceTokenProvider::token_for(
    const runtime::IpAddress &address, std::int64_t time_slot) const {
  const std::size_t address_size =
      address.family == runtime::AddressFamily::IPv4 ? 4 : 16;
  core::ByteBuffer message;
  message.reserve(1 + address_size + 8);
  message.push_back(address.family == runtime::AddressFamily::IPv4
                        ? std::byte{4}
                        : std::byte{6});
  for (std::size_t index = 0; index < address_size; ++index)
    message.push_back(static_cast<std::byte>(address.bytes[index]));
  const auto unsigned_slot = static_cast<std::uint64_t>(time_slot);
  for (int shift = 56; shift >= 0; shift -= 8)
    message.push_back(
        static_cast<std::byte>((unsigned_slot >> shift) & 0xff));

  core::ByteBuffer key;
  key.reserve(secret_.bytes.size());
  for (const auto byte : secret_.bytes)
    key.push_back(static_cast<std::byte>(byte));
  try {
    const auto digest = core::hmac_sha256(key, message);
    core::ByteBuffer result;
    result.reserve(16);
    for (std::size_t index = 0; index < 16; ++index)
      result.push_back(static_cast<std::byte>(digest.bytes[index]));
    return result;
  } catch (const std::exception &exception) {
    return std::unexpected(core::Error{
        core::ErrorCode::Internal,
        std::string{"Could not create announce token: "} + exception.what()});
  }
}

core::Result<core::ByteBuffer> RotatingAnnounceTokenProvider::issue(
    const runtime::IpAddress &address, core::Timestamp now) const {
  return token_for(address, slot(now));
}

core::Result<bool> RotatingAnnounceTokenProvider::validate(
    core::ByteView token, const runtime::IpAddress &address,
    core::Timestamp now) const {
  const auto current_slot = slot(now);
  auto current = token_for(address, current_slot);
  if (!current)
    return std::unexpected(current.error());
  if (core::constant_time_equal(token, *current))
    return true;
  if (current_slot == std::numeric_limits<std::int64_t>::min())
    return false;
  auto previous = token_for(address, current_slot - 1);
  if (!previous)
    return std::unexpected(previous.error());
  return core::constant_time_equal(token, *previous);
}

} // namespace sakuin::dht

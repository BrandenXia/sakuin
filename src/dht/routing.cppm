export module sakuin.dht.routing;

import std;

import sakuin.core.bytes;
import sakuin.core.result;
import sakuin.core.time;
import sakuin.dht.krpc;
import sakuin.runtime.datagram;

export namespace sakuin::dht {

inline constexpr std::size_t k_bucket_size = 8;

struct NodeContact {
  krpc::NodeId id;
  runtime::DatagramEndpoint endpoint;
  core::Timestamp last_seen;

  friend bool operator==(const NodeContact &, const NodeContact &) = default;
};

enum class RoutingMutation {
  IgnoredSelf,
  Inserted,
  Refreshed,
  ProbeRequired,
};

struct RoutingUpdate {
  RoutingMutation mutation{RoutingMutation::IgnoredSelf};
  std::optional<NodeContact> least_recently_seen;
};

std::optional<std::size_t> bucket_index(const krpc::NodeId &local,
                                        const krpc::NodeId &remote) noexcept;

bool closer_to(const krpc::NodeId &left, const krpc::NodeId &right,
               const krpc::NodeId &target) noexcept;

class RoutingTable {
public:
  explicit RoutingTable(krpc::NodeId local_id) : local_id_(local_id) {}

  RoutingTable(const RoutingTable &) = delete;
  RoutingTable &operator=(const RoutingTable &) = delete;

  const krpc::NodeId &local_id() const noexcept { return local_id_; }

  // Existing contacts are moved to the most-recent end. A full bucket is not
  // mutated: the caller receives its least-recent contact and decides how to
  // probe it. This keeps network liveness policy out of the container.
  RoutingUpdate observe(NodeContact contact);

  // Replaces an explicitly failed incumbent only when both nodes belong to the
  // same bucket. Returns false if the table changed while a probe was pending.
  bool replace_unresponsive(const krpc::NodeId &incumbent,
                            NodeContact replacement);

  bool remove(const krpc::NodeId &id);
  std::vector<NodeContact> closest(const krpc::NodeId &target,
                                   std::size_t limit = k_bucket_size) const;
  std::vector<NodeContact> all_contacts() const;
  std::size_t size() const;

private:
  using Bucket = std::list<NodeContact>;

  krpc::NodeId local_id_;
  mutable std::mutex mutex_;
  std::array<Bucket, 160> buckets_;
  std::size_t size_{};
};

core::Result<core::ByteBuffer>
encode_compact_nodes(std::span<const NodeContact> contacts,
                     runtime::AddressFamily family);

core::Result<std::vector<NodeContact>>
decode_compact_nodes(core::ByteView encoded, runtime::AddressFamily family,
                     core::Timestamp observed_at);

} // namespace sakuin::dht

namespace sakuin::dht {
namespace {

core::Error malformed(std::string message) {
  return {core::ErrorCode::InvalidArgument, std::move(message)};
}

} // namespace

std::optional<std::size_t>
bucket_index(const krpc::NodeId &local, const krpc::NodeId &remote) noexcept {
  for (std::size_t index = 0; index < local.bytes.size(); ++index) {
    const auto difference =
        static_cast<std::uint8_t>(local.bytes[index] ^ remote.bytes[index]);
    if (difference != 0) {
      const auto exponent_in_byte = std::bit_width(difference) - 1;
      return (local.bytes.size() - index - 1) * 8 + exponent_in_byte;
    }
  }
  return std::nullopt;
}

bool closer_to(const krpc::NodeId &left, const krpc::NodeId &right,
               const krpc::NodeId &target) noexcept {
  for (std::size_t index = 0; index < target.bytes.size(); ++index) {
    const auto left_distance =
        static_cast<std::uint8_t>(left.bytes[index] ^ target.bytes[index]);
    const auto right_distance =
        static_cast<std::uint8_t>(right.bytes[index] ^ target.bytes[index]);
    if (left_distance != right_distance)
      return left_distance < right_distance;
  }
  return false;
}

RoutingUpdate RoutingTable::observe(NodeContact contact) {
  const auto index = bucket_index(local_id_, contact.id);
  if (!index)
    return {.mutation = RoutingMutation::IgnoredSelf};

  std::scoped_lock lock{mutex_};
  auto &bucket = buckets_[*index];
  const auto existing = std::ranges::find(bucket, contact.id, &NodeContact::id);
  if (existing != bucket.end()) {
    bucket.erase(existing);
    bucket.push_back(std::move(contact));
    return {.mutation = RoutingMutation::Refreshed};
  }
  if (bucket.size() < k_bucket_size) {
    bucket.push_back(std::move(contact));
    ++size_;
    return {.mutation = RoutingMutation::Inserted};
  }
  return {.mutation = RoutingMutation::ProbeRequired,
          .least_recently_seen = bucket.front()};
}

bool RoutingTable::replace_unresponsive(const krpc::NodeId &incumbent,
                                        NodeContact replacement) {
  const auto old_index = bucket_index(local_id_, incumbent);
  const auto new_index = bucket_index(local_id_, replacement.id);
  if (!old_index || old_index != new_index)
    return false;

  std::scoped_lock lock{mutex_};
  auto &bucket = buckets_[*old_index];
  const auto stale = std::ranges::find(bucket, incumbent, &NodeContact::id);
  if (stale == bucket.end())
    return false;
  const auto duplicate =
      std::ranges::find(bucket, replacement.id, &NodeContact::id);
  if (duplicate != bucket.end() && duplicate != stale)
    return false;
  bucket.erase(stale);
  bucket.push_back(std::move(replacement));
  return true;
}

bool RoutingTable::remove(const krpc::NodeId &id) {
  const auto index = bucket_index(local_id_, id);
  if (!index)
    return false;
  std::scoped_lock lock{mutex_};
  auto &bucket = buckets_[*index];
  const auto found = std::ranges::find(bucket, id, &NodeContact::id);
  if (found == bucket.end())
    return false;
  bucket.erase(found);
  --size_;
  return true;
}

std::vector<NodeContact>
RoutingTable::closest(const krpc::NodeId &target, std::size_t limit) const {
  auto result = all_contacts();
  std::ranges::sort(result, [&](const auto &left, const auto &right) {
    return closer_to(left.id, right.id, target);
  });
  if (result.size() > limit)
    result.resize(limit);
  return result;
}

std::vector<NodeContact> RoutingTable::all_contacts() const {
  std::vector<NodeContact> result;
  std::scoped_lock lock{mutex_};
  result.reserve(size_);
  for (const auto &bucket : buckets_)
    result.insert(result.end(), bucket.begin(), bucket.end());
  return result;
}

std::size_t RoutingTable::size() const {
  std::scoped_lock lock{mutex_};
  return size_;
}

core::Result<core::ByteBuffer>
encode_compact_nodes(std::span<const NodeContact> contacts,
                     runtime::AddressFamily family) {
  const std::size_t address_size =
      family == runtime::AddressFamily::IPv4 ? 4 : 16;
  const std::size_t record_size = 20 + address_size + 2;
  if (contacts.size() > std::numeric_limits<std::size_t>::max() / record_size)
    return std::unexpected(malformed("Compact node list is too large"));

  core::ByteBuffer result;
  result.reserve(contacts.size() * record_size);
  for (const auto &contact : contacts) {
    if (contact.endpoint.address.family != family)
      return std::unexpected(
          malformed("Compact node list contains a different address family"));
    if (contact.endpoint.port == 0)
      return std::unexpected(
          malformed("Compact node endpoint port must be nonzero"));
    for (const auto byte : contact.id.bytes)
      result.push_back(static_cast<std::byte>(byte));
    for (std::size_t index = 0; index < address_size; ++index)
      result.push_back(
          static_cast<std::byte>(contact.endpoint.address.bytes[index]));
    result.push_back(static_cast<std::byte>(contact.endpoint.port >> 8));
    result.push_back(static_cast<std::byte>(contact.endpoint.port & 0xff));
  }
  return result;
}

core::Result<std::vector<NodeContact>>
decode_compact_nodes(core::ByteView encoded, runtime::AddressFamily family,
                     core::Timestamp observed_at) {
  const std::size_t address_size =
      family == runtime::AddressFamily::IPv4 ? 4 : 16;
  const std::size_t record_size = 20 + address_size + 2;
  if (encoded.size() % record_size != 0)
    return std::unexpected(malformed("Compact node list has invalid length"));

  std::vector<NodeContact> result;
  result.reserve(encoded.size() / record_size);
  for (std::size_t offset = 0; offset < encoded.size(); offset += record_size) {
    NodeContact contact{.last_seen = observed_at};
    for (std::size_t index = 0; index < contact.id.bytes.size(); ++index)
      contact.id.bytes[index] =
          std::to_integer<std::uint8_t>(encoded[offset + index]);
    contact.endpoint.address.family = family;
    for (std::size_t index = 0; index < address_size; ++index)
      contact.endpoint.address.bytes[index] = std::to_integer<std::uint8_t>(
          encoded[offset + contact.id.bytes.size() + index]);
    const auto port_offset = offset + contact.id.bytes.size() + address_size;
    contact.endpoint.port = static_cast<std::uint16_t>(
        (std::to_integer<std::uint16_t>(encoded[port_offset]) << 8) |
        std::to_integer<std::uint16_t>(encoded[port_offset + 1]));
    if (contact.endpoint.port == 0)
      return std::unexpected(
          malformed("Compact node endpoint port must be nonzero"));
    result.push_back(std::move(contact));
  }
  return result;
}

} // namespace sakuin::dht

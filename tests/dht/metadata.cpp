import std;

import sakuin.core;
import sakuin.dht;

namespace {

void append_u32(sakuin::core::ByteBuffer &output, std::uint32_t value) {
  output.push_back(static_cast<std::byte>(value >> 24));
  output.push_back(static_cast<std::byte>(value >> 16));
  output.push_back(static_cast<std::byte>(value >> 8));
  output.push_back(static_cast<std::byte>(value));
}

sakuin::core::ByteBuffer peer_handshake(const sakuin::core::InfoHash &hash) {
  constexpr std::string_view protocol{"BitTorrent protocol"};
  sakuin::core::ByteBuffer result;
  result.push_back(std::byte{19});
  const auto name = std::as_bytes(std::span{protocol});
  result.insert(result.end(), name.begin(), name.end());
  std::array<std::byte, 8> reserved{};
  reserved[5] = std::byte{0x10};
  result.insert(result.end(), reserved.begin(), reserved.end());
  for (const auto byte : hash.bytes)
    result.push_back(static_cast<std::byte>(byte));
  result.insert(result.end(), 20, std::byte{0x55});
  return result;
}

sakuin::core::ByteBuffer
extended_frame(std::uint8_t extension,
               sakuin::dht::bencode::Value::Dictionary dictionary,
               sakuin::core::ByteView suffix = {}) {
  auto encoded = sakuin::dht::bencode::encode(
      sakuin::dht::bencode::Value{std::move(dictionary)}, 128U * 1024U);
  if (!encoded)
    return {};
  sakuin::core::ByteBuffer result;
  append_u32(result,
             static_cast<std::uint32_t>(encoded->size() + suffix.size() + 2));
  result.push_back(std::byte{20});
  result.push_back(static_cast<std::byte>(extension));
  result.insert(result.end(), encoded->begin(), encoded->end());
  result.insert(result.end(), suffix.begin(), suffix.end());
  return result;
}

sakuin::core::ByteBuffer extension_handshake(std::size_t size) {
  using namespace sakuin::dht;
  bencode::Value::Dictionary extensions;
  extensions.emplace("ut_metadata", bencode::Value{std::int64_t{3}});
  bencode::Value::Dictionary dictionary;
  dictionary.emplace("m", bencode::Value{std::move(extensions)});
  dictionary.emplace("metadata_size",
                     bencode::Value{static_cast<std::int64_t>(size)});
  return extended_frame(0, std::move(dictionary));
}

sakuin::core::ByteBuffer data_frame(std::size_t piece, std::size_t total,
                                    sakuin::core::ByteView data) {
  using namespace sakuin::dht;
  bencode::Value::Dictionary dictionary;
  dictionary.emplace("msg_type", bencode::Value{std::int64_t{1}});
  dictionary.emplace("piece", bencode::Value{static_cast<std::int64_t>(piece)});
  dictionary.emplace("total_size",
                     bencode::Value{static_cast<std::int64_t>(total)});
  return extended_frame(1, std::move(dictionary), data);
}

std::pair<sakuin::core::InfoHash, sakuin::core::ByteBuffer> metadata() {
  using namespace sakuin;
  dht::bencode::Value::Dictionary dictionary;
  dictionary.emplace("length", dht::bencode::Value{std::int64_t{5}});
  dictionary.emplace("name", dht::bencode::Value{core::ByteBuffer{
                                 std::byte{'t'}, std::byte{'e'}, std::byte{'s'},
                                 std::byte{'t'}}});
  dictionary.emplace("piece length", dht::bencode::Value{std::int64_t{16'384}});
  dictionary.emplace(
      "pieces", dht::bencode::Value{core::ByteBuffer(16'800, std::byte{0x33})});
  auto encoded = dht::bencode::encode(
      dht::bencode::Value{std::move(dictionary)}, 128U * 1024U);
  const auto digest = core::sha1(*encoded);
  return {core::InfoHash{.bytes = digest.bytes}, std::move(*encoded)};
}

sakuin::core::Result<sakuin::dht::MetadataExchangeOutput>
prepare(sakuin::dht::MetadataExchange &exchange,
        const sakuin::core::InfoHash &hash, std::size_t size) {
  auto started = exchange.start();
  if (!started || started->sends.size() != 1 ||
      started->sends.front().size() != 68)
    return std::unexpected(sakuin::core::Error{
        sakuin::core::ErrorCode::Internal, "Bad initial exchange output"});
  const auto handshake = peer_handshake(hash);
  if (auto first = exchange.consume(std::span{handshake}.first(17));
      !first || !first->sends.empty())
    return std::unexpected(
        first ? sakuin::core::Error{sakuin::core::ErrorCode::Internal,
                                    "Early handshake output"}
              : first.error());
  auto second = exchange.consume(std::span{handshake}.subspan(17));
  if (!second || second->sends.size() != 1)
    return std::unexpected(
        second ? sakuin::core::Error{sakuin::core::ErrorCode::Internal,
                                     "Missing extension handshake"}
               : second.error());
  return exchange.consume(extension_handshake(size));
}

} // namespace

int main() {
  using namespace sakuin;
  auto [hash, encoded] = metadata();
  dht::PeerId peer_id{};
  std::ranges::iota(peer_id, std::uint8_t{1});
  auto exchange = dht::MetadataExchange::create(
      hash, peer_id, {.maximum_outstanding_requests = 2});
  if (!exchange)
    return 1;
  auto prepared = prepare(**exchange, hash, encoded.size());
  if (!prepared || prepared->sends.size() != 2 ||
      (*exchange)->metadata_size() != encoded.size() ||
      (*exchange)->buffered_metadata_bytes() != 0)
    return 2;
  for (const auto &request : prepared->sends)
    if (request.size() < 7 || request[4] != std::byte{20} ||
        request[5] != std::byte{3})
      return 3;

  constexpr std::size_t piece_size = 16U * 1024U;
  auto last = (*exchange)->consume(
      data_frame(1, encoded.size(), std::span{encoded}.subspan(piece_size)));
  if (!last || last->metadata || (*exchange)->complete() ||
      (*exchange)->buffered_metadata_bytes() != encoded.size() - piece_size)
    return 4;
  auto first = (*exchange)->consume(
      data_frame(0, encoded.size(), std::span{encoded}.first(piece_size)));
  if (!first || !first->metadata || *first->metadata != encoded ||
      !(*exchange)->complete() || (*exchange)->buffered_metadata_bytes() != 0)
    return 5;

  // A peer may advertise the configured maximum and then stall. Preparing
  // that exchange should retain only piece bookkeeping, not a 4 MiB payload.
  auto stalled = dht::MetadataExchange::create(hash, peer_id);
  if (!stalled || !prepare(**stalled, hash, 4U * 1024U * 1024U) ||
      (*stalled)->metadata_size() != 4U * 1024U * 1024U ||
      (*stalled)->buffered_metadata_bytes() != 0)
    return 11;

  auto corrupt = dht::MetadataExchange::create(
      hash, peer_id, {.maximum_outstanding_requests = 2});
  if (!corrupt || !prepare(**corrupt, hash, encoded.size()))
    return 6;
  if (!(*corrupt)->consume(data_frame(1, encoded.size(),
                                      std::span{encoded}.subspan(piece_size))))
    return 7;
  auto damaged =
      core::ByteBuffer{encoded.begin(), encoded.begin() + piece_size};
  damaged.front() ^= std::byte{1};
  auto rejected = (*corrupt)->consume(data_frame(0, encoded.size(), damaged));
  if (rejected || rejected.error().code != core::ErrorCode::ChecksumMismatch)
    return 8;

  auto no_extension = peer_handshake(hash);
  no_extension[25] = std::byte{0};
  auto unsupported = dht::MetadataExchange::create(hash, peer_id);
  if (!unsupported || !(*unsupported)->start())
    return 9;
  auto result = (*unsupported)->consume(no_extension);
  if (result || result.error().code != core::ErrorCode::UnsupportedFormat)
    return 10;
  return 0;
}

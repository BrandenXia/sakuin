export module sakuin.dht.metadata;

import std;

import sakuin.core.bytes;
import sakuin.core.hash;
import sakuin.core.ids;
import sakuin.core.result;
import sakuin.dht.bencode;

export namespace sakuin::dht {

using PeerId = std::array<std::uint8_t, 20>;

struct MetadataExchangeOptions {
  std::size_t maximum_metadata_bytes{4U * 1024U * 1024U};
  std::size_t maximum_frame_bytes{128U * 1024U};
  std::size_t maximum_outstanding_requests{4};
  std::uint8_t local_metadata_extension_id{1};
};

struct MetadataExchangeOutput {
  std::vector<core::ByteBuffer> sends;
  std::optional<core::ByteBuffer> metadata;
};

// Pure BEP 3/9/10 state machine. It owns framing and metadata assembly but no
// socket, scheduler, timer, or worker lifetime.
class MetadataExchange {
public:
  static core::Result<std::unique_ptr<MetadataExchange>>
  create(core::InfoHash info_hash, PeerId peer_id,
         MetadataExchangeOptions options = {});

  core::Result<MetadataExchangeOutput> start();
  core::Result<MetadataExchangeOutput> consume(core::ByteView bytes);

  bool complete() const noexcept { return complete_; }
  std::optional<std::size_t> metadata_size() const noexcept {
    return metadata_size_;
  }
  // Bytes retained from pieces received so far. Merely advertising a large
  // metadata_size must not reserve that amount for a peer which then stalls.
  std::size_t buffered_metadata_bytes() const noexcept {
    return buffered_metadata_bytes_;
  }

private:
  MetadataExchange(core::InfoHash info_hash, PeerId peer_id,
                   MetadataExchangeOptions options)
      : info_hash_(info_hash), peer_id_(peer_id), options_(options) {}

  core::Result<void> handle_handshake(MetadataExchangeOutput &output);
  core::Result<void> handle_frame(core::ByteView frame,
                                  MetadataExchangeOutput &output);
  core::Result<void> handle_extension_handshake(core::ByteView payload,
                                                MetadataExchangeOutput &output);
  core::Result<void> handle_metadata(core::ByteView payload,
                                     MetadataExchangeOutput &output);
  core::Result<void> queue_requests(MetadataExchangeOutput &output);
  core::Result<void> finish(MetadataExchangeOutput &output);

  core::InfoHash info_hash_;
  PeerId peer_id_;
  MetadataExchangeOptions options_;
  core::ByteBuffer input_;
  std::vector<core::ByteBuffer> metadata_pieces_;
  std::vector<bool> requested_;
  std::vector<bool> received_;
  std::optional<std::size_t> metadata_size_;
  std::optional<std::uint8_t> remote_metadata_extension_id_;
  std::size_t outstanding_{};
  std::size_t received_count_{};
  std::size_t buffered_metadata_bytes_{};
  bool started_{};
  bool handshake_received_{};
  bool complete_{};
};

} // namespace sakuin::dht

namespace sakuin::dht {
namespace {

constexpr std::size_t metadata_piece_bytes = 16U * 1024U;
constexpr std::string_view protocol_name{"BitTorrent protocol"};
constexpr std::uint8_t extended_message_id = 20;

core::Error protocol_error(std::string message) {
  return {core::ErrorCode::InvalidArgument, std::move(message)};
}

void append_u32(core::ByteBuffer &output, std::uint32_t value) {
  output.push_back(static_cast<std::byte>(value >> 24));
  output.push_back(static_cast<std::byte>(value >> 16));
  output.push_back(static_cast<std::byte>(value >> 8));
  output.push_back(static_cast<std::byte>(value));
}

std::uint32_t read_u32(core::ByteView input) {
  std::uint32_t result{};
  for (std::size_t index = 0; index < 4; ++index)
    result = (result << 8) | std::to_integer<std::uint8_t>(input[index]);
  return result;
}

core::ByteBuffer bytes(std::string_view value) {
  const auto view = std::as_bytes(std::span{value});
  return {view.begin(), view.end()};
}

core::Result<core::ByteBuffer>
extended_frame(std::uint8_t extension_id, bencode::Value::Dictionary dictionary,
               std::size_t maximum_frame_bytes) {
  auto encoded = bencode::encode(bencode::Value{std::move(dictionary)},
                                 maximum_frame_bytes);
  if (!encoded)
    return std::unexpected(encoded.error());
  const auto payload_size = encoded->size() + 2;
  if (payload_size > maximum_frame_bytes ||
      payload_size > std::numeric_limits<std::uint32_t>::max())
    return std::unexpected(protocol_error("Peer message exceeds frame limit"));
  core::ByteBuffer output;
  output.reserve(payload_size + 4);
  append_u32(output, static_cast<std::uint32_t>(payload_size));
  output.push_back(static_cast<std::byte>(extended_message_id));
  output.push_back(static_cast<std::byte>(extension_id));
  output.insert(output.end(), encoded->begin(), encoded->end());
  return output;
}

core::Result<std::int64_t>
required_integer(const bencode::Value::Dictionary &dictionary,
                 std::string_view name) {
  const auto found = dictionary.find(name);
  if (found == dictionary.end() || !found->second.integer())
    return std::unexpected(protocol_error(
        "Metadata message is missing integer " + std::string{name}));
  return *found->second.integer();
}

core::ByteView hash_view(const core::Hash160 &hash) {
  return {reinterpret_cast<const std::byte *>(hash.bytes.data()),
          hash.bytes.size()};
}

core::ByteView info_hash_view(const core::InfoHash &hash) {
  return {reinterpret_cast<const std::byte *>(hash.bytes.data()),
          hash.bytes.size()};
}

} // namespace

core::Result<std::unique_ptr<MetadataExchange>>
MetadataExchange::create(core::InfoHash info_hash, PeerId peer_id,
                         MetadataExchangeOptions options) {
  if (options.maximum_metadata_bytes == 0 ||
      options.maximum_frame_bytes < metadata_piece_bytes + 128 ||
      options.maximum_outstanding_requests == 0 ||
      options.local_metadata_extension_id == 0)
    return std::unexpected(protocol_error("Invalid metadata exchange limits"));
  return std::unique_ptr<MetadataExchange>{
      new MetadataExchange{info_hash, peer_id, options}};
}

core::Result<MetadataExchangeOutput> MetadataExchange::start() {
  if (started_)
    return std::unexpected(core::Error{core::ErrorCode::Conflict,
                                       "Metadata exchange already started"});
  started_ = true;
  core::ByteBuffer handshake;
  handshake.reserve(68);
  handshake.push_back(static_cast<std::byte>(protocol_name.size()));
  const auto protocol = std::as_bytes(std::span{protocol_name});
  handshake.insert(handshake.end(), protocol.begin(), protocol.end());
  std::array<std::byte, 8> reserved{};
  reserved[5] = std::byte{0x10};
  handshake.insert(handshake.end(), reserved.begin(), reserved.end());
  for (const auto byte : info_hash_.bytes)
    handshake.push_back(static_cast<std::byte>(byte));
  for (const auto byte : peer_id_)
    handshake.push_back(static_cast<std::byte>(byte));
  MetadataExchangeOutput output;
  output.sends.push_back(std::move(handshake));
  return output;
}

core::Result<void>
MetadataExchange::handle_handshake(MetadataExchangeOutput &output) {
  if (input_.size() < 68)
    return {};
  if (std::to_integer<std::uint8_t>(input_[0]) != protocol_name.size() ||
      !std::ranges::equal(std::span{input_}.subspan(1, protocol_name.size()),
                          std::as_bytes(std::span{protocol_name})))
    return std::unexpected(protocol_error("Invalid BitTorrent handshake"));
  if ((std::to_integer<std::uint8_t>(input_[25]) & 0x10U) == 0)
    return std::unexpected(
        core::Error{core::ErrorCode::UnsupportedFormat,
                    "Peer does not advertise the BEP 10 extension protocol"});
  if (!std::ranges::equal(std::span{input_}.subspan(28, 20),
                          info_hash_view(info_hash_)))
    return std::unexpected(
        protocol_error("Peer handshake has the wrong infohash"));
  input_.erase(input_.begin(), input_.begin() + 68);
  handshake_received_ = true;

  bencode::Value::Dictionary extensions;
  extensions.emplace("ut_metadata", bencode::Value{static_cast<std::int64_t>(
                                        options_.local_metadata_extension_id)});
  bencode::Value::Dictionary handshake;
  handshake.emplace("m", bencode::Value{std::move(extensions)});
  handshake.emplace("v", bencode::Value{bytes("Sakuin")});
  auto frame =
      extended_frame(0, std::move(handshake), options_.maximum_frame_bytes);
  if (!frame)
    return std::unexpected(frame.error());
  output.sends.push_back(std::move(*frame));
  return {};
}

core::Result<void>
MetadataExchange::handle_extension_handshake(core::ByteView payload,
                                             MetadataExchangeOutput &output) {
  auto parsed =
      bencode::parse(payload, {.maximum_bytes = options_.maximum_frame_bytes});
  if (!parsed || !parsed->dictionary())
    return std::unexpected(
        parsed ? protocol_error("Extension handshake is not a dictionary")
               : parsed.error());
  const auto &dictionary = *parsed->dictionary();
  if (const auto extensions = dictionary.find("m");
      extensions != dictionary.end()) {
    const auto *supported = extensions->second.dictionary();
    if (!supported)
      return std::unexpected(
          protocol_error("Extension handshake m value is not a dictionary"));
    if (const auto metadata = supported->find("ut_metadata");
        metadata != supported->end()) {
      const auto *identifier = metadata->second.integer();
      if (!identifier || *identifier < 0 || *identifier > 255)
        return std::unexpected(
            protocol_error("Peer ut_metadata identifier is invalid"));
      if (*identifier == 0)
        return std::unexpected(
            core::Error{core::ErrorCode::UnsupportedFormat,
                        "Peer disabled the ut_metadata extension"});
      remote_metadata_extension_id_ = static_cast<std::uint8_t>(*identifier);
    }
  }
  if (const auto size = dictionary.find("metadata_size");
      size != dictionary.end()) {
    const auto *value = size->second.integer();
    if (!value || *value <= 0 ||
        static_cast<std::uint64_t>(*value) > options_.maximum_metadata_bytes)
      return std::unexpected(
          protocol_error("Peer metadata_size is invalid or exceeds limit"));
    const auto new_size = static_cast<std::size_t>(*value);
    if (metadata_size_ && *metadata_size_ != new_size)
      return std::unexpected(
          protocol_error("Peer changed metadata_size during exchange"));
    if (!metadata_size_) {
      metadata_size_ = new_size;
      const auto pieces =
          (new_size + metadata_piece_bytes - 1) / metadata_piece_bytes;
      metadata_pieces_.resize(pieces);
      requested_.assign(pieces, false);
      received_.assign(pieces, false);
    }
  }
  if (!remote_metadata_extension_id_ || !metadata_size_)
    return {};
  return queue_requests(output);
}

core::Result<void>
MetadataExchange::queue_requests(MetadataExchangeOutput &output) {
  if (!remote_metadata_extension_id_)
    return {};
  for (std::size_t piece = 0;
       piece < requested_.size() &&
       outstanding_ < options_.maximum_outstanding_requests;
       ++piece) {
    if (requested_[piece])
      continue;
    bencode::Value::Dictionary request;
    request.emplace("msg_type", bencode::Value{std::int64_t{0}});
    request.emplace("piece", bencode::Value{static_cast<std::int64_t>(piece)});
    auto frame =
        extended_frame(*remote_metadata_extension_id_, std::move(request),
                       options_.maximum_frame_bytes);
    if (!frame)
      return std::unexpected(frame.error());
    requested_[piece] = true;
    ++outstanding_;
    output.sends.push_back(std::move(*frame));
  }
  return {};
}

core::Result<void> MetadataExchange::finish(MetadataExchangeOutput &output) {
  core::ByteBuffer metadata;
  metadata.reserve(*metadata_size_);
  for (const auto &piece : metadata_pieces_)
    metadata.insert(metadata.end(), piece.begin(), piece.end());
  if (metadata.size() != *metadata_size_)
    return std::unexpected(core::Error{
        core::ErrorCode::Internal,
        "Buffered metadata size does not match peer metadata_size"});
  std::vector<core::ByteBuffer>{}.swap(metadata_pieces_);
  buffered_metadata_bytes_ = 0;

  core::Hash160 digest;
  try {
    digest = core::sha1(metadata);
  } catch (const std::exception &exception) {
    return std::unexpected(
        core::Error{core::ErrorCode::Internal,
                    std::string{"Could not validate torrent metadata: "} +
                        exception.what()});
  }
  if (!core::constant_time_equal(hash_view(digest), info_hash_view(info_hash_)))
    return std::unexpected(
        core::Error{core::ErrorCode::ChecksumMismatch,
                    "Torrent metadata does not match the requested infohash"});
  complete_ = true;
  output.metadata = std::move(metadata);
  return {};
}

core::Result<void>
MetadataExchange::handle_metadata(core::ByteView payload,
                                  MetadataExchangeOutput &output) {
  auto parsed = bencode::parse_prefix(
      payload, {.maximum_bytes = options_.maximum_frame_bytes});
  if (!parsed || !parsed->value.dictionary())
    return std::unexpected(
        parsed ? protocol_error("Metadata header is not a dictionary")
               : parsed.error());
  const auto &dictionary = *parsed->value.dictionary();
  auto type = required_integer(dictionary, "msg_type");
  auto piece_value = required_integer(dictionary, "piece");
  if (!type || !piece_value)
    return std::unexpected(type ? piece_value.error() : type.error());
  if (*piece_value < 0 ||
      static_cast<std::uint64_t>(*piece_value) >= requested_.size())
    return std::unexpected(protocol_error("Metadata piece index is invalid"));
  const auto piece = static_cast<std::size_t>(*piece_value);

  if (*type == 0) {
    if (!remote_metadata_extension_id_)
      return {};
    bencode::Value::Dictionary reject;
    reject.emplace("msg_type", bencode::Value{std::int64_t{2}});
    reject.emplace("piece", bencode::Value{*piece_value});
    auto frame =
        extended_frame(*remote_metadata_extension_id_, std::move(reject),
                       options_.maximum_frame_bytes);
    if (!frame)
      return std::unexpected(frame.error());
    output.sends.push_back(std::move(*frame));
    return {};
  }
  if (*type == 2)
    return std::unexpected(core::Error{core::ErrorCode::NotFound,
                                       "Peer rejected a metadata piece"});
  if (*type != 1)
    return {};
  if (!metadata_size_ || !requested_[piece])
    return std::unexpected(
        protocol_error("Peer sent an unsolicited metadata piece"));
  auto total_size = required_integer(dictionary, "total_size");
  if (!total_size || *total_size < 0 ||
      static_cast<std::uint64_t>(*total_size) != *metadata_size_)
    return std::unexpected(total_size
                               ? protocol_error("Metadata total_size changed")
                               : total_size.error());
  const auto data = payload.subspan(parsed->consumed);
  const auto offset = piece * metadata_piece_bytes;
  const auto expected =
      std::min(metadata_piece_bytes, *metadata_size_ - offset);
  if (data.size() != expected)
    return std::unexpected(
        protocol_error("Metadata piece has an invalid byte length"));
  if (received_[piece]) {
    if (!std::ranges::equal(data, metadata_pieces_[piece]))
      return std::unexpected(
          protocol_error("Peer sent conflicting duplicate metadata"));
    return {};
  }
  metadata_pieces_[piece].assign(data.begin(), data.end());
  buffered_metadata_bytes_ += data.size();
  received_[piece] = true;
  ++received_count_;
  --outstanding_;
  if (received_count_ == received_.size())
    return finish(output);
  return queue_requests(output);
}

core::Result<void>
MetadataExchange::handle_frame(core::ByteView frame,
                               MetadataExchangeOutput &output) {
  if (frame.empty())
    return {};
  if (std::to_integer<std::uint8_t>(frame.front()) != extended_message_id)
    return {};
  if (frame.size() < 2)
    return std::unexpected(
        protocol_error("Extended peer message is truncated"));
  const auto extension = std::to_integer<std::uint8_t>(frame[1]);
  const auto payload = frame.subspan(2);
  if (extension == 0)
    return handle_extension_handshake(payload, output);
  if (extension != options_.local_metadata_extension_id)
    return {};
  return handle_metadata(payload, output);
}

core::Result<MetadataExchangeOutput>
MetadataExchange::consume(core::ByteView bytes) {
  if (!started_)
    return std::unexpected(core::Error{core::ErrorCode::Conflict,
                                       "Metadata exchange is not started"});
  if (complete_)
    return MetadataExchangeOutput{};
  if (bytes.size() > options_.maximum_frame_bytes + 68 - input_.size())
    return std::unexpected(
        protocol_error("Peer input buffer exceeds configured limit"));
  input_.insert(input_.end(), bytes.begin(), bytes.end());
  MetadataExchangeOutput output;
  if (!handshake_received_) {
    if (auto handled = handle_handshake(output); !handled)
      return std::unexpected(handled.error());
    if (!handshake_received_)
      return output;
  }
  while (input_.size() >= 4) {
    const auto length = read_u32(input_);
    if (length > options_.maximum_frame_bytes)
      return std::unexpected(protocol_error("Peer frame exceeds size limit"));
    if (input_.size() < 4 + static_cast<std::size_t>(length))
      break;
    if (length != 0) {
      const core::ByteView frame{input_.data() + 4, length};
      if (auto handled = handle_frame(frame, output); !handled)
        return std::unexpected(handled.error());
    }
    input_.erase(input_.begin(), input_.begin() + 4 + length);
    if (complete_)
      break;
  }
  return output;
}

} // namespace sakuin::dht

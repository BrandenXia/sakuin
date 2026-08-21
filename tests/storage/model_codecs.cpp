import std;

import sakuin.core;
import sakuin.model.observation;
import sakuin.model.torrent;
import sakuin.storage.codec.model_records;

namespace {

bool same_time(sakuin::core::Timestamp left, sakuin::core::Timestamp right) {
  return left.time_since_epoch() == right.time_since_epoch();
}

} // namespace

int main() {
  using namespace sakuin;

  core::InfoHash hash;
  for (std::size_t index = 0; index < hash.bytes.size(); ++index)
    hash.bytes[index] = static_cast<std::uint8_t>(index);

  const auto first = core::Timestamp{std::chrono::duration_cast<
      core::Timestamp::duration>(std::chrono::nanoseconds{123456789})};
  const auto last = core::Timestamp{std::chrono::duration_cast<
      core::Timestamp::duration>(std::chrono::nanoseconds{987654321})};
  core::ByteBuffer encoded;

  storage::ObservationRecordCodec observation_codec;
  const model::ObservationRecord observation{hash, first};
  if (!observation_codec.encode(observation, encoded) || encoded.size() != 28)
    return 1;
  auto decoded_observation = observation_codec.decode(encoded);
  if (!decoded_observation || decoded_observation->info_hash != hash ||
      !same_time(decoded_observation->observed_at, first))
    return 2;

  storage::TorrentRecordCodec torrent_codec;
  const model::TorrentRecord torrent{
      .info_hash = hash,
      .first_seen = first,
      .last_seen = last,
      .name = std::string{"Example"},
      .total_size = 30,
      .files = {{"a.bin", 10}, {"nested/b.bin", 20}},
  };
  if (!torrent_codec.encode(torrent, encoded))
    return 3;
  auto decoded_torrent = torrent_codec.decode(encoded);
  if (!decoded_torrent || decoded_torrent->info_hash != hash ||
      !same_time(decoded_torrent->first_seen, first) ||
      !same_time(decoded_torrent->last_seen, last) ||
      decoded_torrent->name != torrent.name ||
      decoded_torrent->total_size != torrent.total_size ||
      decoded_torrent->files.size() != 2 ||
      decoded_torrent->files[1].path != "nested/b.bin" ||
      decoded_torrent->files[1].size != 20)
    return 4;

  encoded.pop_back();
  auto truncated = torrent_codec.decode(encoded);
  if (truncated || truncated.error().code != core::ErrorCode::CorruptSegment)
    return 5;

  core::ByteBuffer empty;
  if (!torrent_codec.encode(
          model::TorrentRecord{.info_hash = hash,
                               .first_seen = first,
                               .last_seen = last,
                               .name = std::nullopt,
                               .total_size = 0,
                               .files = {}},
          empty))
    return 6;
  auto decoded_empty = torrent_codec.decode(empty);
  if (!decoded_empty || decoded_empty->name || !decoded_empty->files.empty())
    return 7;

  return 0;
}

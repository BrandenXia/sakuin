import std;

import sakuin.core;
import sakuin.model.observation;
import sakuin.model.torrent;
import sakuin.storage;

int main() {
  using namespace sakuin;

  static_assert(std::same_as<decltype(storage::RecordLocation::block),
                             std::uint32_t>);
  static_assert(std::is_abstract_v<storage::SegmentReader>);
  static_assert(std::is_abstract_v<storage::RecordCodec<model::TorrentRecord>>);

  const storage::RecordLocation location{2, 16, 32};
  if (location != storage::RecordLocation{2, 16, 32})
    return 1;

  const storage::SegmentHeader header{
      .format_version = {1, 0},
      .schema_version = {1},
      .encoding = storage::SegmentEncoding::RowV1,
      .tier = storage::SegmentTier::Warm,
      .compression = storage::CompressionCodec::Zstd,
      .target_block_size = 64 * 1024,
  };
  if (header.tier != storage::SegmentTier::Warm ||
      header.encoding != storage::SegmentEncoding::RowV1)
    return 2;

  const model::TorrentRecord torrent{
      .info_hash = {},
      .first_seen = {},
      .last_seen = {},
      .name = std::string{"example"},
      .total_size = 42,
      .files = {{"file.bin", 42}},
  };
  if (torrent.files.size() != 1 || torrent.total_size != 42)
    return 3;

  return 0;
}

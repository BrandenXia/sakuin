import std;

import sakuin.core;
import sakuin.dht;
import sakuin.integration.dht_storage;
import sakuin.storage;
import sakuin.storage.dataset.observations;

namespace {

struct TemporaryDirectory {
  std::filesystem::path path;
  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path, ignored);
  }
};

sakuin::core::ByteBuffer bytes(std::string_view text) {
  const auto view = std::as_bytes(std::span{text});
  return {view.begin(), view.end()};
}

sakuin::core::ByteBuffer packet(std::string_view prefix, std::uint8_t hash,
                                std::string_view suffix) {
  auto result = bytes(prefix);
  result.insert(result.end(), 20, static_cast<std::byte>(hash));
  const auto suffix_bytes = std::as_bytes(std::span{suffix});
  result.insert(result.end(), suffix_bytes.begin(), suffix_bytes.end());
  return result;
}

} // namespace

int main() {
  using namespace sakuin;
  const auto nonce = std::to_string(
      std::chrono::steady_clock::now().time_since_epoch().count());
  TemporaryDirectory directory{std::filesystem::temp_directory_path() /
                               ("sakuin-dht-ingest-" + nonce)};
  storage::LocalBlobStore blobs{directory.path / "blobs"};
  auto catalog =
      storage::LocalManifestCatalog::open(directory.path / "catalog", blobs);
  if (!catalog)
    return 1;
  storage::ObservationDataset dataset{blobs, **catalog};
  integration::BufferedObservationSink sink{dataset, 2};
  dht::ObservationIngestor ingestor{sink};

  const auto get_peers = packet(
      "d1:ad2:id20:abcdefghijklmnopqrst9:info_hash20:", 0x11,
      "e1:q9:get_peers1:t2:aa1:y1:qe");
  const auto announce = packet(
      "d1:ad2:id20:abcdefghijklmnopqrst9:info_hash20:", 0x22,
      "4:porti6881e5:token3:abce1:q13:announce_peer1:t2:bb1:y1:qe");
  const auto ping = bytes(
      "d1:ad2:id20:abcdefghijklmnopqrste1:q4:ping1:t2:cc1:y1:qe");
  const auto first_time = core::Timestamp{core::Timestamp::duration{100}};
  const auto second_time = core::Timestamp{core::Timestamp::duration{200}};
  auto first = ingestor.ingest(get_peers, first_time);
  auto ignored = ingestor.ingest(ping, first_time);
  auto second = ingestor.ingest(announce, second_time);
  if (!first || !*first || !ignored || *ignored || !second || !*second ||
      sink.pending() != 0 || (*catalog)->current_id().generation != 1)
    return 2;

  auto snapshot = dataset.snapshot();
  auto stream = (*snapshot)->scan({});
  for (const auto [expected_hash, expected_time] :
       {std::pair{std::uint8_t{0x11}, first_time},
        std::pair{std::uint8_t{0x22}, second_time}}) {
    auto next = (*stream)->next();
    if (!next || !*next || (*next)->info_hash.bytes[0] != expected_hash ||
        (*next)->observed_at != expected_time)
      return 3;
  }
  auto end = (*stream)->next();
  if (!end || end->has_value())
    return 4;

  const auto malformed = packet(
      "d1:ad2:id3:bad9:info_hash20:", 0x33,
      "e1:q9:get_peers1:t2:dd1:y1:qe");
  auto rejected = ingestor.ingest(malformed, second_time);
  if (rejected || rejected.error().code != core::ErrorCode::InvalidArgument)
    return 5;
  return 0;
}

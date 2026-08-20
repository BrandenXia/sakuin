export module sakuin.model.torrent;

import std;

import sakuin.core.ids;
import sakuin.core.time;

export namespace sakuin::model {

struct FileRecord {
  std::string path;
  std::uint64_t size;

};

struct TorrentRecord {
  core::InfoHash info_hash;

  core::Timestamp first_seen;
  core::Timestamp last_seen;

  std::optional<std::string> name;

  std::uint64_t total_size;
  std::vector<FileRecord> files;

};

} // namespace sakuin::model

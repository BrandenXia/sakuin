export module sakuin.model.torrent;

import std;

import sakuin.core.ids;
import sakuin.core.time;

export namespace sakuin::model {

struct FileRecord {
  std::string path;
  std::uint64_t size;

  friend bool operator==(const FileRecord &, const FileRecord &) = default;
};

struct TorrentRecord {
  core::InfoHash info_hash;

  core::Timestamp first_seen;
  core::Timestamp last_seen;

  std::optional<std::string> name;

  std::uint64_t total_size;
  std::vector<FileRecord> files;

  friend bool operator==(const TorrentRecord &,
                         const TorrentRecord &) = default;
};

} // namespace sakuin::model

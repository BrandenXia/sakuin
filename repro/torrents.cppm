export module sakuin.storage.dataset.torrents;

import std;

import sakuin.core.result;
import sakuin.model.torrent;
import sakuin.storage.dataset.stream;

export namespace sakuin::storage {

struct TorrentChangeCursor {
  std::uint64_t source_generation{};
};

struct TorrentChangeScan {
  bool full_rebuild_required{};
  TorrentChangeCursor cursor;
  std::unique_ptr<RecordStream<model::TorrentRecord>> records;
};

class TorrentDataset {
public:
  core::Result<TorrentChangeScan>
  changes_since(TorrentChangeCursor cursor = {}) const;
};

} // namespace sakuin::storage

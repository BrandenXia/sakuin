export module sakuin.storage.dataset.torrents;

import std;

import sakuin.core.bytes;
import sakuin.core.ids;
import sakuin.core.result;
import sakuin.core.time;
import sakuin.model.torrent;
import sakuin.storage.blob.store;
import sakuin.storage.catalog.manifest;
import sakuin.storage.codec.model_records;
import sakuin.storage.dataset.dataset;
import sakuin.storage.dataset.snapshot;
import sakuin.storage.dataset.stream;
import sakuin.storage.dataset.write_session;
import sakuin.storage.format.row_v1;
import sakuin.storage.format.segment;

export namespace sakuin::storage {

struct AllTorrents {};

class TorrentDataset final
    : public KeyedDataset<model::TorrentRecord, core::InfoHash, AllTorrents> {
public:
  TorrentDataset(BlobStore &blobs, ManifestCatalog &catalog,
                 SegmentHeader segment_header = {});

  core::Result<
      std::shared_ptr<const Snapshot<model::TorrentRecord, AllTorrents>>>
  snapshot() const override;

  core::Result<std::shared_ptr<
      const KeyedSnapshot<model::TorrentRecord, core::InfoHash, AllTorrents>>>
  keyed_snapshot() const override;

  core::Result<std::unique_ptr<WriteSession<model::TorrentRecord>>>
  begin_write() override;

  // Atomically merges observation history from the current keyed value with
  // validated metadata. A concurrent catalog publish returns Conflict without
  // overwriting the winner, so an orchestrator may safely retry.
  core::Result<CommitResult> enrich(model::TorrentRecord metadata);

private:
  BlobStore *blobs_;
  ManifestCatalog *catalog_;
  SegmentHeader segment_header_;
  TorrentRecordCodec codec_;
};

} // namespace sakuin::storage

namespace sakuin::storage {
namespace {

struct InfoHashHasher {
  std::size_t operator()(const core::InfoHash &hash) const noexcept {
    std::size_t value = 1469598103934665603ULL;
    for (const auto byte : hash.bytes) {
      value ^= byte;
      value *= 1099511628211ULL;
    }
    return value;
  }
};

core::Result<std::unique_ptr<RowV1SegmentReader>>
open_torrent_segment(BlobStore &blobs, const SegmentDescriptor &descriptor,
                     const TorrentRecordCodec &codec) {
  if (descriptor.encoding != SegmentEncoding::RowV1 ||
      descriptor.schema_id != TorrentRecordSchema ||
      descriptor.schema_version.value != codec.version().value)
    return std::unexpected(
        core::Error{core::ErrorCode::UnsupportedFormat,
                    "Torrent segment uses an unsupported encoding or schema"});

  auto opened = RowV1SegmentReader::open(blobs, descriptor.object);
  if (!opened)
    return std::unexpected(opened.error());
  const auto &header = (*opened)->header();
  if (header.schema_id != descriptor.schema_id ||
      header.schema_version != descriptor.schema_version ||
      header.format_version != descriptor.format_version ||
      header.encoding != descriptor.encoding ||
      header.compression != descriptor.compression ||
      header.tier != descriptor.tier ||
      (*opened)->record_count() != descriptor.record_count)
    return std::unexpected(
        core::Error{core::ErrorCode::CorruptSegment,
                    "Torrent manifest metadata does not match its segment"});
  return opened;
}

core::Result<model::TorrentRecord>
read_torrent(RowV1SegmentReader &reader, std::uint64_t ordinal,
             const TorrentRecordCodec &codec) {
  auto location = reader.location(ordinal);
  if (!location)
    return std::unexpected(location.error());
  auto encoded = reader.read(*location);
  if (!encoded)
    return std::unexpected(encoded.error());
  return codec.decode(*encoded);
}

class TorrentStream final : public RecordStream<model::TorrentRecord> {
public:
  TorrentStream(std::shared_ptr<const ManifestPin> pin, BlobStore &blobs,
                const TorrentRecordCodec &codec)
      : pin_(std::move(pin)), blobs_(&blobs), codec_(codec),
        segment_index_(pin_->manifest().segments.size()) {}

  core::Result<std::optional<model::TorrentRecord>> next() override {
    while (segment_index_ > 0 || reader_) {
      if (!reader_) {
        --segment_index_;
        auto opened = open_torrent_segment(
            *blobs_, pin_->manifest().segments[segment_index_], codec_);
        if (!opened)
          return std::unexpected(opened.error());
        reader_ = std::move(*opened);
        record_index_ = reader_->record_count();
      }
      while (record_index_ > 0) {
        auto decoded = read_torrent(*reader_, --record_index_, codec_);
        if (!decoded)
          return std::unexpected(decoded.error());
        if (seen_.insert(decoded->info_hash).second)
          return std::optional<model::TorrentRecord>{std::move(*decoded)};
      }
      reader_.reset();
    }
    return std::optional<model::TorrentRecord>{};
  }

private:
  std::shared_ptr<const ManifestPin> pin_;
  BlobStore *blobs_;
  TorrentRecordCodec codec_;
  std::unordered_set<core::InfoHash, InfoHashHasher> seen_;
  std::size_t segment_index_;
  std::uint64_t record_index_{};
  std::unique_ptr<RowV1SegmentReader> reader_;
};

class TorrentSnapshot final
    : public KeyedSnapshot<model::TorrentRecord, core::InfoHash, AllTorrents> {
public:
  TorrentSnapshot(std::shared_ptr<const ManifestPin> pin, BlobStore &blobs,
                  const TorrentRecordCodec &codec)
      : KeyedSnapshot(std::move(pin)), blobs_(&blobs), codec_(codec),
        created_at_(std::chrono::system_clock::now()) {}

  core::Timestamp created_at() const noexcept override { return created_at_; }

  core::Result<std::optional<model::TorrentRecord>>
  get(const core::InfoHash &key) const override {
    const auto &segments = manifest().segments;
    for (auto segment = segments.rbegin(); segment != segments.rend();
         ++segment) {
      auto reader = open_torrent_segment(*blobs_, *segment, codec_);
      if (!reader)
        return std::unexpected(reader.error());
      for (auto ordinal = (*reader)->record_count(); ordinal > 0; --ordinal) {
        auto decoded = read_torrent(**reader, ordinal - 1, codec_);
        if (!decoded)
          return std::unexpected(decoded.error());
        if (decoded->info_hash == key)
          return std::optional<model::TorrentRecord>{std::move(*decoded)};
      }
    }
    return std::optional<model::TorrentRecord>{};
  }

  // HOT segments are unsorted. Until WARM keyed compaction is available, scans
  // walk newest-to-oldest and retain only the first value for each InfoHash.
  core::Result<std::unique_ptr<RecordStream<model::TorrentRecord>>>
  scan(const AllTorrents &) const override {
    return std::unique_ptr<RecordStream<model::TorrentRecord>>{
        std::make_unique<TorrentStream>(pin(), *blobs_, codec_)};
  }

private:
  BlobStore *blobs_;
  TorrentRecordCodec codec_;
  core::Timestamp created_at_;
};

class TorrentWriteSession final : public WriteSession<model::TorrentRecord> {
public:
  TorrentWriteSession(BlobStore &blobs, ManifestCatalog &catalog,
                      const TorrentRecordCodec &codec,
                      std::shared_ptr<const ManifestPin> base,
                      std::unique_ptr<RowV1SegmentWriter> writer)
      : blobs_(&blobs), catalog_(&catalog), codec_(codec),
        base_(std::move(base)), writer_(std::move(writer)) {}

  ~TorrentWriteSession() override { abort(); }

  core::Result<void> append(const model::TorrentRecord &record) override {
    if (!active_)
      return std::unexpected(core::Error{core::ErrorCode::InvalidArgument,
                                         "Write session is no longer active"});
    core::ByteBuffer encoded;
    if (auto result = codec_.encode(record, encoded); !result)
      return result;
    if (auto result = writer_->append(encoded); !result)
      return result;
    minimum_ =
        minimum_ ? std::min(*minimum_, record.first_seen) : record.first_seen;
    maximum_ =
        maximum_ ? std::max(*maximum_, record.last_seen) : record.last_seen;
    ++count_;
    return {};
  }

  core::Result<void>
  append(std::span<const model::TorrentRecord> records) override {
    for (const auto &record : records) {
      if (auto result = append(record); !result)
        return result;
    }
    return {};
  }

  core::Result<CommitResult> commit() override {
    if (!active_)
      return std::unexpected(core::Error{core::ErrorCode::InvalidArgument,
                                         "Write session is no longer active"});
    if (count_ == 0) {
      abort();
      return CommitResult{.generation = base_->manifest().id.generation};
    }
    if (catalog_->current_id() != base_->manifest().id) {
      abort();
      return std::unexpected(core::Error{core::ErrorCode::Conflict,
                                         "Dataset changed during write"});
    }
    auto descriptor = writer_->finalize();
    if (!descriptor)
      return std::unexpected(descriptor.error());
    active_ = false;
    descriptor->min_timestamp = minimum_;
    descriptor->max_timestamp = maximum_;
    auto verified = RowV1SegmentReader::open(*blobs_, descriptor->object);
    if (!verified)
      return std::unexpected(verified.error());
    if (auto integrity = (*verified)->verify(); !integrity)
      return std::unexpected(integrity.error());
    if ((*verified)->record_count() != descriptor->record_count)
      return std::unexpected(core::Error{core::ErrorCode::CorruptSegment,
                                         "Segment verification failed"});

    auto segments = base_->manifest().segments;
    segments.push_back(*descriptor);
    auto published =
        catalog_->publish(base_->manifest().id, std::move(segments));
    if (!published)
      return std::unexpected(published.error());
    return CommitResult{.generation = published->generation};
  }

  void abort() noexcept override {
    if (active_) {
      active_ = false;
      writer_->abort();
    }
  }

private:
  BlobStore *blobs_;
  ManifestCatalog *catalog_;
  TorrentRecordCodec codec_;
  std::shared_ptr<const ManifestPin> base_;
  std::unique_ptr<RowV1SegmentWriter> writer_;
  std::optional<core::Timestamp> minimum_;
  std::optional<core::Timestamp> maximum_;
  std::uint64_t count_{};
  bool active_{true};
};

core::Result<std::shared_ptr<const TorrentSnapshot>>
make_torrent_snapshot(ManifestCatalog &catalog, BlobStore &blobs,
                      const TorrentRecordCodec &codec) {
  auto pin = catalog.pin_current();
  if (!pin)
    return std::unexpected(pin.error());
  return std::shared_ptr<const TorrentSnapshot>{
      std::make_shared<TorrentSnapshot>(std::move(*pin), blobs, codec)};
}

} // namespace

TorrentDataset::TorrentDataset(BlobStore &blobs, ManifestCatalog &catalog,
                               SegmentHeader header)
    : blobs_(&blobs), catalog_(&catalog), segment_header_(header) {
  segment_header_.schema_id = TorrentRecordSchema;
  segment_header_.schema_version.value = codec_.version().value;
}

core::Result<std::shared_ptr<const Snapshot<model::TorrentRecord, AllTorrents>>>
TorrentDataset::snapshot() const {
  auto snapshot = make_torrent_snapshot(*catalog_, *blobs_, codec_);
  if (!snapshot)
    return std::unexpected(snapshot.error());
  return std::shared_ptr<const Snapshot<model::TorrentRecord, AllTorrents>>{
      std::move(*snapshot)};
}

core::Result<std::shared_ptr<
    const KeyedSnapshot<model::TorrentRecord, core::InfoHash, AllTorrents>>>
TorrentDataset::keyed_snapshot() const {
  auto snapshot = make_torrent_snapshot(*catalog_, *blobs_, codec_);
  if (!snapshot)
    return std::unexpected(snapshot.error());
  return std::shared_ptr<
      const KeyedSnapshot<model::TorrentRecord, core::InfoHash, AllTorrents>>{
      std::move(*snapshot)};
}

core::Result<std::unique_ptr<WriteSession<model::TorrentRecord>>>
TorrentDataset::begin_write() {
  auto base = catalog_->pin_current();
  if (!base)
    return std::unexpected(base.error());
  auto writer = RowV1SegmentWriter::create(*blobs_, segment_header_);
  if (!writer)
    return std::unexpected(writer.error());
  return std::unique_ptr<WriteSession<model::TorrentRecord>>{
      std::make_unique<TorrentWriteSession>(
          *blobs_, *catalog_, codec_, std::move(*base), std::move(*writer))};
}

core::Result<CommitResult>
TorrentDataset::enrich(model::TorrentRecord metadata) {
  if (!metadata.name || metadata.name->empty() || metadata.files.empty() ||
      metadata.first_seen > metadata.last_seen)
    return std::unexpected(core::Error{core::ErrorCode::InvalidArgument,
                                       "Torrent enrichment requires validated "
                                       "metadata and a valid seen range"});
  std::uint64_t total{};
  for (const auto &file : metadata.files) {
    if (file.path.empty() ||
        file.size > std::numeric_limits<std::uint64_t>::max() - total)
      return std::unexpected(
          core::Error{core::ErrorCode::InvalidArgument,
                      "Torrent enrichment contains an invalid file list"});
    total += file.size;
  }
  if (total != metadata.total_size)
    return std::unexpected(
        core::Error{core::ErrorCode::InvalidArgument,
                    "Torrent enrichment total size does not match its files"});

  auto base = catalog_->pin_current();
  if (!base)
    return std::unexpected(base.error());
  TorrentSnapshot snapshot{*base, *blobs_, codec_};
  auto existing = snapshot.get(metadata.info_hash);
  if (!existing)
    return std::unexpected(existing.error());
  if (*existing) {
    metadata.first_seen =
        std::min(metadata.first_seen, (*existing)->first_seen);
    metadata.last_seen = std::max(metadata.last_seen, (*existing)->last_seen);
  }

  auto writer = RowV1SegmentWriter::create(*blobs_, segment_header_);
  if (!writer)
    return std::unexpected(writer.error());
  TorrentWriteSession write{*blobs_, *catalog_, codec_, std::move(*base),
                            std::move(*writer)};
  if (auto appended = write.append(metadata); !appended)
    return std::unexpected(appended.error());
  return write.commit();
}

} // namespace sakuin::storage

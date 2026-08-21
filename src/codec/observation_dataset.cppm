export module sakuin.storage.dataset.observations;

import std;

import sakuin.core.bytes;
import sakuin.core.result;
import sakuin.core.time;
import sakuin.model.observation;
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

struct AllObservations {};

class ObservationDataset final
    : public Dataset<model::ObservationRecord, AllObservations> {
public:
  ObservationDataset(BlobStore &blobs, ManifestCatalog &catalog,
                     SegmentHeader segment_header = {});

  core::Result<std::shared_ptr<const Snapshot<model::ObservationRecord,
                                               AllObservations>>>
  snapshot() const override;
  core::Result<std::unique_ptr<WriteSession<model::ObservationRecord>>>
  begin_write() override;

private:
  BlobStore *blobs_;
  ManifestCatalog *catalog_;
  SegmentHeader segment_header_;
  ObservationRecordCodec codec_;
};

} // namespace sakuin::storage

namespace sakuin::storage {
namespace {

class ObservationStream final : public RecordStream<model::ObservationRecord> {
public:
  ObservationStream(std::shared_ptr<const ManifestPin> pin, BlobStore &blobs,
                    const ObservationRecordCodec &codec)
      : pin_(std::move(pin)), blobs_(&blobs), codec_(codec) {}

  core::Result<std::optional<model::ObservationRecord>> next() override {
    while (segment_index_ < pin_->manifest().segments.size()) {
      if (!reader_) {
        const auto &descriptor = pin_->manifest().segments[segment_index_];
        if (descriptor.encoding != SegmentEncoding::RowV1 ||
            descriptor.schema_id != ObservationRecordSchema ||
            descriptor.schema_version.value != codec_.version().value)
          return std::unexpected(core::Error{
              core::ErrorCode::UnsupportedFormat,
              "Observation segment uses an unsupported encoding or schema"});
        auto opened = RowV1SegmentReader::open(
            *blobs_, descriptor.object);
        if (!opened)
          return std::unexpected(opened.error());
        if ((*opened)->header().schema_id != descriptor.schema_id ||
            (*opened)->header().schema_version != descriptor.schema_version ||
            (*opened)->header().format_version != descriptor.format_version ||
            (*opened)->header().encoding != descriptor.encoding ||
            (*opened)->header().compression != descriptor.compression ||
            (*opened)->header().tier != descriptor.tier ||
            (*opened)->record_count() != descriptor.record_count)
          return std::unexpected(core::Error{
              core::ErrorCode::CorruptSegment,
              "Observation manifest metadata does not match its segment"});
        reader_ = std::move(*opened);
      }
      if (record_index_ < reader_->record_count()) {
        auto location = reader_->location(record_index_++);
        if (!location)
          return std::unexpected(location.error());
        auto encoded = reader_->read(*location);
        if (!encoded)
          return std::unexpected(encoded.error());
        auto decoded = codec_.decode(*encoded);
        if (!decoded)
          return std::unexpected(decoded.error());
        return std::optional<model::ObservationRecord>{std::move(*decoded)};
      }
      reader_.reset();
      record_index_ = 0;
      ++segment_index_;
    }
    return std::optional<model::ObservationRecord>{};
  }

private:
  std::shared_ptr<const ManifestPin> pin_;
  BlobStore *blobs_;
  ObservationRecordCodec codec_;
  std::size_t segment_index_{};
  std::uint64_t record_index_{};
  std::unique_ptr<RowV1SegmentReader> reader_;
};

class ObservationSnapshot final
    : public Snapshot<model::ObservationRecord, AllObservations> {
public:
  ObservationSnapshot(std::shared_ptr<const ManifestPin> pin, BlobStore &blobs,
                      const ObservationRecordCodec &codec)
      : Snapshot(std::move(pin)), blobs_(&blobs), codec_(codec),
        created_at_(std::chrono::system_clock::now()) {}

  core::Timestamp created_at() const noexcept override { return created_at_; }

  core::Result<std::unique_ptr<RecordStream<model::ObservationRecord>>>
  scan(const AllObservations &) const override {
    return std::unique_ptr<RecordStream<model::ObservationRecord>>{
        std::make_unique<ObservationStream>(pin(), *blobs_, codec_)};
  }

private:
  BlobStore *blobs_;
  ObservationRecordCodec codec_;
  core::Timestamp created_at_;
};

class ObservationWriteSession final
    : public WriteSession<model::ObservationRecord> {
public:
  ObservationWriteSession(BlobStore &blobs, ManifestCatalog &catalog,
                          const ObservationRecordCodec &codec,
                          std::shared_ptr<const ManifestPin> base,
                          std::unique_ptr<RowV1SegmentWriter> writer)
      : blobs_(&blobs), catalog_(&catalog), codec_(codec),
        base_(std::move(base)), writer_(std::move(writer)) {}

  ~ObservationWriteSession() override { abort(); }

  core::Result<void> append(const model::ObservationRecord &record) override {
    if (!active_)
      return std::unexpected(core::Error{core::ErrorCode::InvalidArgument,
                                         "Write session is no longer active"});
    core::ByteBuffer encoded;
    if (auto result = codec_.encode(record, encoded); !result)
      return result;
    if (auto result = writer_->append(encoded); !result)
      return result;
    minimum_ = minimum_ ? std::min(*minimum_, record.observed_at)
                        : record.observed_at;
    maximum_ = maximum_ ? std::max(*maximum_, record.observed_at)
                        : record.observed_at;
    ++count_;
    return {};
  }

  core::Result<void>
  append(std::span<const model::ObservationRecord> records) override {
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
      writer_->abort();
      active_ = false;
      return CommitResult{.generation = base_->manifest().id.generation};
    }
    if (catalog_->current_id() != base_->manifest().id)
      return std::unexpected(core::Error{core::ErrorCode::Conflict,
                                         "Dataset changed during write"});
    auto descriptor = writer_->finalize();
    if (!descriptor)
      return std::unexpected(descriptor.error());
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
    if (count_ != 0)
      segments.push_back(*descriptor);
    auto published = catalog_->publish(base_->manifest().id, std::move(segments));
    if (!published)
      return std::unexpected(published.error());
    active_ = false;
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
  ObservationRecordCodec codec_;
  std::shared_ptr<const ManifestPin> base_;
  std::unique_ptr<RowV1SegmentWriter> writer_;
  std::optional<core::Timestamp> minimum_;
  std::optional<core::Timestamp> maximum_;
  std::uint64_t count_{};
  bool active_{true};
};

} // namespace

ObservationDataset::ObservationDataset(BlobStore &blobs, ManifestCatalog &catalog,
                                       SegmentHeader header)
    : blobs_(&blobs), catalog_(&catalog), segment_header_(header) {
  segment_header_.schema_version.value = codec_.version().value;
  segment_header_.schema_id = ObservationRecordSchema;
}

core::Result<std::shared_ptr<const Snapshot<model::ObservationRecord,
                                             AllObservations>>>
ObservationDataset::snapshot() const {
  auto pin = catalog_->pin_current();
  if (!pin)
    return std::unexpected(pin.error());
  return std::shared_ptr<const Snapshot<model::ObservationRecord,
                                        AllObservations>>{
      std::make_shared<ObservationSnapshot>(std::move(*pin), *blobs_, codec_)};
}

core::Result<std::unique_ptr<WriteSession<model::ObservationRecord>>>
ObservationDataset::begin_write() {
  auto base = catalog_->pin_current();
  if (!base)
    return std::unexpected(base.error());
  auto writer = RowV1SegmentWriter::create(*blobs_, segment_header_);
  if (!writer)
    return std::unexpected(writer.error());
  return std::unique_ptr<WriteSession<model::ObservationRecord>>{
      std::make_unique<ObservationWriteSession>(
          *blobs_, *catalog_, codec_, std::move(*base),
          std::move(*writer))};
}

} // namespace sakuin::storage

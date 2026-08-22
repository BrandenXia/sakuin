export module sakuin.integration.work_results;

import std;

import sakuin.core.bytes;
import sakuin.core.result;
import sakuin.core.time;
import sakuin.dht.observation;
import sakuin.model.observation;
import sakuin.model.torrent;
import sakuin.scheduler.work;
import sakuin.storage.admin.compaction;
import sakuin.storage.blob.store;
import sakuin.storage.catalog.manifest;
import sakuin.storage.codec.model_records;
import sakuin.storage.dataset.torrents;
import sakuin.storage.format.row_v1;
import sakuin.storage.format.segment;

export namespace sakuin::integration {

inline constexpr storage::SchemaId WorkResultReceiptSchema{100};

struct WorkResultReceipt {
  scheduler::WorkId id;
  scheduler::WorkResultKind kind{};

  friend bool operator==(const WorkResultReceipt &,
                         const WorkResultReceipt &) = default;
};

// Constructs the content-derived id expected by the durable inbox.
scheduler::WorkResultBatch
make_work_result_batch(scheduler::WorkResultKind kind,
                       core::ByteBuffer payload);

core::Result<core::ByteBuffer> encode_observation_result_batch(
    std::span<const model::ObservationRecord> observations);
core::Result<std::vector<model::ObservationRecord>>
decode_observation_result_batch(core::ByteView payload,
                                std::size_t maximum_records = 65'536);

core::Result<core::ByteBuffer> encode_torrent_metadata_result_batch(
    std::span<const model::TorrentRecord> torrents);
core::Result<std::vector<model::TorrentRecord>>
decode_torrent_metadata_result_batch(
    core::ByteView payload, std::size_t maximum_records = 1'024,
    storage::TorrentRecordDecodeLimits limits = {});

// DHT-facing batching adapter for a remote crawler. It knows only logical
// observations and the generic result channel, never canonical segment or
// filesystem details.
class RemoteObservationSink final : public dht::ObservationSink {
public:
  RemoteObservationSink(scheduler::WorkResultPublisher &publisher,
                        std::string worker, std::size_t batch_size);
  ~RemoteObservationSink();

  core::Result<void>
  observe(const model::ObservationRecord &observation) override;
  core::Result<void> flush();
  std::size_t pending() const noexcept;

private:
  core::Result<void> flush_locked();

  scheduler::WorkResultPublisher *publisher_;
  std::string worker_;
  std::size_t batch_size_{};
  mutable std::mutex mutex_;
  std::vector<model::ObservationRecord> pending_;
};

// Canonical, backend-neutral result inbox. Each acknowledged batch is retained
// as a compact content identity in a verified immutable RowV1 object. A retry
// with the same content id is idempotent, including after process restart and
// receipt compaction. Legacy schema-v1 full-payload receipts remain readable.
class CanonicalWorkResultInbox final : public scheduler::WorkResultPublisher {
public:
  CanonicalWorkResultInbox(storage::BlobStore &blobs,
                           storage::ManifestCatalog &catalog,
                           storage::SegmentHeader header = {});

  core::Result<bool> publish_result(std::string_view worker,
                                    scheduler::WorkResultBatch batch) override;
  core::Result<std::vector<WorkResultReceipt>> scan_receipts() const;
  core::Result<bool> contains(const scheduler::WorkResultBatch &batch) const;
  core::Result<storage::CompactionResult>
  compact(const storage::CompactionPolicy &policy = {});

private:
  core::Result<void> load_index_locked() const;

  storage::BlobStore *blobs_;
  storage::ManifestCatalog *catalog_;
  storage::SegmentHeader header_;
  mutable std::mutex mutex_;
  mutable bool index_loaded_{};
  mutable std::uint64_t indexed_generation_{};
  mutable std::map<scheduler::WorkId, scheduler::WorkResultKind> index_;
};

// Converts a remotely produced logical observation batch into a canonical
// observation segment. The result id becomes the logical segment id, making
// retry detection part of the same atomic observation-manifest publication.
class CanonicalObservationResultPublisher final
    : public scheduler::WorkResultPublisher {
public:
  CanonicalObservationResultPublisher(storage::BlobStore &blobs,
                                      storage::ManifestCatalog &catalog,
                                      storage::SegmentHeader header = {});

  core::Result<bool> publish_result(std::string_view worker,
                                    scheduler::WorkResultBatch batch) override;

private:
  storage::BlobStore *blobs_;
  storage::ManifestCatalog *catalog_;
  storage::SegmentHeader header_;
  mutable std::mutex mutex_;
};

// Validates remote metadata and idempotently enriches the coordinator-owned
// keyed torrent dataset. Identical replay is a no-op at the dataset boundary.
class CanonicalTorrentMetadataResultPublisher final
    : public scheduler::WorkResultPublisher {
public:
  CanonicalTorrentMetadataResultPublisher(
      storage::TorrentDataset &torrents,
      std::size_t maximum_conflict_attempts = 3,
      std::function<void(std::uint64_t)> on_torrent_committed = {});

  core::Result<bool> publish_result(std::string_view worker,
                                    scheduler::WorkResultBatch batch) override;
  void set_on_torrent_committed(
      std::function<void(std::uint64_t)> on_torrent_committed);

private:
  storage::TorrentDataset *torrents_;
  std::size_t maximum_conflict_attempts_{};
  std::function<void(std::uint64_t)> on_torrent_committed_;
  std::mutex mutex_;
};

// Routes each bounded result kind to its coordinator-owned canonical dataset,
// then appends the exact batch to a durable receipt log before acknowledging
// success. A recorded receipt makes ordinary replay independent of later data
// compaction while failed receipt publication remains safely retryable.
class CanonicalWorkResultRouter final : public scheduler::WorkResultPublisher {
public:
  CanonicalWorkResultRouter(CanonicalObservationResultPublisher &observations,
                            CanonicalTorrentMetadataResultPublisher &metadata,
                            CanonicalWorkResultInbox &inbox)
      : observations_(&observations), metadata_(&metadata), inbox_(&inbox) {}

  core::Result<bool> publish_result(std::string_view worker,
                                    scheduler::WorkResultBatch batch) override;

private:
  CanonicalObservationResultPublisher *observations_;
  CanonicalTorrentMetadataResultPublisher *metadata_;
  CanonicalWorkResultInbox *inbox_;
  std::mutex mutex_;
};

} // namespace sakuin::integration

namespace sakuin::integration {
namespace {

void append_u32(core::ByteBuffer &output, std::uint32_t value) {
  for (unsigned shift = 0; shift < 32; shift += 8)
    output.push_back(static_cast<std::byte>(value >> shift));
}

core::Result<std::uint32_t> read_u32(core::ByteView input,
                                     std::size_t &position) {
  if (position > input.size() || input.size() - position < 4)
    return std::unexpected(core::Error{core::ErrorCode::CorruptSegment,
                                       "Work-result record is truncated"});
  std::uint32_t result{};
  for (unsigned shift = 0; shift < 32; shift += 8)
    result |= static_cast<std::uint32_t>(
                  std::to_integer<std::uint8_t>(input[position++]))
              << shift;
  return result;
}

constexpr std::array<core::Byte, 8> ObservationBatchMagic{
    core::Byte{'S'}, core::Byte{'A'}, core::Byte{'K'}, core::Byte{'O'},
    core::Byte{'B'}, core::Byte{'S'}, core::Byte{'1'}, core::Byte{0}};

constexpr std::array<core::Byte, 8> TorrentMetadataBatchMagic{
    core::Byte{'S'}, core::Byte{'A'}, core::Byte{'K'}, core::Byte{'M'},
    core::Byte{'T'}, core::Byte{'D'}, core::Byte{'1'}, core::Byte{0}};

bool valid_result_kind(std::uint8_t kind) {
  return kind >= static_cast<std::uint8_t>(
                     scheduler::WorkResultKind::ObservationBatch) &&
         kind <= static_cast<std::uint8_t>(
                     scheduler::WorkResultKind::TorrentMetadataBatch);
}

core::ByteBuffer encode_receipt(const WorkResultReceipt &receipt) {
  core::ByteBuffer encoded;
  encoded.reserve(17);
  for (const auto byte : receipt.id.bytes)
    encoded.push_back(static_cast<std::byte>(byte));
  encoded.push_back(static_cast<std::byte>(receipt.kind));
  return encoded;
}

core::Result<WorkResultReceipt> decode_receipt(core::ByteView encoded,
                                               std::uint32_t schema_version) {
  if ((schema_version == 1 && encoded.size() < 21) ||
      (schema_version == 2 && encoded.size() != 17))
    return std::unexpected(core::Error{core::ErrorCode::CorruptSegment,
                                       "Work-result receipt size is invalid"});
  WorkResultReceipt receipt;
  for (std::size_t index = 0; index < receipt.id.bytes.size(); ++index)
    receipt.id.bytes[index] = std::to_integer<std::uint8_t>(encoded[index]);
  const auto kind = std::to_integer<std::uint8_t>(encoded[16]);
  if (!valid_result_kind(kind))
    return std::unexpected(core::Error{core::ErrorCode::CorruptSegment,
                                       "Work-result kind is invalid"});
  receipt.kind = static_cast<scheduler::WorkResultKind>(kind);
  if (schema_version == 2)
    return receipt;

  // Schema v1 stored the entire result payload. Read and validate it during
  // migration so a corrupt legacy receipt cannot suppress canonical data.
  std::size_t position = 17;
  auto size = read_u32(encoded, position);
  if (!size || *size == 0 || encoded.size() - position != *size)
    return std::unexpected(size ? core::Error{core::ErrorCode::CorruptSegment,
                                              "Work-result length is invalid"}
                                : size.error());
  const auto payload = encoded.subspan(position, *size);
  const auto expected =
      scheduler::content_work_result_id(receipt.kind, payload);
  if (expected != receipt.id)
    return std::unexpected(core::Error{core::ErrorCode::ChecksumMismatch,
                                       "Work-result content id is invalid"});
  return receipt;
}

core::Result<std::vector<WorkResultReceipt>>
read_receipt_segment(storage::BlobStore &blobs,
                     const storage::SegmentDescriptor &descriptor) {
  if (descriptor.encoding != storage::SegmentEncoding::RowV1 ||
      descriptor.schema_id != WorkResultReceiptSchema ||
      (descriptor.schema_version.value != 1 &&
       descriptor.schema_version.value != 2) ||
      (descriptor.schema_version.value == 1 && descriptor.record_count != 1))
    return std::unexpected(
        core::Error{core::ErrorCode::UnsupportedFormat,
                    "Work-result inbox segment uses an unsupported schema"});
  auto reader = storage::RowV1SegmentReader::open(blobs, descriptor.object);
  if (!reader)
    return std::unexpected(reader.error());
  const auto &header = (*reader)->header();
  if (header.format_version != descriptor.format_version ||
      header.schema_id != descriptor.schema_id ||
      header.schema_version != descriptor.schema_version ||
      header.encoding != descriptor.encoding ||
      header.tier != descriptor.tier ||
      header.compression != descriptor.compression)
    return std::unexpected(core::Error{
        core::ErrorCode::CorruptSegment,
        "Work-result manifest metadata does not match its segment"});
  if (auto verified = (*reader)->verify(); !verified)
    return std::unexpected(verified.error());
  if ((*reader)->record_count() != descriptor.record_count)
    return std::unexpected(core::Error{core::ErrorCode::CorruptSegment,
                                       "Work-result receipt count differs"});
  std::vector<WorkResultReceipt> receipts;
  receipts.reserve(static_cast<std::size_t>(descriptor.record_count));
  for (std::uint64_t ordinal = 0; ordinal < descriptor.record_count;
       ++ordinal) {
    auto location = (*reader)->location(ordinal);
    if (!location)
      return std::unexpected(location.error());
    auto encoded = (*reader)->read(*location);
    if (!encoded)
      return std::unexpected(encoded.error());
    auto receipt = decode_receipt(*encoded, descriptor.schema_version.value);
    if (!receipt)
      return std::unexpected(receipt.error());
    receipts.push_back(*receipt);
  }
  return receipts;
}

} // namespace

scheduler::WorkResultBatch
make_work_result_batch(scheduler::WorkResultKind kind,
                       core::ByteBuffer payload) {
  const auto id = scheduler::content_work_result_id(kind, payload);
  return {.id = id, .kind = kind, .payload = std::move(payload)};
}

core::Result<core::ByteBuffer> encode_observation_result_batch(
    std::span<const model::ObservationRecord> observations) {
  if (observations.empty() ||
      observations.size() > std::numeric_limits<std::uint32_t>::max())
    return std::unexpected(core::Error{
        core::ErrorCode::InvalidArgument,
        "Observation result batch must contain a bounded nonempty set"});
  storage::ObservationRecordCodec codec;
  core::ByteBuffer output{ObservationBatchMagic.begin(),
                          ObservationBatchMagic.end()};
  append_u32(output, static_cast<std::uint32_t>(observations.size()));
  core::ByteBuffer encoded;
  for (const auto &observation : observations) {
    if (auto result = codec.encode(observation, encoded); !result)
      return std::unexpected(result.error());
    append_u32(output, static_cast<std::uint32_t>(encoded.size()));
    output.insert(output.end(), encoded.begin(), encoded.end());
  }
  return output;
}

core::Result<std::vector<model::ObservationRecord>>
decode_observation_result_batch(core::ByteView payload,
                                std::size_t maximum_records) {
  if (payload.size() < ObservationBatchMagic.size() + 4 ||
      !std::ranges::equal(payload.first(ObservationBatchMagic.size()),
                          ObservationBatchMagic))
    return std::unexpected(core::Error{core::ErrorCode::CorruptSegment,
                                       "Observation result magic is invalid"});
  std::size_t position = ObservationBatchMagic.size();
  auto count = read_u32(payload, position);
  if (!count || *count == 0 || *count > maximum_records)
    return std::unexpected(
        count ? core::Error{core::ErrorCode::QuotaExceeded,
                            "Observation result count exceeds limit"}
              : count.error());
  storage::ObservationRecordCodec codec;
  std::vector<model::ObservationRecord> observations;
  observations.reserve(*count);
  for (std::uint32_t index = 0; index < *count; ++index) {
    auto size = read_u32(payload, position);
    if (!size || position > payload.size() || payload.size() - position < *size)
      return std::unexpected(
          size ? core::Error{core::ErrorCode::CorruptSegment,
                             "Observation result is truncated"}
               : size.error());
    auto decoded = codec.decode(payload.subspan(position, *size));
    if (!decoded)
      return std::unexpected(decoded.error());
    observations.push_back(std::move(*decoded));
    position += *size;
  }
  if (position != payload.size())
    return std::unexpected(
        core::Error{core::ErrorCode::CorruptSegment,
                    "Observation result contains trailing bytes"});
  return observations;
}

core::Result<core::ByteBuffer> encode_torrent_metadata_result_batch(
    std::span<const model::TorrentRecord> torrents) {
  if (torrents.empty() ||
      torrents.size() > std::numeric_limits<std::uint32_t>::max())
    return std::unexpected(core::Error{
        core::ErrorCode::InvalidArgument,
        "Torrent metadata result must contain a bounded nonempty set"});
  storage::TorrentRecordCodec codec;
  core::ByteBuffer output{TorrentMetadataBatchMagic.begin(),
                          TorrentMetadataBatchMagic.end()};
  append_u32(output, static_cast<std::uint32_t>(torrents.size()));
  core::ByteBuffer encoded;
  for (const auto &torrent : torrents) {
    if (auto result = codec.encode(torrent, encoded); !result)
      return std::unexpected(result.error());
    if (encoded.size() > std::numeric_limits<std::uint32_t>::max())
      return std::unexpected(core::Error{
          core::ErrorCode::InvalidArgument,
          "Encoded torrent metadata exceeds the batch record limit"});
    append_u32(output, static_cast<std::uint32_t>(encoded.size()));
    output.insert(output.end(), encoded.begin(), encoded.end());
  }
  return output;
}

core::Result<std::vector<model::TorrentRecord>>
decode_torrent_metadata_result_batch(
    core::ByteView payload, std::size_t maximum_records,
    storage::TorrentRecordDecodeLimits limits) {
  if (payload.size() < TorrentMetadataBatchMagic.size() + 4 ||
      !std::ranges::equal(payload.first(TorrentMetadataBatchMagic.size()),
                          TorrentMetadataBatchMagic))
    return std::unexpected(
        core::Error{core::ErrorCode::CorruptSegment,
                    "Torrent metadata result magic is invalid"});
  std::size_t position = TorrentMetadataBatchMagic.size();
  auto count = read_u32(payload, position);
  if (!count || *count == 0 || *count > maximum_records)
    return std::unexpected(
        count ? core::Error{core::ErrorCode::QuotaExceeded,
                            "Torrent metadata result count exceeds limit"}
              : count.error());
  storage::TorrentRecordCodec codec;
  std::vector<model::TorrentRecord> torrents;
  torrents.reserve(*count);
  for (std::uint32_t index = 0; index < *count; ++index) {
    auto size = read_u32(payload, position);
    if (!size || position > payload.size() || payload.size() - position < *size)
      return std::unexpected(
          size ? core::Error{core::ErrorCode::CorruptSegment,
                             "Torrent metadata result is truncated"}
               : size.error());
    auto decoded = codec.decode(payload.subspan(position, *size), limits);
    if (!decoded)
      return std::unexpected(decoded.error());
    torrents.push_back(std::move(*decoded));
    position += *size;
  }
  if (position != payload.size())
    return std::unexpected(
        core::Error{core::ErrorCode::CorruptSegment,
                    "Torrent metadata result contains trailing bytes"});
  return torrents;
}

RemoteObservationSink::RemoteObservationSink(
    scheduler::WorkResultPublisher &publisher, std::string worker,
    std::size_t batch_size)
    : publisher_(&publisher), worker_(std::move(worker)),
      batch_size_(batch_size) {
  if (batch_size_ != 0)
    pending_.reserve(batch_size_);
}

RemoteObservationSink::~RemoteObservationSink() { static_cast<void>(flush()); }

core::Result<void>
RemoteObservationSink::observe(const model::ObservationRecord &observation) {
  std::lock_guard lock{mutex_};
  if (batch_size_ == 0 || worker_.empty())
    return std::unexpected(core::Error{core::ErrorCode::InvalidArgument,
                                       "Remote observation sink is invalid"});
  pending_.push_back(observation);
  if (pending_.size() >= batch_size_)
    return flush_locked();
  return {};
}

core::Result<void> RemoteObservationSink::flush() {
  std::lock_guard lock{mutex_};
  return flush_locked();
}

std::size_t RemoteObservationSink::pending() const noexcept {
  std::lock_guard lock{mutex_};
  return pending_.size();
}

core::Result<void> RemoteObservationSink::flush_locked() {
  if (pending_.empty())
    return {};
  auto payload = encode_observation_result_batch(pending_);
  if (!payload)
    return std::unexpected(payload.error());
  auto batch = make_work_result_batch(
      scheduler::WorkResultKind::ObservationBatch, std::move(*payload));
  auto published = publisher_->publish_result(worker_, std::move(batch));
  if (!published)
    return std::unexpected(published.error());
  pending_.clear();
  return {};
}

CanonicalWorkResultInbox::CanonicalWorkResultInbox(
    storage::BlobStore &blobs, storage::ManifestCatalog &catalog,
    storage::SegmentHeader header)
    : blobs_(&blobs), catalog_(&catalog), header_(header) {
  header_.format_version = {1, 1};
  header_.schema_id = WorkResultReceiptSchema;
  header_.schema_version = {2};
  header_.encoding = storage::SegmentEncoding::RowV1;
  header_.tier = storage::SegmentTier::Hot;
}

core::Result<bool>
CanonicalWorkResultInbox::publish_result(std::string_view worker,
                                         scheduler::WorkResultBatch batch) {
  if (worker.empty() || worker.size() > 128)
    return std::unexpected(core::Error{core::ErrorCode::InvalidArgument,
                                       "Work-result worker id is invalid"});
  if (!valid_result_kind(static_cast<std::uint8_t>(batch.kind)) ||
      batch.payload.empty() ||
      batch.id != scheduler::content_work_result_id(batch.kind, batch.payload))
    return std::unexpected(core::Error{core::ErrorCode::ChecksumMismatch,
                                       "Work-result content id is invalid"});
  const WorkResultReceipt receipt{.id = batch.id, .kind = batch.kind};
  const auto encoded = encode_receipt(receipt);

  std::lock_guard lock{mutex_};
  if (auto indexed = load_index_locked(); !indexed)
    return std::unexpected(indexed.error());
  if (const auto found = index_.find(batch.id); found != index_.end()) {
    if (found->second == batch.kind)
      return false;
    return std::unexpected(core::Error{
        core::ErrorCode::Conflict,
        "Work-result id is already associated with different content"});
  }
  auto base = catalog_->pin_current();
  if (!base)
    return std::unexpected(base.error());

  auto writer = storage::RowV1SegmentWriter::create(*blobs_, header_);
  if (!writer)
    return std::unexpected(writer.error());
  if (auto appended = (*writer)->append(encoded); !appended)
    return std::unexpected(appended.error());
  auto descriptor = (*writer)->finalize();
  if (!descriptor)
    return std::unexpected(descriptor.error());
  auto verified = read_receipt_segment(*blobs_, *descriptor);
  if (!verified || verified->size() != 1 || verified->front() != receipt)
    return std::unexpected(
        verified ? core::Error{core::ErrorCode::CorruptSegment,
                               "Work-result receipt verification failed"}
                 : verified.error());
  auto segments = (*base)->manifest().segments;
  segments.push_back(*descriptor);
  auto published =
      catalog_->publish((*base)->manifest().id, std::move(segments));
  if (!published)
    return std::unexpected(published.error());
  index_.emplace(batch.id, batch.kind);
  indexed_generation_ = published->generation;
  return true;
}

core::Result<void> CanonicalWorkResultInbox::load_index_locked() const {
  const auto current = catalog_->current_id().generation;
  if (index_loaded_ && indexed_generation_ == current)
    return {};
  auto pin = catalog_->pin_current();
  if (!pin)
    return std::unexpected(pin.error());
  std::map<scheduler::WorkId, scheduler::WorkResultKind> loaded;
  for (const auto &segment : (*pin)->manifest().segments) {
    auto receipts = read_receipt_segment(*blobs_, segment);
    if (!receipts)
      return std::unexpected(receipts.error());
    for (const auto &receipt : *receipts) {
      const auto [found, inserted] = loaded.emplace(receipt.id, receipt.kind);
      if (!inserted && found->second != receipt.kind)
        return std::unexpected(
            core::Error{core::ErrorCode::Conflict,
                        "Work-result inbox contains a conflicting content id"});
    }
  }
  index_ = std::move(loaded);
  index_loaded_ = true;
  indexed_generation_ = (*pin)->manifest().id.generation;
  return {};
}

core::Result<std::vector<WorkResultReceipt>>
CanonicalWorkResultInbox::scan_receipts() const {
  std::lock_guard lock{mutex_};
  if (auto indexed = load_index_locked(); !indexed)
    return std::unexpected(indexed.error());
  std::vector<WorkResultReceipt> receipts;
  receipts.reserve(index_.size());
  for (const auto &[id, kind] : index_)
    receipts.push_back({.id = id, .kind = kind});
  return receipts;
}

core::Result<bool> CanonicalWorkResultInbox::contains(
    const scheduler::WorkResultBatch &batch) const {
  if (!valid_result_kind(static_cast<std::uint8_t>(batch.kind)) ||
      batch.payload.empty() ||
      batch.id != scheduler::content_work_result_id(batch.kind, batch.payload))
    return std::unexpected(core::Error{core::ErrorCode::ChecksumMismatch,
                                       "Work-result content id is invalid"});
  std::lock_guard lock{mutex_};
  if (auto indexed = load_index_locked(); !indexed)
    return std::unexpected(indexed.error());
  if (const auto found = index_.find(batch.id); found != index_.end()) {
    if (found->second == batch.kind)
      return true;
    return std::unexpected(
        core::Error{core::ErrorCode::Conflict,
                    "Work-result id is associated with a different kind"});
  }
  return false;
}

core::Result<storage::CompactionResult>
CanonicalWorkResultInbox::compact(const storage::CompactionPolicy &policy) {
  if (policy.minimum_segment_count < 2 || policy.target_block_size == 0)
    return std::unexpected(core::Error{
        core::ErrorCode::InvalidArgument,
        "Receipt compaction requires at least two segments and a nonzero "
        "block size"});

  std::lock_guard lock{mutex_};
  if (auto indexed = load_index_locked(); !indexed)
    return std::unexpected(indexed.error());
  auto base = catalog_->pin_current();
  if (!base)
    return std::unexpected(base.error());
  const auto &manifest = (*base)->manifest();
  if (manifest.segments.empty())
    return storage::CompactionResult{.source_generation =
                                         manifest.id.generation};
  const bool has_legacy = std::ranges::any_of(
      manifest.segments, [](const storage::SegmentDescriptor &segment) {
        return segment.schema_version.value == 1;
      });
  if (!has_legacy && manifest.segments.size() < policy.minimum_segment_count)
    return storage::CompactionResult{.source_generation =
                                         manifest.id.generation};

  auto output_header = header_;
  output_header.target_block_size = policy.target_block_size;
  output_header.compression = policy.compression;
  auto writer = storage::RowV1SegmentWriter::create(
      *blobs_, output_header,
      storage::RowV1WriterOptions{.compression_level =
                                      policy.compression_level});
  if (!writer)
    return std::unexpected(writer.error());
  std::uint64_t bytes_before{};
  for (const auto &segment : manifest.segments)
    bytes_before += segment.physical_size;
  for (const auto &[id, kind] : index_) {
    const auto encoded = encode_receipt({.id = id, .kind = kind});
    if (auto appended = (*writer)->append(encoded); !appended) {
      (*writer)->abort();
      return std::unexpected(appended.error());
    }
  }
  auto replacement = (*writer)->finalize();
  if (!replacement)
    return std::unexpected(replacement.error());
  auto verified = read_receipt_segment(*blobs_, *replacement);
  if (!verified || verified->size() != index_.size())
    return std::unexpected(
        verified ? core::Error{core::ErrorCode::CorruptSegment,
                               "Compacted receipt set is incomplete"}
                 : verified.error());
  for (const auto &receipt : *verified) {
    const auto found = index_.find(receipt.id);
    if (found == index_.end() || found->second != receipt.kind)
      return std::unexpected(
          core::Error{core::ErrorCode::CorruptSegment,
                      "Compacted receipt set contains an unexpected identity"});
  }
  auto published = catalog_->publish(manifest.id, {*replacement});
  if (!published)
    return std::unexpected(published.error());
  indexed_generation_ = published->generation;
  return storage::CompactionResult{.source_generation = published->generation,
                                   .segments_created = 1,
                                   .segments_removed = manifest.segments.size(),
                                   .bytes_before = bytes_before,
                                   .bytes_after = replacement->physical_size};
}

CanonicalObservationResultPublisher::CanonicalObservationResultPublisher(
    storage::BlobStore &blobs, storage::ManifestCatalog &catalog,
    storage::SegmentHeader header)
    : blobs_(&blobs), catalog_(&catalog), header_(header) {
  storage::ObservationRecordCodec codec;
  header_.format_version = {1, 1};
  header_.schema_id = storage::ObservationRecordSchema;
  header_.schema_version = {.value = codec.version().value};
  header_.encoding = storage::SegmentEncoding::RowV1;
  header_.tier = storage::SegmentTier::Hot;
}

core::Result<bool> CanonicalObservationResultPublisher::publish_result(
    std::string_view worker, scheduler::WorkResultBatch batch) {
  if (worker.empty() || worker.size() > 128 ||
      batch.kind != scheduler::WorkResultKind::ObservationBatch)
    return std::unexpected(core::Error{
        core::ErrorCode::InvalidArgument,
        "Canonical observation publisher received an invalid result"});
  if (batch.id != scheduler::content_work_result_id(batch.kind, batch.payload))
    return std::unexpected(core::Error{core::ErrorCode::ChecksumMismatch,
                                       "Work-result content id is invalid"});
  auto observations = decode_observation_result_batch(batch.payload);
  if (!observations)
    return std::unexpected(observations.error());

  storage::ObservationRecordCodec codec;
  auto writer = storage::RowV1SegmentWriter::create(*blobs_, header_);
  if (!writer)
    return std::unexpected(writer.error());
  core::ByteBuffer encoded;
  std::optional<core::Timestamp> minimum;
  std::optional<core::Timestamp> maximum;
  for (const auto &observation : *observations) {
    if (auto result = codec.encode(observation, encoded); !result)
      return std::unexpected(result.error());
    if (auto result = (*writer)->append(encoded); !result)
      return std::unexpected(result.error());
    minimum = minimum ? std::min(*minimum, observation.observed_at)
                      : observation.observed_at;
    maximum = maximum ? std::max(*maximum, observation.observed_at)
                      : observation.observed_at;
  }
  auto descriptor = (*writer)->finalize();
  if (!descriptor)
    return std::unexpected(descriptor.error());
  std::ranges::copy(batch.id.bytes, descriptor->id.bytes.begin());
  descriptor->min_timestamp = minimum;
  descriptor->max_timestamp = maximum;
  auto verified =
      storage::RowV1SegmentReader::open(*blobs_, descriptor->object);
  if (!verified)
    return std::unexpected(verified.error());
  if (auto integrity = (*verified)->verify(); !integrity)
    return std::unexpected(integrity.error());
  if ((*verified)->record_count() != descriptor->record_count)
    return std::unexpected(
        core::Error{core::ErrorCode::CorruptSegment,
                    "Observation result verification failed"});

  std::lock_guard lock{mutex_};
  auto base = catalog_->pin_current();
  if (!base)
    return std::unexpected(base.error());
  for (const auto &existing : (*base)->manifest().segments) {
    if (existing.id != descriptor->id)
      continue;
    if (existing.object == descriptor->object)
      return false;
    return std::unexpected(core::Error{
        core::ErrorCode::Conflict,
        "Work-result id is already associated with different observations"});
  }
  auto segments = (*base)->manifest().segments;
  segments.push_back(*descriptor);
  auto published =
      catalog_->publish((*base)->manifest().id, std::move(segments));
  if (!published)
    return std::unexpected(published.error());
  return true;
}

CanonicalTorrentMetadataResultPublisher::
    CanonicalTorrentMetadataResultPublisher(
        storage::TorrentDataset &torrents,
        std::size_t maximum_conflict_attempts,
        std::function<void(std::uint64_t)> on_torrent_committed)
    : torrents_(&torrents),
      maximum_conflict_attempts_(maximum_conflict_attempts),
      on_torrent_committed_(std::move(on_torrent_committed)) {}

core::Result<bool> CanonicalTorrentMetadataResultPublisher::publish_result(
    std::string_view worker, scheduler::WorkResultBatch batch) {
  if (worker.empty() || worker.size() > 128 ||
      batch.kind != scheduler::WorkResultKind::TorrentMetadataBatch ||
      maximum_conflict_attempts_ == 0)
    return std::unexpected(core::Error{
        core::ErrorCode::InvalidArgument,
        "Canonical torrent metadata publisher received an invalid result"});
  if (batch.id != scheduler::content_work_result_id(batch.kind, batch.payload))
    return std::unexpected(core::Error{core::ErrorCode::ChecksumMismatch,
                                       "Work-result content id is invalid"});
  auto records = decode_torrent_metadata_result_batch(batch.payload);
  if (!records)
    return std::unexpected(records.error());

  std::lock_guard lock{mutex_};
  bool changed = false;
  std::optional<std::uint64_t> last_generation;
  for (auto &record : *records) {
    for (std::size_t attempt = 0; attempt < maximum_conflict_attempts_;
         ++attempt) {
      auto before = torrents_->keyed_snapshot();
      if (!before)
        return std::unexpected(before.error());
      const auto prior_generation = (*before)->id().generation;
      auto committed = torrents_->enrich(record);
      if (committed) {
        changed = changed || committed->generation != prior_generation;
        last_generation = committed->generation;
        break;
      }
      if (committed.error().code != core::ErrorCode::Conflict ||
          attempt + 1 == maximum_conflict_attempts_)
        return std::unexpected(committed.error());
    }
  }
  if (changed && last_generation && on_torrent_committed_) {
    try {
      on_torrent_committed_(*last_generation);
    } catch (...) {
      // Canonical publication succeeded; derived-index wakeups are advisory.
    }
  }
  return changed;
}

void CanonicalTorrentMetadataResultPublisher::set_on_torrent_committed(
    std::function<void(std::uint64_t)> on_torrent_committed) {
  std::lock_guard lock{mutex_};
  on_torrent_committed_ = std::move(on_torrent_committed);
}

core::Result<bool>
CanonicalWorkResultRouter::publish_result(std::string_view worker,
                                          scheduler::WorkResultBatch batch) {
  std::lock_guard lock{mutex_};
  auto recorded = inbox_->contains(batch);
  if (!recorded)
    return std::unexpected(recorded.error());
  if (*recorded)
    return false;

  auto materialized = batch.kind == scheduler::WorkResultKind::ObservationBatch
                          ? observations_->publish_result(worker, batch)
                          : metadata_->publish_result(worker, batch);
  if (!materialized)
    return std::unexpected(materialized.error());
  auto receipt = inbox_->publish_result(worker, std::move(batch));
  if (!receipt)
    return std::unexpected(receipt.error());
  return *receipt;
}

} // namespace sakuin::integration

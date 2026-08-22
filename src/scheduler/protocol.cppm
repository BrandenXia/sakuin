export module sakuin.scheduler.protocol;

import std;

import sakuin.core.bytes;
import sakuin.core.result;
import sakuin.core.time;
import sakuin.scheduler.work;

export namespace sakuin::scheduler {

inline constexpr std::uint16_t WorkProtocolVersion = 1;

enum class WorkProtocolOperation : std::uint8_t {
  Submit = 1,
  RegisterWorker = 2,
  Heartbeat = 3,
  Lease = 4,
  Renew = 5,
  Complete = 6,
  Fail = 7,
  UnregisterWorker = 8,
  Snapshot = 9,
};

struct WorkProtocolLimits {
  std::size_t maximum_frame_bytes{2U * 1024U * 1024U};
  std::size_t maximum_work_payload_bytes{1U * 1024U * 1024U};
  std::size_t maximum_leases_per_response{1'024};
  std::size_t maximum_error_message_bytes{4U * 1024U};
  core::Duration maximum_retry_delay{std::chrono::days{30}};
};

struct SubmitWorkRequest {
  WorkItem item;
};

struct RegisterWorkerRequest {
  WorkerDescriptor worker;
};

struct WorkerHeartbeatRequest {
  std::string worker;
};

struct LeaseWorkRequest {
  std::string worker;
  std::size_t maximum{1};
};

struct LeaseMutationRequest {
  std::string worker;
  LeaseId lease{};
};

struct FailWorkRequest {
  std::string worker;
  LeaseId lease{};
  bool retryable{};
  core::Duration retry_delay{};
};

struct UnregisterWorkerRequest {
  std::string worker;
};

struct WorkSnapshotRequest {};

using WorkProtocolRequestPayload =
    std::variant<SubmitWorkRequest, RegisterWorkerRequest,
                 WorkerHeartbeatRequest, LeaseWorkRequest, LeaseMutationRequest,
                 FailWorkRequest, UnregisterWorkerRequest, WorkSnapshotRequest>;

struct WorkProtocolRequest {
  std::uint64_t request_id{};
  WorkProtocolOperation operation{};
  WorkProtocolRequestPayload payload;
};

using WorkProtocolResponsePayload =
    std::variant<std::monostate, bool, std::vector<WorkLease>,
                 WorkCoordinatorSnapshot, core::Error>;

struct WorkProtocolResponse {
  std::uint64_t request_id{};
  WorkProtocolOperation operation{};
  WorkProtocolResponsePayload payload;

  bool succeeded() const noexcept {
    return !std::holds_alternative<core::Error>(payload);
  }
};

core::Result<core::ByteBuffer>
encode_work_request(const WorkProtocolRequest &request,
                    WorkProtocolLimits limits = {});
core::Result<WorkProtocolRequest>
decode_work_request(core::ByteView frame, WorkProtocolLimits limits = {});
core::Result<core::ByteBuffer>
encode_work_response(const WorkProtocolResponse &response,
                     WorkProtocolLimits limits = {});
core::Result<WorkProtocolResponse>
decode_work_response(core::ByteView frame, WorkProtocolLimits limits = {});

// Incrementally extracts complete frames while buffering at most one bounded
// frame. A malformed header poisons the decoder until reset(), which prevents
// attempting to resynchronize on attacker-controlled payload bytes.
class WorkFrameDecoder final {
public:
  explicit WorkFrameDecoder(WorkProtocolLimits limits = {}) : limits_(limits) {}

  core::Result<std::vector<core::ByteBuffer>> feed(core::ByteView bytes);
  void reset() noexcept;
  std::size_t buffered_bytes() const noexcept { return buffer_.size(); }

private:
  WorkProtocolLimits limits_;
  core::ByteBuffer buffer_;
  std::size_t expected_bytes_{};
  bool failed_{};
};

class WorkProtocolDispatcher final {
public:
  using Clock = std::function<core::Timestamp()>;

  explicit WorkProtocolDispatcher(
      WorkCoordinator &coordinator,
      Clock clock = [] { return std::chrono::system_clock::now(); })
      : coordinator_(&coordinator), clock_(std::move(clock)) {}

  WorkProtocolResponse dispatch(const WorkProtocolRequest &request);
  core::Result<core::ByteBuffer> handle(core::ByteView frame,
                                        WorkProtocolLimits limits = {});

private:
  WorkCoordinator *coordinator_;
  Clock clock_;
};

} // namespace sakuin::scheduler

namespace sakuin::scheduler {
namespace {

constexpr std::array<core::Byte, 4> Magic{core::Byte{'S'}, core::Byte{'K'},
                                          core::Byte{'W'}, core::Byte{'K'}};
constexpr std::size_t HeaderBytes = 20;

enum class FrameKind : std::uint8_t { Request = 1, Response = 2 };

core::Error invalid(std::string message) {
  return {core::ErrorCode::InvalidArgument, std::move(message)};
}

core::Error corrupt(std::string message) {
  return {core::ErrorCode::CorruptSegment, std::move(message)};
}

bool valid_operation(std::uint8_t value) {
  return value >= static_cast<std::uint8_t>(WorkProtocolOperation::Submit) &&
         value <= static_cast<std::uint8_t>(WorkProtocolOperation::Snapshot);
}

bool valid_limits(const WorkProtocolLimits &limits) {
  return limits.maximum_frame_bytes >= HeaderBytes &&
         limits.maximum_frame_bytes <= 64U * 1024U * 1024U &&
         limits.maximum_work_payload_bytes > 0 &&
         limits.maximum_work_payload_bytes <= limits.maximum_frame_bytes &&
         limits.maximum_leases_per_response > 0 &&
         limits.maximum_leases_per_response <= 65'536 &&
         limits.maximum_error_message_bytes > 0 &&
         limits.maximum_error_message_bytes <= 65'535 &&
         limits.maximum_retry_delay >= std::chrono::milliseconds{1} &&
         std::chrono::duration_cast<std::chrono::milliseconds>(
             limits.maximum_retry_delay) <= std::chrono::days{365} &&
         std::chrono::duration_cast<core::Duration>(
             std::chrono::duration_cast<std::chrono::milliseconds>(
                 limits.maximum_retry_delay)) == limits.maximum_retry_delay;
}

class Writer {
public:
  void u8(std::uint8_t value) {
    bytes_.push_back(static_cast<core::Byte>(value));
  }

  void u16(std::uint16_t value) {
    for (int shift = 8; shift >= 0; shift -= 8)
      u8(static_cast<std::uint8_t>(value >> shift));
  }

  void u32(std::uint32_t value) {
    for (int shift = 24; shift >= 0; shift -= 8)
      u8(static_cast<std::uint8_t>(value >> shift));
  }

  void u64(std::uint64_t value) {
    for (int shift = 56; shift >= 0; shift -= 8)
      u8(static_cast<std::uint8_t>(value >> shift));
  }

  void i32(std::int32_t value) { u32(std::bit_cast<std::uint32_t>(value)); }
  void i64(std::int64_t value) { u64(std::bit_cast<std::uint64_t>(value)); }

  void raw(core::ByteView bytes) {
    bytes_.insert(bytes_.end(), bytes.begin(), bytes.end());
  }

  core::Result<void> string(std::string_view value,
                            std::size_t maximum = 65'535) {
    if (value.size() > maximum ||
        value.size() > std::numeric_limits<std::uint16_t>::max())
      return std::unexpected(invalid("Work-protocol string exceeds its limit"));
    u16(static_cast<std::uint16_t>(value.size()));
    raw(std::as_bytes(std::span{value}));
    return {};
  }

  core::ByteBuffer take() { return std::move(bytes_); }

private:
  core::ByteBuffer bytes_;
};

class Reader {
public:
  explicit Reader(core::ByteView bytes) : bytes_(bytes) {}

  core::Result<std::uint8_t> u8() {
    if (remaining() < 1)
      return std::unexpected(corrupt("Work-protocol payload is truncated"));
    return std::to_integer<std::uint8_t>(bytes_[position_++]);
  }

  core::Result<std::uint16_t> u16() {
    auto value = unsigned_integer<std::uint16_t>(2);
    return value;
  }

  core::Result<std::uint32_t> u32() {
    auto value = unsigned_integer<std::uint32_t>(4);
    return value;
  }

  core::Result<std::uint64_t> u64() {
    auto value = unsigned_integer<std::uint64_t>(8);
    return value;
  }

  core::Result<std::int32_t> i32() {
    auto value = u32();
    if (!value)
      return std::unexpected(value.error());
    return std::bit_cast<std::int32_t>(*value);
  }

  core::Result<std::int64_t> i64() {
    auto value = u64();
    if (!value)
      return std::unexpected(value.error());
    return std::bit_cast<std::int64_t>(*value);
  }

  core::Result<core::ByteView> raw(std::size_t size) {
    if (size > remaining())
      return std::unexpected(corrupt("Work-protocol payload is truncated"));
    const auto result = bytes_.subspan(position_, size);
    position_ += size;
    return result;
  }

  core::Result<std::string> string(std::size_t maximum = 65'535) {
    auto size = u16();
    if (!size)
      return std::unexpected(size.error());
    if (*size > maximum)
      return std::unexpected(corrupt("Work-protocol string exceeds its limit"));
    auto value = raw(*size);
    if (!value)
      return std::unexpected(value.error());
    return std::string{reinterpret_cast<const char *>(value->data()),
                       value->size()};
  }

  std::size_t remaining() const noexcept { return bytes_.size() - position_; }
  bool empty() const noexcept { return remaining() == 0; }

private:
  template <std::unsigned_integral T>
  core::Result<T> unsigned_integer(std::size_t size) {
    if (remaining() < size)
      return std::unexpected(corrupt("Work-protocol payload is truncated"));
    T result{};
    for (std::size_t index = 0; index < size; ++index)
      result = static_cast<T>((result << 8U) | std::to_integer<std::uint8_t>(
                                                   bytes_[position_ + index]));
    position_ += size;
    return result;
  }

  core::ByteView bytes_;
  std::size_t position_{};
};

core::Result<std::int64_t> milliseconds(core::Timestamp timestamp) {
  if (timestamp == core::Timestamp::min())
    return std::numeric_limits<std::int64_t>::min();
  if (timestamp == core::Timestamp::max())
    return std::numeric_limits<std::int64_t>::max();
  const auto duration = timestamp.time_since_epoch();
  const auto result =
      std::chrono::duration_cast<std::chrono::milliseconds>(duration);
  if (std::chrono::duration_cast<core::Timestamp::duration>(result) != duration)
    return std::unexpected(
        invalid("Work-protocol timestamps must have millisecond precision"));
  return result.count();
}

core::Result<core::Timestamp> timestamp(std::int64_t milliseconds_value) {
  if (milliseconds_value == std::numeric_limits<std::int64_t>::min())
    return core::Timestamp::min();
  if (milliseconds_value == std::numeric_limits<std::int64_t>::max())
    return core::Timestamp::max();
  const auto value = std::chrono::milliseconds{milliseconds_value};
  const auto minimum = std::chrono::duration_cast<std::chrono::milliseconds>(
      core::Timestamp::duration::min());
  const auto maximum = std::chrono::duration_cast<std::chrono::milliseconds>(
      core::Timestamp::duration::max());
  if (value < minimum || value > maximum)
    return std::unexpected(
        corrupt("Work-protocol timestamp is outside the supported range"));
  return core::Timestamp{
      std::chrono::duration_cast<core::Timestamp::duration>(value)};
}

core::Result<std::uint64_t>
duration_milliseconds(core::Duration duration,
                      const WorkProtocolLimits &limits) {
  if (duration < core::Duration::zero())
    return std::unexpected(invalid("Retry delay cannot be negative"));
  if (duration > limits.maximum_retry_delay)
    return std::unexpected(invalid("Retry delay exceeds the protocol limit"));
  const auto value =
      std::chrono::duration_cast<std::chrono::milliseconds>(duration);
  if (std::chrono::duration_cast<core::Duration>(value) != duration)
    return std::unexpected(
        invalid("Retry delay must have millisecond precision"));
  return static_cast<std::uint64_t>(value.count());
}

core::Result<core::Duration>
duration_from_milliseconds(std::uint64_t milliseconds_value,
                           const WorkProtocolLimits &limits) {
  if (milliseconds_value >
      static_cast<std::uint64_t>(std::chrono::milliseconds::max().count()))
    return std::unexpected(
        corrupt("Retry delay is outside the supported range"));
  const auto result = std::chrono::duration_cast<core::Duration>(
      std::chrono::milliseconds{milliseconds_value});
  if (result > limits.maximum_retry_delay)
    return std::unexpected(corrupt("Retry delay exceeds the protocol limit"));
  return result;
}

core::Result<core::Timestamp> add_duration(core::Timestamp base,
                                           core::Duration duration) {
  if (duration < core::Duration::zero())
    return std::unexpected(invalid("Retry delay cannot be negative"));
  const auto delta =
      std::chrono::duration_cast<core::Timestamp::duration>(duration);
  if (std::chrono::duration_cast<core::Duration>(delta) != duration)
    return std::unexpected(
        invalid("Retry delay cannot be represented by the coordinator clock"));
  if (delta > core::Timestamp::duration::zero() &&
      base.time_since_epoch() > core::Timestamp::duration::max() - delta)
    return std::unexpected(
        invalid("Retry time is outside the coordinator clock range"));
  return core::Timestamp{base.time_since_epoch() + delta};
}

core::Timestamp protocol_now(core::Timestamp value) {
  const auto milliseconds =
      std::chrono::floor<std::chrono::milliseconds>(value.time_since_epoch());
  return core::Timestamp{
      std::chrono::duration_cast<core::Timestamp::duration>(milliseconds)};
}

core::Result<std::uint8_t> work_class_value(WorkClass value) {
  switch (value) {
  case WorkClass::DhtCrawl:
    return 1;
  case WorkClass::MetadataFetch:
    return 2;
  case WorkClass::Materialization:
    return 3;
  case WorkClass::Compaction:
    return 4;
  }
  return std::unexpected(invalid("Unknown work class"));
}

core::Result<WorkClass> work_class(std::uint8_t value) {
  switch (value) {
  case 1:
    return WorkClass::DhtCrawl;
  case 2:
    return WorkClass::MetadataFetch;
  case 3:
    return WorkClass::Materialization;
  case 4:
    return WorkClass::Compaction;
  default:
    return std::unexpected(corrupt("Unknown work class in protocol payload"));
  }
}

core::Result<void> write_work_item(Writer &writer, const WorkItem &item,
                                   const WorkProtocolLimits &limits) {
  if (item.payload.size() > limits.maximum_work_payload_bytes ||
      item.payload.size() > std::numeric_limits<std::uint32_t>::max() ||
      item.maximum_attempts == 0)
    return std::unexpected(invalid("Work item exceeds protocol limits"));
  auto encoded_class = work_class_value(item.work_class);
  auto not_before = milliseconds(item.not_before);
  if (!encoded_class || !not_before)
    return std::unexpected(encoded_class ? not_before.error()
                                         : encoded_class.error());
  writer.raw(std::as_bytes(std::span{item.id.bytes}));
  writer.u8(*encoded_class);
  writer.i32(item.priority);
  writer.i64(*not_before);
  writer.u32(item.maximum_attempts);
  writer.u32(static_cast<std::uint32_t>(item.payload.size()));
  writer.raw(item.payload);
  return {};
}

core::Result<WorkItem> read_work_item(Reader &reader,
                                      const WorkProtocolLimits &limits) {
  auto id = reader.raw(WorkId{}.bytes.size());
  auto encoded_class = reader.u8();
  auto priority = reader.i32();
  auto not_before = reader.i64();
  auto attempts = reader.u32();
  auto payload_size = reader.u32();
  if (!id || !encoded_class || !priority || !not_before || !attempts ||
      !payload_size)
    return std::unexpected(!id              ? id.error()
                           : !encoded_class ? encoded_class.error()
                           : !priority      ? priority.error()
                           : !not_before    ? not_before.error()
                           : !attempts      ? attempts.error()
                                            : payload_size.error());
  if (*attempts == 0 || *payload_size > limits.maximum_work_payload_bytes)
    return std::unexpected(corrupt("Work item exceeds protocol limits"));
  auto decoded_class = work_class(*encoded_class);
  auto decoded_time = timestamp(*not_before);
  auto payload = reader.raw(*payload_size);
  if (!decoded_class || !decoded_time || !payload)
    return std::unexpected(!decoded_class  ? decoded_class.error()
                           : !decoded_time ? decoded_time.error()
                                           : payload.error());
  WorkItem result{.work_class = *decoded_class,
                  .payload = {payload->begin(), payload->end()},
                  .priority = *priority,
                  .not_before = *decoded_time,
                  .maximum_attempts = *attempts};
  for (std::size_t index = 0; index < result.id.bytes.size(); ++index)
    result.id.bytes[index] = std::to_integer<std::uint8_t>((*id)[index]);
  return result;
}

core::Result<void> write_worker(Writer &writer,
                                const WorkerDescriptor &worker) {
  if (worker.capabilities.empty() || worker.capabilities.size() > 4)
    return std::unexpected(invalid("Worker capabilities are invalid"));
  if (auto written = writer.string(worker.id, 128); !written)
    return written;
  writer.u8(static_cast<std::uint8_t>(worker.capabilities.size()));
  for (const auto capability : worker.capabilities) {
    auto value = work_class_value(capability);
    if (!value)
      return std::unexpected(value.error());
    writer.u8(*value);
  }
  return {};
}

core::Result<WorkerDescriptor> read_worker(Reader &reader) {
  auto id = reader.string(128);
  auto count = reader.u8();
  if (!id || !count)
    return std::unexpected(id ? count.error() : id.error());
  if (*count == 0 || *count > 4)
    return std::unexpected(corrupt("Worker capabilities are invalid"));
  WorkerDescriptor result{.id = std::move(*id)};
  for (std::uint8_t index = 0; index < *count; ++index) {
    auto value = reader.u8();
    if (!value)
      return std::unexpected(value.error());
    auto capability = work_class(*value);
    if (!capability)
      return std::unexpected(capability.error());
    result.capabilities.push_back(*capability);
  }
  return result;
}

core::Result<void> write_worker_id(Writer &writer, std::string_view worker) {
  return writer.string(worker, 128);
}

core::Result<void> write_lease(Writer &writer, const WorkLease &lease,
                               const WorkProtocolLimits &limits) {
  auto expires = milliseconds(lease.expires_at);
  if (!expires || lease.id == 0 || lease.attempt == 0)
    return std::unexpected(expires ? invalid("Work lease is invalid")
                                   : expires.error());
  writer.u64(lease.id);
  writer.u32(lease.attempt);
  writer.i64(*expires);
  return write_work_item(writer, lease.item, limits);
}

core::Result<WorkLease> read_lease(Reader &reader,
                                   const WorkProtocolLimits &limits) {
  auto id = reader.u64();
  auto attempt = reader.u32();
  auto expires = reader.i64();
  if (!id || !attempt || !expires)
    return std::unexpected(!id        ? id.error()
                           : !attempt ? attempt.error()
                                      : expires.error());
  if (*id == 0 || *attempt == 0)
    return std::unexpected(corrupt("Work lease is invalid"));
  auto decoded_time = timestamp(*expires);
  auto item = read_work_item(reader, limits);
  if (!decoded_time || !item)
    return std::unexpected(decoded_time ? item.error() : decoded_time.error());
  return WorkLease{.id = *id,
                   .item = std::move(*item),
                   .attempt = *attempt,
                   .expires_at = *decoded_time};
}

std::uint8_t error_code(core::ErrorCode code) {
  switch (code) {
  case core::ErrorCode::NotFound:
    return 1;
  case core::ErrorCode::InvalidArgument:
    return 2;
  case core::ErrorCode::IoError:
    return 3;
  case core::ErrorCode::PermissionDenied:
    return 4;
  case core::ErrorCode::StorageUnavailable:
    return 5;
  case core::ErrorCode::CorruptSegment:
    return 6;
  case core::ErrorCode::ChecksumMismatch:
    return 7;
  case core::ErrorCode::UnsupportedFormat:
    return 8;
  case core::ErrorCode::InvalidManifest:
    return 9;
  case core::ErrorCode::Conflict:
    return 10;
  case core::ErrorCode::InvalidQuery:
    return 11;
  case core::ErrorCode::Timeout:
    return 12;
  case core::ErrorCode::QuotaExceeded:
    return 13;
  case core::ErrorCode::Internal:
    return 14;
  }
  return 14;
}

core::Result<core::ErrorCode> error_code(std::uint8_t value) {
  if (value < 1 || value > 14)
    return std::unexpected(corrupt("Unknown work-protocol error code"));
  return static_cast<core::ErrorCode>(value - 1);
}

core::Result<void> finish(Reader &reader) {
  if (!reader.empty())
    return std::unexpected(corrupt("Work-protocol payload has trailing bytes"));
  return {};
}

core::Result<core::ByteBuffer>
frame(FrameKind kind, WorkProtocolOperation operation, std::uint64_t request_id,
      core::ByteBuffer payload, const WorkProtocolLimits &limits) {
  if (!valid_limits(limits) || request_id == 0 ||
      payload.size() > std::numeric_limits<std::uint32_t>::max() ||
      HeaderBytes + payload.size() > limits.maximum_frame_bytes)
    return std::unexpected(invalid("Work-protocol frame exceeds its limits"));
  Writer writer;
  writer.raw(Magic);
  writer.u16(WorkProtocolVersion);
  writer.u8(static_cast<std::uint8_t>(kind));
  writer.u8(static_cast<std::uint8_t>(operation));
  writer.u64(request_id);
  writer.u32(static_cast<std::uint32_t>(payload.size()));
  writer.raw(payload);
  return writer.take();
}

struct ParsedFrame {
  WorkProtocolOperation operation;
  std::uint64_t request_id{};
  core::ByteView payload;
};

core::Result<ParsedFrame> parse_frame(core::ByteView encoded,
                                      FrameKind expected_kind,
                                      const WorkProtocolLimits &limits) {
  if (!valid_limits(limits))
    return std::unexpected(invalid("Work-protocol limits are invalid"));
  if (encoded.size() < HeaderBytes)
    return std::unexpected(corrupt("Work-protocol frame is truncated"));
  if (!std::ranges::equal(encoded.first(Magic.size()), Magic))
    return std::unexpected(corrupt("Work-protocol frame magic is invalid"));
  Reader header{encoded.subspan(Magic.size(), HeaderBytes - Magic.size())};
  auto version = header.u16();
  auto kind = header.u8();
  auto operation = header.u8();
  auto request_id = header.u64();
  auto payload_size = header.u32();
  if (!version || !kind || !operation || !request_id || !payload_size)
    return std::unexpected(corrupt("Work-protocol frame header is truncated"));
  if (*version != WorkProtocolVersion)
    return std::unexpected(
        core::Error{core::ErrorCode::UnsupportedFormat,
                    "Unsupported distributed-work protocol version"});
  if (*kind != static_cast<std::uint8_t>(expected_kind))
    return std::unexpected(corrupt("Unexpected work-protocol frame kind"));
  if (!valid_operation(*operation) || *request_id == 0)
    return std::unexpected(corrupt("Work-protocol frame header is invalid"));
  if (*payload_size > limits.maximum_frame_bytes - HeaderBytes ||
      encoded.size() != HeaderBytes + *payload_size)
    return std::unexpected(corrupt("Work-protocol frame length is invalid"));
  return ParsedFrame{.operation =
                         static_cast<WorkProtocolOperation>(*operation),
                     .request_id = *request_id,
                     .payload = encoded.subspan(HeaderBytes, *payload_size)};
}

bool operation_matches(const WorkProtocolRequest &request) {
  switch (request.operation) {
  case WorkProtocolOperation::Submit:
    return std::holds_alternative<SubmitWorkRequest>(request.payload);
  case WorkProtocolOperation::RegisterWorker:
    return std::holds_alternative<RegisterWorkerRequest>(request.payload);
  case WorkProtocolOperation::Heartbeat:
    return std::holds_alternative<WorkerHeartbeatRequest>(request.payload);
  case WorkProtocolOperation::Lease:
    return std::holds_alternative<LeaseWorkRequest>(request.payload);
  case WorkProtocolOperation::Renew:
  case WorkProtocolOperation::Complete:
    return std::holds_alternative<LeaseMutationRequest>(request.payload);
  case WorkProtocolOperation::Fail:
    return std::holds_alternative<FailWorkRequest>(request.payload);
  case WorkProtocolOperation::UnregisterWorker:
    return std::holds_alternative<UnregisterWorkerRequest>(request.payload);
  case WorkProtocolOperation::Snapshot:
    return std::holds_alternative<WorkSnapshotRequest>(request.payload);
  }
  return false;
}

bool response_matches(const WorkProtocolResponse &response) {
  if (std::holds_alternative<core::Error>(response.payload))
    return true;
  switch (response.operation) {
  case WorkProtocolOperation::Submit:
  case WorkProtocolOperation::RegisterWorker:
    return std::holds_alternative<bool>(response.payload);
  case WorkProtocolOperation::Heartbeat:
  case WorkProtocolOperation::Renew:
  case WorkProtocolOperation::Complete:
  case WorkProtocolOperation::Fail:
  case WorkProtocolOperation::UnregisterWorker:
    return std::holds_alternative<std::monostate>(response.payload);
  case WorkProtocolOperation::Lease:
    return std::holds_alternative<std::vector<WorkLease>>(response.payload);
  case WorkProtocolOperation::Snapshot:
    return std::holds_alternative<WorkCoordinatorSnapshot>(response.payload);
  }
  return false;
}

} // namespace

core::Result<core::ByteBuffer>
encode_work_request(const WorkProtocolRequest &request,
                    WorkProtocolLimits limits) {
  if (!operation_matches(request))
    return std::unexpected(
        invalid("Work request payload does not match operation"));
  Writer payload;
  core::Result<void> written;
  switch (request.operation) {
  case WorkProtocolOperation::Submit:
    written = write_work_item(
        payload, std::get<SubmitWorkRequest>(request.payload).item, limits);
    break;
  case WorkProtocolOperation::RegisterWorker:
    written = write_worker(
        payload, std::get<RegisterWorkerRequest>(request.payload).worker);
    break;
  case WorkProtocolOperation::Heartbeat:
    written = write_worker_id(
        payload, std::get<WorkerHeartbeatRequest>(request.payload).worker);
    break;
  case WorkProtocolOperation::Lease: {
    const auto &value = std::get<LeaseWorkRequest>(request.payload);
    written = write_worker_id(payload, value.worker);
    if (written) {
      if (value.maximum == 0 ||
          value.maximum > limits.maximum_leases_per_response)
        written = std::unexpected(invalid("Lease request maximum is invalid"));
      else
        payload.u32(static_cast<std::uint32_t>(value.maximum));
    }
    break;
  }
  case WorkProtocolOperation::Renew:
  case WorkProtocolOperation::Complete: {
    const auto &value = std::get<LeaseMutationRequest>(request.payload);
    written = write_worker_id(payload, value.worker);
    if (written) {
      if (value.lease == 0)
        written = std::unexpected(invalid("Lease id must be nonzero"));
      else
        payload.u64(value.lease);
    }
    break;
  }
  case WorkProtocolOperation::Fail: {
    const auto &value = std::get<FailWorkRequest>(request.payload);
    written = write_worker_id(payload, value.worker);
    auto delay = duration_milliseconds(value.retry_delay, limits);
    if (written && value.lease != 0 && delay) {
      payload.u64(value.lease);
      payload.u8(value.retryable ? 1 : 0);
      payload.u64(*delay);
    } else if (written) {
      written =
          std::unexpected(value.lease == 0 ? invalid("Lease id must be nonzero")
                                           : delay.error());
    }
    break;
  }
  case WorkProtocolOperation::UnregisterWorker:
    written = write_worker_id(
        payload, std::get<UnregisterWorkerRequest>(request.payload).worker);
    break;
  case WorkProtocolOperation::Snapshot:
    break;
  }
  if (!written)
    return std::unexpected(written.error());
  return frame(FrameKind::Request, request.operation, request.request_id,
               payload.take(), limits);
}

core::Result<WorkProtocolRequest>
decode_work_request(core::ByteView encoded, WorkProtocolLimits limits) {
  auto parsed = parse_frame(encoded, FrameKind::Request, limits);
  if (!parsed)
    return std::unexpected(parsed.error());
  Reader payload{parsed->payload};
  WorkProtocolRequest result{.request_id = parsed->request_id,
                             .operation = parsed->operation};
  switch (parsed->operation) {
  case WorkProtocolOperation::Submit: {
    auto item = read_work_item(payload, limits);
    if (!item)
      return std::unexpected(item.error());
    result.payload = SubmitWorkRequest{.item = std::move(*item)};
    break;
  }
  case WorkProtocolOperation::RegisterWorker: {
    auto worker = read_worker(payload);
    if (!worker)
      return std::unexpected(worker.error());
    result.payload = RegisterWorkerRequest{.worker = std::move(*worker)};
    break;
  }
  case WorkProtocolOperation::Heartbeat: {
    auto worker = payload.string(128);
    if (!worker)
      return std::unexpected(worker.error());
    result.payload = WorkerHeartbeatRequest{.worker = std::move(*worker)};
    break;
  }
  case WorkProtocolOperation::Lease: {
    auto worker = payload.string(128);
    auto maximum = payload.u32();
    if (!worker || !maximum)
      return std::unexpected(worker ? maximum.error() : worker.error());
    if (*maximum == 0 || *maximum > limits.maximum_leases_per_response)
      return std::unexpected(corrupt("Lease request maximum is invalid"));
    result.payload =
        LeaseWorkRequest{.worker = std::move(*worker), .maximum = *maximum};
    break;
  }
  case WorkProtocolOperation::Renew:
  case WorkProtocolOperation::Complete: {
    auto worker = payload.string(128);
    auto lease = payload.u64();
    if (!worker || !lease)
      return std::unexpected(worker ? lease.error() : worker.error());
    if (*lease == 0)
      return std::unexpected(corrupt("Lease id must be nonzero"));
    result.payload =
        LeaseMutationRequest{.worker = std::move(*worker), .lease = *lease};
    break;
  }
  case WorkProtocolOperation::Fail: {
    auto worker = payload.string(128);
    auto lease = payload.u64();
    auto retryable = payload.u8();
    auto delay = payload.u64();
    if (!worker || !lease || !retryable || !delay)
      return std::unexpected(!worker      ? worker.error()
                             : !lease     ? lease.error()
                             : !retryable ? retryable.error()
                                          : delay.error());
    if (*lease == 0 || *retryable > 1)
      return std::unexpected(corrupt("Failed-work request is invalid"));
    auto decoded_delay = duration_from_milliseconds(*delay, limits);
    if (!decoded_delay)
      return std::unexpected(decoded_delay.error());
    result.payload = FailWorkRequest{.worker = std::move(*worker),
                                     .lease = *lease,
                                     .retryable = *retryable != 0,
                                     .retry_delay = *decoded_delay};
    break;
  }
  case WorkProtocolOperation::UnregisterWorker: {
    auto worker = payload.string(128);
    if (!worker)
      return std::unexpected(worker.error());
    result.payload = UnregisterWorkerRequest{.worker = std::move(*worker)};
    break;
  }
  case WorkProtocolOperation::Snapshot:
    result.payload = WorkSnapshotRequest{};
    break;
  }
  if (auto complete = finish(payload); !complete)
    return std::unexpected(complete.error());
  return result;
}

core::Result<core::ByteBuffer>
encode_work_response(const WorkProtocolResponse &response,
                     WorkProtocolLimits limits) {
  if (!response_matches(response))
    return std::unexpected(
        invalid("Work response payload does not match operation"));
  Writer payload;
  if (const auto error = std::get_if<core::Error>(&response.payload)) {
    if (error->message.size() > limits.maximum_error_message_bytes)
      return std::unexpected(
          invalid("Work-protocol error message is too long"));
    payload.u8(0);
    payload.u8(error_code(error->code));
    if (auto written =
            payload.string(error->message, limits.maximum_error_message_bytes);
        !written)
      return std::unexpected(written.error());
  } else {
    payload.u8(1);
    switch (response.operation) {
    case WorkProtocolOperation::Submit:
    case WorkProtocolOperation::RegisterWorker:
      payload.u8(std::get<bool>(response.payload) ? 1 : 0);
      break;
    case WorkProtocolOperation::Heartbeat:
    case WorkProtocolOperation::Renew:
    case WorkProtocolOperation::Complete:
    case WorkProtocolOperation::Fail:
    case WorkProtocolOperation::UnregisterWorker:
      break;
    case WorkProtocolOperation::Lease: {
      const auto &leases = std::get<std::vector<WorkLease>>(response.payload);
      if (leases.size() > limits.maximum_leases_per_response)
        return std::unexpected(invalid("Lease response exceeds its limit"));
      payload.u32(static_cast<std::uint32_t>(leases.size()));
      for (const auto &lease : leases)
        if (auto written = write_lease(payload, lease, limits); !written)
          return std::unexpected(written.error());
      break;
    }
    case WorkProtocolOperation::Snapshot: {
      const auto &snapshot =
          std::get<WorkCoordinatorSnapshot>(response.payload);
      for (const auto count :
           {snapshot.pending, snapshot.leased, snapshot.succeeded,
            snapshot.failed, snapshot.workers})
        payload.u64(count);
      break;
    }
    }
  }
  return frame(FrameKind::Response, response.operation, response.request_id,
               payload.take(), limits);
}

core::Result<WorkProtocolResponse>
decode_work_response(core::ByteView encoded, WorkProtocolLimits limits) {
  auto parsed = parse_frame(encoded, FrameKind::Response, limits);
  if (!parsed)
    return std::unexpected(parsed.error());
  Reader payload{parsed->payload};
  auto success = payload.u8();
  if (!success || *success > 1)
    return std::unexpected(success ? corrupt("Invalid response status")
                                   : success.error());
  WorkProtocolResponse result{.request_id = parsed->request_id,
                              .operation = parsed->operation};
  if (*success == 0) {
    auto encoded_code = payload.u8();
    auto message = payload.string(limits.maximum_error_message_bytes);
    if (!encoded_code || !message)
      return std::unexpected(encoded_code ? message.error()
                                          : encoded_code.error());
    auto code = error_code(*encoded_code);
    if (!code)
      return std::unexpected(code.error());
    result.payload = core::Error{*code, std::move(*message)};
  } else {
    switch (parsed->operation) {
    case WorkProtocolOperation::Submit:
    case WorkProtocolOperation::RegisterWorker: {
      auto value = payload.u8();
      if (!value || *value > 1)
        return std::unexpected(value ? corrupt("Invalid boolean response")
                                     : value.error());
      result.payload = *value != 0;
      break;
    }
    case WorkProtocolOperation::Heartbeat:
    case WorkProtocolOperation::Renew:
    case WorkProtocolOperation::Complete:
    case WorkProtocolOperation::Fail:
    case WorkProtocolOperation::UnregisterWorker:
      result.payload = std::monostate{};
      break;
    case WorkProtocolOperation::Lease: {
      auto count = payload.u32();
      if (!count)
        return std::unexpected(count.error());
      if (*count > limits.maximum_leases_per_response)
        return std::unexpected(corrupt("Lease response exceeds its limit"));
      std::vector<WorkLease> leases;
      leases.reserve(*count);
      for (std::uint32_t index = 0; index < *count; ++index) {
        auto lease = read_lease(payload, limits);
        if (!lease)
          return std::unexpected(lease.error());
        leases.push_back(std::move(*lease));
      }
      result.payload = std::move(leases);
      break;
    }
    case WorkProtocolOperation::Snapshot: {
      std::array<std::uint64_t, 5> counts{};
      for (auto &count : counts) {
        auto value = payload.u64();
        if (!value)
          return std::unexpected(value.error());
        count = *value;
      }
      if (std::ranges::any_of(counts, [](std::uint64_t count) {
            return count > std::numeric_limits<std::size_t>::max();
          }))
        return std::unexpected(corrupt("Snapshot count is too large"));
      result.payload = WorkCoordinatorSnapshot{
          .pending = static_cast<std::size_t>(counts[0]),
          .leased = static_cast<std::size_t>(counts[1]),
          .succeeded = static_cast<std::size_t>(counts[2]),
          .failed = static_cast<std::size_t>(counts[3]),
          .workers = static_cast<std::size_t>(counts[4])};
      break;
    }
    }
  }
  if (auto complete = finish(payload); !complete)
    return std::unexpected(complete.error());
  return result;
}

core::Result<std::vector<core::ByteBuffer>>
WorkFrameDecoder::feed(core::ByteView bytes) {
  if (failed_)
    return std::unexpected(
        core::Error{core::ErrorCode::Conflict,
                    "Work frame decoder must be reset after an error"});
  if (!valid_limits(limits_)) {
    failed_ = true;
    return std::unexpected(invalid("Work-protocol limits are invalid"));
  }
  std::vector<core::ByteBuffer> result;
  std::size_t consumed{};
  while (consumed < bytes.size()) {
    const auto target = expected_bytes_ == 0 ? HeaderBytes : expected_bytes_;
    const auto needed = target - buffer_.size();
    const auto count = std::min(needed, bytes.size() - consumed);
    buffer_.insert(buffer_.end(), bytes.begin() + consumed,
                   bytes.begin() + consumed + count);
    consumed += count;
    if (buffer_.size() < target)
      continue;
    if (expected_bytes_ == 0) {
      if (!std::ranges::equal(std::span{buffer_}.first(Magic.size()), Magic)) {
        failed_ = true;
        return std::unexpected(corrupt("Work-protocol frame magic is invalid"));
      }
      const auto version = (std::to_integer<std::uint16_t>(buffer_[4]) << 8U) |
                           std::to_integer<std::uint16_t>(buffer_[5]);
      if (version != WorkProtocolVersion) {
        failed_ = true;
        return std::unexpected(
            core::Error{core::ErrorCode::UnsupportedFormat,
                        "Unsupported distributed-work protocol version"});
      }
      const auto payload_size =
          (std::to_integer<std::uint32_t>(buffer_[16]) << 24U) |
          (std::to_integer<std::uint32_t>(buffer_[17]) << 16U) |
          (std::to_integer<std::uint32_t>(buffer_[18]) << 8U) |
          std::to_integer<std::uint32_t>(buffer_[19]);
      if (payload_size > limits_.maximum_frame_bytes - HeaderBytes) {
        failed_ = true;
        return std::unexpected(
            corrupt("Work-protocol frame exceeds its limit"));
      }
      expected_bytes_ = HeaderBytes + payload_size;
    }
    if (buffer_.size() == expected_bytes_) {
      result.push_back(std::move(buffer_));
      buffer_.clear();
      expected_bytes_ = 0;
    }
  }
  return result;
}

void WorkFrameDecoder::reset() noexcept {
  buffer_.clear();
  expected_bytes_ = 0;
  failed_ = false;
}

WorkProtocolResponse
WorkProtocolDispatcher::dispatch(const WorkProtocolRequest &request) {
  WorkProtocolResponse response{.request_id = request.request_id,
                                .operation = request.operation};
  const auto fail = [&](core::Error error) {
    response.payload = std::move(error);
    return response;
  };
  try {
    const auto now = protocol_now(clock_());
    switch (request.operation) {
    case WorkProtocolOperation::Submit: {
      auto result = coordinator_->submit(
          std::get<SubmitWorkRequest>(request.payload).item);
      if (!result)
        return fail(result.error());
      response.payload = *result;
      break;
    }
    case WorkProtocolOperation::RegisterWorker: {
      auto result = coordinator_->register_worker(
          std::get<RegisterWorkerRequest>(request.payload).worker, now);
      if (!result)
        return fail(result.error());
      response.payload = *result;
      break;
    }
    case WorkProtocolOperation::Heartbeat: {
      auto result = coordinator_->heartbeat(
          std::get<WorkerHeartbeatRequest>(request.payload).worker, now);
      if (!result)
        return fail(result.error());
      response.payload = std::monostate{};
      break;
    }
    case WorkProtocolOperation::Lease: {
      const auto &value = std::get<LeaseWorkRequest>(request.payload);
      auto result = coordinator_->lease(value.worker, value.maximum, now);
      if (!result)
        return fail(result.error());
      response.payload = std::move(*result);
      break;
    }
    case WorkProtocolOperation::Renew: {
      const auto &value = std::get<LeaseMutationRequest>(request.payload);
      auto result = coordinator_->renew(value.worker, value.lease, now);
      if (!result)
        return fail(result.error());
      response.payload = std::monostate{};
      break;
    }
    case WorkProtocolOperation::Complete: {
      const auto &value = std::get<LeaseMutationRequest>(request.payload);
      auto result = coordinator_->complete(value.worker, value.lease, now);
      if (!result)
        return fail(result.error());
      response.payload = std::monostate{};
      break;
    }
    case WorkProtocolOperation::Fail: {
      const auto &value = std::get<FailWorkRequest>(request.payload);
      auto retry_at = add_duration(now, value.retry_delay);
      if (!retry_at)
        return fail(retry_at.error());
      auto result = coordinator_->fail(value.worker, value.lease,
                                       value.retryable, now, *retry_at);
      if (!result)
        return fail(result.error());
      response.payload = std::monostate{};
      break;
    }
    case WorkProtocolOperation::UnregisterWorker: {
      auto result = coordinator_->unregister_worker(
          std::get<UnregisterWorkerRequest>(request.payload).worker, now);
      if (!result)
        return fail(result.error());
      response.payload = std::monostate{};
      break;
    }
    case WorkProtocolOperation::Snapshot: {
      auto result = coordinator_->snapshot(now);
      if (!result)
        return fail(result.error());
      response.payload = *result;
      break;
    }
    }
  } catch (const std::exception &error) {
    return fail(
        {core::ErrorCode::Internal,
         "Distributed-work dispatch failed: " + std::string{error.what()}});
  } catch (...) {
    return fail({core::ErrorCode::Internal,
                 "Distributed-work dispatch failed with an unknown error"});
  }
  return response;
}

core::Result<core::ByteBuffer>
WorkProtocolDispatcher::handle(core::ByteView encoded,
                               WorkProtocolLimits limits) {
  auto request = decode_work_request(encoded, limits);
  if (!request)
    return std::unexpected(request.error());
  return encode_work_response(dispatch(*request), limits);
}

} // namespace sakuin::scheduler

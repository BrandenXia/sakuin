module;

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

export module sakuin.scheduler.recovery;

import std;

import sakuin.core.bytes;
import sakuin.core.hash;
import sakuin.core.result;
import sakuin.core.time;
import sakuin.scheduler.work;

export namespace sakuin::scheduler {

inline constexpr std::size_t DefaultMaximumWorkCheckpointBytes =
    512U * 1024U * 1024U;

core::Result<core::ByteBuffer> encode_work_recovery_snapshot(
    const WorkRecoverySnapshot &snapshot,
    std::size_t maximum_bytes = DefaultMaximumWorkCheckpointBytes);

core::Result<WorkRecoverySnapshot> decode_work_recovery_snapshot(
    core::ByteView encoded, const WorkCoordinatorOptions &limits,
    std::size_t maximum_bytes = DefaultMaximumWorkCheckpointBytes);

class WorkRecoveryStore {
public:
  virtual ~WorkRecoveryStore() = default;
  virtual core::Result<std::optional<WorkRecoverySnapshot>>
  load(const WorkCoordinatorOptions &limits) = 0;
  virtual core::Result<void> save(const WorkRecoverySnapshot &snapshot) = 0;
};

// Crash-safe local operational checkpoint. The file is atomically replaced
// only after its contents and parent directory are synchronized.
class LocalWorkRecoveryStore final : public WorkRecoveryStore {
public:
  LocalWorkRecoveryStore(
      std::filesystem::path path,
      std::size_t maximum_bytes = DefaultMaximumWorkCheckpointBytes);

  core::Result<std::optional<WorkRecoverySnapshot>>
  load(const WorkCoordinatorOptions &limits) override;
  core::Result<void> save(const WorkRecoverySnapshot &snapshot) override;

  const std::filesystem::path &path() const noexcept { return path_; }

private:
  std::filesystem::path path_;
  std::size_t maximum_bytes_{};
};

// Serializes coordinator mutations and checkpoints every recoverable change.
// Ephemeral registrations, heartbeats, and renewals are intentionally not
// persisted. If a checkpoint fails after an in-memory mutation, the wrapper
// fail-stops and does not acknowledge it; restart loads the last durable state.
class RecoveringWorkCoordinator final : public WorkCoordinator {
public:
  static core::Result<std::unique_ptr<RecoveringWorkCoordinator>>
  create(WorkCoordinatorOptions options,
         std::unique_ptr<WorkRecoveryStore> store);

  static core::Result<std::unique_ptr<RecoveringWorkCoordinator>>
  open_local(WorkCoordinatorOptions options, std::filesystem::path path,
             std::size_t maximum_bytes = DefaultMaximumWorkCheckpointBytes);

  core::Result<bool> submit(WorkItem item) override;
  core::Result<bool> register_worker(WorkerDescriptor worker,
                                     core::Timestamp now) override;
  core::Result<void> heartbeat(std::string_view worker,
                               core::Timestamp now) override;
  core::Result<std::vector<WorkLease>> lease(std::string_view worker,
                                             std::size_t maximum,
                                             core::Timestamp now) override;
  core::Result<void> renew(std::string_view worker, LeaseId lease,
                           core::Timestamp now) override;
  core::Result<void> complete(std::string_view worker, LeaseId lease,
                              core::Timestamp now) override;
  core::Result<void> fail(std::string_view worker, LeaseId lease,
                          bool retryable, core::Timestamp now,
                          core::Timestamp retry_at) override;
  core::Result<void> unregister_worker(std::string_view worker,
                                       core::Timestamp now) override;
  core::Result<WorkCoordinatorSnapshot> snapshot(core::Timestamp now) override;

  bool healthy() const noexcept;

private:
  RecoveringWorkCoordinator(std::unique_ptr<LocalWorkCoordinator> coordinator,
                            std::unique_ptr<WorkRecoveryStore> store)
      : coordinator_(std::move(coordinator)), store_(std::move(store)) {}

  core::Result<void> available_locked() const;
  core::Result<void> checkpoint_locked();

  std::unique_ptr<LocalWorkCoordinator> coordinator_;
  std::unique_ptr<WorkRecoveryStore> store_;
  mutable std::mutex mutex_;
  std::optional<core::Error> fault_;
};

} // namespace sakuin::scheduler

namespace sakuin::scheduler {
namespace {

constexpr std::array<core::Byte, 8> CheckpointMagic{
    core::Byte{'S'}, core::Byte{'A'}, core::Byte{'K'}, core::Byte{'W'},
    core::Byte{'C'}, core::Byte{'P'}, core::Byte{'1'}, core::Byte{0}};
constexpr std::uint32_t CheckpointVersion = 1;
constexpr std::size_t ChecksumBytes = 32;
constexpr std::size_t FixedEntryBytes = 52;

template <std::unsigned_integral Integer>
void append_integer(core::ByteBuffer &output, Integer value) {
  for (std::size_t index = 0; index < sizeof(Integer); ++index) {
    output.push_back(static_cast<core::Byte>(value & 0xffU));
    if constexpr (sizeof(Integer) > 1)
      value >>= 8U;
  }
}

template <std::unsigned_integral Integer>
core::Result<Integer> read_integer(core::ByteView input,
                                   std::size_t &position) {
  if (position > input.size() || input.size() - position < sizeof(Integer))
    return std::unexpected(core::Error{core::ErrorCode::CorruptSegment,
                                       "Work checkpoint is truncated"});
  Integer value{};
  for (std::size_t index = 0; index < sizeof(Integer); ++index)
    value |= static_cast<Integer>(
                 std::to_integer<std::uint8_t>(input[position + index]))
             << (index * 8U);
  position += sizeof(Integer);
  return value;
}

core::Result<std::uint64_t> encoded_timestamp(core::Timestamp timestamp) {
  if (timestamp == core::Timestamp::min())
    return std::bit_cast<std::uint64_t>(
        std::numeric_limits<std::int64_t>::min());
  if (timestamp == core::Timestamp::max())
    return std::bit_cast<std::uint64_t>(
        std::numeric_limits<std::int64_t>::max());
  const auto nanoseconds =
      std::chrono::duration<long double, std::nano>{
          timestamp.time_since_epoch()}
          .count();
  constexpr auto minimum =
      static_cast<long double>(std::numeric_limits<std::int64_t>::min() + 1);
  constexpr auto maximum =
      static_cast<long double>(std::numeric_limits<std::int64_t>::max() - 1);
  if (!std::isfinite(nanoseconds) || nanoseconds < minimum ||
      nanoseconds > maximum)
    return std::unexpected(
        core::Error{core::ErrorCode::InvalidArgument,
                    "Work checkpoint timestamp is outside the portable range"});
  return std::bit_cast<std::uint64_t>(static_cast<std::int64_t>(nanoseconds));
}

core::Timestamp decoded_timestamp(std::uint64_t encoded) {
  const auto value = std::bit_cast<std::int64_t>(encoded);
  if (value == std::numeric_limits<std::int64_t>::min())
    return core::Timestamp::min();
  if (value == std::numeric_limits<std::int64_t>::max())
    return core::Timestamp::max();
  return core::Timestamp{std::chrono::duration_cast<core::Timestamp::duration>(
      std::chrono::nanoseconds{value})};
}

core::Error io_error(std::string action, const std::filesystem::path &path,
                     std::string detail = {}) {
  auto message = std::move(action) + " '" + path.string() + "'";
  if (!detail.empty())
    message += ": " + detail;
  return {core::ErrorCode::IoError, std::move(message)};
}

core::Result<void> sync_directory(const std::filesystem::path &path) {
  const auto directory = path.empty() ? std::filesystem::path{"."} : path;
  const int descriptor =
      ::open(directory.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (descriptor < 0)
    return std::unexpected(io_error("Could not open checkpoint directory",
                                    directory, std::strerror(errno)));
  if (::fsync(descriptor) != 0) {
    const auto detail = std::string{std::strerror(errno)};
    ::close(descriptor);
    return std::unexpected(
        io_error("Could not sync checkpoint directory", directory, detail));
  }
  ::close(descriptor);
  return {};
}

core::Result<void> write_all(int descriptor, core::ByteView input,
                             const std::filesystem::path &path) {
  std::size_t position{};
  while (position < input.size()) {
    const auto written =
        ::write(descriptor, input.data() + position, input.size() - position);
    if (written < 0 && errno == EINTR)
      continue;
    if (written <= 0)
      return std::unexpected(
          io_error("Could not write work checkpoint", path,
                   written < 0 ? std::strerror(errno) : "short write"));
    position += static_cast<std::size_t>(written);
  }
  return {};
}

} // namespace

core::Result<core::ByteBuffer>
encode_work_recovery_snapshot(const WorkRecoverySnapshot &snapshot,
                              std::size_t maximum_bytes) {
  constexpr std::size_t HeaderBytes = 24;
  if (maximum_bytes < HeaderBytes + ChecksumBytes ||
      snapshot.entries.size() > std::numeric_limits<std::uint32_t>::max())
    return std::unexpected(
        core::Error{core::ErrorCode::QuotaExceeded,
                    "Work checkpoint exceeds its configured encoding limits"});

  core::ByteBuffer output{CheckpointMagic.begin(), CheckpointMagic.end()};
  append_integer(output, CheckpointVersion);
  append_integer(output, snapshot.next_sequence);
  append_integer(output, static_cast<std::uint32_t>(snapshot.entries.size()));
  for (const auto &entry : snapshot.entries) {
    if (entry.item.payload.size() > std::numeric_limits<std::uint32_t>::max() ||
        output.size() > maximum_bytes - ChecksumBytes ||
        FixedEntryBytes > maximum_bytes - ChecksumBytes - output.size() ||
        entry.item.payload.size() >
            maximum_bytes - ChecksumBytes - output.size() - FixedEntryBytes)
      return std::unexpected(
          core::Error{core::ErrorCode::QuotaExceeded,
                      "Work checkpoint exceeds its configured byte limit"});
    const auto timestamp = encoded_timestamp(entry.item.not_before);
    if (!timestamp)
      return std::unexpected(timestamp.error());
    for (const auto byte : entry.item.id.bytes)
      output.push_back(static_cast<core::Byte>(byte));
    append_integer(output, static_cast<std::uint8_t>(entry.item.work_class));
    append_integer(output, static_cast<std::uint8_t>(entry.state));
    append_integer(output, std::uint16_t{});
    append_integer(output, std::bit_cast<std::uint32_t>(entry.item.priority));
    append_integer(output, *timestamp);
    append_integer(output, entry.item.maximum_attempts);
    append_integer(output, entry.attempts);
    append_integer(output, entry.sequence);
    append_integer(output,
                   static_cast<std::uint32_t>(entry.item.payload.size()));
    output.insert(output.end(), entry.item.payload.begin(),
                  entry.item.payload.end());
  }
  const auto checksum = core::sha256(output);
  for (const auto byte : checksum.bytes)
    output.push_back(static_cast<core::Byte>(byte));
  return output;
}

core::Result<WorkRecoverySnapshot>
decode_work_recovery_snapshot(core::ByteView encoded,
                              const WorkCoordinatorOptions &limits,
                              std::size_t maximum_bytes) {
  constexpr std::size_t MinimumBytes = 24 + ChecksumBytes;
  if (encoded.size() < MinimumBytes || encoded.size() > maximum_bytes ||
      !std::ranges::equal(encoded.first(CheckpointMagic.size()),
                          CheckpointMagic))
    return std::unexpected(
        core::Error{core::ErrorCode::CorruptSegment,
                    "Work checkpoint header or size is invalid"});
  const auto checksum_offset = encoded.size() - ChecksumBytes;
  const auto checksum = core::sha256(encoded.first(checksum_offset));
  const auto stored = encoded.subspan(checksum_offset, ChecksumBytes);
  const auto expected = std::as_bytes(std::span{checksum.bytes});
  if (!core::constant_time_equal(stored, expected))
    return std::unexpected(core::Error{core::ErrorCode::ChecksumMismatch,
                                       "Work checkpoint checksum mismatch"});

  std::size_t position = CheckpointMagic.size();
  auto version = read_integer<std::uint32_t>(encoded, position);
  auto next_sequence = read_integer<std::uint64_t>(encoded, position);
  auto count = read_integer<std::uint32_t>(encoded, position);
  if (!version || !next_sequence || !count)
    return std::unexpected(
        !version ? version.error()
                 : (!next_sequence ? next_sequence.error() : count.error()));
  if (*version != CheckpointVersion)
    return std::unexpected(core::Error{core::ErrorCode::UnsupportedFormat,
                                       "Unsupported work checkpoint version"});
  if (*count > limits.maximum_work_items ||
      *count > (checksum_offset - position) / FixedEntryBytes)
    return std::unexpected(
        core::Error{core::ErrorCode::QuotaExceeded,
                    "Work checkpoint item count exceeds configured limits"});

  WorkRecoverySnapshot result{.next_sequence = *next_sequence};
  result.entries.reserve(*count);
  for (std::uint32_t index = 0; index < *count; ++index) {
    if (position > checksum_offset ||
        checksum_offset - position < FixedEntryBytes)
      return std::unexpected(core::Error{core::ErrorCode::CorruptSegment,
                                         "Work checkpoint entry is truncated"});
    WorkId id;
    for (auto &byte : id.bytes)
      byte = std::to_integer<std::uint8_t>(encoded[position++]);
    auto work_class = read_integer<std::uint8_t>(encoded, position);
    auto state = read_integer<std::uint8_t>(encoded, position);
    auto reserved = read_integer<std::uint16_t>(encoded, position);
    auto priority = read_integer<std::uint32_t>(encoded, position);
    auto not_before = read_integer<std::uint64_t>(encoded, position);
    auto maximum_attempts = read_integer<std::uint32_t>(encoded, position);
    auto attempts = read_integer<std::uint32_t>(encoded, position);
    auto sequence = read_integer<std::uint64_t>(encoded, position);
    auto payload_size = read_integer<std::uint32_t>(encoded, position);
    if (!work_class || !state || !reserved || !priority || !not_before ||
        !maximum_attempts || !attempts || !sequence || !payload_size)
      return std::unexpected(core::Error{core::ErrorCode::CorruptSegment,
                                         "Work checkpoint entry is truncated"});
    if (*reserved != 0 ||
        *work_class > static_cast<std::uint8_t>(WorkClass::Compaction) ||
        *state > static_cast<std::uint8_t>(WorkRecoveryState::Failed) ||
        *payload_size > limits.maximum_payload_bytes ||
        position > checksum_offset ||
        checksum_offset - position < *payload_size)
      return std::unexpected(
          core::Error{core::ErrorCode::CorruptSegment,
                      "Work checkpoint entry violates encoding limits"});
    core::ByteBuffer payload(encoded.begin() + position,
                             encoded.begin() + position + *payload_size);
    position += *payload_size;
    result.entries.push_back(
        {.item = {.id = id,
                  .work_class = static_cast<WorkClass>(*work_class),
                  .payload = std::move(payload),
                  .priority = std::bit_cast<std::int32_t>(*priority),
                  .not_before = decoded_timestamp(*not_before),
                  .maximum_attempts = *maximum_attempts},
         .state = static_cast<WorkRecoveryState>(*state),
         .sequence = *sequence,
         .attempts = *attempts});
  }
  if (position != checksum_offset)
    return std::unexpected(core::Error{core::ErrorCode::CorruptSegment,
                                       "Work checkpoint has trailing bytes"});
  return result;
}

LocalWorkRecoveryStore::LocalWorkRecoveryStore(std::filesystem::path path,
                                               std::size_t maximum_bytes)
    : path_(std::move(path)), maximum_bytes_(maximum_bytes) {}

core::Result<std::optional<WorkRecoverySnapshot>>
LocalWorkRecoveryStore::load(const WorkCoordinatorOptions &limits) {
  if (path_.empty() || maximum_bytes_ < 56)
    return std::unexpected(
        core::Error{core::ErrorCode::InvalidArgument,
                    "Local work recovery path or byte limit is invalid"});
  const int descriptor = ::open(path_.c_str(), O_RDONLY | O_CLOEXEC);
  if (descriptor < 0) {
    if (errno == ENOENT)
      return std::optional<WorkRecoverySnapshot>{};
    return std::unexpected(io_error("Could not open work checkpoint", path_,
                                    std::strerror(errno)));
  }
  struct stat status{};
  if (::fstat(descriptor, &status) != 0) {
    const auto detail = std::string{std::strerror(errno)};
    ::close(descriptor);
    return std::unexpected(
        io_error("Could not size work checkpoint", path_, detail));
  }
  if (status.st_size < 0) {
    ::close(descriptor);
    return std::unexpected(core::Error{core::ErrorCode::CorruptSegment,
                                       "Work checkpoint " + path_.string() +
                                           " has an invalid file size"});
  }
  if (static_cast<std::uintmax_t>(status.st_size) > maximum_bytes_) {
    ::close(descriptor);
    return std::unexpected(core::Error{core::ErrorCode::QuotaExceeded,
                                       "Work checkpoint " + path_.string() +
                                           " exceeds its byte limit"});
  }
  core::ByteBuffer encoded(static_cast<std::size_t>(status.st_size));
  std::size_t position{};
  while (position < encoded.size()) {
    const auto read = ::read(descriptor, encoded.data() + position,
                             encoded.size() - position);
    if (read < 0 && errno == EINTR)
      continue;
    if (read <= 0) {
      const auto detail =
          read < 0 ? std::string{std::strerror(errno)} : "unexpected EOF";
      ::close(descriptor);
      return std::unexpected(
          io_error("Could not read work checkpoint", path_, detail));
    }
    position += static_cast<std::size_t>(read);
  }
  ::close(descriptor);
  auto decoded = decode_work_recovery_snapshot(encoded, limits, maximum_bytes_);
  if (!decoded)
    return std::unexpected(decoded.error());
  return std::optional<WorkRecoverySnapshot>{std::move(*decoded)};
}

core::Result<void>
LocalWorkRecoveryStore::save(const WorkRecoverySnapshot &snapshot) {
  if (path_.empty() || maximum_bytes_ < 56)
    return std::unexpected(
        core::Error{core::ErrorCode::InvalidArgument,
                    "Local work recovery path or byte limit is invalid"});
  auto encoded = encode_work_recovery_snapshot(snapshot, maximum_bytes_);
  if (!encoded)
    return std::unexpected(encoded.error());
  const auto parent = path_.parent_path().empty() ? std::filesystem::path{"."}
                                                  : path_.parent_path();
  std::error_code directory_error;
  const bool created =
      std::filesystem::create_directories(parent, directory_error);
  if (directory_error)
    return std::unexpected(
        io_error("Could not create work checkpoint directory", parent,
                 directory_error.message()));
  if (created) {
    std::filesystem::permissions(parent, std::filesystem::perms::owner_all,
                                 std::filesystem::perm_options::replace,
                                 directory_error);
    if (directory_error)
      return std::unexpected(
          io_error("Could not secure work checkpoint directory", parent,
                   directory_error.message()));
  }

  auto temporary = path_;
  temporary += ".temporary";
  const int descriptor =
      ::open(temporary.c_str(), O_CREAT | O_TRUNC | O_WRONLY | O_CLOEXEC, 0600);
  if (descriptor < 0)
    return std::unexpected(io_error("Could not create work checkpoint",
                                    temporary, std::strerror(errno)));
  auto written = write_all(descriptor, *encoded, temporary);
  if (!written || ::fsync(descriptor) != 0) {
    const auto error = written ? io_error("Could not sync work checkpoint",
                                          temporary, std::strerror(errno))
                               : written.error();
    ::close(descriptor);
    return std::unexpected(error);
  }
  if (::close(descriptor) != 0)
    return std::unexpected(io_error("Could not close work checkpoint",
                                    temporary, std::strerror(errno)));
  std::error_code rename_error;
  std::filesystem::rename(temporary, path_, rename_error);
  if (rename_error)
    return std::unexpected(io_error("Could not publish work checkpoint", path_,
                                    rename_error.message()));
  return sync_directory(parent);
}

core::Result<std::unique_ptr<RecoveringWorkCoordinator>>
RecoveringWorkCoordinator::create(WorkCoordinatorOptions options,
                                  std::unique_ptr<WorkRecoveryStore> store) {
  if (!store)
    return std::unexpected(core::Error{core::ErrorCode::InvalidArgument,
                                       "Work recovery store is required"});
  auto recovered = store->load(options);
  if (!recovered)
    return std::unexpected(recovered.error());
  auto coordinator =
      *recovered ? LocalWorkCoordinator::create(options, std::move(**recovered))
                 : LocalWorkCoordinator::create(options);
  if (!coordinator)
    return std::unexpected(coordinator.error());
  return std::unique_ptr<RecoveringWorkCoordinator>{
      new RecoveringWorkCoordinator{std::move(*coordinator), std::move(store)}};
}

core::Result<std::unique_ptr<RecoveringWorkCoordinator>>
RecoveringWorkCoordinator::open_local(WorkCoordinatorOptions options,
                                      std::filesystem::path path,
                                      std::size_t maximum_bytes) {
  return create(options, std::make_unique<LocalWorkRecoveryStore>(
                             std::move(path), maximum_bytes));
}

core::Result<void> RecoveringWorkCoordinator::available_locked() const {
  if (fault_)
    return std::unexpected(*fault_);
  return {};
}

core::Result<void> RecoveringWorkCoordinator::checkpoint_locked() {
  auto saved = store_->save(coordinator_->recovery_snapshot());
  if (!saved) {
    fault_ = saved.error();
    return std::unexpected(saved.error());
  }
  return {};
}

core::Result<bool> RecoveringWorkCoordinator::submit(WorkItem item) {
  std::lock_guard lock{mutex_};
  if (auto ready = available_locked(); !ready)
    return std::unexpected(ready.error());
  auto result = coordinator_->submit(std::move(item));
  if (!result || !*result)
    return result;
  if (auto saved = checkpoint_locked(); !saved)
    return std::unexpected(saved.error());
  return true;
}

core::Result<bool>
RecoveringWorkCoordinator::register_worker(WorkerDescriptor worker,
                                           core::Timestamp now) {
  std::lock_guard lock{mutex_};
  if (auto ready = available_locked(); !ready)
    return std::unexpected(ready.error());
  return coordinator_->register_worker(std::move(worker), now);
}

core::Result<void> RecoveringWorkCoordinator::heartbeat(std::string_view worker,
                                                        core::Timestamp now) {
  std::lock_guard lock{mutex_};
  if (auto ready = available_locked(); !ready)
    return ready;
  return coordinator_->heartbeat(worker, now);
}

core::Result<std::vector<WorkLease>>
RecoveringWorkCoordinator::lease(std::string_view worker, std::size_t maximum,
                                 core::Timestamp now) {
  std::lock_guard lock{mutex_};
  if (auto ready = available_locked(); !ready)
    return std::unexpected(ready.error());
  auto result = coordinator_->lease(worker, maximum, now);
  if (!result || result->empty())
    return result;
  if (auto saved = checkpoint_locked(); !saved)
    return std::unexpected(saved.error());
  return result;
}

core::Result<void> RecoveringWorkCoordinator::renew(std::string_view worker,
                                                    LeaseId lease,
                                                    core::Timestamp now) {
  std::lock_guard lock{mutex_};
  if (auto ready = available_locked(); !ready)
    return ready;
  return coordinator_->renew(worker, lease, now);
}

core::Result<void> RecoveringWorkCoordinator::complete(std::string_view worker,
                                                       LeaseId lease,
                                                       core::Timestamp now) {
  std::lock_guard lock{mutex_};
  if (auto ready = available_locked(); !ready)
    return ready;
  auto result = coordinator_->complete(worker, lease, now);
  if (!result)
    return result;
  return checkpoint_locked();
}

core::Result<void> RecoveringWorkCoordinator::fail(std::string_view worker,
                                                   LeaseId lease,
                                                   bool retryable,
                                                   core::Timestamp now,
                                                   core::Timestamp retry_at) {
  std::lock_guard lock{mutex_};
  if (auto ready = available_locked(); !ready)
    return ready;
  auto result = coordinator_->fail(worker, lease, retryable, now, retry_at);
  if (!result)
    return result;
  return checkpoint_locked();
}

core::Result<void>
RecoveringWorkCoordinator::unregister_worker(std::string_view worker,
                                             core::Timestamp now) {
  std::lock_guard lock{mutex_};
  if (auto ready = available_locked(); !ready)
    return ready;
  auto result = coordinator_->unregister_worker(worker, now);
  if (!result)
    return result;
  return checkpoint_locked();
}

core::Result<WorkCoordinatorSnapshot>
RecoveringWorkCoordinator::snapshot(core::Timestamp now) {
  std::lock_guard lock{mutex_};
  if (auto ready = available_locked(); !ready)
    return std::unexpected(ready.error());
  return coordinator_->snapshot(now);
}

bool RecoveringWorkCoordinator::healthy() const noexcept {
  std::lock_guard lock{mutex_};
  return !fault_.has_value();
}

} // namespace sakuin::scheduler

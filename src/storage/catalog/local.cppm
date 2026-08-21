module;

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>

export module sakuin.storage.catalog.local;

import std;

import sakuin.core.bytes;
import sakuin.core.ids;
import sakuin.core.result;
import sakuin.core.time;
import sakuin.storage.blob.store;
import sakuin.storage.catalog.manifest;
import sakuin.storage.format.block;
import sakuin.storage.format.segment;

export namespace sakuin::storage {

class LocalManifestCatalog final : public ManifestCatalog {
public:
  static core::Result<std::unique_ptr<LocalManifestCatalog>>
  open(std::filesystem::path root, BlobStore &blobs);

  ~LocalManifestCatalog() override;
  core::Result<std::shared_ptr<const ManifestPin>> pin_current() const override;
  SnapshotId current_id() const noexcept override;
  bool is_pinned(SnapshotId id) const noexcept override;
  core::Result<GcResult> garbage_collect() override;
  core::Result<SnapshotId>
  publish(SnapshotId expected_current,
          std::vector<SegmentDescriptor> segments) override;

private:
  struct Impl;
  explicit LocalManifestCatalog(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> impl_;
};

} // namespace sakuin::storage

namespace sakuin::storage {
namespace {

constexpr std::array<std::byte, 8> ManifestMagic{
    std::byte{'S'}, std::byte{'A'}, std::byte{'K'}, std::byte{'M'},
    std::byte{'A'}, std::byte{'N'}, std::byte{'1'}, std::byte{0}};

core::Error io_error(std::string action, const std::filesystem::path &path,
                     std::string detail = {}) {
  auto message = std::move(action) + " '" + path.string() + "'";
  if (!detail.empty())
    message += ": " + detail;
  return {core::ErrorCode::IoError, std::move(message)};
}

core::Error invalid_manifest(std::string message) {
  return {core::ErrorCode::InvalidManifest, std::move(message)};
}

struct ObjectIdHasher {
  std::size_t operator()(const core::ObjectId &id) const noexcept {
    std::size_t value = 1469598103934665603ULL;
    for (const auto byte : id.bytes) {
      value ^= byte;
      value *= 1099511628211ULL;
    }
    return value;
  }
};

template <std::unsigned_integral Integer>
void append_integer(core::ByteBuffer &output, Integer value) {
  for (std::size_t index = 0; index < sizeof(Integer); ++index) {
    output.push_back(static_cast<std::byte>(value & 0xffU));
    if constexpr (sizeof(Integer) > 1)
      value >>= 8U;
  }
}

template <std::unsigned_integral Integer>
core::Result<Integer> read_integer(core::ByteView input,
                                   std::size_t &position) {
  if (position > input.size() || input.size() - position < sizeof(Integer))
    return std::unexpected(invalid_manifest("Manifest is truncated"));
  Integer value{};
  for (std::size_t index = 0; index < sizeof(Integer); ++index)
    value |= static_cast<Integer>(
                 std::to_integer<std::uint8_t>(input[position + index]))
             << (index * 8U);
  position += sizeof(Integer);
  return value;
}

std::uint32_t crc32c(core::ByteView input) {
  std::uint32_t crc = 0xffffffffU;
  for (const auto byte : input) {
    crc ^= std::to_integer<std::uint8_t>(byte);
    for (int bit = 0; bit < 8; ++bit)
      crc = (crc >> 1U) ^ (0x82f63b78U & (0U - (crc & 1U)));
  }
  return ~crc;
}

void append_timestamp(core::ByteBuffer &output,
                      const std::optional<core::Timestamp> &timestamp) {
  append_integer(output, static_cast<std::uint8_t>(timestamp.has_value()));
  if (timestamp) {
    const auto ticks = std::chrono::duration_cast<std::chrono::nanoseconds>(
                           timestamp->time_since_epoch())
                           .count();
    append_integer(output, std::bit_cast<std::uint64_t>(ticks));
  }
}

core::Result<std::optional<core::Timestamp>>
read_timestamp(core::ByteView input, std::size_t &position) {
  auto present = read_integer<std::uint8_t>(input, position);
  if (!present || *present > 1)
    return std::unexpected(
        invalid_manifest("Invalid manifest timestamp marker"));
  if (*present == 0)
    return std::optional<core::Timestamp>{};
  auto encoded = read_integer<std::uint64_t>(input, position);
  if (!encoded)
    return std::unexpected(encoded.error());
  const auto ticks = std::bit_cast<std::int64_t>(*encoded);
  return std::optional<core::Timestamp>{
      core::Timestamp{std::chrono::duration_cast<core::Timestamp::duration>(
          std::chrono::nanoseconds{ticks})}};
}

core::ByteBuffer encode_manifest(const Manifest &manifest) {
  core::ByteBuffer output{ManifestMagic.begin(), ManifestMagic.end()};
  append_integer(output, manifest.format_version);
  append_integer(output, manifest.id.generation);
  append_integer(output, static_cast<std::uint32_t>(manifest.segments.size()));
  for (const auto &segment : manifest.segments) {
    for (auto byte : segment.id.bytes)
      output.push_back(static_cast<std::byte>(byte));
    for (auto byte : segment.object.bytes)
      output.push_back(static_cast<std::byte>(byte));
    append_integer(output, static_cast<std::uint8_t>(segment.tier));
    append_integer(output, static_cast<std::uint8_t>(segment.encoding));
    append_integer(output, static_cast<std::uint8_t>(segment.compression));
    append_integer(output, std::uint8_t{});
    append_integer(output, segment.record_count);
    append_integer(output, segment.logical_size);
    append_integer(output, segment.physical_size);
    append_timestamp(output, segment.min_timestamp);
    append_timestamp(output, segment.max_timestamp);
    append_integer(output, segment.format_version.major);
    append_integer(output, segment.format_version.minor);
    append_integer(output, segment.schema_id.value);
    append_integer(output, segment.schema_version.value);
  }
  append_integer(output, crc32c(output));
  return output;
}

template <std::size_t Size>
core::Result<std::array<std::uint8_t, Size>> read_array(core::ByteView input,
                                                        std::size_t &position) {
  if (position > input.size() || input.size() - position < Size)
    return std::unexpected(invalid_manifest("Manifest ID is truncated"));
  std::array<std::uint8_t, Size> result{};
  for (std::size_t index = 0; index < Size; ++index)
    result[index] = std::to_integer<std::uint8_t>(input[position + index]);
  position += Size;
  return result;
}

core::Result<Manifest> decode_manifest(core::ByteView input) {
  if (input.size() < ManifestMagic.size() + 20 ||
      !std::equal(ManifestMagic.begin(), ManifestMagic.end(), input.begin()))
    return std::unexpected(invalid_manifest("Invalid manifest magic"));
  const auto stored_checksum_offset = input.size() - sizeof(std::uint32_t);
  std::size_t checksum_position = stored_checksum_offset;
  auto stored_checksum = read_integer<std::uint32_t>(input, checksum_position);
  if (!stored_checksum ||
      crc32c(input.first(stored_checksum_offset)) != *stored_checksum)
    return std::unexpected(core::Error{core::ErrorCode::ChecksumMismatch,
                                       "Manifest checksum mismatch"});

  std::size_t position = ManifestMagic.size();
  auto format = read_integer<std::uint32_t>(input, position);
  auto generation = read_integer<std::uint64_t>(input, position);
  auto count = read_integer<std::uint32_t>(input, position);
  if (!format || !generation || !count)
    return std::unexpected(invalid_manifest("Manifest header is truncated"));
  if (*format != 1 && *format != 2)
    return std::unexpected(core::Error{core::ErrorCode::UnsupportedFormat,
                                       "Unsupported manifest format"});
  constexpr std::size_t LegacyMinimumDescriptorSize = 86;
  constexpr std::size_t CurrentMinimumDescriptorSize = 90;
  const auto minimum_descriptor_size =
      *format == 1 ? LegacyMinimumDescriptorSize : CurrentMinimumDescriptorSize;
  if (*count > (stored_checksum_offset - position) / minimum_descriptor_size)
    return std::unexpected(
        invalid_manifest("Manifest segment count exceeds its encoded size"));

  Manifest manifest{.format_version = *format, .id = {*generation}};
  manifest.segments.reserve(*count);
  for (std::uint32_t index = 0; index < *count; ++index) {
    auto segment_id = read_array<16>(input, position);
    auto object_id = read_array<32>(input, position);
    auto tier = read_integer<std::uint8_t>(input, position);
    auto encoding = read_integer<std::uint8_t>(input, position);
    auto compression = read_integer<std::uint8_t>(input, position);
    auto reserved = read_integer<std::uint8_t>(input, position);
    auto records = read_integer<std::uint64_t>(input, position);
    auto logical = read_integer<std::uint64_t>(input, position);
    auto physical = read_integer<std::uint64_t>(input, position);
    if (!segment_id || !object_id || !tier || !encoding || !compression ||
        !reserved || !records || !logical || !physical || *reserved != 0 ||
        *tier > static_cast<std::uint8_t>(SegmentTier::Cold) ||
        *encoding > static_cast<std::uint8_t>(SegmentEncoding::RowV1) ||
        *compression > static_cast<std::uint8_t>(CompressionCodec::Zstd))
      return std::unexpected(invalid_manifest("Invalid segment descriptor"));
    auto minimum = read_timestamp(input, position);
    auto maximum = read_timestamp(input, position);
    auto major = read_integer<std::uint16_t>(input, position);
    auto minor = read_integer<std::uint16_t>(input, position);
    core::Result<std::uint32_t> schema_id = std::uint32_t{};
    if (*format >= 2)
      schema_id = read_integer<std::uint32_t>(input, position);
    auto schema = read_integer<std::uint32_t>(input, position);
    if (!minimum || !maximum || !major || !minor || !schema_id || !schema)
      return std::unexpected(invalid_manifest("Truncated segment descriptor"));
    if (*minimum && *maximum && **minimum > **maximum)
      return std::unexpected(
          invalid_manifest("Segment timestamp range is inverted"));
    manifest.segments.push_back(
        {.id = {.bytes = *segment_id},
         .object = {.bytes = *object_id},
         .tier = static_cast<SegmentTier>(*tier),
         .record_count = *records,
         .logical_size = *logical,
         .physical_size = *physical,
         .min_timestamp = *minimum,
         .max_timestamp = *maximum,
         .format_version = {*major, *minor},
         .schema_id = {*schema_id},
         .schema_version = {*schema},
         .encoding = static_cast<SegmentEncoding>(*encoding),
         .compression = static_cast<CompressionCodec>(*compression)});
  }
  if (position != stored_checksum_offset)
    return std::unexpected(invalid_manifest("Manifest has trailing bytes"));
  return manifest;
}

core::Result<void> sync_path(const std::filesystem::path &path,
                             bool directory) {
  const int flags = O_RDONLY | (directory ? O_DIRECTORY : 0);
  const int descriptor = ::open(path.c_str(), flags);
  if (descriptor < 0)
    return std::unexpected(
        io_error("Could not open", path, std::strerror(errno)));
  if (::fsync(descriptor) != 0) {
    auto detail = std::string{std::strerror(errno)};
    ::close(descriptor);
    return std::unexpected(io_error("Could not sync", path, detail));
  }
  ::close(descriptor);
  return {};
}

core::Result<void> write_atomic(const std::filesystem::path &path,
                                core::ByteView data) {
  const auto temporary = path.string() + ".temporary";
  {
    std::ofstream output{temporary, std::ios::binary | std::ios::trunc};
    output.write(reinterpret_cast<const char *>(data.data()),
                 static_cast<std::streamsize>(data.size()));
    output.flush();
    if (!output)
      return std::unexpected(io_error("Could not write", temporary));
  }
  if (auto synced = sync_path(temporary, false); !synced)
    return synced;
  std::error_code error;
  std::filesystem::rename(temporary, path, error);
  if (error)
    return std::unexpected(
        io_error("Could not publish", path, error.message()));
  return sync_path(path.parent_path(), true);
}

core::Result<core::ByteBuffer> read_file(const std::filesystem::path &path) {
  std::ifstream input{path, std::ios::binary | std::ios::ate};
  if (!input)
    return std::unexpected(io_error("Could not open", path));
  const auto size = input.tellg();
  if (size < 0)
    return std::unexpected(io_error("Could not size", path));
  core::ByteBuffer bytes(static_cast<std::size_t>(size));
  input.seekg(0);
  input.read(reinterpret_cast<char *>(bytes.data()), size);
  if (!input)
    return std::unexpected(io_error("Could not read", path));
  return bytes;
}

class LocalManifestPin final : public ManifestPin {
public:
  explicit LocalManifestPin(std::shared_ptr<const Manifest> manifest)
      : manifest_(std::move(manifest)) {}
  const Manifest &manifest() const noexcept override { return *manifest_; }

private:
  std::shared_ptr<const Manifest> manifest_;
};

} // namespace

struct LocalManifestCatalog::Impl {
  std::filesystem::path root;
  BlobStore *blobs{};
  mutable std::mutex mutex;
  std::shared_ptr<const Manifest> current;
  mutable std::vector<std::weak_ptr<const ManifestPin>> pins;
};

LocalManifestCatalog::LocalManifestCatalog(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}
LocalManifestCatalog::~LocalManifestCatalog() = default;

core::Result<std::unique_ptr<LocalManifestCatalog>>
LocalManifestCatalog::open(std::filesystem::path root, BlobStore &blobs) {
  std::error_code error;
  std::filesystem::create_directories(root / "manifests", error);
  if (error)
    return std::unexpected(
        io_error("Could not create catalog", root, error.message()));
  auto current = std::make_shared<Manifest>(Manifest{.id = {0}});
  const auto pointer = root / "CURRENT";
  if (std::filesystem::exists(pointer, error)) {
    auto bytes = read_file(pointer);
    if (!bytes)
      return std::unexpected(bytes.error());
    const std::string generation_text{
        reinterpret_cast<const char *>(bytes->data()), bytes->size()};
    std::uint64_t generation{};
    auto [end, conversion_error] = std::from_chars(
        generation_text.data(), generation_text.data() + generation_text.size(),
        generation);
    if (conversion_error != std::errc{} ||
        end != generation_text.data() + generation_text.size())
      return std::unexpected(
          invalid_manifest("CURRENT contains invalid generation"));
    auto manifest_bytes = read_file(root / "manifests" /
                                    (std::to_string(generation) + ".manifest"));
    if (!manifest_bytes)
      return std::unexpected(manifest_bytes.error());
    auto decoded = decode_manifest(*manifest_bytes);
    if (!decoded || decoded->id.generation != generation)
      return std::unexpected(
          decoded ? invalid_manifest("CURRENT generation mismatch")
                  : decoded.error());
    current = std::make_shared<Manifest>(std::move(*decoded));
  } else if (error) {
    return std::unexpected(
        io_error("Could not inspect catalog", pointer, error.message()));
  }
  auto impl = std::make_unique<Impl>();
  impl->root = std::move(root);
  impl->blobs = &blobs;
  impl->current = std::move(current);
  return std::unique_ptr<LocalManifestCatalog>{
      new LocalManifestCatalog{std::move(impl)}};
}

core::Result<std::shared_ptr<const ManifestPin>>
LocalManifestCatalog::pin_current() const {
  std::lock_guard lock{impl_->mutex};
  auto pin = std::make_shared<LocalManifestPin>(impl_->current);
  impl_->pins.push_back(pin);
  return std::shared_ptr<const ManifestPin>{std::move(pin)};
}

SnapshotId LocalManifestCatalog::current_id() const noexcept {
  std::lock_guard lock{impl_->mutex};
  return impl_->current->id;
}

bool LocalManifestCatalog::is_pinned(SnapshotId id) const noexcept {
  std::lock_guard lock{impl_->mutex};
  bool found{};
  std::erase_if(impl_->pins, [&](const auto &weak) {
    auto pin = weak.lock();
    if (!pin)
      return true;
    found = found || pin->manifest().id == id;
    return false;
  });
  return found;
}

core::Result<GcResult> LocalManifestCatalog::garbage_collect() {
  std::lock_guard lock{impl_->mutex};

  std::unordered_set<std::uint64_t> protected_generations{
      impl_->current->id.generation};
  std::vector<std::shared_ptr<const ManifestPin>> live_pins;
  std::erase_if(impl_->pins, [&](const auto &weak) {
    auto pin = weak.lock();
    if (!pin)
      return true;
    protected_generations.insert(pin->manifest().id.generation);
    live_pins.push_back(std::move(pin));
    return false;
  });

  std::unordered_set<core::ObjectId, ObjectIdHasher> protected_objects;
  for (const auto &segment : impl_->current->segments)
    protected_objects.insert(segment.object);
  for (const auto &pin : live_pins) {
    for (const auto &segment : pin->manifest().segments)
      protected_objects.insert(segment.object);
  }

  const auto manifests_directory = impl_->root / "manifests";
  std::vector<std::pair<std::filesystem::path, Manifest>> retired;
  std::error_code iteration_error;
  for (std::filesystem::directory_iterator
           iterator{manifests_directory, iteration_error},
       end;
       !iteration_error && iterator != end;
       iterator.increment(iteration_error)) {
    if (!iterator->is_regular_file() ||
        iterator->path().extension() != ".manifest")
      continue;
    auto bytes = read_file(iterator->path());
    if (!bytes)
      return std::unexpected(bytes.error());
    auto manifest = decode_manifest(*bytes);
    if (!manifest)
      return std::unexpected(manifest.error());
    if (!protected_generations.contains(manifest->id.generation))
      retired.emplace_back(iterator->path(), std::move(*manifest));
  }
  if (iteration_error)
    return std::unexpected(io_error("Could not enumerate", manifests_directory,
                                    iteration_error.message()));

  std::unordered_map<core::ObjectId, std::uint64_t, ObjectIdHasher> candidates;
  for (const auto &[path, manifest] : retired) {
    for (const auto &segment : manifest.segments) {
      if (!protected_objects.contains(segment.object))
        candidates.try_emplace(segment.object, segment.physical_size);
    }
  }

  GcResult result{};
  for (const auto &[object, described_size] : candidates) {
    std::uint64_t size = described_size;
    auto reader = impl_->blobs->open(object);
    if (reader)
      size = (*reader)->size();
    else if (reader.error().code != core::ErrorCode::NotFound)
      return std::unexpected(reader.error());

    auto removed = impl_->blobs->remove(object);
    if (!removed && removed.error().code != core::ErrorCode::NotFound)
      return std::unexpected(removed.error());
    if (removed) {
      ++result.objects_deleted;
      result.bytes_reclaimed += size;
    }
  }

  for (const auto &[path, manifest] : retired) {
    std::error_code error;
    std::filesystem::remove(path, error);
    if (error)
      return std::unexpected(
          io_error("Could not remove retired manifest", path, error.message()));
  }
  if (!retired.empty()) {
    if (auto synced = sync_path(manifests_directory, true); !synced)
      return std::unexpected(synced.error());
  }
  return result;
}

core::Result<SnapshotId>
LocalManifestCatalog::publish(SnapshotId expected_current,
                              std::vector<SegmentDescriptor> segments) {
  std::lock_guard lock{impl_->mutex};
  if (expected_current != impl_->current->id)
    return std::unexpected(
        core::Error{core::ErrorCode::Conflict, "Manifest generation changed"});
  for (const auto &segment : segments) {
    auto exists = impl_->blobs->exists(segment.object);
    if (!exists)
      return std::unexpected(exists.error());
    if (!*exists)
      return std::unexpected(
          core::Error{core::ErrorCode::InvalidManifest,
                      "Manifest references a missing segment"});
  }
  if (segments.size() > std::numeric_limits<std::uint32_t>::max())
    return std::unexpected(core::Error{core::ErrorCode::InvalidArgument,
                                       "Manifest has too many segments"});
  Manifest manifest{.id = {impl_->current->id.generation + 1},
                    .segments = std::move(segments)};
  auto encoded = encode_manifest(manifest);
  const auto manifest_path =
      impl_->root / "manifests" /
      (std::to_string(manifest.id.generation) + ".manifest");
  if (auto written = write_atomic(manifest_path, encoded); !written)
    return std::unexpected(written.error());
  const auto generation = std::to_string(manifest.id.generation);
  const auto generation_bytes = std::as_bytes(std::span{generation});
  if (auto written = write_atomic(impl_->root / "CURRENT", generation_bytes);
      !written)
    return std::unexpected(written.error());
  impl_->current = std::make_shared<Manifest>(std::move(manifest));
  return impl_->current->id;
}

} // namespace sakuin::storage

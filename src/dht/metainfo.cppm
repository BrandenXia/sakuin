export module sakuin.dht.metainfo;

import std;

import sakuin.core.bytes;
import sakuin.core.hash;
import sakuin.core.ids;
import sakuin.core.result;
import sakuin.core.time;
import sakuin.dht.bencode;
import sakuin.model.torrent;

export namespace sakuin::dht {

struct MetainfoDecodeLimits {
  std::size_t maximum_metadata_bytes{4U * 1024U * 1024U};
  std::size_t maximum_files{100'000};
  std::size_t maximum_name_bytes{32U * 1024U};
  std::size_t maximum_path_bytes{64U * 1024U};
};

// Decodes a raw BEP 3 info dictionary only after independently validating its
// v1 info-hash. The resulting record is ready for keyed dataset enrichment.
core::Result<model::TorrentRecord>
decode_metainfo(core::InfoHash expected_info_hash,
                core::ByteView info_dictionary, core::Timestamp observed_at,
                const MetainfoDecodeLimits &limits = {});

} // namespace sakuin::dht

namespace sakuin::dht {
namespace {

core::Error invalid(std::string message) {
  return {core::ErrorCode::InvalidArgument, std::move(message)};
}

bool valid_utf8(core::ByteView input) noexcept {
  const auto byte = [&](std::size_t index) {
    return std::to_integer<std::uint8_t>(input[index]);
  };
  const auto continuation = [&](std::size_t index) {
    return index < input.size() && (byte(index) & 0xc0U) == 0x80U;
  };

  for (std::size_t index = 0; index < input.size();) {
    const auto lead = byte(index);
    if (lead <= 0x7fU) {
      ++index;
      continue;
    }
    if (lead >= 0xc2U && lead <= 0xdfU) {
      if (!continuation(index + 1))
        return false;
      index += 2;
      continue;
    }
    if (lead >= 0xe0U && lead <= 0xefU) {
      if (index + 2 >= input.size() || !continuation(index + 1) ||
          !continuation(index + 2))
        return false;
      const auto second = byte(index + 1);
      if ((lead == 0xe0U && second < 0xa0U) ||
          (lead == 0xedU && second >= 0xa0U))
        return false;
      index += 3;
      continue;
    }
    if (lead >= 0xf0U && lead <= 0xf4U) {
      if (index + 3 >= input.size() || !continuation(index + 1) ||
          !continuation(index + 2) || !continuation(index + 3))
        return false;
      const auto second = byte(index + 1);
      if ((lead == 0xf0U && second < 0x90U) ||
          (lead == 0xf4U && second >= 0x90U))
        return false;
      index += 4;
      continue;
    }
    return false;
  }
  return true;
}

core::Result<std::string> text(const bencode::Value &value,
                               std::string_view field,
                               std::size_t maximum_bytes) {
  const auto *bytes = value.string();
  if (!bytes)
    return std::unexpected(invalid(std::string{field} + " must be a string"));
  if (bytes->empty() || bytes->size() > maximum_bytes)
    return std::unexpected(
        invalid(std::string{field} + " has an invalid length"));
  if (std::ranges::find(*bytes, std::byte{}) != bytes->end())
    return std::unexpected(
        invalid(std::string{field} + " contains a NUL byte"));
  if (!valid_utf8(*bytes))
    return std::unexpected(invalid(std::string{field} + " is not valid UTF-8"));
  return std::string{reinterpret_cast<const char *>(bytes->data()),
                     bytes->size()};
}

core::Result<std::uint64_t> length(const bencode::Value &value,
                                   std::string_view field) {
  const auto *integer = value.integer();
  if (!integer || *integer < 0)
    return std::unexpected(
        invalid(std::string{field} + " must be a nonnegative integer"));
  return static_cast<std::uint64_t>(*integer);
}

const bencode::Value *find(const bencode::Value::Dictionary &dictionary,
                           std::string_view key) noexcept {
  const auto found = dictionary.find(key);
  return found == dictionary.end() ? nullptr : &found->second;
}

core::Result<std::string> decode_path(const bencode::Value &value,
                                      std::size_t maximum_path_bytes) {
  const auto *components = value.list();
  if (!components || components->empty())
    return std::unexpected(
        invalid("Torrent file path must be a nonempty list"));

  std::string result;
  for (const auto &component_value : *components) {
    auto component = text(component_value, "Torrent file path component",
                          maximum_path_bytes);
    if (!component)
      return std::unexpected(component.error());
    if (*component == "." || *component == ".." ||
        component->find('/') != std::string::npos ||
        component->find('\\') != std::string::npos)
      return std::unexpected(invalid("Torrent file path component is unsafe"));
    const auto separator = result.empty() ? 0U : 1U;
    if (result.size() > maximum_path_bytes ||
        separator > maximum_path_bytes - result.size() ||
        component->size() > maximum_path_bytes - result.size() - separator)
      return std::unexpected(invalid("Torrent file path is too long"));
    if (!result.empty())
      result.push_back('/');
    result.append(*component);
  }
  return result;
}

} // namespace

core::Result<model::TorrentRecord>
decode_metainfo(core::InfoHash expected_info_hash,
                core::ByteView info_dictionary, core::Timestamp observed_at,
                const MetainfoDecodeLimits &limits) {
  if (limits.maximum_metadata_bytes == 0 || limits.maximum_files == 0 ||
      limits.maximum_name_bytes == 0 || limits.maximum_path_bytes == 0)
    return std::unexpected(invalid("Metainfo decode limits must be nonzero"));
  if (info_dictionary.empty() ||
      info_dictionary.size() > limits.maximum_metadata_bytes)
    return std::unexpected(
        invalid("Torrent info dictionary has an invalid size"));

  const auto digest = core::sha1(info_dictionary);
  if (!core::constant_time_equal(
          core::ByteView{
              reinterpret_cast<const std::byte *>(digest.bytes.data()),
              digest.bytes.size()},
          core::ByteView{reinterpret_cast<const std::byte *>(
                             expected_info_hash.bytes.data()),
                         expected_info_hash.bytes.size()}))
    return std::unexpected(
        core::Error{core::ErrorCode::ChecksumMismatch,
                    "Torrent info dictionary hash mismatch"});

  bencode::ParseLimits parse_limits;
  parse_limits.maximum_bytes = limits.maximum_metadata_bytes;
  parse_limits.maximum_depth = 32;
  parse_limits.maximum_values = 500'000;
  parse_limits.require_canonical_dictionary_order = true;
  auto decoded = bencode::parse(info_dictionary, parse_limits);
  if (!decoded)
    return std::unexpected(decoded.error());
  const auto *dictionary = decoded->dictionary();
  if (!dictionary)
    return std::unexpected(invalid("Torrent info value must be a dictionary"));

  const auto *name_value = find(*dictionary, "name");
  if (!name_value)
    return std::unexpected(invalid("Torrent info dictionary has no name"));
  auto name = text(*name_value, "Torrent name", limits.maximum_name_bytes);
  if (!name)
    return std::unexpected(name.error());

  const auto *single_length = find(*dictionary, "length");
  const auto *files_value = find(*dictionary, "files");
  if ((single_length == nullptr) == (files_value == nullptr))
    return std::unexpected(
        invalid("Torrent must contain exactly one of length or files"));

  model::TorrentRecord record{.info_hash = expected_info_hash,
                              .first_seen = observed_at,
                              .last_seen = observed_at,
                              .name = *name,
                              .total_size = 0,
                              .files = {}};
  if (single_length) {
    auto size = length(*single_length, "Torrent length");
    if (!size)
      return std::unexpected(size.error());
    record.total_size = *size;
    record.files.push_back(model::FileRecord{.path = *name, .size = *size});
    return record;
  }

  const auto *files = files_value->list();
  if (!files || files->empty() || files->size() > limits.maximum_files)
    return std::unexpected(invalid("Torrent files list has an invalid length"));
  record.files.reserve(files->size());
  for (const auto &file_value : *files) {
    const auto *file = file_value.dictionary();
    if (!file)
      return std::unexpected(
          invalid("Torrent file entry must be a dictionary"));
    const auto *file_length = find(*file, "length");
    const auto *file_path = find(*file, "path");
    if (!file_length || !file_path)
      return std::unexpected(
          invalid("Torrent file entry requires length and path"));
    auto size = length(*file_length, "Torrent file length");
    if (!size)
      return std::unexpected(size.error());
    auto path = decode_path(*file_path, limits.maximum_path_bytes);
    if (!path)
      return std::unexpected(path.error());
    if (*size > std::numeric_limits<std::uint64_t>::max() - record.total_size)
      return std::unexpected(invalid("Torrent total size overflows uint64"));
    record.total_size += *size;
    record.files.push_back(
        model::FileRecord{.path = std::move(*path), .size = *size});
  }
  return record;
}

} // namespace sakuin::dht

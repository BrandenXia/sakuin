import std;

import sakuin.core;
import sakuin.dht;

namespace {

sakuin::core::ByteBuffer bytes(std::string_view text) {
  const auto view = std::as_bytes(std::span{text});
  return {view.begin(), view.end()};
}

sakuin::dht::bencode::Value string(std::string_view text) {
  return sakuin::dht::bencode::Value{bytes(text)};
}

std::pair<sakuin::core::InfoHash, sakuin::core::ByteBuffer>
encode(sakuin::dht::bencode::Value::Dictionary dictionary) {
  auto encoded = sakuin::dht::bencode::encode(
      sakuin::dht::bencode::Value{std::move(dictionary)}, 1024U * 1024U);
  if (!encoded)
    return {};
  const auto digest = sakuin::core::sha1(*encoded);
  return {sakuin::core::InfoHash{.bytes = digest.bytes}, std::move(*encoded)};
}

sakuin::dht::bencode::Value
path(std::initializer_list<std::string_view> parts) {
  sakuin::dht::bencode::Value::List result;
  for (const auto part : parts)
    result.push_back(string(part));
  return sakuin::dht::bencode::Value{std::move(result)};
}

sakuin::dht::bencode::Value file(std::int64_t size,
                                 sakuin::dht::bencode::Value file_path) {
  using namespace sakuin::dht;
  bencode::Value::Dictionary result;
  result.emplace("length", bencode::Value{size});
  result.emplace("path", std::move(file_path));
  return bencode::Value{std::move(result)};
}

} // namespace

int main() {
  using namespace sakuin;
  const auto observed = core::Timestamp{std::chrono::seconds{42}};

  dht::bencode::Value::Dictionary single;
  single.emplace("length", dht::bencode::Value{std::int64_t{5}});
  single.emplace("name", string("test"));
  single.emplace("piece length", dht::bencode::Value{std::int64_t{16'384}});
  single.emplace("pieces", dht::bencode::Value{core::ByteBuffer(20)});
  auto [single_hash, single_bytes] = encode(std::move(single));
  auto decoded = dht::decode_metainfo(single_hash, single_bytes, observed);
  if (!decoded || decoded->info_hash != single_hash ||
      decoded->first_seen != observed || decoded->last_seen != observed ||
      decoded->name != "test" || decoded->total_size != 5 ||
      decoded->files.size() != 1 || decoded->files[0].path != "test" ||
      decoded->files[0].size != 5)
    return 1;

  dht::bencode::Value::List files;
  files.push_back(file(7, path({"dir", "a.bin"})));
  files.push_back(file(11, path({"b.bin"})));
  dht::bencode::Value::Dictionary multi;
  multi.emplace("files", dht::bencode::Value{std::move(files)});
  multi.emplace("name", string("root"));
  multi.emplace("piece length", dht::bencode::Value{std::int64_t{16'384}});
  multi.emplace("pieces", dht::bencode::Value{core::ByteBuffer(20)});
  auto [multi_hash, multi_bytes] = encode(std::move(multi));
  auto decoded_multi = dht::decode_metainfo(multi_hash, multi_bytes, observed);
  if (!decoded_multi || decoded_multi->total_size != 18 ||
      decoded_multi->files.size() != 2 ||
      decoded_multi->files[0].path != "dir/a.bin" ||
      decoded_multi->files[1].path != "b.bin")
    return 2;

  auto wrong_hash = single_hash;
  wrong_hash.bytes[0] ^= 1U;
  auto corrupt = dht::decode_metainfo(wrong_hash, single_bytes, observed);
  if (corrupt || corrupt.error().code != core::ErrorCode::ChecksumMismatch)
    return 3;

  const auto noncanonical = bytes("d4:name4:test6:lengthi5ee");
  const auto noncanonical_digest = core::sha1(noncanonical);
  auto unordered =
      dht::decode_metainfo(core::InfoHash{.bytes = noncanonical_digest.bytes},
                           noncanonical, observed);
  if (unordered || unordered.error().code != core::ErrorCode::InvalidArgument) {
    if (unordered)
      std::cerr << "noncanonical dictionary was accepted\n";
    else
      std::cerr << "noncanonical dictionary error: "
                << unordered.error().message << '\n';
    return 4;
  }

  dht::bencode::Value::List unsafe_files;
  unsafe_files.push_back(file(1, path({"..", "secret"})));
  dht::bencode::Value::Dictionary unsafe_info;
  unsafe_info.emplace("files", dht::bencode::Value{std::move(unsafe_files)});
  unsafe_info.emplace("name", string("root"));
  auto [unsafe_hash, unsafe_bytes] = encode(std::move(unsafe_info));
  auto unsafe = dht::decode_metainfo(unsafe_hash, unsafe_bytes, observed);
  if (unsafe || unsafe.error().code != core::ErrorCode::InvalidArgument)
    return 5;

  return 0;
}

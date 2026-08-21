export module sakuin.dht.bencode;

import std;

import sakuin.core.bytes;
import sakuin.core.result;

export namespace sakuin::dht::bencode {

struct Value {
  using List = std::vector<Value>;
  using Dictionary = std::map<std::string, Value, std::less<>>;
  using Data = std::variant<std::int64_t, core::ByteBuffer, List, Dictionary>;

  Data data;

  const std::int64_t *integer() const noexcept;
  const core::ByteBuffer *string() const noexcept;
  const List *list() const noexcept;
  const Dictionary *dictionary() const noexcept;
};

struct ParseLimits {
  std::size_t maximum_bytes{65'507};
  std::size_t maximum_depth{16};
  std::size_t maximum_values{4'096};
  bool require_canonical_dictionary_order{};
};

struct ParsedValue {
  Value value;
  std::size_t consumed{};
};

core::Result<Value> parse(core::ByteView input, const ParseLimits &limits = {});
core::Result<ParsedValue> parse_prefix(core::ByteView input,
                                       const ParseLimits &limits = {});

core::Result<core::ByteBuffer> encode(const Value &value,
                                      std::size_t maximum_bytes = 65'507);

} // namespace sakuin::dht::bencode

namespace sakuin::dht::bencode {
namespace {

core::Error malformed(std::string message) {
  return {core::ErrorCode::InvalidArgument, std::move(message)};
}

class Parser {
public:
  Parser(core::ByteView input, ParseLimits limits)
      : input_(input), limits_(limits) {}

  core::Result<Value> value(std::size_t depth = 0) {
    if (depth > limits_.maximum_depth)
      return std::unexpected(malformed("Bencode nesting limit exceeded"));
    if (++values_ > limits_.maximum_values)
      return std::unexpected(malformed("Bencode value limit exceeded"));
    if (position_ == input_.size())
      return std::unexpected(malformed("Bencode value is truncated"));
    const auto marker = character(position_);
    if (marker == 'i')
      return integer_value();
    if (marker == 'l')
      return list_value(depth);
    if (marker == 'd')
      return dictionary_value(depth);
    if (marker >= '0' && marker <= '9')
      return string_value();
    return std::unexpected(malformed("Invalid bencode type marker"));
  }

  bool finished() const noexcept { return position_ == input_.size(); }
  std::size_t position() const noexcept { return position_; }

private:
  char character(std::size_t offset) const noexcept {
    return static_cast<char>(std::to_integer<unsigned char>(input_[offset]));
  }

  core::Result<Value> integer_value() {
    const auto begin = ++position_;
    const auto end = find('e');
    if (!end)
      return std::unexpected(end.error());
    if (*end == begin)
      return std::unexpected(malformed("Empty bencode integer"));
    std::string_view text{reinterpret_cast<const char *>(input_.data() + begin),
                          *end - begin};
    if ((text.size() > 1 && text.front() == '0') ||
        (text.size() > 1 && text[0] == '-' && text[1] == '0'))
      return std::unexpected(malformed("Non-canonical bencode integer"));
    std::int64_t result{};
    const auto [parsed, error] =
        std::from_chars(text.data(), text.data() + text.size(), result);
    if (error != std::errc{} || parsed != text.data() + text.size())
      return std::unexpected(malformed("Invalid bencode integer"));
    position_ = *end + 1;
    return Value{result};
  }

  core::Result<Value> string_value() {
    const auto begin = position_;
    const auto colon = find(':');
    if (!colon)
      return std::unexpected(colon.error());
    std::string_view length_text{
        reinterpret_cast<const char *>(input_.data() + begin), *colon - begin};
    if (length_text.empty() ||
        (length_text.size() > 1 && length_text.front() == '0'))
      return std::unexpected(malformed("Invalid bencode string length"));
    std::size_t length{};
    const auto [parsed, error] = std::from_chars(
        length_text.data(), length_text.data() + length_text.size(), length);
    if (error != std::errc{} ||
        parsed != length_text.data() + length_text.size())
      return std::unexpected(malformed("Invalid bencode string length"));
    position_ = *colon + 1;
    if (length > input_.size() - position_)
      return std::unexpected(malformed("Bencode string is truncated"));
    core::ByteBuffer result{input_.begin() + position_,
                            input_.begin() + position_ + length};
    position_ += length;
    return Value{std::move(result)};
  }

  core::Result<Value> list_value(std::size_t depth) {
    ++position_;
    Value::List result;
    while (position_ < input_.size() && character(position_) != 'e') {
      auto element = value(depth + 1);
      if (!element)
        return std::unexpected(element.error());
      result.push_back(std::move(*element));
    }
    if (position_ == input_.size())
      return std::unexpected(malformed("Bencode list is truncated"));
    ++position_;
    return Value{std::move(result)};
  }

  core::Result<Value> dictionary_value(std::size_t depth) {
    ++position_;
    Value::Dictionary result;
    std::optional<std::string> previous_key;
    while (position_ < input_.size() && character(position_) != 'e') {
      auto key = string_value();
      if (!key)
        return std::unexpected(key.error());
      const auto *key_bytes = key->string();
      std::string key_text{reinterpret_cast<const char *>(key_bytes->data()),
                           key_bytes->size()};
      if (limits_.require_canonical_dictionary_order && previous_key &&
          key_text <= *previous_key)
        return std::unexpected(
            malformed("Non-canonical bencode dictionary key order"));
      previous_key = key_text;
      auto element = value(depth + 1);
      if (!element)
        return std::unexpected(element.error());
      if (!result.emplace(std::move(key_text), std::move(*element)).second)
        return std::unexpected(malformed("Duplicate bencode dictionary key"));
    }
    if (position_ == input_.size())
      return std::unexpected(malformed("Bencode dictionary is truncated"));
    ++position_;
    return Value{std::move(result)};
  }

  core::Result<std::size_t> find(char target) const {
    for (auto index = position_; index < input_.size(); ++index) {
      if (character(index) == target)
        return index;
    }
    return std::unexpected(malformed("Bencode delimiter is missing"));
  }

  core::ByteView input_;
  ParseLimits limits_;
  std::size_t position_{};
  std::size_t values_{};
};

} // namespace

const std::int64_t *Value::integer() const noexcept {
  return std::get_if<std::int64_t>(&data);
}
const core::ByteBuffer *Value::string() const noexcept {
  return std::get_if<core::ByteBuffer>(&data);
}
const Value::List *Value::list() const noexcept {
  return std::get_if<List>(&data);
}
const Value::Dictionary *Value::dictionary() const noexcept {
  return std::get_if<Dictionary>(&data);
}

core::Result<Value> parse(core::ByteView input, const ParseLimits &limits) {
  auto parsed = parse_prefix(input, limits);
  if (!parsed)
    return std::unexpected(parsed.error());
  if (parsed->consumed != input.size())
    return std::unexpected(malformed("Bencode input has trailing bytes"));
  return std::move(parsed->value);
}

core::Result<ParsedValue> parse_prefix(core::ByteView input,
                                       const ParseLimits &limits) {
  if (input.size() > limits.maximum_bytes)
    return std::unexpected(malformed("Bencode input exceeds packet limit"));
  Parser parser{input, limits};
  auto result = parser.value();
  if (!result)
    return std::unexpected(result.error());
  return ParsedValue{.value = std::move(*result),
                     .consumed = parser.position()};
}

namespace {

core::Result<void> append_encoded(const Value &value, core::ByteBuffer &output,
                                  std::size_t maximum) {
  const auto ensure = [&](std::size_t additional) -> core::Result<void> {
    if (additional > maximum - output.size())
      return std::unexpected(malformed("Encoded bencode exceeds size limit"));
    return {};
  };
  if (const auto *integer = value.integer()) {
    const auto text = std::to_string(*integer);
    if (auto room = ensure(text.size() + 2); !room)
      return room;
    output.push_back(std::byte{'i'});
    const auto bytes = std::as_bytes(std::span{text});
    output.insert(output.end(), bytes.begin(), bytes.end());
    output.push_back(std::byte{'e'});
    return {};
  }
  if (const auto *string = value.string()) {
    const auto length = std::to_string(string->size());
    if (auto room = ensure(length.size() + 1 + string->size()); !room)
      return room;
    const auto bytes = std::as_bytes(std::span{length});
    output.insert(output.end(), bytes.begin(), bytes.end());
    output.push_back(std::byte{':'});
    output.insert(output.end(), string->begin(), string->end());
    return {};
  }
  if (const auto *list = value.list()) {
    if (auto room = ensure(1); !room)
      return room;
    output.push_back(std::byte{'l'});
    for (const auto &element : *list) {
      if (auto encoded = append_encoded(element, output, maximum); !encoded)
        return encoded;
    }
    if (auto room = ensure(1); !room)
      return room;
    output.push_back(std::byte{'e'});
    return {};
  }
  const auto &dictionary = *value.dictionary();
  if (auto room = ensure(1); !room)
    return room;
  output.push_back(std::byte{'d'});
  for (const auto &[key, element] : dictionary) {
    core::ByteBuffer key_bytes;
    const auto view = std::as_bytes(std::span{key});
    key_bytes.assign(view.begin(), view.end());
    if (auto encoded =
            append_encoded(Value{std::move(key_bytes)}, output, maximum);
        !encoded)
      return encoded;
    if (auto encoded = append_encoded(element, output, maximum); !encoded)
      return encoded;
  }
  if (auto room = ensure(1); !room)
    return room;
  output.push_back(std::byte{'e'});
  return {};
}

} // namespace

core::Result<core::ByteBuffer> encode(const Value &value,
                                      std::size_t maximum_bytes) {
  core::ByteBuffer output;
  if (auto encoded = append_encoded(value, output, maximum_bytes); !encoded)
    return std::unexpected(encoded.error());
  return output;
}

} // namespace sakuin::dht::bencode

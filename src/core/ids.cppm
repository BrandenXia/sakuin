export module sakuin.core.ids;

import std;

export namespace sakuin::core {

struct InfoHash {
  std::array<std::uint8_t, 20> bytes{};

  friend bool operator==(const InfoHash &, const InfoHash &) = default;
};

struct SegmentId {
  std::array<std::uint8_t, 16> bytes{};

  friend bool operator==(const SegmentId &, const SegmentId &) = default;
};

struct ObjectId {
  std::array<std::uint8_t, 32> bytes{};

  friend bool operator==(const ObjectId &, const ObjectId &) = default;
};

} // namespace sakuin::core

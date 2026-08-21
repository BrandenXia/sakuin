export module sakuin.core.hash;

import std;

import sakuin.core.bytes;

export namespace sakuin::core {

struct Hash256 {
  std::array<std::uint8_t, 32> bytes;

  friend bool operator==(const Hash256 &, const Hash256 &) = default;
};

class Sha256Hasher {
public:
  Sha256Hasher();
  ~Sha256Hasher();

  Sha256Hasher(Sha256Hasher &&) noexcept;
  Sha256Hasher &operator=(Sha256Hasher &&) noexcept;
  Sha256Hasher(const Sha256Hasher &) = delete;
  Sha256Hasher &operator=(const Sha256Hasher &) = delete;

  void update(ByteView data);
  Hash256 finalize();

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

Hash256 sha256(ByteView data);
Hash256 hmac_sha256(ByteView key, ByteView data);
bool constant_time_equal(ByteView left, ByteView right) noexcept;

} // namespace sakuin::core

module;

#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/sha.h>

module sakuin.core.hash;

namespace sakuin::core {

struct Sha256Hasher::Impl {
  EVP_MD_CTX *context{EVP_MD_CTX_new()};
  bool finalized{};
};

Sha256Hasher::Sha256Hasher() : impl_(std::make_unique<Impl>()) {
  if (impl_->context == nullptr)
    throw std::runtime_error("Failed to create EVP_MD_CTX");

  if (EVP_DigestInit_ex(impl_->context, EVP_sha256(), nullptr) != 1) {
    EVP_MD_CTX_free(impl_->context);
    impl_->context = nullptr;
    throw std::runtime_error("Failed to initialize SHA-256 digest");
  }
}

Sha256Hasher::~Sha256Hasher() {
  if (impl_ && impl_->context != nullptr)
    EVP_MD_CTX_free(impl_->context);
}

Sha256Hasher::Sha256Hasher(Sha256Hasher &&) noexcept = default;
Sha256Hasher &Sha256Hasher::operator=(Sha256Hasher &&) noexcept = default;

void Sha256Hasher::update(ByteView data) {
  if (!impl_ || impl_->finalized)
    throw std::logic_error("Cannot update a finalized SHA-256 digest");

  if (EVP_DigestUpdate(impl_->context, data.data(), data.size()) != 1)
    throw std::runtime_error("Failed to update SHA-256 digest");
}

Hash256 Sha256Hasher::finalize() {
  if (!impl_ || impl_->finalized)
    throw std::logic_error("SHA-256 digest has already been finalized");

  Hash256 result;
  unsigned int hash_len = 0;

  if (EVP_DigestFinal_ex(impl_->context, result.bytes.data(), &hash_len) != 1)
    throw std::runtime_error("Failed to finalize SHA-256 digest");

  impl_->finalized = true;
  return result;
}

Hash256 sha256(ByteView data) {
  Sha256Hasher hasher;
  hasher.update(data);
  return hasher.finalize();
}

Hash256 hmac_sha256(ByteView key, ByteView data) {
  if (key.size() > static_cast<std::size_t>(std::numeric_limits<int>::max()))
    throw std::invalid_argument("HMAC key is too large");
  Hash256 result;
  unsigned int hash_len{};
  if (HMAC(EVP_sha256(), key.data(), static_cast<int>(key.size()),
           reinterpret_cast<const unsigned char *>(data.data()), data.size(),
           result.bytes.data(), &hash_len) == nullptr ||
      hash_len != result.bytes.size())
    throw std::runtime_error("Failed to compute HMAC-SHA-256");
  return result;
}

bool constant_time_equal(ByteView left, ByteView right) noexcept {
  if (left.size() != right.size())
    return false;
  std::uint8_t difference{};
  for (std::size_t index = 0; index < left.size(); ++index)
    difference |= std::to_integer<std::uint8_t>(left[index] ^ right[index]);
  return difference == 0;
}

} // namespace sakuin::core

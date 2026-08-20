module;

#include <openssl/evp.h>
#include <openssl/sha.h>

module sakuin.core.hash;

namespace sakuin::core {

Hash256 sha256(ByteView data) {
  EVP_MD_CTX *mdctx = EVP_MD_CTX_new();
  if (mdctx == nullptr)
    throw std::runtime_error("Failed to create EVP_MD_CTX");

  if (EVP_DigestInit_ex(mdctx, EVP_sha256(), nullptr) != 1) {
    EVP_MD_CTX_free(mdctx);
    throw std::runtime_error("Failed to initialize SHA-256 digest");
  }

  if (EVP_DigestUpdate(mdctx, data.data(), data.size()) != 1) {
    EVP_MD_CTX_free(mdctx);
    throw std::runtime_error("Failed to update SHA-256 digest");
  }

  Hash256 result;
  unsigned int hash_len = 0;

  if (EVP_DigestFinal_ex(mdctx, result.bytes.data(), &hash_len) != 1) {
    EVP_MD_CTX_free(mdctx);
    throw std::runtime_error("Failed to finalize SHA-256 digest");
  }

  EVP_MD_CTX_free(mdctx);

  return result;
}

} // namespace sakuin::core

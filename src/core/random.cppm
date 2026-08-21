module;

#include <openssl/crypto.h>
#include <openssl/rand.h>

export module sakuin.core.random;

import std;

import sakuin.core.bytes;
import sakuin.core.result;

export namespace sakuin::core {

Result<void> secure_random(MutableByteView output);
void secure_clear(MutableByteView output) noexcept;

} // namespace sakuin::core

namespace sakuin::core {

Result<void> secure_random(MutableByteView output) {
  while (!output.empty()) {
    const auto count = static_cast<int>(std::min<std::size_t>(
        output.size(),
        static_cast<std::size_t>(std::numeric_limits<int>::max())));
    if (RAND_bytes(reinterpret_cast<unsigned char *>(output.data()), count) !=
        1)
      return std::unexpected(
          Error{ErrorCode::Internal, "Cryptographic random generation failed"});
    output = output.subspan(static_cast<std::size_t>(count));
  }
  return {};
}

void secure_clear(MutableByteView output) noexcept {
  OPENSSL_cleanse(output.data(), output.size());
}

} // namespace sakuin::core

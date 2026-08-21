export module sakuin.core.result;

import std;

export namespace sakuin::core {

enum class ErrorCode {
  NotFound,
  InvalidArgument,
  IoError,
  PermissionDenied,
  StorageUnavailable,
  CorruptSegment,
  ChecksumMismatch,
  UnsupportedFormat,
  InvalidManifest,
  Conflict,
  InvalidQuery,
  Timeout,
  QuotaExceeded,
  Internal,
};

struct Error {
  ErrorCode code;
  std::string message;
};

template <typename T> using Result = std::expected<T, Error>;

} // namespace sakuin::core

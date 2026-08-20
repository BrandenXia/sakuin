export module sakuin.core.result;

import std;

export namespace sakuin::core {

enum class ErrorCode {
  Io,
  InvalidArgument,
  CorruptData,
  UnsupportedVersion,
  NotFound,
  Timeout,
  Internal,
};

struct Error {
  ErrorCode code;
  std::string message;
};

template <typename T> using Result = std::expected<T, Error>;

} // namespace sakuin::core

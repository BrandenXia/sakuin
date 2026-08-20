export module sakuin.storage.dataset.write_session;

import std;

import sakuin.core.result;

export namespace sakuin::storage {

struct CommitResult {
  std::uint64_t generation{};
};

template <typename Record> class WriteSession {
public:
  virtual ~WriteSession() = default;

  WriteSession(const WriteSession &) = delete;
  WriteSession &operator=(const WriteSession &) = delete;

  virtual core::Result<void> append(const Record &record) = 0;

  virtual core::Result<void> append(std::span<const Record> records) = 0;

  // After new segment objects are durable and integrity-verified, atomically
  // publishes a manifest generation that references them. Failure leaves the
  // previously published generation visible.
  virtual core::Result<CommitResult> commit() = 0;

  // Discards all uncommitted writes.
  virtual void abort() noexcept = 0;

protected:
  WriteSession() = default;
};

} // namespace sakuin::storage

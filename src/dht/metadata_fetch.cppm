export module sakuin.dht.metadata_fetch;

import std;

import sakuin.core.bytes;
import sakuin.core.ids;
import sakuin.core.result;
import sakuin.core.time;
import sakuin.dht.metainfo;
import sakuin.dht.metadata;
import sakuin.model.torrent;
import sakuin.runtime.stream;

export namespace sakuin::dht {

struct MetadataFetchOptions {
  MetadataExchangeOptions exchange;
  MetainfoDecodeLimits metainfo;
};

class MetadataFetchObserver {
public:
  virtual ~MetadataFetchObserver() = default;
  virtual core::Result<void>
  on_metadata_fetched(model::TorrentRecord record) = 0;
  virtual void on_metadata_fetch_failed(core::Error error) = 0;
};

// Own this session, its observer, and its transport until stop() returns. The
// transport serializes callbacks; a future sender/receiver adapter can own the
// same lifetime without changing the peer-wire state machine.
class MetadataFetchSession final : private runtime::StreamReceiver {
public:
  static core::Result<std::unique_ptr<MetadataFetchSession>>
  create(core::InfoHash info_hash, PeerId peer_id, core::Timestamp observed_at,
         runtime::StreamTransport &transport, MetadataFetchObserver &observer,
         MetadataFetchOptions options = {});

  ~MetadataFetchSession();

  core::Result<void> start();
  void stop() noexcept;
  bool complete() const noexcept;

private:
  MetadataFetchSession(core::InfoHash info_hash,
                       std::unique_ptr<MetadataExchange> exchange,
                       core::Timestamp observed_at,
                       runtime::StreamTransport &transport,
                       MetadataFetchObserver &observer,
                       MetainfoDecodeLimits metainfo_limits);

  void on_stream_connected() override;
  void on_stream_data(core::ByteBuffer bytes) override;
  void on_stream_closed(std::optional<core::Error> error) override;

  void dispatch(MetadataExchangeOutput output);
  void fail(core::Error error, bool stop_transport = true) noexcept;

  core::InfoHash info_hash_;
  std::unique_ptr<MetadataExchange> exchange_;
  core::Timestamp observed_at_;
  runtime::StreamTransport &transport_;
  MetadataFetchObserver &observer_;
  MetainfoDecodeLimits metainfo_limits_;
  std::atomic<bool> started_{};
  std::atomic<bool> terminal_{};
  std::atomic<bool> succeeded_{};
};

} // namespace sakuin::dht

namespace sakuin::dht {

MetadataFetchSession::MetadataFetchSession(
    core::InfoHash info_hash, std::unique_ptr<MetadataExchange> exchange,
    core::Timestamp observed_at, runtime::StreamTransport &transport,
    MetadataFetchObserver &observer, MetainfoDecodeLimits metainfo_limits)
    : info_hash_(info_hash), exchange_(std::move(exchange)),
      observed_at_(observed_at), transport_(transport), observer_(observer),
      metainfo_limits_(metainfo_limits) {}

MetadataFetchSession::~MetadataFetchSession() { stop(); }

core::Result<std::unique_ptr<MetadataFetchSession>>
MetadataFetchSession::create(core::InfoHash info_hash, PeerId peer_id,
                             core::Timestamp observed_at,
                             runtime::StreamTransport &transport,
                             MetadataFetchObserver &observer,
                             MetadataFetchOptions options) {
  auto exchange =
      MetadataExchange::create(info_hash, peer_id, options.exchange);
  if (!exchange)
    return std::unexpected(exchange.error());
  if (options.metainfo.maximum_metadata_bytes == 0 ||
      options.metainfo.maximum_files == 0 ||
      options.metainfo.maximum_name_bytes == 0 ||
      options.metainfo.maximum_path_bytes == 0)
    return std::unexpected(
        core::Error{core::ErrorCode::InvalidArgument,
                    "Metadata fetch metainfo limits must all be nonzero"});
  if (options.exchange.maximum_metadata_bytes >
      options.metainfo.maximum_metadata_bytes)
    return std::unexpected(
        core::Error{core::ErrorCode::InvalidArgument,
                    "Exchange metadata limit exceeds metainfo decode limit"});
  return std::unique_ptr<MetadataFetchSession>{
      new MetadataFetchSession{info_hash, std::move(*exchange), observed_at,
                               transport, observer, options.metainfo}};
}

core::Result<void> MetadataFetchSession::start() {
  bool expected = false;
  if (!started_.compare_exchange_strong(expected, true,
                                        std::memory_order_acq_rel))
    return std::unexpected(core::Error{core::ErrorCode::Conflict,
                                       "Metadata fetch already started"});
  auto started = transport_.start(*this);
  if (!started) {
    terminal_.store(true, std::memory_order_release);
    return std::unexpected(started.error());
  }
  return {};
}

void MetadataFetchSession::stop() noexcept {
  terminal_.store(true, std::memory_order_release);
  transport_.stop();
}

bool MetadataFetchSession::complete() const noexcept {
  return succeeded_.load(std::memory_order_acquire);
}

void MetadataFetchSession::on_stream_connected() {
  if (terminal_.load(std::memory_order_acquire))
    return;
  auto output = exchange_->start();
  if (!output) {
    fail(output.error());
    return;
  }
  dispatch(std::move(*output));
}

void MetadataFetchSession::on_stream_data(core::ByteBuffer bytes) {
  if (terminal_.load(std::memory_order_acquire))
    return;
  auto output = exchange_->consume(bytes);
  if (!output) {
    fail(output.error());
    return;
  }
  dispatch(std::move(*output));
}

void MetadataFetchSession::on_stream_closed(std::optional<core::Error> error) {
  if (terminal_.load(std::memory_order_acquire))
    return;
  fail(error ? std::move(*error)
             : core::Error{core::ErrorCode::IoError,
                           "TCP peer closed before metadata completed"},
       false);
}

void MetadataFetchSession::dispatch(MetadataExchangeOutput output) {
  for (auto &bytes : output.sends) {
    auto sent = transport_.send(std::move(bytes));
    if (!sent) {
      fail(sent.error());
      return;
    }
  }
  if (!output.metadata)
    return;

  auto record = decode_metainfo(info_hash_, *output.metadata, observed_at_,
                                metainfo_limits_);
  if (!record) {
    fail(record.error());
    return;
  }
  try {
    auto accepted = observer_.on_metadata_fetched(std::move(*record));
    if (!accepted) {
      fail(accepted.error());
      return;
    }
  } catch (const std::exception &exception) {
    fail(core::Error{core::ErrorCode::Internal,
                     std::string{"Metadata observer threw: "} +
                         exception.what()});
    return;
  } catch (...) {
    fail(core::Error{core::ErrorCode::Internal,
                     "Metadata observer threw an unknown exception"});
    return;
  }
  if (terminal_.exchange(true, std::memory_order_acq_rel))
    return;
  succeeded_.store(true, std::memory_order_release);
  transport_.stop();
}

void MetadataFetchSession::fail(core::Error error,
                                bool stop_transport) noexcept {
  if (terminal_.exchange(true, std::memory_order_acq_rel))
    return;
  if (stop_transport)
    transport_.stop();
  try {
    observer_.on_metadata_fetch_failed(std::move(error));
  } catch (...) {
    // Observer callbacks may not unwind through the runtime thread.
  }
}

} // namespace sakuin::dht

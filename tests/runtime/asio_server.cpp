import std;

import sakuin.core;
import sakuin.runtime;
import sakuin.runtime.asio_server;
import sakuin.runtime.asio_stream;

namespace {

class Handler final : public sakuin::runtime::StreamServerHandler,
                      public sakuin::runtime::StreamServerEvents {
public:
  void on_stream_session_opened(
      sakuin::runtime::StreamServerSession &session) override {
    std::lock_guard lock{mutex_};
    opened_ = session.id();
    remote_ = session.remote_endpoint();
    condition_.notify_all();
  }

  void on_stream_session_data(sakuin::runtime::StreamServerSession &session,
                              sakuin::core::ByteBuffer bytes) override {
    auto sent = session.send(bytes);
    std::lock_guard lock{mutex_};
    received_.insert(received_.end(), bytes.begin(), bytes.end());
    if (!sent)
      error_ = sent.error();
    condition_.notify_all();
  }

  void
  on_stream_session_closed(sakuin::runtime::StreamSessionId session,
                           std::optional<sakuin::core::Error> error) override {
    std::lock_guard lock{mutex_};
    closed_ = session;
    close_error_ = std::move(error);
    condition_.notify_all();
  }

  void on_stream_server_error(sakuin::core::Error error) override {
    std::lock_guard lock{mutex_};
    error_ = std::move(error);
    condition_.notify_all();
  }

  bool wait_for_data(std::string_view expected) {
    std::unique_lock lock{mutex_};
    condition_.wait_for(lock, std::chrono::seconds{5}, [&] {
      return received_.size() >= expected.size() || error_.has_value();
    });
    return !error_ && opened_ != 0 &&
           std::ranges::equal(received_, std::as_bytes(std::span{expected}));
  }

  bool wait_for_idle_close() {
    std::unique_lock lock{mutex_};
    condition_.wait_for(lock, std::chrono::seconds{5},
                        [&] { return closed_ != 0 || error_.has_value(); });
    return !error_ && closed_ == opened_ && close_error_ &&
           close_error_->code == sakuin::core::ErrorCode::Timeout;
  }

  sakuin::runtime::StreamEndpoint remote() const {
    std::lock_guard lock{mutex_};
    return remote_;
  }

private:
  mutable std::mutex mutex_;
  std::condition_variable condition_;
  sakuin::runtime::StreamSessionId opened_{};
  sakuin::runtime::StreamSessionId closed_{};
  sakuin::runtime::StreamEndpoint remote_{};
  sakuin::core::ByteBuffer received_;
  std::optional<sakuin::core::Error> close_error_;
  std::optional<sakuin::core::Error> error_;
};

class Receiver final : public sakuin::runtime::StreamReceiver {
public:
  sakuin::runtime::StreamTransport *transport{};

  void on_stream_connected() override {
    constexpr std::string_view message{"ping"};
    const auto input = std::as_bytes(std::span{message});
    auto sent = transport->send({input.begin(), input.end()});
    std::lock_guard lock{mutex_};
    if (!sent)
      error_ = sent.error();
    condition_.notify_all();
  }

  void on_stream_data(sakuin::core::ByteBuffer bytes) override {
    std::lock_guard lock{mutex_};
    received_.insert(received_.end(), bytes.begin(), bytes.end());
    condition_.notify_all();
  }

  void on_stream_closed(std::optional<sakuin::core::Error> error) override {
    std::lock_guard lock{mutex_};
    closed_ = true;
    if (error)
      error_ = std::move(error);
    condition_.notify_all();
  }

  bool wait_for(std::string_view expected) {
    std::unique_lock lock{mutex_};
    condition_.wait_for(lock, std::chrono::seconds{5}, [&] {
      return received_.size() >= expected.size() || error_.has_value();
    });
    return !error_ &&
           std::ranges::equal(received_, std::as_bytes(std::span{expected}));
  }

private:
  std::mutex mutex_;
  std::condition_variable condition_;
  sakuin::core::ByteBuffer received_;
  std::optional<sakuin::core::Error> error_;
  bool closed_{};
};

} // namespace

int main() {
  using namespace sakuin;
  auto server = runtime::AsioTcpStreamServer::create(
      {.bind_to = {.address = runtime::IpAddress::loopback_v4(), .port = 0},
       .idle_timeout = std::chrono::milliseconds{100},
       .read_buffer_bytes = 64,
       .maximum_queued_write_bytes = 64,
       .maximum_connections = 2});
  if (!server || (*server)->local_endpoint().port == 0)
    return 1;
  Handler handler;
  if (!(*server)->start(handler, &handler))
    return 2;

  runtime::AsioTcpStreamTransportFactory clients;
  auto client = clients.create({.remote = (*server)->local_endpoint(),
                                .connect_timeout = std::chrono::seconds{2},
                                .idle_timeout = std::chrono::seconds{2},
                                .read_buffer_bytes = 64,
                                .maximum_queued_write_bytes = 64});
  if (!client)
    return 3;
  Receiver receiver;
  receiver.transport = client->get();
  if (!(*client)->start(receiver) || !receiver.wait_for("ping") ||
      !handler.wait_for_data("ping"))
    return 4;
  if (handler.remote().address != runtime::IpAddress::loopback_v4() ||
      handler.remote().port == 0)
    return 5;
  if (!handler.wait_for_idle_close())
    return 6;
  (*client)->stop();
  (*server)->stop();

  auto invalid = runtime::AsioTcpStreamServer::create(
      {.bind_to = {.address = runtime::IpAddress::loopback_v4(), .port = 0},
       .read_buffer_bytes = 0});
  if (invalid || invalid.error().code != core::ErrorCode::InvalidArgument)
    return 7;
  return 0;
}

#include <asio.hpp>

import std;

import sakuin.core;
import sakuin.runtime;
import sakuin.runtime.asio_stream;

namespace {

class Receiver final : public sakuin::runtime::StreamReceiver {
public:
  sakuin::runtime::StreamTransport *transport{};
  std::optional<sakuin::core::ByteBuffer> initial_write;

  void on_stream_connected() override {
    auto sent = initial_write ? transport->send(*initial_write)
                              : sakuin::core::Result<void>{};
    std::lock_guard lock{mutex_};
    connected_ = true;
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
    return !error_ && connected_ &&
           std::ranges::equal(received_, std::as_bytes(std::span{expected}));
  }

  bool wait_for_error(sakuin::core::ErrorCode expected) {
    std::unique_lock lock{mutex_};
    condition_.wait_for(lock, std::chrono::seconds{5},
                        [&] { return error_.has_value(); });
    return connected_ && error_ && error_->code == expected;
  }

private:
  std::mutex mutex_;
  std::condition_variable condition_;
  sakuin::core::ByteBuffer received_;
  std::optional<sakuin::core::Error> error_;
  bool connected_{};
  bool closed_{};
};

} // namespace

int main() {
  using namespace sakuin;
  asio::io_context context;
  asio::ip::tcp::acceptor acceptor{context,
                                   {asio::ip::address_v4::loopback(), 0}};
  const auto port = acceptor.local_endpoint().port();
  std::atomic<bool> server_ok{};
  std::jthread server{[&] {
    asio::ip::tcp::socket socket{context};
    asio::error_code error;
    acceptor.accept(socket, error);
    if (error)
      return;
    std::array<char, 4> input{};
    asio::read(socket, asio::buffer(input), error);
    if (error || std::string_view{input.data(), input.size()} != "ping")
      return;
    asio::write(socket, asio::buffer(std::string_view{"pong"}), error);
    server_ok.store(!error, std::memory_order_release);
  }};

  runtime::StreamTransportOptions options{
      .remote = {.address = runtime::IpAddress::loopback_v4(), .port = port},
      .connect_timeout = std::chrono::seconds{2},
      .idle_timeout = std::chrono::seconds{2},
      .read_buffer_bytes = 64,
      .maximum_queued_write_bytes = 64};
  runtime::AsioTcpStreamTransportFactory factory;
  auto transport = factory.create(options);
  if (!transport)
    return 1;
  Receiver receiver;
  receiver.transport = transport->get();
  constexpr std::string_view ping_text{"ping"};
  const auto ping = std::as_bytes(std::span{ping_text});
  receiver.initial_write = core::ByteBuffer{ping.begin(), ping.end()};
  if (!(*transport)->start(receiver))
    return 2;
  if (!receiver.wait_for("pong"))
    return 3;
  (*transport)->stop();
  server.join();
  if (!server_ok.load(std::memory_order_acquire) ||
      (*transport)->remote_endpoint() != options.remote)
    return 4;
  if ((*transport)->send({std::byte{1}}))
    return 5;

  asio::io_context idle_context;
  asio::ip::tcp::acceptor idle_acceptor{idle_context,
                                        {asio::ip::address_v4::loopback(), 0}};
  const auto idle_port = idle_acceptor.local_endpoint().port();
  std::jthread idle_server{[&] {
    asio::ip::tcp::socket socket{idle_context};
    asio::error_code error;
    idle_acceptor.accept(socket, error);
    if (!error)
      std::this_thread::sleep_for(std::chrono::milliseconds{250});
  }};
  auto idle_transport =
      factory.create({.remote = {.address = runtime::IpAddress::loopback_v4(),
                                 .port = idle_port},
                      .connect_timeout = std::chrono::seconds{2},
                      .idle_timeout = std::chrono::milliseconds{50},
                      .read_buffer_bytes = 64,
                      .maximum_queued_write_bytes = 64});
  if (!idle_transport)
    return 6;
  Receiver idle_receiver;
  idle_receiver.transport = idle_transport->get();
  if (!(*idle_transport)->start(idle_receiver) ||
      !idle_receiver.wait_for_error(core::ErrorCode::Timeout))
    return 7;
  (*idle_transport)->stop();
  idle_server.join();
  return 0;
}

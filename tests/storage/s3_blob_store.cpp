#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

import std;

import sakuin.core;
import sakuin.storage;

namespace {

struct TemporaryDirectory {
  std::filesystem::path path;
  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path, ignored);
  }
};

std::string lowercase(std::string value) {
  std::ranges::transform(value, value.begin(), [](unsigned char character) {
    return static_cast<char>(std::tolower(character));
  });
  return value;
}

class FakeS3 final {
public:
  FakeS3() {
    socket_ = ::socket(AF_INET, SOCK_STREAM, 0);
    int reuse = 1;
    ::setsockopt(socket_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    sockaddr_in address{.sin_family = AF_INET,
                        .sin_port = 0,
                        .sin_addr = {.s_addr = htonl(INADDR_LOOPBACK)}};
    if (socket_ < 0 ||
        ::bind(socket_, reinterpret_cast<sockaddr *>(&address),
               sizeof(address)) != 0 ||
        ::listen(socket_, 16) != 0)
      throw std::runtime_error{"Could not start fake S3 server"};
    socklen_t size = sizeof(address);
    if (::getsockname(socket_, reinterpret_cast<sockaddr *>(&address), &size) !=
        0)
      throw std::runtime_error{"Could not inspect fake S3 server"};
    port_ = ntohs(address.sin_port);
    thread_ = std::thread{[this] { serve(); }};
  }

  ~FakeS3() {
    stopped_.store(true);
    ::shutdown(socket_, SHUT_RDWR);
    ::close(socket_);
    if (thread_.joinable())
      thread_.join();
  }

  std::uint16_t port() const noexcept { return port_; }
  bool observed_signed_request() const noexcept { return signed_.load(); }
  std::size_t put_requests() const noexcept { return put_requests_.load(); }
  std::size_t head_requests() const noexcept { return head_requests_.load(); }

  void fail_next_puts(std::size_t count) noexcept {
    transient_put_failures_.store(count);
  }

  void corrupt(std::string value) {
    std::scoped_lock lock{mutex_};
    for (auto &[_, body] : objects_)
      body = value;
  }

private:
  static void send_all(int connection, std::string_view data) {
    while (!data.empty()) {
      const auto written = ::send(connection, data.data(), data.size(), 0);
      if (written <= 0)
        return;
      data.remove_prefix(static_cast<std::size_t>(written));
    }
  }

  void serve() {
    while (!stopped_.load()) {
      const auto connection = ::accept(socket_, nullptr, nullptr);
      if (connection < 0)
        continue;
      handle(connection);
      ::close(connection);
    }
  }

  void handle(int connection) {
    std::string request;
    std::array<char, 4096> buffer{};
    std::size_t header_end = std::string::npos;
    while ((header_end = request.find("\r\n\r\n")) == std::string::npos) {
      const auto count = ::recv(connection, buffer.data(), buffer.size(), 0);
      if (count <= 0)
        return;
      request.append(buffer.data(), static_cast<std::size_t>(count));
    }
    const auto header_text = request.substr(0, header_end + 2);
    std::istringstream headers{header_text};
    std::string request_line;
    std::getline(headers, request_line);
    if (!request_line.empty() && request_line.back() == '\r')
      request_line.pop_back();
    std::istringstream line{request_line};
    std::string method;
    std::string target;
    line >> method >> target;

    std::size_t content_length{};
    std::string header;
    bool authorization{};
    bool payload_hash{};
    while (std::getline(headers, header)) {
      if (!header.empty() && header.back() == '\r')
        header.pop_back();
      const auto normalized = lowercase(header);
      if (normalized.starts_with("content-length:"))
        content_length = static_cast<std::size_t>(
            std::stoull(header.substr(header.find(':') + 1)));
      authorization |= normalized.starts_with("authorization:") &&
                       normalized.contains("aws4-hmac-sha256");
      payload_hash |= normalized.starts_with("x-amz-content-sha256:");
    }
    signed_.store(signed_.load() || (authorization && payload_hash));

    std::string body = request.substr(header_end + 4);
    while (body.size() < content_length) {
      const auto count = ::recv(connection, buffer.data(), buffer.size(), 0);
      if (count <= 0)
        return;
      body.append(buffer.data(), static_cast<std::size_t>(count));
    }
    body.resize(content_length);

    std::string response_body;
    int status{};
    {
      std::scoped_lock lock{mutex_};
      if (method == "PUT") {
        put_requests_.fetch_add(1);
        if (transient_put_failures_.load() > 0) {
          transient_put_failures_.fetch_sub(1);
          status = 503;
        } else {
          objects_[target] = std::move(body);
          status = 200;
        }
      } else if (method == "HEAD") {
        head_requests_.fetch_add(1);
        const auto found = objects_.find(target);
        if (found == objects_.end())
          status = 404;
        else {
          status = 200;
          response_body = found->second;
        }
      } else if (method == "GET") {
        const auto found = objects_.find(target);
        if (found == objects_.end())
          status = 404;
        else {
          status = 200;
          response_body = found->second;
        }
      } else if (method == "DELETE") {
        objects_.erase(target);
        status = 204;
      } else {
        status = 405;
      }
    }
    const auto reason = status == 200   ? "OK"
                        : status == 204 ? "No Content"
                        : status == 404 ? "Not Found"
                        : status == 503 ? "Service Unavailable"
                                        : "Method Not Allowed";
    auto response =
        "HTTP/1.1 " + std::to_string(status) + " " + reason +
        "\r\nContent-Length: " + std::to_string(response_body.size()) +
        "\r\nConnection: close\r\n\r\n";
    if (method != "HEAD")
      response += response_body;
    send_all(connection, response);
  }

  int socket_{-1};
  std::uint16_t port_{};
  std::thread thread_;
  std::atomic<bool> stopped_{};
  std::atomic<bool> signed_{};
  std::atomic<std::size_t> put_requests_{};
  std::atomic<std::size_t> head_requests_{};
  std::atomic<std::size_t> transient_put_failures_{};
  std::mutex mutex_;
  std::unordered_map<std::string, std::string> objects_;
};

} // namespace

int main() {
  using namespace sakuin;
  const auto nonce = std::to_string(
      std::chrono::steady_clock::now().time_since_epoch().count());
  TemporaryDirectory directory{std::filesystem::temp_directory_path() /
                               ("sakuin-s3-test-" + nonce)};
  FakeS3 server;
  auto store = storage::S3BlobStore::open(
      {.endpoint = "http://127.0.0.1:" + std::to_string(server.port()),
       .bucket = "test-bucket",
       .region = "us-test-1",
       .prefix = "/catalog/test/",
       .staging_directory = directory.path,
       .access_key_id = "test-access-key",
       .secret_access_key = "test-secret-key",
       .session_token = "test-session-token",
       .maximum_attempts = 3,
       .retry_delay = std::chrono::milliseconds::zero(),
       .verify_tls = false});
  if (!store)
    return 1;

  const core::ByteBuffer payload{std::byte{0x10}, std::byte{0x20},
                                 std::byte{0x30}, std::byte{0x40}};
  auto writer = (*store)->create();
  if (!writer || !(*writer)->write(payload))
    return 2;
  server.fail_next_puts(2);
  auto id = (*writer)->finalize();
  if (!id || !server.observed_signed_request() || server.put_requests() != 3)
    return 3;
  auto found = (*store)->exists(*id);
  if (!found || !*found)
    return 4;

  auto reader = (*store)->open(*id);
  if (!reader || (*reader)->size() != payload.size())
    return 5;
  core::ByteBuffer output(payload.size());
  auto read = (*reader)->read(output);
  std::array<std::byte, 2> middle{};
  auto random = (*reader)->read_at(1, middle);
  if (!read || *read != payload.size() || output != payload || !random ||
      *random != middle.size() || middle[0] != payload[1] ||
      middle[1] != payload[2])
    return 6;
  reader->reset();

  server.corrupt("bad!");
  auto corrupt = (*store)->open(*id);
  if (corrupt || corrupt.error().code != core::ErrorCode::ChecksumMismatch)
    return 7;

  if (!(*store)->remove(*id))
    return 8;
  found = (*store)->exists(*id);
  if (!found || *found)
    return 9;
  const auto head_requests = server.head_requests();
  auto missing = (*store)->open(*id);
  if (missing || missing.error().code != core::ErrorCode::NotFound ||
      server.head_requests() != head_requests + 1)
    return 10;
  return 0;
}

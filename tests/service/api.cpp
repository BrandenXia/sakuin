#include <asio.hpp>

import std;

import sakuin.api;
import sakuin.api.credentials;
import sakuin.config;
import sakuin.core;
import sakuin.model.torrent;
import sakuin.search.rebuild;
import sakuin.service.api;
import sakuin.service.storage;
import sakuin.storage.dataset.torrents;
import sakuin.storage.dataset.write_session;

namespace {

struct TemporaryDirectory {
  TemporaryDirectory() {
    path = std::filesystem::temp_directory_path() /
           ("sakuin-api-service-" +
            std::to_string(
                std::chrono::steady_clock::now().time_since_epoch().count()));
  }
  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path, ignored);
  }
  std::filesystem::path path;
};

class Observer final : public sakuin::service::ApiServiceObserver {
public:
  void on_api_error(sakuin::core::Error) override {
    {
      std::lock_guard lock{mutex};
      ++errors;
    }
    changed.notify_all();
  }

  void on_search_index_refreshed(
      sakuin::search::SearchRebuildResult result) override {
    {
      std::lock_guard lock{mutex};
      generation = std::max(generation, result.source_generation);
    }
    changed.notify_all();
  }

  bool wait_for_generation(std::uint64_t expected) {
    std::unique_lock lock{mutex};
    return changed.wait_for(lock, std::chrono::seconds{2},
                            [&] { return generation >= expected; });
  }

  std::mutex mutex;
  std::condition_variable changed;
  std::uint64_t generation{};
  std::size_t errors{};
};

std::uint16_t available_port() {
  asio::io_context context;
  asio::ip::tcp::acceptor acceptor{context,
                                   {asio::ip::address_v4::loopback(), 0}};
  return acceptor.local_endpoint().port();
}

std::string request(std::uint16_t port, std::string_view wire) {
  asio::io_context context;
  asio::ip::tcp::socket socket{context};
  socket.connect({asio::ip::address_v4::loopback(), port});
  asio::write(socket, asio::buffer(wire));
  std::string response;
  std::array<char, 4096> buffer{};
  asio::error_code error;
  while (true) {
    const auto count = socket.read_some(asio::buffer(buffer), error);
    response.append(buffer.data(), count);
    if (error == asio::error::eof)
      break;
    if (error)
      return {};
  }
  return response;
}

std::string bearer(std::string_view key_id,
                   const sakuin::api::ApiKeySecret &secret) {
  constexpr std::string_view digits{"0123456789abcdef"};
  std::string result{"Bearer sakuin_"};
  result.append(key_id).push_back('_');
  for (const auto byte : secret.bytes) {
    result.push_back(digits[byte >> 4]);
    result.push_back(digits[byte & 0xf]);
  }
  return result;
}

} // namespace

int main() {
  using namespace sakuin;
  TemporaryDirectory temporary;
  auto storage_configuration = config::defaults().storage;
  storage_configuration.local_root = temporary.path / "data";
  auto canonical = service::LocalCanonicalStorage::open(storage_configuration);
  if (!canonical)
    return 1;

  const auto credentials = temporary.path / "credentials";
  auto credential_store = api::LocalApiCredentialStore::initialize(credentials);
  if (!credential_store)
    return 2;
  auto api_configuration = config::defaults().api;
  api_configuration.credential_store_directory = credentials;
  api_configuration.listen_port = available_port();
  api_configuration.rate_limit.enabled = false;

  Observer observer;
  auto api_service = service::LocalApiService::create(
      api_configuration, (*canonical)->torrents(), observer);
  if (!api_service ||
      (*api_service)->local_endpoint().port != api_configuration.listen_port ||
      !(*api_service)->start() || !(*api_service)->running() ||
      (*api_service)->start())
    return 3;
  const auto health = request(api_configuration.listen_port,
                              "GET /v1/health HTTP/1.1\r\nHost: "
                              "localhost\r\nConnection: close\r\n\r\n");
  if (!health.starts_with("HTTP/1.1 200 OK\r\n") ||
      !health.ends_with("\r\n\r\n{\"status\":\"ok\"}"))
    return 4;

  api::ApiKeySecret secret;
  std::ranges::iota(secret.bytes, std::uint8_t{33});
  auto pepper = (*credential_store)->pepper();
  if (!pepper)
    return 5;
  auto verifier = api::derive_api_key_verifier(
      *pepper, "reader",
      {reinterpret_cast<const std::byte *>(secret.bytes.data()),
       secret.bytes.size()});
  if (!verifier ||
      !(*credential_store)
           ->insert({.key_id = "reader",
                     .verifier = *verifier,
                     .permissions = {api::Permission::Search}}) ||
      !(*api_service)->reload_credentials())
    return 5;

  model::TorrentRecord record;
  record.info_hash.bytes.fill(0x42);
  record.first_seen = core::Timestamp{core::Timestamp::duration{10}};
  record.last_seen = core::Timestamp{core::Timestamp::duration{20}};
  record.name = "Sakuin Linux Image";
  record.total_size = 4096;
  record.files.push_back({.path = "sakuin.iso", .size = 4096});
  auto write = (*canonical)->torrents().begin_write();
  if (!write || !(*write)->append(record))
    return 6;
  auto committed = (*write)->commit();
  if (!committed)
    return 7;
  (*api_service)->request_search_refresh(committed->generation);
  if (!observer.wait_for_generation(committed->generation))
    return 8;

  const auto token = bearer("reader", secret);
  const auto search = request(
      api_configuration.listen_port,
      "GET /v1/search?q=linux HTTP/1.1\r\nHost: localhost\r\nAuthorization: " +
          token + "\r\nConnection: close\r\n\r\n");
  if (!search.starts_with("HTTP/1.1 200 OK\r\n") ||
      !search.contains("Sakuin Linux Image") ||
      !search.contains("\"source_generation\":1")) {
    std::cerr << search << '\n';
    return 9;
  }

  (*api_service)->stop();
  if ((*api_service)->running() || observer.errors != 0)
    return 10;

  auto invalid_tls = api_configuration;
  invalid_tls.tls_certificate_chain_file = "chain.pem";
  if (service::LocalApiService::create(invalid_tls, (*canonical)->torrents(),
                                       observer))
    return 11;
  auto invalid_address = api_configuration;
  invalid_address.listen_address = "not a valid listen address !!!";
  invalid_address.listen_port = available_port();
  if (service::LocalApiService::create(invalid_address,
                                       (*canonical)->torrents(), observer))
    return 12;
  return 0;
}

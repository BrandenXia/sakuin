module;

#include <curl/curl.h>
#include <unistd.h>

export module sakuin.storage.blob.s3;

import std;

import sakuin.core.bytes;
import sakuin.core.hash;
import sakuin.core.ids;
import sakuin.core.result;
import sakuin.storage.blob.reader;
import sakuin.storage.blob.store;
import sakuin.storage.blob.writer;

export namespace sakuin::storage {

struct S3BlobStoreOptions {
  std::string endpoint;
  std::string bucket;
  std::string region{"us-east-1"};
  std::string prefix{"sakuin"};
  std::filesystem::path staging_directory;
  std::string access_key_id;
  std::string secret_access_key;
  std::string session_token;
  std::chrono::milliseconds connect_timeout{std::chrono::seconds{10}};
  std::chrono::milliseconds request_timeout{std::chrono::minutes{5}};
  std::size_t maximum_attempts{3};
  std::chrono::milliseconds retry_delay{std::chrono::milliseconds{200}};
  bool verify_tls{true};
};

// S3 stores the same SHA-256-addressed immutable objects as LocalBlobStore.
// Manifests and operational locks deliberately remain outside this class.
class S3BlobStore final : public BlobStore {
public:
  struct State;

  static core::Result<std::unique_ptr<S3BlobStore>>
  open(S3BlobStoreOptions options);
  static core::Result<std::unique_ptr<S3BlobStore>>
  open_from_environment(S3BlobStoreOptions options);

  core::Result<std::unique_ptr<BlobWriter>> create() override;
  core::Result<std::unique_ptr<BlobReader>> open(core::ObjectId id) override;
  core::Result<bool> exists(core::ObjectId id) override;
  core::Result<void> remove(core::ObjectId id) override;

private:
  explicit S3BlobStore(std::shared_ptr<State> state);
  std::shared_ptr<State> state_;
};

} // namespace sakuin::storage

namespace sakuin::storage {
namespace {

std::string object_name(const core::ObjectId &id) {
  constexpr std::string_view digits{"0123456789abcdef"};
  std::string result(id.bytes.size() * 2, '0');
  for (std::size_t index = 0; index < id.bytes.size(); ++index) {
    result[index * 2] = digits[id.bytes[index] >> 4U];
    result[index * 2 + 1] = digits[id.bytes[index] & 0x0fU];
  }
  return result;
}

core::ObjectId object_id(const core::Hash256 &hash) {
  return {.bytes = hash.bytes};
}

std::string uri_component(std::string_view value, bool preserve_slash = false) {
  constexpr std::string_view digits{"0123456789ABCDEF"};
  std::string result;
  result.reserve(value.size());
  for (const auto character : value) {
    const auto byte = static_cast<unsigned char>(character);
    if (std::isalnum(byte) || character == '-' || character == '_' ||
        character == '.' || character == '~' ||
        (preserve_slash && character == '/')) {
      result.push_back(character);
      continue;
    }
    result.push_back('%');
    result.push_back(digits[byte >> 4U]);
    result.push_back(digits[byte & 0x0fU]);
  }
  return result;
}

std::optional<std::string> environment(std::string_view name) {
  if (const auto *value = std::getenv(std::string{name}.c_str()); value)
    return std::string{value};
  return std::nullopt;
}

core::Error
storage_error(std::string operation, long status,
              core::ErrorCode code = core::ErrorCode::StorageUnavailable) {
  auto message = "S3 " + std::move(operation);
  if (status != 0)
    message += " returned HTTP " + std::to_string(status);
  return {code, std::move(message)};
}

core::Error curl_error(std::string operation, CURLcode code,
                       std::string_view detail) {
  auto message = "S3 " + std::move(operation) + " failed: ";
  message += detail.empty() ? curl_easy_strerror(code) : detail;
  auto error_code = core::ErrorCode::StorageUnavailable;
  if (code == CURLE_OPERATION_TIMEDOUT)
    error_code = core::ErrorCode::Timeout;
  else if (code == CURLE_URL_MALFORMAT || code == CURLE_UNSUPPORTED_PROTOCOL)
    error_code = core::ErrorCode::InvalidArgument;
  else if (code == CURLE_READ_ERROR || code == CURLE_WRITE_ERROR)
    error_code = core::ErrorCode::IoError;
  else if (code == CURLE_FAILED_INIT || code == CURLE_OUT_OF_MEMORY ||
           code == CURLE_BAD_FUNCTION_ARGUMENT)
    error_code = core::ErrorCode::Internal;
  return {error_code, std::move(message)};
}

core::Error status_error(std::string operation, long status) {
  if (status == 401 || status == 403)
    return storage_error(std::move(operation), status,
                         core::ErrorCode::PermissionDenied);
  if (status == 404)
    return storage_error(std::move(operation), status,
                         core::ErrorCode::NotFound);
  if (status == 409 || status == 412)
    return storage_error(std::move(operation), status,
                         core::ErrorCode::Conflict);
  if (status == 429 || status == 507)
    return storage_error(std::move(operation), status,
                         core::ErrorCode::QuotaExceeded);
  return storage_error(std::move(operation), status);
}

std::string normalized_prefix(std::string value) {
  while (!value.empty() && value.front() == '/')
    value.erase(value.begin());
  while (!value.empty() && value.back() == '/')
    value.pop_back();
  return value;
}

core::Result<core::ObjectId> hash_file(const std::filesystem::path &path) {
  std::ifstream input{path, std::ios::binary};
  if (!input)
    return std::unexpected(
        core::Error{core::ErrorCode::IoError, "Could not open staged S3 blob"});
  core::Sha256Hasher hasher;
  std::array<std::byte, 64U * 1024U> buffer{};
  while (input) {
    input.read(reinterpret_cast<char *>(buffer.data()), buffer.size());
    const auto count = static_cast<std::size_t>(input.gcount());
    if (count != 0)
      hasher.update(std::span{buffer}.first(count));
  }
  if (!input.eof())
    return std::unexpected(
        core::Error{core::ErrorCode::IoError, "Could not read staged S3 blob"});
  return object_id(hasher.finalize());
}

std::filesystem::path temporary_path(const std::filesystem::path &directory) {
  static std::atomic<std::uint64_t> sequence{};
  const auto nonce =
      std::to_string(::getpid()) + "-" + std::to_string(sequence.fetch_add(1)) +
      "-" +
      std::to_string(
          std::chrono::steady_clock::now().time_since_epoch().count());
  return directory / nonce;
}

class StagedBlobReader final : public BlobReader {
public:
  StagedBlobReader(std::filesystem::path path, std::uint64_t size)
      : path_(std::move(path)), stream_(path_, std::ios::binary), size_(size) {}

  ~StagedBlobReader() override {
    stream_.close();
    std::error_code ignored;
    std::filesystem::remove(path_, ignored);
  }

  bool is_open() const noexcept { return stream_.is_open(); }

  core::Result<std::size_t> read(core::MutableByteView buffer) override {
    stream_.read(reinterpret_cast<char *>(buffer.data()),
                 static_cast<std::streamsize>(buffer.size()));
    const auto count = static_cast<std::size_t>(stream_.gcount());
    if (stream_.bad())
      return std::unexpected(core::Error{core::ErrorCode::IoError,
                                         "Could not read staged S3 blob"});
    return count;
  }

  core::Result<std::size_t> read_at(std::uint64_t offset,
                                    core::MutableByteView buffer) override {
    std::ifstream stream{path_, std::ios::binary};
    if (!stream)
      return std::unexpected(core::Error{core::ErrorCode::IoError,
                                         "Could not open staged S3 blob"});
    stream.seekg(static_cast<std::streamoff>(offset));
    if (!stream)
      return std::unexpected(core::Error{core::ErrorCode::IoError,
                                         "Could not seek staged S3 blob"});
    stream.read(reinterpret_cast<char *>(buffer.data()),
                static_cast<std::streamsize>(buffer.size()));
    const auto count = static_cast<std::size_t>(stream.gcount());
    if (stream.bad())
      return std::unexpected(core::Error{core::ErrorCode::IoError,
                                         "Could not read staged S3 blob"});
    return count;
  }

  std::uint64_t size() const noexcept override { return size_; }

private:
  std::filesystem::path path_;
  std::ifstream stream_;
  std::uint64_t size_{};
};

} // namespace

struct S3BlobStore::State {
  struct Response {
    long status{};
    std::uint64_t content_length{};
  };

  explicit State(S3BlobStoreOptions configured)
      : options(std::move(configured)) {}

  std::string key(const core::ObjectId &id) const {
    const auto name = object_name(id);
    auto result = options.prefix;
    if (!result.empty())
      result += '/';
    return result + "objects/" + name.substr(0, 2) + "/" + name;
  }

  std::string url(const core::ObjectId &id) const {
    return options.endpoint + "/" + uri_component(options.bucket) + "/" +
           uri_component(key(id), true);
  }

  core::Result<Response>
  request_once(std::string_view method, core::ObjectId id,
               const std::filesystem::path *upload = nullptr,
               const std::filesystem::path *download = nullptr) const {
    static const auto initialized = curl_global_init(CURL_GLOBAL_DEFAULT);
    if (initialized != CURLE_OK)
      return std::unexpected(
          curl_error("client initialization", initialized, {}));

    std::unique_ptr<CURL, decltype(&curl_easy_cleanup)> handle{
        curl_easy_init(), curl_easy_cleanup};
    if (!handle)
      return std::unexpected(core::Error{core::ErrorCode::Internal,
                                         "Could not allocate S3 HTTP client"});

    std::array<char, CURL_ERROR_SIZE> detail{};
    const auto request_url = url(id);
    const auto authentication = "aws:amz:" + options.region + ":s3";
    const auto user_password =
        options.access_key_id + ":" + options.secret_access_key;
    auto set = [&](CURLoption option, auto value) -> core::Result<void> {
      const auto code = curl_easy_setopt(handle.get(), option, value);
      if (code != CURLE_OK)
        return std::unexpected(curl_error("client configuration", code, {}));
      return {};
    };
    for (auto configured :
         {set(CURLOPT_URL, request_url.c_str()),
          set(CURLOPT_AWS_SIGV4, authentication.c_str()),
          set(CURLOPT_USERPWD, user_password.c_str()),
          set(CURLOPT_ERRORBUFFER, detail.data()),
          set(CURLOPT_CONNECTTIMEOUT_MS,
              static_cast<long>(options.connect_timeout.count())),
          set(CURLOPT_TIMEOUT_MS,
              static_cast<long>(options.request_timeout.count())),
          set(CURLOPT_NOSIGNAL, 1L), set(CURLOPT_FOLLOWLOCATION, 0L),
          set(CURLOPT_SSL_VERIFYPEER, options.verify_tls ? 1L : 0L),
          set(CURLOPT_SSL_VERIFYHOST, options.verify_tls ? 2L : 0L)})
      if (!configured)
        return std::unexpected(configured.error());

    std::unique_ptr<curl_slist, decltype(&curl_slist_free_all)> headers{
        nullptr, curl_slist_free_all};
    auto add_header = [&](std::string value) -> core::Result<void> {
      auto *updated = curl_slist_append(headers.get(), value.c_str());
      if (!updated)
        return std::unexpected(core::Error{core::ErrorCode::Internal,
                                           "Could not allocate S3 headers"});
      headers.release();
      headers.reset(updated);
      return {};
    };
    if (!options.session_token.empty()) {
      if (auto added =
              add_header("x-amz-security-token: " + options.session_token);
          !added)
        return std::unexpected(added.error());
    }

    std::unique_ptr<std::FILE, decltype(&std::fclose)> file{nullptr,
                                                            std::fclose};
    if (method == "HEAD") {
      if (auto configured = set(CURLOPT_NOBODY, 1L); !configured)
        return std::unexpected(configured.error());
    } else if (method == "DELETE") {
      if (auto configured = set(CURLOPT_CUSTOMREQUEST, "DELETE"); !configured)
        return std::unexpected(configured.error());
    } else if (method == "PUT") {
      file.reset(std::fopen(upload->c_str(), "rb"));
      if (!file)
        return std::unexpected(core::Error{core::ErrorCode::IoError,
                                           "Could not open staged S3 upload"});
      std::error_code size_error;
      const auto size = std::filesystem::file_size(*upload, size_error);
      if (size_error)
        return std::unexpected(core::Error{core::ErrorCode::IoError,
                                           "Could not size staged S3 upload"});
      if (auto added = add_header("x-amz-content-sha256: " + object_name(id));
          !added)
        return std::unexpected(added.error());
      if (auto added = add_header("Expect:"); !added)
        return std::unexpected(added.error());
      for (auto configured :
           {set(CURLOPT_UPLOAD, 1L), set(CURLOPT_READDATA, file.get()),
            set(CURLOPT_INFILESIZE_LARGE, static_cast<curl_off_t>(size))})
        if (!configured)
          return std::unexpected(configured.error());
    } else if (method == "GET") {
      file.reset(std::fopen(download->c_str(), "wb"));
      if (!file)
        return std::unexpected(core::Error{
            core::ErrorCode::IoError, "Could not create staged S3 download"});
      if (auto configured = set(CURLOPT_WRITEDATA, file.get()); !configured)
        return std::unexpected(configured.error());
    }
    if (headers) {
      if (auto configured = set(CURLOPT_HTTPHEADER, headers.get()); !configured)
        return std::unexpected(configured.error());
    }

    const auto performed = curl_easy_perform(handle.get());
    if (file && std::fflush(file.get()) != 0)
      return std::unexpected(core::Error{core::ErrorCode::IoError,
                                         "Could not flush staged S3 transfer"});
    if (performed != CURLE_OK)
      return std::unexpected(
          curl_error(std::string{method}, performed, detail.data()));
    long status{};
    curl_off_t content_length{-1};
    curl_easy_getinfo(handle.get(), CURLINFO_RESPONSE_CODE, &status);
    curl_easy_getinfo(handle.get(), CURLINFO_CONTENT_LENGTH_DOWNLOAD_T,
                      &content_length);
    return Response{.status = status,
                    .content_length =
                        content_length < 0
                            ? 0
                            : static_cast<std::uint64_t>(content_length)};
  }

  core::Result<Response>
  request(std::string_view method, core::ObjectId id,
          const std::filesystem::path *upload = nullptr,
          const std::filesystem::path *download = nullptr) const {
    for (std::size_t attempt = 0; attempt < options.maximum_attempts;
         ++attempt) {
      auto response = request_once(method, id, upload, download);
      const bool retryable =
          response
              ? (response->status == 408 || response->status == 425 ||
                 response->status == 429 ||
                 (response->status >= 500 && response->status < 600))
              : (response.error().code == core::ErrorCode::Timeout ||
                 response.error().code == core::ErrorCode::StorageUnavailable);
      if (!retryable || attempt + 1 == options.maximum_attempts)
        return response;
      std::this_thread::sleep_for(options.retry_delay *
                                  (std::size_t{1} << attempt));
    }
    std::unreachable();
  }

  S3BlobStoreOptions options;
};

namespace {

class S3BlobWriter final : public BlobWriter {
public:
  S3BlobWriter(std::shared_ptr<S3BlobStore::State> state,
               std::filesystem::path path)
      : state_(std::move(state)), path_(std::move(path)),
        stream_(path_, std::ios::binary | std::ios::trunc) {}

  ~S3BlobWriter() override { abort(); }
  bool is_open() const noexcept { return stream_.is_open(); }

  core::Result<void> write(core::ByteView data) override {
    if (!active_)
      return std::unexpected(core::Error{core::ErrorCode::InvalidArgument,
                                         "Blob writer is no longer active"});
    stream_.write(reinterpret_cast<const char *>(data.data()),
                  static_cast<std::streamsize>(data.size()));
    if (!stream_)
      return std::unexpected(core::Error{core::ErrorCode::IoError,
                                         "Could not write staged S3 blob"});
    hasher_.update(data);
    return {};
  }

  core::Result<core::ObjectId> finalize() override {
    if (!active_)
      return std::unexpected(core::Error{core::ErrorCode::InvalidArgument,
                                         "Blob writer is no longer active"});
    stream_.flush();
    if (!stream_)
      return std::unexpected(core::Error{core::ErrorCode::IoError,
                                         "Could not flush staged S3 blob"});
    stream_.close();
    const auto id = object_id(hasher_.finalize());
    auto uploaded = state_->request("PUT", id, &path_);
    if (!uploaded)
      return std::unexpected(uploaded.error());
    if (uploaded->status < 200 || uploaded->status >= 300)
      return std::unexpected(status_error("PUT", uploaded->status));
    std::error_code remove_error;
    std::filesystem::remove(path_, remove_error);
    if (remove_error)
      return std::unexpected(core::Error{core::ErrorCode::IoError,
                                         "Could not remove staged S3 upload"});
    active_ = false;
    return id;
  }

  void abort() noexcept override {
    if (!active_)
      return;
    active_ = false;
    stream_.close();
    std::error_code ignored;
    std::filesystem::remove(path_, ignored);
  }

private:
  std::shared_ptr<S3BlobStore::State> state_;
  std::filesystem::path path_;
  std::ofstream stream_;
  core::Sha256Hasher hasher_;
  bool active_{true};
};

} // namespace

S3BlobStore::S3BlobStore(std::shared_ptr<State> state)
    : state_(std::move(state)) {}

core::Result<std::unique_ptr<S3BlobStore>>
S3BlobStore::open(S3BlobStoreOptions options) {
  while (!options.endpoint.empty() && options.endpoint.back() == '/')
    options.endpoint.pop_back();
  options.prefix = normalized_prefix(std::move(options.prefix));
  if ((!options.endpoint.starts_with("https://") &&
       !options.endpoint.starts_with("http://")) ||
      options.bucket.empty() || options.region.empty() ||
      options.staging_directory.empty() ||
      options.connect_timeout.count() <= 0 ||
      options.request_timeout.count() <= 0 || options.maximum_attempts == 0 ||
      options.maximum_attempts > 10 || options.retry_delay.count() < 0 ||
      options.access_key_id.empty() || options.secret_access_key.empty())
    return std::unexpected(core::Error{
        core::ErrorCode::InvalidArgument,
        "S3 blob storage requires an HTTP(S) endpoint, bucket, region, staging "
        "directory, timeouts, bounded retry settings, and credentials"});
  std::error_code directory_error;
  std::filesystem::create_directories(options.staging_directory,
                                      directory_error);
  if (directory_error)
    return std::unexpected(core::Error{
        core::ErrorCode::IoError,
        "Could not create S3 staging directory: " + directory_error.message()});
  return std::unique_ptr<S3BlobStore>{
      new S3BlobStore{std::make_shared<State>(std::move(options))}};
}

core::Result<std::unique_ptr<S3BlobStore>>
S3BlobStore::open_from_environment(S3BlobStoreOptions options) {
  options.access_key_id = environment("AWS_ACCESS_KEY_ID").value_or("");
  options.secret_access_key = environment("AWS_SECRET_ACCESS_KEY").value_or("");
  options.session_token = environment("AWS_SESSION_TOKEN").value_or("");
  return open(std::move(options));
}

core::Result<std::unique_ptr<BlobWriter>> S3BlobStore::create() {
  const auto path = temporary_path(state_->options.staging_directory);
  auto writer = std::make_unique<S3BlobWriter>(state_, path);
  if (!writer->is_open())
    return std::unexpected(core::Error{core::ErrorCode::IoError,
                                       "Could not create staged S3 blob"});
  return std::unique_ptr<BlobWriter>{std::move(writer)};
}

core::Result<std::unique_ptr<BlobReader>> S3BlobStore::open(core::ObjectId id) {
  auto metadata = state_->request("HEAD", id);
  if (!metadata)
    return std::unexpected(metadata.error());
  if (metadata->status < 200 || metadata->status >= 300)
    return std::unexpected(status_error("HEAD", metadata->status));
  const auto path = temporary_path(state_->options.staging_directory);
  auto downloaded = state_->request("GET", id, nullptr, &path);
  if (!downloaded) {
    std::error_code ignored;
    std::filesystem::remove(path, ignored);
    return std::unexpected(downloaded.error());
  }
  if (downloaded->status < 200 || downloaded->status >= 300) {
    std::error_code ignored;
    std::filesystem::remove(path, ignored);
    return std::unexpected(status_error("GET", downloaded->status));
  }
  std::error_code size_error;
  const auto size = std::filesystem::file_size(path, size_error);
  auto actual = hash_file(path);
  if (size_error || !actual || size != metadata->content_length ||
      *actual != id) {
    std::error_code ignored;
    std::filesystem::remove(path, ignored);
    return std::unexpected(core::Error{
        size_error || !actual ? core::ErrorCode::IoError
                              : core::ErrorCode::ChecksumMismatch,
        "Downloaded S3 blob failed size or content-address verification"});
  }
  auto reader = std::make_unique<StagedBlobReader>(path, size);
  if (!reader->is_open())
    return std::unexpected(
        core::Error{core::ErrorCode::IoError, "Could not open staged S3 blob"});
  return std::unique_ptr<BlobReader>{std::move(reader)};
}

core::Result<bool> S3BlobStore::exists(core::ObjectId id) {
  auto response = state_->request("HEAD", id);
  if (!response)
    return std::unexpected(response.error());
  if (response->status == 404)
    return false;
  if (response->status < 200 || response->status >= 300)
    return std::unexpected(status_error("HEAD", response->status));
  return true;
}

core::Result<void> S3BlobStore::remove(core::ObjectId id) {
  auto found = exists(id);
  if (!found)
    return std::unexpected(found.error());
  if (!*found)
    return std::unexpected(
        core::Error{core::ErrorCode::NotFound, "S3 blob does not exist"});
  auto response = state_->request("DELETE", id);
  if (!response)
    return std::unexpected(response.error());
  if (response->status < 200 || response->status >= 300)
    return std::unexpected(status_error("DELETE", response->status));
  return {};
}

} // namespace sakuin::storage

import std;

import sakuin.core;
import sakuin.runtime;
import sakuin.scheduler;

namespace {

sakuin::core::Timestamp seconds(std::int64_t value) {
  return sakuin::core::Timestamp{
      std::chrono::duration_cast<sakuin::core::Timestamp::duration>(
          std::chrono::seconds{value})};
}

sakuin::core::ByteBuffer bytes(std::string_view value) {
  const auto input = std::as_bytes(std::span{value});
  return {input.begin(), input.end()};
}

sakuin::scheduler::WorkItem item(std::string_view payload) {
  auto encoded = bytes(payload);
  return {.id = sakuin::scheduler::content_work_id(encoded),
          .work_class = sakuin::scheduler::WorkClass::MetadataFetch,
          .payload = std::move(encoded),
          .priority = 7,
          .not_before = seconds(0),
          .maximum_attempts = 3};
}

sakuin::core::Result<sakuin::scheduler::WorkProtocolResponse>
exchange(sakuin::scheduler::WorkProtocolDispatcher &dispatcher,
         const sakuin::scheduler::WorkProtocolRequest &request) {
  auto encoded = sakuin::scheduler::encode_work_request(request);
  if (!encoded)
    return std::unexpected(encoded.error());
  auto response = dispatcher.handle(*encoded);
  if (!response)
    return std::unexpected(response.error());
  return sakuin::scheduler::decode_work_response(*response);
}

} // namespace

int main() {
  using namespace sakuin;
  auto coordinator = scheduler::LocalWorkCoordinator::create(
      {.maximum_work_items = 16,
       .maximum_payload_bytes = 1'024,
       .worker_timeout = std::chrono::seconds{20},
       .lease_duration = std::chrono::seconds{5}});
  if (!coordinator)
    return 1;

  auto now = seconds(1);
  scheduler::WorkProtocolDispatcher dispatcher{**coordinator,
                                               [&] { return now; }};
  const auto work = item("metadata");
  auto submitted = exchange(
      dispatcher, {.request_id = 1,
                   .operation = scheduler::WorkProtocolOperation::Submit,
                   .payload = scheduler::SubmitWorkRequest{.item = work}});
  if (!submitted || submitted->request_id != 1 || !submitted->succeeded() ||
      !std::get<bool>(submitted->payload))
    return 2;

  const scheduler::WorkerDescriptor worker{
      .id = "remote-metadata-1",
      .capabilities = {scheduler::WorkClass::MetadataFetch}};
  auto registered =
      exchange(dispatcher,
               {.request_id = 2,
                .operation = scheduler::WorkProtocolOperation::RegisterWorker,
                .payload = scheduler::RegisterWorkerRequest{.worker = worker}});
  if (!registered || !registered->succeeded() ||
      !std::get<bool>(registered->payload))
    return 3;

  now = seconds(2);
  auto leased = exchange(
      dispatcher, {.request_id = 3,
                   .operation = scheduler::WorkProtocolOperation::Lease,
                   .payload = scheduler::LeaseWorkRequest{.worker = worker.id,
                                                          .maximum = 1}});
  if (!leased || !leased->succeeded())
    return 4;
  const auto &leases =
      std::get<std::vector<scheduler::WorkLease>>(leased->payload);
  if (leases.size() != 1 || leases.front().item != work ||
      leases.front().expires_at != seconds(7))
    return 5;

  now = seconds(3);
  auto completed = exchange(
      dispatcher, {.request_id = 4,
                   .operation = scheduler::WorkProtocolOperation::Complete,
                   .payload = scheduler::LeaseMutationRequest{
                       .worker = worker.id, .lease = leases.front().id}});
  if (!completed || !completed->succeeded() ||
      !std::holds_alternative<std::monostate>(completed->payload))
    return 6;

  auto snapshot = exchange(
      dispatcher, {.request_id = 5,
                   .operation = scheduler::WorkProtocolOperation::Snapshot,
                   .payload = scheduler::WorkSnapshotRequest{}});
  if (!snapshot || !snapshot->succeeded() ||
      std::get<scheduler::WorkCoordinatorSnapshot>(snapshot->payload)
              .succeeded != 1)
    return 7;

  auto missing = exchange(
      dispatcher,
      {.request_id = 6,
       .operation = scheduler::WorkProtocolOperation::Heartbeat,
       .payload = scheduler::WorkerHeartbeatRequest{.worker = "missing"}});
  if (!missing || missing->succeeded() ||
      std::get<core::Error>(missing->payload).code != core::ErrorCode::NotFound)
    return 8;

  auto first = scheduler::encode_work_request(
      {.request_id = 7,
       .operation = scheduler::WorkProtocolOperation::Heartbeat,
       .payload = scheduler::WorkerHeartbeatRequest{.worker = worker.id}});
  auto second = scheduler::encode_work_request(
      {.request_id = 8,
       .operation = scheduler::WorkProtocolOperation::Snapshot,
       .payload = scheduler::WorkSnapshotRequest{}});
  if (!first || !second)
    return 9;
  core::ByteBuffer joined = *first;
  joined.insert(joined.end(), second->begin(), second->end());
  scheduler::WorkFrameDecoder decoder;
  auto prefix = decoder.feed(std::span{joined}.first(7));
  auto suffix = decoder.feed(std::span{joined}.subspan(7));
  if (!prefix || !prefix->empty() || !suffix || suffix->size() != 2 ||
      scheduler::decode_work_request((*suffix)[0])->request_id != 7 ||
      scheduler::decode_work_request((*suffix)[1])->request_id != 8 ||
      decoder.buffered_bytes() != 0)
    return 10;

  auto corrupt_magic = *first;
  corrupt_magic[0] = core::Byte{0};
  auto corrupt = scheduler::decode_work_request(corrupt_magic);
  if (corrupt || corrupt.error().code != core::ErrorCode::CorruptSegment)
    return 11;
  auto unsupported = *first;
  unsupported[5] = core::Byte{0xff};
  auto version = scheduler::decode_work_request(unsupported);
  if (version || version.error().code != core::ErrorCode::UnsupportedFormat)
    return 12;

  scheduler::WorkFrameDecoder poisoned;
  auto bad = poisoned.feed(corrupt_magic);
  auto still_bad = poisoned.feed(*first);
  if (bad || still_bad || still_bad.error().code != core::ErrorCode::Conflict)
    return 13;
  poisoned.reset();
  auto recovered = poisoned.feed(*first);
  if (!recovered || recovered->size() != 1)
    return 14;

  auto mismatched = scheduler::encode_work_request(
      {.request_id = 9,
       .operation = scheduler::WorkProtocolOperation::Snapshot,
       .payload = scheduler::WorkerHeartbeatRequest{.worker = worker.id}});
  if (mismatched || mismatched.error().code != core::ErrorCode::InvalidArgument)
    return 15;
  scheduler::WorkProtocolLimits tiny{.maximum_frame_bytes = 32,
                                     .maximum_work_payload_bytes = 8};
  if (scheduler::encode_work_request(
          {.request_id = 10,
           .operation = scheduler::WorkProtocolOperation::Submit,
           .payload = scheduler::SubmitWorkRequest{.item = work}},
          tiny))
    return 16;

  auto immediate = work;
  immediate.not_before = core::Timestamp::min();
  auto encoded_immediate = scheduler::encode_work_request(
      {.request_id = 11,
       .operation = scheduler::WorkProtocolOperation::Submit,
       .payload = scheduler::SubmitWorkRequest{.item = immediate}});
  if (!encoded_immediate)
    return 17;
  auto decoded_immediate = scheduler::decode_work_request(*encoded_immediate);
  if (!decoded_immediate ||
      std::get<scheduler::SubmitWorkRequest>(decoded_immediate->payload)
              .item.not_before != core::Timestamp::min())
    return 18;

  auto excessive_retry = scheduler::encode_work_request(
      {.request_id = 12,
       .operation = scheduler::WorkProtocolOperation::Fail,
       .payload =
           scheduler::FailWorkRequest{.worker = worker.id,
                                      .lease = 1,
                                      .retryable = true,
                                      .retry_delay = std::chrono::days{31}}});
  if (excessive_retry ||
      excessive_retry.error().code != core::ErrorCode::InvalidArgument)
    return 19;
  scheduler::WorkResultBatch result{
      .id = scheduler::content_work_result_id(
          scheduler::WorkResultKind::ObservationBatch, bytes("observations")),
      .kind = scheduler::WorkResultKind::ObservationBatch,
      .payload = bytes("observations")};
  auto encoded_result = scheduler::encode_work_request(
      {.request_id = 13,
       .operation = scheduler::WorkProtocolOperation::PublishResult,
       .payload = scheduler::PublishWorkResultRequest{.worker = worker.id,
                                                      .result = result}});
  auto decoded_result = encoded_result
                            ? scheduler::decode_work_request(*encoded_result)
                            : core::Result<scheduler::WorkProtocolRequest>{
                                  std::unexpected(encoded_result.error())};
  if (!decoded_result ||
      std::get<scheduler::PublishWorkResultRequest>(decoded_result->payload)
              .result != result)
    return 20;
  scheduler::WorkProtocolLimits small_result;
  small_result.maximum_result_payload_bytes = 4;
  if (scheduler::encode_work_request(
          {.request_id = 14,
           .operation = scheduler::WorkProtocolOperation::PublishResult,
           .payload = scheduler::PublishWorkResultRequest{.worker = worker.id,
                                                          .result = result}},
          small_result))
    return 21;
  auto encoded_traffic = scheduler::encode_work_request(
      {.request_id = 15,
       .operation = scheduler::WorkProtocolOperation::AcquireTraffic,
       .payload = scheduler::AcquireTrafficRequest{
           .worker = worker.id,
           .traffic = {.direction = runtime::TrafficDirection::Outbound,
                       .traffic_class = 4,
                       .bytes = 65'536}}});
  auto decoded_traffic = encoded_traffic
                             ? scheduler::decode_work_request(*encoded_traffic)
                             : core::Result<scheduler::WorkProtocolRequest>{
                                   std::unexpected(encoded_traffic.error())};
  if (!decoded_traffic ||
      std::get<scheduler::AcquireTrafficRequest>(decoded_traffic->payload)
              .traffic.bytes != 65'536)
    return 22;
  scheduler::WorkProtocolResponse traffic_response{
      .request_id = 15,
      .operation = scheduler::WorkProtocolOperation::AcquireTraffic,
      .payload = scheduler::TrafficGrant{
          .bytes = 32'768, .retry_after = std::chrono::milliseconds{250}}};
  auto encoded_traffic_response =
      scheduler::encode_work_response(traffic_response);
  auto decoded_traffic_response =
      encoded_traffic_response
          ? scheduler::decode_work_response(*encoded_traffic_response)
          : core::Result<scheduler::WorkProtocolResponse>{
                std::unexpected(encoded_traffic_response.error())};
  if (!decoded_traffic_response ||
      std::get<scheduler::TrafficGrant>(decoded_traffic_response->payload) !=
          std::get<scheduler::TrafficGrant>(traffic_response.payload))
    return 23;
  return 0;
}

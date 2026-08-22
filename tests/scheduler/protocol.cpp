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
         const sakuin::scheduler::WorkProtocolRequest &request,
         sakuin::scheduler::WorkProtocolLimits limits = {}) {
  auto encoded = sakuin::scheduler::encode_work_request(request, limits);
  if (!encoded)
    return std::unexpected(encoded.error());
  auto response = dispatcher.handle(*encoded, limits);
  if (!response)
    return std::unexpected(response.error());
  return sakuin::scheduler::decode_work_response(*response, limits);
}

class ResultPublisher final : public sakuin::scheduler::WorkResultPublisher {
public:
  sakuin::core::Result<bool>
  publish_result(std::string_view worker,
                 sakuin::scheduler::WorkResultBatch batch) override {
    if (worker.empty())
      return std::unexpected(sakuin::core::Error{
          sakuin::core::ErrorCode::InvalidArgument, "worker is empty"});
    const auto inserted = ids.insert(batch.id).second;
    if (inserted)
      batches.push_back(std::move(batch));
    return inserted;
  }

  std::set<sakuin::scheduler::WorkId> ids;
  std::vector<sakuin::scheduler::WorkResultBatch> batches;
};

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

  scheduler::WorkProtocolLimits chunk_limits{
      .maximum_frame_bytes = 256,
      .maximum_work_payload_bytes = 64,
      .maximum_result_payload_bytes = 64,
      .maximum_chunked_result_bytes = 256,
      .maximum_result_reassembly_bytes = 512,
      .maximum_result_transfers = 2,
      .result_transfer_timeout = std::chrono::seconds{1}};
  ResultPublisher publisher;
  now = seconds(100);
  scheduler::WorkProtocolDispatcher chunk_dispatcher{
      **coordinator, [&] { return now; }, &publisher, nullptr, chunk_limits};
  core::ByteBuffer large(150, core::Byte{0x5a});
  const auto large_id = scheduler::content_work_result_id(
      scheduler::WorkResultKind::ObservationBatch, large);
  const auto chunk_request = [&](std::uint64_t request_id, scheduler::WorkId id,
                                 std::size_t offset, std::size_t count) {
    return scheduler::WorkProtocolRequest{
        .request_id = request_id,
        .operation = scheduler::WorkProtocolOperation::PublishResultChunk,
        .payload = scheduler::PublishWorkResultChunkRequest{
            .worker = worker.id,
            .id = id,
            .kind = scheduler::WorkResultKind::ObservationBatch,
            .total_bytes = large.size(),
            .offset = offset,
            .chunk = {large.begin() + offset, large.begin() + offset + count}}};
  };
  auto first_chunk = exchange(chunk_dispatcher,
                              chunk_request(16, large_id, 0, 64), chunk_limits);
  auto second_chunk = exchange(
      chunk_dispatcher, chunk_request(17, large_id, 64, 64), chunk_limits);
  auto final_chunk = exchange(
      chunk_dispatcher, chunk_request(18, large_id, 128, 22), chunk_limits);
  if (!first_chunk || !std::get<bool>(first_chunk->payload) || !second_chunk ||
      !std::get<bool>(second_chunk->payload) || !final_chunk ||
      !std::get<bool>(final_chunk->payload) || publisher.batches.size() != 1 ||
      publisher.batches.front().payload != large)
    return 24;

  auto retry_first = exchange(chunk_dispatcher,
                              chunk_request(19, large_id, 0, 64), chunk_limits);
  auto retry_second = exchange(
      chunk_dispatcher, chunk_request(20, large_id, 64, 64), chunk_limits);
  auto retry_final = exchange(
      chunk_dispatcher, chunk_request(21, large_id, 128, 22), chunk_limits);
  if (!retry_first || !retry_second || !retry_final ||
      std::get<bool>(retry_final->payload) || publisher.batches.size() != 1)
    return 25;

  auto out_of_order = exchange(
      chunk_dispatcher, chunk_request(22, large_id, 64, 64), chunk_limits);
  if (!out_of_order || out_of_order->succeeded() ||
      std::get<core::Error>(out_of_order->payload).code !=
          core::ErrorCode::Conflict)
    return 26;

  auto invalid_first =
      exchange(chunk_dispatcher, chunk_request(23, {}, 0, 64), chunk_limits);
  auto invalid_second =
      exchange(chunk_dispatcher, chunk_request(24, {}, 64, 64), chunk_limits);
  auto invalid_final =
      exchange(chunk_dispatcher, chunk_request(25, {}, 128, 22), chunk_limits);
  if (!invalid_first || !invalid_second || !invalid_final ||
      invalid_final->succeeded() ||
      std::get<core::Error>(invalid_final->payload).code !=
          core::ErrorCode::InvalidArgument ||
      publisher.batches.size() != 1)
    return 27;

  auto expiring_payload = large;
  expiring_payload.front() = core::Byte{0x33};
  const auto expiring_id = scheduler::content_work_result_id(
      scheduler::WorkResultKind::ObservationBatch, expiring_payload);
  auto expiring_start = exchange(
      chunk_dispatcher, chunk_request(26, expiring_id, 0, 64), chunk_limits);
  now = seconds(102);
  auto expired_continuation = exchange(
      chunk_dispatcher, chunk_request(27, expiring_id, 64, 64), chunk_limits);
  if (!expiring_start || !expired_continuation ||
      expired_continuation->succeeded() ||
      std::get<core::Error>(expired_continuation->payload).code !=
          core::ErrorCode::Conflict)
    return 28;
  auto transfer_a = large_id;
  auto transfer_b = large_id;
  auto transfer_c = large_id;
  transfer_a.bytes[0] ^= 0x11;
  transfer_b.bytes[0] ^= 0x22;
  transfer_c.bytes[0] ^= 0x33;
  auto active_a = exchange(chunk_dispatcher,
                           chunk_request(28, transfer_a, 0, 64), chunk_limits);
  auto active_b = exchange(chunk_dispatcher,
                           chunk_request(29, transfer_b, 0, 64), chunk_limits);
  auto over_limit = exchange(
      chunk_dispatcher, chunk_request(30, transfer_c, 0, 64), chunk_limits);
  if (!active_a || !active_b || !over_limit || over_limit->succeeded() ||
      std::get<core::Error>(over_limit->payload).code !=
          core::ErrorCode::QuotaExceeded)
    return 29;
  return 0;
}

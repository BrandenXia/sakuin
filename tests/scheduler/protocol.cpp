import std;

import sakuin.core;
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
  unsupported[5] = core::Byte{2};
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
  return 0;
}

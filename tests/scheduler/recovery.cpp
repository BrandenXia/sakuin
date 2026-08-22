import std;

import sakuin.core;
import sakuin.scheduler;

namespace {

struct TemporaryDirectory {
  std::filesystem::path path;
  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path, ignored);
  }
};

sakuin::core::Timestamp seconds(std::int64_t value) {
  return sakuin::core::Timestamp{
      std::chrono::duration_cast<sakuin::core::Timestamp::duration>(
          std::chrono::seconds{value})};
}

sakuin::scheduler::WorkItem item(std::string_view text, std::int32_t priority,
                                 std::uint32_t maximum_attempts = 3,
                                 std::int64_t not_before = 0) {
  const auto view = std::as_bytes(std::span{text});
  sakuin::core::ByteBuffer payload{view.begin(), view.end()};
  return {.id = sakuin::scheduler::content_work_id(payload),
          .work_class = sakuin::scheduler::WorkClass::MetadataFetch,
          .payload = std::move(payload),
          .priority = priority,
          .not_before = seconds(not_before),
          .maximum_attempts = maximum_attempts};
}

class FailingStore final : public sakuin::scheduler::WorkRecoveryStore {
public:
  sakuin::core::Result<std::optional<sakuin::scheduler::WorkRecoverySnapshot>>
  load(const sakuin::scheduler::WorkCoordinatorOptions &) override {
    return std::optional<sakuin::scheduler::WorkRecoverySnapshot>{};
  }

  sakuin::core::Result<void>
  save(const sakuin::scheduler::WorkRecoverySnapshot &) override {
    return std::unexpected(sakuin::core::Error{sakuin::core::ErrorCode::IoError,
                                               "injected checkpoint failure"});
  }
};

} // namespace

int main() {
  using namespace sakuin;
  const auto nonce = std::to_string(
      std::chrono::steady_clock::now().time_since_epoch().count());
  TemporaryDirectory directory{std::filesystem::temp_directory_path() /
                               ("sakuin-work-recovery-" + nonce)};
  const auto checkpoint = directory.path / "state" / "work.checkpoint";
  const scheduler::WorkCoordinatorOptions options{
      .maximum_work_items = 16,
      .maximum_payload_bytes = 128,
      .worker_timeout = std::chrono::seconds{100},
      .lease_duration = std::chrono::seconds{5}};
  const scheduler::WorkerDescriptor worker{
      .id = "recovery-worker",
      .capabilities = {scheduler::WorkClass::MetadataFetch}};
  const auto completed = item("completed", 40);
  const auto interrupted = item("interrupted", 30);
  const auto delayed = item("delayed", 20, 3);
  const auto exhausted = item("exhausted", 10, 1);

  {
    auto coordinator =
        scheduler::RecoveringWorkCoordinator::open_local(options, checkpoint);
    if (!coordinator || !(*coordinator)->healthy() ||
        !(*coordinator)->submit(completed).value_or(false) ||
        !(*coordinator)->submit(interrupted).value_or(false) ||
        !(*coordinator)->submit(delayed).value_or(false) ||
        !(*coordinator)->submit(exhausted).value_or(false) ||
        !(*coordinator)->register_worker(worker, seconds(0)))
      return 1;
    auto leases = (*coordinator)->lease(worker.id, 4, seconds(0));
    if (!leases || leases->size() != 4)
      return 2;
    const auto find_lease = [&](const scheduler::WorkItem &work) {
      return std::ranges::find_if(
          *leases, [&](const auto &lease) { return lease.item.id == work.id; });
    };
    const auto completed_lease = find_lease(completed);
    const auto delayed_lease = find_lease(delayed);
    if (completed_lease == leases->end() || delayed_lease == leases->end() ||
        !(*coordinator)->complete(worker.id, completed_lease->id, seconds(1)) ||
        !(*coordinator)
             ->fail(worker.id, delayed_lease->id, true, seconds(1),
                    seconds(50)))
      return 3;
    // The interrupted and exhausted leases intentionally remain live here.
    // Their worker registration and lease ids must disappear on reopen.
  }

  {
    auto coordinator =
        scheduler::RecoveringWorkCoordinator::open_local(options, checkpoint);
    if (!coordinator)
      return 4;
    auto state = (*coordinator)->snapshot(seconds(10));
    if (!state || state->workers != 0 || state->succeeded != 1 ||
        state->pending != 2 || state->leased != 0 || state->failed != 1 ||
        (*coordinator)->submit(completed).value_or(true))
      return 5;
    if (!(*coordinator)->register_worker(worker, seconds(10)))
      return 6;
    auto ready = (*coordinator)->lease(worker.id, 4, seconds(10));
    if (!ready || ready->size() != 1 ||
        ready->front().item.id != interrupted.id ||
        ready->front().attempt != 2 ||
        !(*coordinator)->complete(worker.id, ready->front().id, seconds(11)))
      return 7;
    auto early = (*coordinator)->lease(worker.id, 1, seconds(49));
    auto retry = (*coordinator)->lease(worker.id, 1, seconds(50));
    if (!early || !early->empty() || !retry || retry->size() != 1 ||
        retry->front().item.id != delayed.id || retry->front().attempt != 2 ||
        !(*coordinator)
             ->fail(worker.id, retry->front().id, false, seconds(51),
                    seconds(51)))
      return 8;
  }

  auto final =
      scheduler::RecoveringWorkCoordinator::open_local(options, checkpoint);
  auto final_state = final ? (*final)->snapshot(seconds(60))
                           : core::Result<scheduler::WorkCoordinatorSnapshot>{
                                 std::unexpected(final.error())};
  if (!final_state || final_state->succeeded != 2 || final_state->failed != 2 ||
      final_state->pending != 0 || final_state->leased != 0)
    return 9;

  scheduler::WorkRecoverySnapshot codec_snapshot{
      .next_sequence = 1,
      .entries = {{.item = {.id = completed.id,
                            .work_class = completed.work_class,
                            .payload = completed.payload,
                            .priority = completed.priority,
                            .not_before = core::Timestamp::min(),
                            .maximum_attempts = completed.maximum_attempts},
                   .state = scheduler::WorkRecoveryState::Succeeded,
                   .sequence = 1,
                   .attempts = 1}}};
  auto encoded = scheduler::encode_work_recovery_snapshot(codec_snapshot);
  auto decoded =
      encoded ? scheduler::decode_work_recovery_snapshot(*encoded, options)
              : core::Result<scheduler::WorkRecoverySnapshot>{
                    std::unexpected(encoded.error())};
  if (!decoded || *decoded != codec_snapshot ||
      scheduler::encode_work_recovery_snapshot(codec_snapshot, 64))
    return 10;

  auto failing = scheduler::RecoveringWorkCoordinator::create(
      options, std::make_unique<FailingStore>());
  auto failed_submit =
      failing ? (*failing)->submit(item("not-acknowledged", 1))
              : core::Result<bool>{std::unexpected(failing.error())};
  auto faulted_snapshot =
      failing ? (*failing)->snapshot(seconds(0))
              : core::Result<scheduler::WorkCoordinatorSnapshot>{
                    std::unexpected(failing.error())};
  if (failed_submit || failed_submit.error().code != core::ErrorCode::IoError ||
      !failing || (*failing)->healthy() || faulted_snapshot ||
      faulted_snapshot.error().code != core::ErrorCode::IoError)
    return 11;

  std::ifstream input{checkpoint, std::ios::binary | std::ios::ate};
  if (!input)
    return 12;
  const auto size = input.tellg();
  core::ByteBuffer corrupt(static_cast<std::size_t>(size));
  input.seekg(0);
  input.read(reinterpret_cast<char *>(corrupt.data()), size);
  input.close();
  corrupt[24] ^= std::byte{1};
  std::ofstream output{checkpoint, std::ios::binary | std::ios::trunc};
  output.write(reinterpret_cast<const char *>(corrupt.data()), size);
  output.close();
  auto rejected =
      scheduler::RecoveringWorkCoordinator::open_local(options, checkpoint);
  if (rejected || rejected.error().code != core::ErrorCode::ChecksumMismatch)
    return 13;
  return 0;
}

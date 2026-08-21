export module sakuin.dht.metadata_controller;

import std;

import sakuin.core.result;
import sakuin.core.time;
import sakuin.dht.metadata;
import sakuin.dht.metadata_candidate;
import sakuin.dht.metadata_fetch;
import sakuin.dht.metadata_queue;
import sakuin.model.torrent;
import sakuin.runtime.stream;

export namespace sakuin::dht {

struct MetadataControllerOptions {
  MetadataQueueOptions queue;
  runtime::StreamTransportOptions transport;
  MetadataFetchOptions fetch;
};

// Owner-thread orchestration for callback transports. Runtime callbacks only
// enqueue terminal values; poll() joins/reaps transports, advances queue state,
// and invokes storage-facing observers. A sender/receiver owner can replace
// this pump later without changing queue, protocol, or transport contracts.
class MetadataAcquisitionController {
public:
  static core::Result<std::unique_ptr<MetadataAcquisitionController>>
  create(PeerId peer_id, runtime::StreamTransportFactory &factory,
         MetadataFetchObserver &sink, MetadataControllerOptions options = {});

  ~MetadataAcquisitionController();

  core::Result<bool> offer(PeerMetadataCandidate candidate);
  core::Result<void> poll(core::Timestamp now);
  void stop() noexcept;

  std::size_t queued() const noexcept;
  std::size_t in_flight() const noexcept;
  std::size_t pending_storage() const noexcept;

private:
  struct Completion {
    std::uint64_t ticket{};
    std::variant<model::TorrentRecord, core::Error> value;
  };
  class SessionObserver final : public MetadataFetchObserver {
  public:
    SessionObserver(MetadataAcquisitionController &owner, std::uint64_t ticket);
    core::Result<void>
    on_metadata_fetched(model::TorrentRecord record) override;
    void on_metadata_fetch_failed(core::Error error) override;

  private:
    MetadataAcquisitionController *owner_;
    std::uint64_t ticket_;
  };
  struct ActiveFetch {
    std::shared_ptr<runtime::StreamTransport> transport;
    std::unique_ptr<SessionObserver> observer;
    std::unique_ptr<MetadataFetchSession> session;
  };

  MetadataAcquisitionController(PeerId peer_id,
                                runtime::StreamTransportFactory &factory,
                                MetadataFetchObserver &sink,
                                MetadataControllerOptions options,
                                std::unique_ptr<MetadataCandidateQueue> queue);

  void enqueue(Completion completion) noexcept;
  void report_failure(core::Error error) noexcept;
  static MetadataFetchOutcome outcome(const core::Error &error) noexcept;

  PeerId peer_id_;
  runtime::StreamTransportFactory *factory_;
  MetadataFetchObserver *sink_;
  MetadataControllerOptions options_;
  std::unique_ptr<MetadataCandidateQueue> queue_;
  std::unordered_map<std::uint64_t, ActiveFetch> active_;
  std::deque<model::TorrentRecord> pending_storage_;
  mutable std::mutex completion_mutex_;
  std::vector<Completion> completions_;
  bool stopped_{};
};

} // namespace sakuin::dht

namespace sakuin::dht {

MetadataAcquisitionController::SessionObserver::SessionObserver(
    MetadataAcquisitionController &owner, std::uint64_t ticket)
    : owner_(&owner), ticket_(ticket) {}

core::Result<void>
MetadataAcquisitionController::SessionObserver::on_metadata_fetched(
    model::TorrentRecord record) {
  owner_->enqueue(Completion{.ticket = ticket_, .value = std::move(record)});
  return {};
}

void MetadataAcquisitionController::SessionObserver::on_metadata_fetch_failed(
    core::Error error) {
  owner_->enqueue(Completion{.ticket = ticket_, .value = std::move(error)});
}

MetadataAcquisitionController::MetadataAcquisitionController(
    PeerId peer_id, runtime::StreamTransportFactory &factory,
    MetadataFetchObserver &sink, MetadataControllerOptions options,
    std::unique_ptr<MetadataCandidateQueue> queue)
    : peer_id_(peer_id), factory_(&factory), sink_(&sink),
      options_(std::move(options)), queue_(std::move(queue)) {}

MetadataAcquisitionController::~MetadataAcquisitionController() { stop(); }

core::Result<std::unique_ptr<MetadataAcquisitionController>>
MetadataAcquisitionController::create(PeerId peer_id,
                                      runtime::StreamTransportFactory &factory,
                                      MetadataFetchObserver &sink,
                                      MetadataControllerOptions options) {
  auto queue = MetadataCandidateQueue::create(options.queue);
  if (!queue)
    return std::unexpected(queue.error());
  if (options.transport.connect_timeout <= core::Duration::zero() ||
      options.transport.idle_timeout <= core::Duration::zero() ||
      options.transport.read_buffer_bytes == 0 ||
      options.transport.maximum_queued_write_bytes == 0)
    return std::unexpected(
        core::Error{core::ErrorCode::InvalidArgument,
                    "Invalid metadata controller transport limits"});
  return std::unique_ptr<MetadataAcquisitionController>{
      new MetadataAcquisitionController{peer_id, factory, sink,
                                        std::move(options), std::move(*queue)}};
}

core::Result<bool>
MetadataAcquisitionController::offer(PeerMetadataCandidate candidate) {
  if (stopped_)
    return std::unexpected(core::Error{
        core::ErrorCode::Conflict, "Metadata acquisition controller stopped"});
  return queue_->offer(std::move(candidate));
}

void MetadataAcquisitionController::enqueue(Completion completion) noexcept {
  try {
    std::lock_guard lock{completion_mutex_};
    completions_.push_back(std::move(completion));
  } catch (...) {
    // An allocation failure is terminal for this individual callback. The
    // transport/session will still be stopped by controller destruction.
  }
}

MetadataFetchOutcome
MetadataAcquisitionController::outcome(const core::Error &error) noexcept {
  switch (error.code) {
  case core::ErrorCode::IoError:
  case core::ErrorCode::StorageUnavailable:
  case core::ErrorCode::Timeout:
    return MetadataFetchOutcome::RetryableFailure;
  default:
    return MetadataFetchOutcome::PermanentFailure;
  }
}

void MetadataAcquisitionController::report_failure(core::Error error) noexcept {
  try {
    sink_->on_metadata_fetch_failed(std::move(error));
  } catch (...) {
    // Sink callbacks may not unwind through the controller.
  }
}

core::Result<void> MetadataAcquisitionController::poll(core::Timestamp now) {
  if (stopped_)
    return std::unexpected(core::Error{
        core::ErrorCode::Conflict, "Metadata acquisition controller stopped"});

  std::vector<Completion> completed;
  {
    std::lock_guard lock{completion_mutex_};
    completed.swap(completions_);
  }
  std::optional<core::Error> first_error;
  for (auto &completion : completed) {
    const auto active = active_.find(completion.ticket);
    if (active == active_.end())
      continue;
    active->second.session->stop();
    active_.erase(active);

    if (auto *record = std::get_if<model::TorrentRecord>(&completion.value)) {
      if (auto advanced = queue_->complete(completion.ticket,
                                           MetadataFetchOutcome::Success, now);
          !advanced) {
        if (!first_error)
          first_error = advanced.error();
        report_failure(advanced.error());
      } else {
        pending_storage_.push_back(std::move(*record));
      }
      continue;
    }
    auto error = std::move(std::get<core::Error>(completion.value));
    if (auto advanced =
            queue_->complete(completion.ticket, outcome(error), now);
        !advanced && !first_error)
      first_error = advanced.error();
    report_failure(std::move(error));
  }

  const auto storage_count = pending_storage_.size();
  for (std::size_t index = 0; index < storage_count; ++index) {
    auto record = std::move(pending_storage_.front());
    pending_storage_.pop_front();
    try {
      auto stored = sink_->on_metadata_fetched(record);
      if (!stored) {
        if (!first_error)
          first_error = stored.error();
        pending_storage_.push_back(std::move(record));
        report_failure(stored.error());
      }
    } catch (const std::exception &exception) {
      core::Error error{core::ErrorCode::Internal,
                        std::string{"Metadata sink threw: "} +
                            exception.what()};
      if (!first_error)
        first_error = error;
      pending_storage_.push_back(std::move(record));
      report_failure(std::move(error));
    } catch (...) {
      core::Error error{core::ErrorCode::Internal,
                        "Metadata sink threw an unknown exception"};
      if (!first_error)
        first_error = error;
      pending_storage_.push_back(std::move(record));
      report_failure(std::move(error));
    }
  }

  for (auto &ticket : queue_->ready(now)) {
    auto transport_options = options_.transport;
    transport_options.remote = ticket.candidate.peer;
    auto transport = factory_->create(std::move(transport_options));
    if (!transport) {
      queue_->complete(ticket.id, outcome(transport.error()), now);
      report_failure(transport.error());
      if (!first_error)
        first_error = transport.error();
      continue;
    }
    auto observer = std::make_unique<SessionObserver>(*this, ticket.id);
    auto session = MetadataFetchSession::create(
        ticket.candidate.info_hash, peer_id_, ticket.candidate.observed_at,
        **transport, *observer, options_.fetch);
    if (!session) {
      queue_->complete(ticket.id, outcome(session.error()), now);
      report_failure(session.error());
      if (!first_error)
        first_error = session.error();
      continue;
    }
    ActiveFetch active{.transport = std::move(*transport),
                       .observer = std::move(observer),
                       .session = std::move(*session)};
    auto started = active.session->start();
    if (!started) {
      queue_->complete(ticket.id, outcome(started.error()), now);
      report_failure(started.error());
      if (!first_error)
        first_error = started.error();
      continue;
    }
    active_.emplace(ticket.id, std::move(active));
  }

  if (first_error)
    return std::unexpected(std::move(*first_error));
  return {};
}

void MetadataAcquisitionController::stop() noexcept {
  if (stopped_)
    return;
  stopped_ = true;
  for (auto &[_, active] : active_)
    active.session->stop();
  active_.clear();
  std::lock_guard lock{completion_mutex_};
  completions_.clear();
}

std::size_t MetadataAcquisitionController::queued() const noexcept {
  return queue_->queued();
}

std::size_t MetadataAcquisitionController::in_flight() const noexcept {
  return queue_->in_flight();
}

std::size_t MetadataAcquisitionController::pending_storage() const noexcept {
  return pending_storage_.size();
}

} // namespace sakuin::dht

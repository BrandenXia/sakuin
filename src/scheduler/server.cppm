export module sakuin.scheduler.server;

import std;

import sakuin.core.bytes;
import sakuin.core.result;
import sakuin.runtime.datagram;
import sakuin.runtime.stream;
import sakuin.scheduler.protocol;

export namespace sakuin::scheduler {

class WorkProtocolAccessPolicy {
public:
  virtual ~WorkProtocolAccessPolicy() = default;
  virtual core::Result<void> authorize(runtime::StreamEndpoint peer,
                                       const WorkProtocolRequest &request) = 0;
};

// Safe default for the initial single-node deployment. Remote deployments must
// inject a credential- or channel-identity-aware policy instead of widening
// this policy implicitly.
class LoopbackWorkProtocolAccessPolicy final : public WorkProtocolAccessPolicy {
public:
  core::Result<void> authorize(runtime::StreamEndpoint peer,
                               const WorkProtocolRequest &request) override;
};

class WorkProtocolServiceObserver {
public:
  virtual ~WorkProtocolServiceObserver() = default;
  virtual void on_work_protocol_error(runtime::StreamSessionId session,
                                      core::Error error) = 0;
};

// Adapts the versioned work protocol to any Sakuin StreamServer. Coordinator
// calls stay transport-independent and all peer-supplied bytes remain bounded
// by WorkProtocolLimits.
class WorkProtocolService final : public runtime::StreamServerHandler {
public:
  WorkProtocolService(WorkProtocolDispatcher &dispatcher,
                      WorkProtocolAccessPolicy &access,
                      WorkProtocolLimits limits = {},
                      WorkProtocolServiceObserver *observer = nullptr)
      : dispatcher_(&dispatcher), access_(&access), limits_(limits),
        observer_(observer) {}

  void on_stream_session_opened(runtime::StreamServerSession &session) override;
  void on_stream_session_data(runtime::StreamServerSession &session,
                              core::ByteBuffer bytes) override;
  void on_stream_session_closed(runtime::StreamSessionId session,
                                std::optional<core::Error> error) override;

private:
  struct Session {
    runtime::StreamEndpoint peer;
    WorkFrameDecoder decoder;
  };

  void report(runtime::StreamSessionId session, core::Error error) noexcept;
  void fail(runtime::StreamServerSession &session, core::Error error) noexcept;

  WorkProtocolDispatcher *dispatcher_;
  WorkProtocolAccessPolicy *access_;
  WorkProtocolLimits limits_;
  WorkProtocolServiceObserver *observer_;
  std::map<runtime::StreamSessionId, Session> sessions_;
};

} // namespace sakuin::scheduler

namespace sakuin::scheduler {
namespace {

bool loopback(runtime::IpAddress address) {
  if (address.family == runtime::AddressFamily::IPv4)
    return address.bytes[0] == 127;
  return std::ranges::all_of(std::span{address.bytes}.first(15),
                             [](std::uint8_t value) { return value == 0; }) &&
         address.bytes[15] == 1;
}

} // namespace

core::Result<void>
LoopbackWorkProtocolAccessPolicy::authorize(runtime::StreamEndpoint peer,
                                            const WorkProtocolRequest &) {
  if (!loopback(peer.address))
    return std::unexpected(
        core::Error{core::ErrorCode::PermissionDenied,
                    "Distributed-work access is restricted to loopback peers"});
  return {};
}

void WorkProtocolService::report(runtime::StreamSessionId session,
                                 core::Error error) noexcept {
  if (!observer_)
    return;
  try {
    observer_->on_work_protocol_error(session, std::move(error));
  } catch (...) {
    // Observability callbacks may not unwind through the runtime thread.
  }
}

void WorkProtocolService::fail(runtime::StreamServerSession &session,
                               core::Error error) noexcept {
  report(session.id(), std::move(error));
  session.close();
}

void WorkProtocolService::on_stream_session_opened(
    runtime::StreamServerSession &session) {
  auto [_, inserted] = sessions_.try_emplace(
      session.id(), Session{.peer = session.remote_endpoint(),
                            .decoder = WorkFrameDecoder{limits_}});
  if (!inserted)
    fail(session, {core::ErrorCode::Conflict,
                   "Distributed-work session id is already active"});
}

void WorkProtocolService::on_stream_session_data(
    runtime::StreamServerSession &session, core::ByteBuffer bytes) {
  const auto found = sessions_.find(session.id());
  if (found == sessions_.end()) {
    fail(session, {core::ErrorCode::NotFound,
                   "Distributed-work session is not registered"});
    return;
  }
  auto frames = found->second.decoder.feed(bytes);
  if (!frames) {
    fail(session, frames.error());
    return;
  }
  for (const auto &frame : *frames) {
    auto request = decode_work_request(frame, limits_);
    if (!request) {
      fail(session, request.error());
      return;
    }
    WorkProtocolResponse response;
    try {
      auto authorized = access_->authorize(found->second.peer, *request);
      response = authorized
                     ? dispatcher_->dispatch(*request)
                     : WorkProtocolResponse{.request_id = request->request_id,
                                            .operation = request->operation,
                                            .payload = authorized.error()};
    } catch (const std::exception &exception) {
      fail(session, {core::ErrorCode::Internal,
                     std::string{"Distributed-work access policy threw: "} +
                         exception.what()});
      return;
    } catch (...) {
      fail(session,
           {core::ErrorCode::Internal,
            "Distributed-work access policy threw an unknown exception"});
      return;
    }
    auto encoded = encode_work_response(response, limits_);
    if (!encoded) {
      fail(session, encoded.error());
      return;
    }
    if (auto sent = session.send(std::move(*encoded)); !sent) {
      fail(session, sent.error());
      return;
    }
  }
}

void WorkProtocolService::on_stream_session_closed(
    runtime::StreamSessionId session, std::optional<core::Error> error) {
  sessions_.erase(session);
  if (error)
    report(session, std::move(*error));
}

} // namespace sakuin::scheduler

export module sakuin.service.distributed;

import std;

import sakuin.config.model;
import sakuin.core.result;
import sakuin.runtime.stream;
import sakuin.scheduler.traffic;
import sakuin.scheduler.work;

export namespace sakuin::service {

class DistributedWorkServiceObserver {
public:
  virtual ~DistributedWorkServiceObserver() = default;
  virtual void
  on_distributed_work_error(std::optional<runtime::StreamSessionId> session,
                            core::Error error) = 0;
};

// Hosts the replaceable operational work coordinator over the Sakuin stream
// boundary. Plain TCP remains loopback-only; remote listeners require mTLS and
// bind each protocol worker id to its certificate principal.
class DistributedWorkService final {
public:
  static core::Result<std::unique_ptr<DistributedWorkService>>
  create(const config::DistributedConfig &configuration,
         scheduler::WorkCoordinator &coordinator,
         DistributedWorkServiceObserver *observer = nullptr,
         scheduler::WorkResultPublisher *results = nullptr,
         scheduler::TrafficGrantSource *traffic = nullptr);

  ~DistributedWorkService();

  core::Result<void> start();
  void stop() noexcept;
  bool running() const noexcept;
  runtime::StreamEndpoint local_endpoint() const noexcept;

private:
  struct Impl;
  explicit DistributedWorkService(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> impl_;
};

} // namespace sakuin::service

#pragma once

#include <atomic>
#include <chrono>
#include <thread>
#include <unordered_map>

#ifdef DORY_TIDIER_ON
#include "../../default-config.hpp"
#endif

#include <dory/shared/logger.hpp>

#include <dory/third-party/sync/spsc.hpp>

namespace dory::membership::internal {

template <typename ProcIdType>
using NotifierQueue = dory::third_party::sync::SpscQueue<ProcIdType>;

template <typename ProcIdType>
class FailureBroadcaster {
 public:
  FailureBroadcaster()
      : from_hb_poller(QueueSize),
        from_kernel_poller(QueueSize),
        membership_changes(QueueSize),
        LOGGER_INIT(logger, "FailureBroadcaster") {}

  NotifierQueue<ProcIdType> &heartbeatNotificationQueue() {
    return from_hb_poller;
  }
  NotifierQueue<ProcIdType> &kernelNotificationQueue() {
    return from_kernel_poller;
  }

  void membershipRemoved(ProcIdType proc_id) {
    if (!membership_changes.enqueue(proc_id)) {
      throw std::runtime_error(
          "Notifying the re-broadcaster about a membership change failed due "
          "to allocation error");
    }
  }

  void tick() {
    ProcIdType failed_proc_id;

    if (from_hb_poller.try_dequeue(failed_proc_id)) {
      auto now = std::chrono::steady_clock::now();
      failures.insert({failed_proc_id, now});

      LOGGER_DEBUG(logger,
                   "Process {} was captured by the heartbeat poller as failed.",
                   failed_proc_id);
    }

    if (from_kernel_poller.try_dequeue(failed_proc_id)) {
      auto now = std::chrono::steady_clock::now();
      failures.insert({failed_proc_id, now});

      LOGGER_DEBUG(logger,
                   "Process {} was captured by the kernel poller as failed.",
                   failed_proc_id);
    }

    stopRebroadcasting();
    rebroadcastFailures();
  }

 private:
  void stopRebroadcasting() {
    while (true) {
      ProcIdType failed_proc_id;
      bool succeeded = membership_changes.try_dequeue(failed_proc_id);

      if (!succeeded) {
        break;
      }

      auto erased = failures.erase(failed_proc_id);

      if (erased == 0) {
        LOGGER_WARN(logger,
                    "Excluded process {} from re-broadcasting before detecting "
                    "the failure",
                    failed_proc_id);
      }
    }
  }

  void rebroadcastFailures() {
    // Disabled for now
    return;

    auto now = std::chrono::steady_clock::now();

    for (auto &[proc_id, tp] : failures) {
      auto passed = tp + Timeout;
      auto time_diff =
          std::chrono::duration_cast<std::chrono::milliseconds>(now - tp);
      if (time_diff > Timeout) {
        LOGGER_DEBUG(logger, "Broadcasting failure for {}", proc_id);
        tp = now;
      }
    }
  }

  NotifierQueue<ProcIdType> from_hb_poller;
  NotifierQueue<ProcIdType> from_kernel_poller;
  NotifierQueue<ProcIdType> membership_changes;

  std::unordered_map<ProcIdType, std::chrono::steady_clock::time_point>
      failures;

  static int constexpr QueueSize = 128;
  static std::chrono::milliseconds constexpr Timeout =
      std::chrono::milliseconds(10);

  LOGGER_DECL(logger);
};
}  // namespace dory::membership::internal

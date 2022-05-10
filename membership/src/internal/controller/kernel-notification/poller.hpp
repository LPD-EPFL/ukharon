#pragma once

#include <atomic>
#include <chrono>
#include <list>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <utility>

#ifdef DORY_TIDIER_ON
#include "../../../default-config.hpp"
#endif

#include <dory/conn/ud.hpp>

#include <dory/memory/pool/pool-allocator.hpp>

#include "../failure-broadcaster.hpp"

#include "../config.hpp"

namespace dory::membership::internal {

template <typename ProcIdType>
class KernelPoller {
 public:
  using PollType =
      int;  // Arbitrary type as the content in in the immediate field
  using AllocatedType = dory::conn::UdReceiveSlot<PollType>;
  using AllocatorType = dory::memory::pool::PoolAllocator<AllocatedType>;

  KernelPoller(std::shared_ptr<dory::conn::UnreliableDatagram> const &ud,
               AllocatorType &allocator,
               FailureNotifier &broadcast_failure_notifier)
      : ud{ud},
        allocator{allocator},
        broadcast_failure_notifier{broadcast_failure_notifier} {}

  void tick() {
    while (auto *mem = allocator.create()) {
      ud->postRecv(reinterpret_cast<uintptr_t>(mem), mem, sizeof(PollType));
    }

    wce.resize(ToPoll);
    if (!ud->pollCqIsOk<dory::conn::UnreliableDatagram::RecvCQ>(wce)) {
      throw std::runtime_error("Polling error.");
    }

    for (auto &wc : wce) {
      if (wc.status != IBV_WC_SUCCESS) {
        throw std::runtime_error("WC not successful.");
      }

      // Release slot
      auto *mem = reinterpret_cast<AllocatedType *>(wc.wr_id);
      allocator.destroy(mem);

      auto proc_id = static_cast<ProcIdType>(wc.imm_data);

      // Notify re-broadcast failure notifier
      if (!broadcast_failure_notifier.enqueue(proc_id)) {
        throw std::runtime_error(
            "Enqueuing the failed notif. to the re-broadcaster failed due to "
            "allocation error");
      }

      // Notify all registered queues
      {
        std::scoped_lock<std::mutex> lock(failure_notifiers_mtx);
        for (auto &failure_notifier : failure_notifiers) {
          if (!failure_notifier.enqueue(proc_id)) {
            throw std::runtime_error(
                "Enqueuing the failed notif. failed due to allocation error");
          }
        }
      }
    }
  }

  FailureNotifier &registerFailureNotifier() {
    std::scoped_lock<std::mutex> lock(failure_notifiers_mtx);
    failure_notifiers.emplace_back();
    return failure_notifiers.back();
  }

  std::shared_ptr<dory::conn::UnreliableDatagram> ud;
  AllocatorType &allocator;
  FailureNotifier &broadcast_failure_notifier;
  std::mutex failure_notifiers_mtx;
  std::list<FailureNotifier> failure_notifiers;

  std::vector<struct ibv_wc> wce;
  static size_t constexpr ToPoll = 1;
};
}  // namespace dory::membership::internal

#pragma once

#include <atomic>
#include <chrono>
#include <stdexcept>
#include <thread>
#include <utility>

#ifdef DORY_TIDIER_ON
#include "../../../default-config.hpp"
#endif

#include <dory/conn/ud.hpp>

#include <dory/memory/pool/pool-allocator.hpp>

#include "../config.hpp"

namespace dory::membership::internal {
class MembershipPoller {
 public:
  using PollType = FullMembership;
  using AllocatedType = conn::UdReceiveSlot<PollType>;
  using AllocatorType = memory::pool::PoolAllocator<AllocatedType>;
  using NotifierQueue = third_party::sync::BlockingSpscQueue<PollType>;

  MembershipPoller(std::shared_ptr<conn::UnreliableDatagram> const &ud,
                   AllocatorType &allocator, NotifierQueue &notifier)
      : ud{ud}, allocator{allocator}, notifier{notifier} {}

  std::optional<FullMembership> tick() {
    while (auto *mem = allocator.create()) {
      ud->postRecv(reinterpret_cast<uintptr_t>(mem), mem, sizeof(PollType));
    }

    wce.resize(1);
    if (!ud->pollCqIsOk<conn::UnreliableDatagram::RecvCQ>(wce)) {
      throw std::runtime_error("Polling error.");
    }

    if (wce.empty()) {
      return {};
    }

    auto &wc = wce[0];
    if (wc.status != IBV_WC_SUCCESS) {
      throw std::runtime_error("WC not successful.");
    }

    // Fetch the membership
    auto *raw_membership = reinterpret_cast<AllocatedType *>(wc.wr_id);

    // Release slot
    // TODO(anon): It is wrong to destroy memory and then return it.
    // It works right now because the type of raw_membership does not
    // have a destructor to change the memory and no one uses the allocator
    // before returning.
    allocator.destroy(raw_membership);

    return raw_membership->resp;
  }

  std::shared_ptr<conn::UnreliableDatagram> ud;
  AllocatorType &allocator;
  NotifierQueue &notifier;

  std::vector<struct ibv_wc> wce;
};
}  // namespace dory::membership::internal

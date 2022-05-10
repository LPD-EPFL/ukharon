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

#include "poller.hpp"

#include "../config.hpp"

namespace dory::membership::internal {
class FullMembershipController {
 private:
  using PollerConn = conn::UnreliableDatagram;
  using PoolType = conn::UdReceiveSlot<FullMembership>;
  using PoolAllocator = memory::pool::PoolAllocator<PoolType>;

 public:
  using Notifier = MembershipPoller::NotifierQueue;

  FullMembershipController(MembershipConfig mc)
      : mc{mc},
        pool{mc->allocator->createPool<PoolType>(PoolObjects,
                                                 AllocationAlignment)},
        notifier_queue(QueueSize),
        LOGGER_INIT(logger, "FullMembershipController") {
    LOGGER_INFO(logger, "Setting up the full-membership poller");

    // Full membership poller (Associated with `start`)
    mc->cb->registerCq(CqFullMembership);
    poller = std::make_shared<PollerConn>(
        *mc->cb, ProtectionDomain, MemoryRegion, CqUnused, CqFullMembership);
    poller_mc.emplace(*mc->cb, ProtectionDomain, poller,
                      mc->full_membership_mc_group);

    membership_poller.emplace(poller, *pool, notifier_queue);
  }

  std::optional<FullMembership> tick() { return membership_poller->tick(); }

  Notifier &notifier() { return notifier_queue; }

 private:
  MembershipConfig mc;

  std::unique_ptr<PoolAllocator> pool;
  static size_t constexpr PoolObjects = 128;

  std::shared_ptr<PollerConn> poller;
  Delayed<conn::McGroup> poller_mc;
  Delayed<MembershipPoller> membership_poller;

  static size_t constexpr QueueSize = 16;
  Notifier notifier_queue;

  LOGGER_DECL(logger);
};
}  // namespace dory::membership::internal

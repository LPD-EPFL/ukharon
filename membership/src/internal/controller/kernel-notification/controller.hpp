#pragma once

#ifdef DORY_TIDIER_ON
#include "../../../default-config.hpp"
#endif

#include <dory/conn/ud.hpp>
#include <dory/memory/pool/pool-allocator.hpp>
#include <dory/shared/logger.hpp>

#include "poller.hpp"

#include "../config.hpp"

namespace dory::membership::internal {

class KernelNotificationController {
 private:
  using PoolAllocator = typename KernelPoller<ProcIdType>::AllocatorType;
  using PoolType = typename KernelPoller<ProcIdType>::AllocatedType;
  using PollerConn = conn::UnreliableDatagram;
  using KernelNotificationPoller = KernelPoller<ProcIdType>;

 public:
  KernelNotificationController(
      MembershipConfig mc,
      std::shared_ptr<FailureBroadcaster<ProcIdType>> failure_broadcaster)
      : mc{mc},
        pool{mc->allocator->createPool<PoolType>(PoolObjects,
                                                 AllocationAlignment)},
        failure_broadcaster{failure_broadcaster},
        LOGGER_INIT(logger, "KernelNotificationController") {
    LOGGER_INFO(logger, "Setting up the kernel notification poller");

    mc->cb->registerCq(CqKernelNotification);

    poller =
        std::make_shared<PollerConn>(*mc->cb, ProtectionDomain, MemoryRegion,
                                     CqUnused, CqKernelNotification);

    poller_mc.emplace(*mc->cb, ProtectionDomain, poller,
                      mc->kernel_fd_mc_group);

    kernel_poller.emplace(poller, *pool,
                          failure_broadcaster->kernelNotificationQueue());
  }

  FailureNotifier& registerFailureNotifier() {
    return kernel_poller->registerFailureNotifier();
  }

  void tick() { kernel_poller->tick(); }

  // void emit() {
  //   if (!poller_mc->postSend(0, nullptr, 0, 17)) {
  //     throw std::runtime_error("Multicast error.");
  //   }
  // }

 private:
  MembershipConfig mc;

  std::unique_ptr<PoolAllocator> pool;
  static size_t constexpr PoolObjects = 128;

  std::shared_ptr<PollerConn> poller;
  std::shared_ptr<FailureBroadcaster<ProcIdType>> failure_broadcaster;
  Delayed<conn::McGroup> poller_mc;
  Delayed<KernelNotificationPoller> kernel_poller;

  LOGGER_DECL(logger);
};
}  // namespace dory::membership::internal

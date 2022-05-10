#pragma once

#include <utility>

#ifdef DORY_TIDIER_ON
#include "../../../default-config.hpp"
#endif

#include <dory/conn/manager/uniform-rc.hpp>
#include <dory/shared/logger.hpp>
#include <dory/shared/types.hpp>

#include "../../rpc/unilateral-connection.hpp"
#include "../../rpc/unilateral-connector.hpp"
#include "../failure-broadcaster.hpp"
#include "counter.hpp"

#include "../config.hpp"

namespace dory::membership::internal {

class HeartbeatCounterController {
 public:
  using RpcHandler =
      UnilateralConnectionRpcHandler<ProcIdType, MembershipRpcKind>;

  HeartbeatCounterController(MembershipConfig mc)
      : mc{mc},
        pool{mc->allocator->createPool<SimpleHeartbeat::Type>(
            PoolObjects, sizeof(SimpleHeartbeat::Cacheline))},
        manager{*mc->cb},
        counter{pool->create()},
        LOGGER_INIT(logger, "HeartbeatCounterController") {
    LOGGER_INFO(logger, "Setting up the heartbeat counter");
    manager.usePd(ProtectionDomain);
    manager.useMr(MemoryRegion);
    manager.useSendCq(CqUnused);
    manager.useRecvCq(CqUnused);
    manager.setNewConnectionRights(ctrl::ControlBlock::LOCAL_READ |
                                   ctrl::ControlBlock::LOCAL_WRITE |
                                   ctrl::ControlBlock::REMOTE_READ);

    RpcHandlerMemLoc mem_location;
    mem_location.location = counter;
    mem_location.mr_offset = pool->offset(counter);

    handler = std::make_unique<RpcHandler>(
        mem_location, &manager, MembershipRpcKind::RDMA_HB_CONNECTION);

    LOGGER_INFO(logger, "Setting up the heartbeat counter");

    SimpleHeartbeat::Cacheline c;
    c.location = counter;

    hb_counter.emplace(c);
  }

  void tick() { hb_counter->tick(); }

  std::unique_ptr<RpcHandler> rpcHandler() { return std::move(handler); }

 private:
  using PoolAllocator = memory::pool::PoolAllocator<SimpleHeartbeat::Type>;
  using ConnManager = conn::manager::UniformRcConnectionManager<ProcIdType>;
  using RpcHandlerMemLoc = typename RpcHandler::MrMemoryLocation;

  MembershipConfig mc;

  std::unique_ptr<PoolAllocator> pool;
  static size_t constexpr PoolObjects = 1;

  ConnManager manager;
  std::unique_ptr<RpcHandler> handler;
  SimpleHeartbeat::Type *counter;
  Delayed<SimpleHeartbeat> hb_counter;

  LOGGER_DECL(logger);
};
}  // namespace dory::membership::internal

#pragma once

#ifdef DORY_TIDIER_ON
#include "../../../default-config.hpp"
#endif

#include <dory/ctrl/block.hpp>

#include <dory/conn/rc.hpp>
#include <dory/conn/ud.hpp>

#include <dory/conn/manager/uniform-rc.hpp>
#include <dory/conn/manager/uniform-ud.hpp>

#include <dory/shared/logger.hpp>
#include <dory/shared/units.hpp>

#include <dory/memory/pool/pool-allocator.hpp>

#include <dory/rpc/conn/universal-connection.hpp>

#include "../../../default-config.hpp"

#include "dynamic-connections.hpp"
#include "rpc-slot.hpp"

namespace dory::membership::internal {
struct Connection {
  dory::conn::ReliableConnection *rc{nullptr};
  dory::conn::UnreliableDatagramConnection *ud{nullptr};

  // Use a shared pointer, otherwise the value is not preserved when altering
  // connections. Because the DynamicConnections have two ping-pong set of
  // connections, if the data of this struct is not a pointer, during copying
  // (from ping to pong set of connections) the latest value may be lost.
  std::shared_ptr<bool> active{std::make_shared<bool>(true)};
  void *slot;
};

using CacheRpcHandler =
    dory::rpc::conn::UniversalConnectionRpcHandler<dory::membership::ProcIdType, dory::membership::MembershipRpcKind>;

class CacheConnectionManager : public CacheRpcHandler::AbstractManager {
 private:
  using ProcIdType = CacheRpcHandler::AbstractManager::ProcIdType;
  using RcConnMgr = dory::conn::manager::UniformRcConnectionManager<ProcIdType>;
  using UdConnMgr = dory::conn::manager::UniformUdConnectionManager<ProcIdType>;
  using ConnMap = std::unordered_map<ProcIdType, Connection>;
  using ConnIterator = std::unordered_map<ProcIdType, Connection>::iterator;

 public:
  using ActiveConnection = DynamicConnections<ConnIterator>;
  CacheConnectionManager(MembershipConfig mc)
      : mc{mc}, LOGGER_INIT(logger, "CacheConnectionManager") {
    size_t alignment = 64;
    LOGGER_INFO(logger,
                "Allocating RPC slots for up to {} concurrent clients (1 slot "
                "per client)",
                rpc_slots_num);

    pool = mc->allocator->createPool<RpcSlot>(rpc_slots_num, AllocationAlignment);

    auto &cb = *mc->cb;

    cb.registerCq(CqViewCacheReply);
    rc_manager = std::make_unique<RcConnMgr>(cb);
    rc_manager->usePd(ProtectionDomain);
    rc_manager->useMr(MemoryRegion);
    rc_manager->useSendCq(CqUnused);
    rc_manager->useRecvCq(CqUnused);
    rc_manager->setNewConnectionRights(dory::ctrl::ControlBlock::LOCAL_READ |
                                       dory::ctrl::ControlBlock::LOCAL_WRITE |
                                       dory::ctrl::ControlBlock::REMOTE_READ |
                                       dory::ctrl::ControlBlock::REMOTE_WRITE);

    ud = std::make_shared<dory::conn::UnreliableDatagram>(
        cb, ProtectionDomain, MemoryRegion, CqViewCacheReply, CqUnused);
    ud_manager = std::make_unique<UdConnMgr>(cb, ud);
    ud_manager->usePd(ProtectionDomain);
  }

  std::shared_ptr<dory::conn::UnreliableDatagram> managerUd() {
    return ud;
  }

  std::pair<bool, std::string> handleStep1(
      ProcIdType proc_id,
      CacheRpcHandler::AbstractManager::Parser const &parser) override {
    std::istringstream remote_info(parser.connectionInfo());
    std::string rc_info;
    std::string ud_info;
    remote_info >> rc_info;
    remote_info >> ud_info;

    LOGGER_DEBUG(logger, "Process {} sent ReliableConnection info: {}", proc_id,
                rc_info);

    LOGGER_DEBUG(logger, "Process {} sent UnreliableDatagram info: {}", proc_id,
                ud_info);

    auto &rc = rc_manager->newConnection(proc_id, rc_info);
    auto rc_info_for_remote = rc.remoteInfo().serialize();

    auto &ud = ud_manager->newConnection(proc_id, ud_info);
    auto ud_info_for_remote = ud_manager->remoteInfo().serialize();

    // Store connection
    Connection conn_data;
    conn_data.rc = &rc;
    conn_data.ud = &ud;
    auto *slot = pool->create();

    if (slot == nullptr) {
      // Connection data will be cleared when the client disconnects
      LOGGER_WARN(logger,
                  "Pool allocator is exhausted when trying to allocate for {}",
                  proc_id);
      std::make_pair(false, "");
    }

    conn_data.slot = slot;
    auto offset = pool->offset(slot);

    conns.insert({proc_id, conn_data});

    LOGGER_DEBUG(logger, "Replying to process {}", proc_id);
    std::string local_serialized_info = rc_info_for_remote + " " +
                                        ud_info_for_remote + " " +
                                        std::to_string(offset);

    return std::make_pair(true, local_serialized_info);
  }

  bool handleStep2(ProcIdType proc_id,
                   CacheRpcHandler::AbstractManager::Parser const &parser) override {
    dory::ignore(proc_id);
    dory::ignore(parser);
    dc.alterConnections(conns.begin(), conns.end());
    return true;
  }

  void remove(ProcIdType proc_id) override {
    rc_manager->removeConnection(proc_id);
    ud_manager->removeConnection(proc_id);
    pool->destroy(reinterpret_cast<RpcSlot *>(conns[proc_id].slot));
    conns.erase(proc_id);
  }

  ActiveConnection *connections() { return &dc; }

  std::vector<ProcIdType> collectInactive() override {
    std::vector<ProcIdType> inactive_vec;
    auto *inactive = dc.alterConnections(conns.begin(), conns.end());

    for (auto &[proc_id, c] : *inactive) {
      if (!*c.active) {
        inactive_vec.push_back(proc_id);
      }
    }

    return inactive_vec;
  }

  void markInactive(ProcIdType proc_id) override {
    *conns[proc_id].active = false;
    dc.alterConnections(conns.begin(), conns.end());
  }

 private:
  MembershipConfig mc;

  std::unique_ptr<RcConnMgr> rc_manager;
  std::shared_ptr<dory::conn::UnreliableDatagram> ud;
  std::unique_ptr<UdConnMgr> ud_manager;
  std::unique_ptr<dory::memory::pool::PoolAllocator<RpcSlot>> pool;

  ConnMap conns;
  DynamicConnections<ConnIterator> dc;

  LOGGER_DECL(logger);

public:
  static size_t constexpr rpc_slots_num = 128;

};

static size_t constexpr RpcSlotCnt = CacheConnectionManager::rpc_slots_num;
}

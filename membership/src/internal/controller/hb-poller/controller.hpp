#pragma once

#ifdef DORY_TIDIER_ON
#include "../../../default-config.hpp"
#endif

#include <dory/conn/manager/uniform-rc.hpp>
#include <dory/shared/logger.hpp>
#include <dory/shared/types.hpp>

#include "../hb-counter/counter.hpp"

#include "poller.hpp"
#include "../failure-broadcaster.hpp"
#include "../../rpc/unilateral-connection.hpp"
#include "../../rpc/unilateral-connector.hpp"

#include "../config.hpp"

namespace dory::membership::internal {

class HeartbeatPollerController {
 public:
 private:
  using Broadcaster = FailureBroadcaster<ProcIdType>;
  using RpcClient =
      UnilateralConnectionRpcClient<ProcIdType, MembershipRpcKind::Kind>;

 public:
  HeartbeatPollerController(MembershipConfig mc,
                            std::shared_ptr<Broadcaster> failure_broadcaster)
      : mc{mc},
        failure_broadcaster{failure_broadcaster},
        pool{mc->allocator->createPool<SimpleHeartbeat::Type>(
        PoolObjects, sizeof(SimpleHeartbeat::Cacheline))},
        poller{*mc->cb},
        counter{pool->create()},
        LOGGER_INIT(logger, "HeartbeatPollerController") {
    LOGGER_INFO(logger, "Setting up the heartbeat poller");

    mc->cb->registerCq(CqHeartbeat);
    poller.bindToPd(ProtectionDomain);
    poller.bindToMr(MemoryRegion);
    poller.associateWithCq(CqHeartbeat,
                            CqUnused);  // Use CQ for RDMA Read (post send)
    poller.init(
        ctrl::ControlBlock::LOCAL_READ | ctrl::ControlBlock::LOCAL_WRITE |
        ctrl::ControlBlock::REMOTE_READ | ctrl::ControlBlock::REMOTE_WRITE);

    mc->cb->registerCq(CqPollerBroadcast);
    broadcast_ud = std::make_shared<dory::conn::UnreliableDatagram>(
        *mc->cb, ProtectionDomain, MemoryRegion, CqPollerBroadcast, CqUnused);
    broadcast_group.emplace(*mc->cb, ProtectionDomain, broadcast_ud,
        mc->kernel_fd_mc_group);

    hb_poller.emplace(poller, counter,
      failure_broadcaster->heartbeatNotificationQueue(), *broadcast_group);
  }

  void tick() {
    hb_poller->tick();
  }

  void monitor(ProcIdType connect_to, std::string const &ip, int port) {
    hb_poller->stop();

    // TODO (Hack to be removed):
    // Do not involve quatros in the ring
    std::unordered_set<std::string> quatro_ips = {
      "128.178.154.41",
      "128.178.154.42",
      "128.178.154.60",
      "128.178.154.65"
    };

    if (quatro_ips.find(ip) != quatro_ips.end()) {
      monitored_id = connect_to;
      LOGGER_WARN(logger, "Not monitoring quatro with id {}.", connect_to);
      return;
    }


    cli.emplace(ip, port);
    if (!cli->connect()) {
      hb_poller->broadcastFailure(connect_to);
      LOGGER_WARN(logger, "CLI connect failed for remote heartbeat counter {}, reporting as dead.", connect_to);
      return;
    }

    auto [cli_ok, cli_offset_info] =
        cli->handshake(poller, mc->id, connect_to, MembershipRpcKind::RDMA_HB_CONNECTION);

    if (!cli_ok) {
      hb_poller->broadcastFailure(connect_to);
      LOGGER_WARN(logger, "CLI handshake failure for remote heartbeat counter {}, reporting as dead.", connect_to);
      return;
    }

    auto read_from = poller.remoteBuf() + cli_offset_info;
    hb_poller->start(connect_to, read_from);

    monitored_id = connect_to;

    LOGGER_INFO(logger, "Monitoring {}", connect_to);
  }

  std::optional<ProcIdType> monitored() { return monitored_id; }

 private:
  using PoolAllocator = memory::pool::PoolAllocator<SimpleHeartbeat::Type>;
  using PollerConn = conn::ReliableConnection;
  using HbPoller = HeartbeatPoller<ProcIdType, SimpleHeartbeat::Type>;

  MembershipConfig mc;
  std::shared_ptr<Broadcaster> failure_broadcaster;

  std::unique_ptr<PoolAllocator> pool;
  static size_t constexpr PoolObjects = 1;

  PollerConn poller;
  SimpleHeartbeat::Type *counter;
  Delayed<RpcClient> cli;
  Delayed<HbPoller> hb_poller;
  std::optional<ProcIdType> monitored_id;

  std::shared_ptr<dory::conn::UnreliableDatagram> broadcast_ud;
  Delayed<dory::conn::McGroup> broadcast_group;

  LOGGER_DECL(logger);
};
}  // namespace dory::membership::internal

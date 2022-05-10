#pragma once

#ifdef DORY_TIDIER_ON
#include "../../../default-config.hpp"
#endif

#include <dory/shared/logger.hpp>

#include <dory/conn/ud.hpp>
#include <dory/memory/pool/pool-allocator.hpp>
#include <dory/paxos/majority-reader.hpp>
#include <dory/shared/logger.hpp>
#include <dory/shared/pinning.hpp>
#include <dory/shared/types.hpp>
#include <utility>

// #include <dory/rpc/conn/universal-connector.hpp>

#include "../../rpc/unilateral-connector.hpp"

#include "../local-cache.hpp"
#include "../cache/rpc-slot.hpp"

namespace dory::membership::internal {

class ActiveViewMajorityController {
 private:
  using PoolAllocator = memory::pool::PoolAllocator<paxos::MajorityReader::PolledObj>;
  using PollerConn = conn::ReliableConnection;
  using RpcClient =
      UnilateralConnectionRpcClient<ProcIdType, MembershipRpcKind::Kind>;

 public:
  ActiveViewMajorityController(MembershipConfig mc)
      : mc{mc},
      pool{mc->allocator->createPool<paxos::MajorityReader::PolledObj>(PoolObjects, AllocationAlignment)},
      dv(DeltaMajority, AsyncCoreMajority), LOGGER_INIT(logger, "ActiveViewMajorityController") {
    LOGGER_INFO(logger, "Setting up the active view poller");
    mc->cb->registerCq(CqView);
    auto &send_cq = mc->cb->cq(CqView);
    majority_reader.emplace(MaxViewCnt, send_cq);
  }

  bool monitor(ProcIdType connect_to, std::string const &ip, int port) {
    // if (poller_thread.joinable()) {
    //   throw std::runtime_error("Cannot monitor after starting");
    // }

    if (has_started.load()) {
      throw std::runtime_error("Cannot monitor after starting");
    }

    PollerConn poller(*mc->cb);
    poller.bindToPd(ProtectionDomain);
    poller.bindToMr(MemoryRegion);
    poller.associateWithCq(CqView,
                            CqUnused);  // Use CQ for RDMA Read (post send)
    poller.init(
        ctrl::ControlBlock::LOCAL_READ | ctrl::ControlBlock::LOCAL_WRITE |
        ctrl::ControlBlock::REMOTE_READ | ctrl::ControlBlock::REMOTE_WRITE);

    clients.emplace_back(ip, port);
    auto &cli = clients.back();
    cli.connect();
    auto [cli_ok, cli_offset_info] =
        cli.handshake(poller, mc->id, connect_to, MembershipRpcKind::RDMA_AV_CONNECTION);

    if (!cli_ok) {
      LOGGER_WARN(logger, "Could not connect to the remote accepted slots of {}", connect_to);
      return false;
    }

    auto read_from = cli_offset_info;
    auto *slot = pool->create();
    majority_reader->addAcceptor(connect_to, std::move(poller), read_from, slot);

    return true;
  }

  bool started() {
    return has_started.load();
    // return poller_thread.joinable();
  }

  void tick() {
    if (!has_started.load()) {
      return;
    }

    auto &queue = majority_reader->resolvedRefPointsQueue();

    majority_reader->tick();

    if (!armed) {
      armed = std::chrono::steady_clock::now();
      majority_reader->insertRefPoint();
    }

    paxos::MajorityReader::ResolvedRefPointsQueue::value_type resolved;
    if (!queue.try_dequeue(resolved)) {
      return;
    }

    // TODO(anon): Just returns the index of the slot.
    // By convention, the index corresponds to the view id.
    // This has to be fixed
    if (resolved.second != 0) {
      ViewIdType view_id_active = static_cast<ViewIdType>(resolved.second) - 1;
      dv.renew(view_id_active, armed.value());
    }

    armed.reset();
  }


  void start() {
    bool expected = false;
    if (!has_started.compare_exchange_strong(expected, true)) {
      throw std::runtime_error("Some has already started the ActiveViewMajorityController");
    }

    return;

  //   if (poller_thread.joinable()) {
  //     return;
  //   }

  //   poller_thread = std::thread([this](){
  //     while (true) {
  //       auto &queue = majority_reader->resolvedRefPointsQueue();
  //       auto now = std::chrono::steady_clock::now();
  //       majority_reader->insertRefPoint();

  //       dory::paxos::MajorityReader::ResolvedRefPointsQueue::value_type resolved;
  //       while (!queue.try_dequeue(resolved)) {
  //         majority_reader->tick();
  //       }

  //       // TODO: Just returns the index of the slot.
  //       // By convention, the index corresponds to the view id.
  //       // This has to be fixed

  //       if (resolved.second == 0) {
  //         continue;
  //       }

  //       ViewIdType view_id_active = static_cast<ViewIdType>(resolved.second) - 1;

  //       dv.renew(view_id_active, now);

  //       // // Wait some time
  //       // while (true) {
  //       //   auto end = std::chrono::steady_clock::now();

  //       //   auto diff =
  //       //       std::chrono::duration_cast<std::chrono::microseconds>(end - now);
  //       //   if (diff > Pause) {
  //       //     break;
  //       //   }
  //       // }
  //     }
  //   });

  }

  LocalActiveCache<ViewIdType, FullMembership> &localActiveCache() {
    return dv;
  }

 private:
  MembershipConfig mc;
  std::unique_ptr<PoolAllocator> pool;

  Delayed<paxos::MajorityReader> majority_reader;

  static size_t constexpr PoolObjects = MaxPaxosAcceptorsCnt;

  std::vector<RpcClient> clients;

  std::thread poller_thread;

  ViewIdType view_id = 0;

  LocalActiveCache<ViewIdType, FullMembership> dv;

  std::optional<std::chrono::steady_clock::time_point> armed;

  std::atomic<bool> has_started{false};

  LOGGER_DECL(logger);
};
}  // namespace dory::membership::internal

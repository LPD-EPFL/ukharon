#pragma once

#include <functional>
#include <map>
#include <optional>
#include <utility>
#include <vector>

#ifdef DORY_TIDIER_ON
#include "../../../default-config.hpp"
#endif

#include <dory/shared/logger.hpp>

#include <dory/conn/ud.hpp>
#include <dory/memory/pool/pool-allocator.hpp>
#include <dory/paxos/majority-reader.hpp>
#include <dory/rpc/conn/universal-connector.hpp>
#include <dory/shared/logger.hpp>
#include <dory/shared/types.hpp>

#include "../cache/rpc-slot.hpp"
#include "../config.hpp"
#include "../local-cache.hpp"

namespace dory::membership::internal {

class ActiveViewCacheController {
 private:
  using ResponsePoolAllocator =
      memory::pool::PoolAllocator<conn::UdReceiveSlot<RpcSlot::Response>>;
  using RequestPoolAllocator = memory::pool::PoolAllocator<RpcSlot::Request>;

  using SendConn = conn::ReliableConnection;
  using RecvConnCommon = conn::UnreliableDatagram;

  using RpcClient =
      rpc::conn::UniversalConnectionRpcClient<ProcIdType,
                                              MembershipRpcKind::Kind>;

  struct CacheConn {
    CacheConn(ProcIdType id, SendConn &&rc, uintptr_t slot_addr) noexcept
        : id{id}, rc{std::move(rc)}, slotAddr{slot_addr} {}

    ProcIdType id;
    SendConn rc;
    uintptr_t slotAddr;
  };

 public:
  ActiveViewCacheController(MembershipConfig mc,
                            FailureNotifier &failure_notifier)
      : mc{mc},
        failure_notifier{failure_notifier},
        resp_pool{mc->allocator->createPool<ResponsePoolAllocator::value_type>(
            PoolObjects, AllocationAlignment)},
        req_pool{mc->allocator->createPool<RequestPoolAllocator::value_type>(
            1, AllocationAlignment)},
        dv(DeltaCache, AsyncCoreCache),
        LOGGER_INIT(logger, "ActiveViewCacheController") {
    LOGGER_INFO(logger, "Setting up the active view poller");
    mc->cb->registerCq(CqView);
    auto &send_cq = mc->cb->cq(CqView);
    ud.emplace(*mc->cb, ProtectionDomain, MemoryRegion, CqUnused, CqView);

    query_slot = req_pool->create();
    query_slot->state = RpcSlot::Request::Query;
  }

  bool monitor(ProcIdType connect_to, std::string const &ip, int port) {
    if (has_started.load()) {
      throw std::runtime_error("Cannot monitor after starting");
    }

    auto &cb = *mc->cb;

    SendConn send_conn(cb);
    send_conn.bindToPd(ProtectionDomain);
    send_conn.bindToMr(MemoryRegion);
    send_conn.associateWithCq(CqView,
                              CqUnused);  // Use CQ for RDMA Write (post send)
    send_conn.init(
        ctrl::ControlBlock::LOCAL_READ | ctrl::ControlBlock::LOCAL_WRITE |
        ctrl::ControlBlock::REMOTE_READ | ctrl::ControlBlock::REMOTE_WRITE);

    clients.emplace_back(ip, port);
    auto &cli = clients.back();

    cli.connect();
    auto [cli_ok, cli_offset_info] = cli.handshake<dory::uptrdiff_t>(
        [this, &send_conn]() -> std::pair<bool, std::string> {
          std::string serialized_info =
              send_conn.remoteInfo().serialize() + " " + ud->info().serialize();

          return std::make_pair(true, serialized_info);
        },
        [&send_conn, &cb, connect_to](std::string const &info)
            -> std::pair<bool, std::optional<dory::uptrdiff_t>> {
          std::istringstream remote_info_stream(info);
          std::string rc_info;
          std::string ud_info;
          dory::uptrdiff_t offset_info;
          remote_info_stream >> rc_info;
          remote_info_stream >>
              ud_info;  // Not needed if ud is used only for recv
          remote_info_stream >> offset_info;

          auto remote_rc = dory::conn::RemoteConnection::fromStr(rc_info);
          send_conn.reset();
          send_conn.reinit();
          send_conn.connect(remote_rc, connect_to);

          return std::make_pair(true,
                                std::optional<dory::uptrdiff_t>(offset_info));
        },
        mc->id, dory::membership::RpcKind::RDMA_CACHE_CONNECTION);

    if (!cli_ok) {
      LOGGER_WARN(logger,
                  "Could not connect to the remote accepted slots of {}",
                  connect_to);
      return false;
    }

    auto addr = send_conn.remoteBuf() + cli_offset_info.value();
    cache_conns.try_emplace(connect_to, connect_to, std::move(send_conn), addr);

    if (!prefered_cache) {
      prefered_cache = cache_conns.at(connect_to);
    }

    return true;
  }

  bool started() { return has_started.load(); }

  void tick() {
    if (!has_started.load()) {
      return;
    }

    ProcIdType failure;
    if (failure_notifier.try_dequeue(failure)) {
      if (failure == prefered_cache->get().id) {
        bool found_new_cache = false;
        for (auto &[id, conn] : cache_conns) {
          if (id != failure) {
            prefered_cache = conn;
            LOGGER_DEBUG(logger, "Switching to cache {}", id);
            found_new_cache = true;
            armed.reset();
            break;
          }
        }
        if (!found_new_cache) {
          throw std::runtime_error("Cache failed but only 1 was registered.");
        }
      }
    }

    auto &cache = prefered_cache->get();

    auto &rc = cache.rc;
    auto fetch_from = cache.id;
    auto slot_addr = cache.slotAddr;

    if (armed) {
      wce.resize(2);
      if (!ud->pollCqIsOk<dory::conn::UnreliableDatagram::RecvCQ>(wce)) {
        throw std::runtime_error("Polling error.");
      }

      for (auto const &wc : wce) {
        if (wc.wr_id == 0) {  // Post
          if (wc.status != IBV_WC_SUCCESS) {
            throw std::runtime_error("Cache request post WR failed.");
          }
        } else {  // Recv
          if (wc.status != IBV_WC_SUCCESS) {
            throw std::runtime_error("Cache response recv WR failed.");
          }
          auto *mem =
              reinterpret_cast<ResponsePoolAllocator::value_type *>(wc.wr_id);
          auto proc_id = static_cast<ProcIdType>(wc.imm_data);

          if (proc_id == fetch_from) {
            if (latest_known_view != mem->resp.view_id) {
              dv.barrier(mem->resp.view_id);
              latest_known_view = mem->resp.view_id;
            }
            // In case of compatible view, we want to renew.
            dv.renew(mem->resp.view_id, armed->second);

            armed.reset();
          } else {
            LOGGER_WARN(logger, "Dismissing resp. from old cache.");
          }

          // Release slot
          resp_pool->destroy(mem);
        }
      }
    }

    if (!armed) {
      while (auto *mem = resp_pool->create()) {
        ud->postRecv(reinterpret_cast<uintptr_t>(mem), mem,
                     sizeof(RpcSlot::Response));
      }

      auto now = std::chrono::steady_clock::now();
      armed = {fetch_from, now};

      auto ok = rc.postSendSingle(SendConn::RdmaWrite,
                                  0,  // 0 = Post
                                  query_slot, sizeof(RpcSlot::Request),
                                  slot_addr + offsetof(RpcSlot, req));
      if (!ok) {
        throw std::runtime_error("Failed to post RDMA Write to cache " +
                                 std::to_string(+fetch_from));
      }
    }
  }

  void start() {
    bool expected = false;
    if (!has_started.compare_exchange_strong(expected, true)) {
      throw std::runtime_error(
          "Some has already started the ActiveViewMajorityController");
    }

    if (!prefered_cache) {
      throw std::runtime_error("Cannot start before monitoring a cache.");
    }

    return;
  }

  LocalActiveCache<ViewIdType, FullMembership> &localActiveCache() {
    return dv;
  }

 private:
  MembershipConfig mc;
  FailureNotifier &failure_notifier;
  std::unique_ptr<ResponsePoolAllocator> resp_pool;
  std::unique_ptr<RequestPoolAllocator> req_pool;

  std::vector<struct ibv_wc> wce;

  RequestPoolAllocator::value_type *query_slot;

  static size_t constexpr PoolObjects =
      128;  // Max number of in flight responses

  Delayed<RecvConnCommon> ud;

  std::vector<RpcClient> clients;
  std::map<ProcIdType, CacheConn> cache_conns;
  std::optional<std::reference_wrapper<CacheConn>> prefered_cache;

  LocalActiveCache<ViewIdType, FullMembership> dv;

  std::optional<std::pair<ProcIdType, std::chrono::steady_clock::time_point>>
      armed;
  std::optional<ViewIdType> latest_known_view;

  std::atomic<bool> has_started{false};

  LOGGER_DECL(logger);
};

}  // namespace dory::membership::internal

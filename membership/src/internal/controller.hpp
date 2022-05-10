#pragma once

#include <memory>
#include <string>
#include <optional>
#include <functional>

#ifdef DORY_TIDIER_ON
#include "../default-config.hpp"
#endif

#include <dory/conn/ud.hpp>
#include <dory/ctrl/block.hpp>
#include <dory/memory/pool/pool-allocator.hpp>
#include <dory/rpc/server.hpp>
#include <dory/shared/logger.hpp>
#include <dory/shared/pointer-wrapper.hpp>
#include <dory/shared/units.hpp>
#include <dory/special/heartbeat.hpp>
#include <dory/memstore/store.hpp>

#include "controller/active-view/cache-controller.hpp"
#include "controller/active-view/majority-controller.hpp"
#include "controller/full-membership/controller.hpp"
#include "controller/hb-poller/controller.hpp"
#include "controller/hb-counter/controller.hpp"
#include "controller/kernel-notification/controller.hpp"

#include "controller/cache/connections.hpp"
#include "controller/failure-broadcaster.hpp"
#include "controller/local-cache.hpp"

#include "controller/config.hpp"

namespace dory::membership::internal {

class MembershipController {
 private:
  using FailureHandler = FailureBroadcaster<ProcIdType>;
  using MembershipNotifier = FullMembershipController::Notifier;

 public:
  enum ActiveViewPollingMode {
    None, // For acceptors
    AcceptorMajority, // For cache
    Cache, // For member
  };

  MembershipController(ControlBlock &cb, ProcIdType id,
                       bool with_membership_notifier,
                       ActiveViewPollingMode polling_mode,
                       std::string const &membership_mc_group,
                       std::string const &kernel_notification_mc_group)
      : with_membership_notifier{with_membership_notifier},
        polling_mode{polling_mode},
        membership_notifier(QueueSize),
        failure_notifier(QueueSize),
        LOGGER_INIT(logger, "MembershipController") {
    config = std::make_shared<Config>();
    config->id = id;
    config->full_membership_mc_group = membership_mc_group;
    config->kernel_fd_mc_group = kernel_notification_mc_group;

    // TODO:
    // Refactor this line. For now, it does not delete the pointer, as it gets
    // it from a reference.
    config->cb = std::shared_ptr<ControlBlock>(&cb, [](ControlBlock *) {});
  }

  void initializeControlBlock() {
    LOGGER_INFO(logger,
                "Registering the protection domain in the control block");
    config->cb->registerPd(ProtectionDomain);

    LOGGER_INFO(logger, "Registering the completion queue {}", CqUnused);
    config->cb->registerCq(CqUnused);

    LOGGER_INFO(logger, "Allocating {} bytes for failure detection purposes",
                AllocationSize);
    config->cb->allocateBuffer(BufferName, AllocationSize, AllocationAlignment);
    config->cb->registerMr(
        MemoryRegion, ProtectionDomain, BufferName,
        ctrl::ControlBlock::LOCAL_READ | ctrl::ControlBlock::LOCAL_WRITE |
            ctrl::ControlBlock::REMOTE_READ | ctrl::ControlBlock::REMOTE_WRITE);

    auto *buf = reinterpret_cast<void *>(config->cb->mr(MemoryRegion).addr);
    config->allocator.emplace(buf, AllocationSize, buf);

    heartbeat_counter.emplace(config);

    failure_broadcaster = std::make_shared<FailureHandler>();
    heartbeat_poller.emplace(config, failure_broadcaster);

    kernel_notification_controller.emplace(config, failure_broadcaster);

    if (polling_mode == AcceptorMajority) {
      active_view_majority_controller.emplace(config);
      cache_conn_manager = std::make_unique<CacheConnectionManager>(config);
      cache_active_connections = cache_conn_manager->connections();
      cache_active_connections_ud = cache_conn_manager->managerUd();
      local_active_cache = active_view_majority_controller->localActiveCache();
    } else if (polling_mode == Cache) {
      active_view_cache_controller.emplace(config,
        kernel_notification_controller->registerFailureNotifier());
      local_active_cache = active_view_cache_controller->localActiveCache();
    }

    full_membership_controller.emplace(config);
  }

  void startBackgroundThread() {
    background_thread = std::thread([this](){
      auto &full_membership_notifier = full_membership_controller->notifier();
      while (true) {
        heartbeat_counter->tick();
        kernel_notification_controller->tick();
        heartbeat_poller->tick();
        failure_broadcaster->tick();

        if (active_view_majority_controller) {
          active_view_majority_controller->tick();
        }

        if (active_view_cache_controller) {
          active_view_cache_controller->tick();
        }

        auto const new_membership = full_membership_controller->tick();

        if (with_membership_notifier || local_active_cache) {
          // Poll for new memberships.
          // We are polling in the background as it adds only 2us latency overall
          // (as seen from the app member). It also spares a busy thread, dedicated
          // to only polling for new memberships.
          if (new_membership) {
            auto new_membership_ts = std::chrono::steady_clock::now();

            // Push it to the queue
            if (with_membership_notifier) {
              if (!membership_notifier.enqueue(*new_membership)) {
                throw std::runtime_error("Enqueuing to membership notifier failed due to allocation error");
              }
            }

            if (local_active_cache) {
              local_active_cache->get().discovered(*new_membership);
              // TODO: Improve the abstraction. The problem is:
              // The active_view_cache_controller should barrier the view
              // by itself, otherwise if it barriers the view when the consensus
              // layer broadcasts it, it will silently (and wrongly) bypass the wait
              // time that is imposed by the cache layer.
              // The cache layer can use the broadcast information to set the time
              // it barriers a new view.
              // On the other hand, the member should learn the new view directly from the cache.
              if (active_view_majority_controller) {
                local_active_cache->get().barrier(new_membership->id, new_membership_ts);
              }
            }
          }
        }

        // TODO: The notifier queue is currently created in the full membership controller.
        // It's place is this controller. Move it here.
        // Notify the `poll()` function (for reconnecting the ring)
        if (new_membership && !full_membership_notifier.enqueue(*new_membership)) {
          throw std::runtime_error("Enqueuing to membership connector failed due to allocation error");
        }
      }
    });

    pin_thread_to_core(background_thread, FdCore);
  }

  CacheConnectionManager::ActiveConnection *cacheActiveConnections() {
    if (!cache_active_connections) {
      throw std::runtime_error(
          "The cache manager is accessible only in the AcceptorMajority polling mode");
    }

    return cache_active_connections;
  }

  std::shared_ptr<dory::conn::UnreliableDatagram> underlyingCacheActiveConnectionsUd() {
    if (!cache_active_connections_ud) {
      throw std::runtime_error(
          "The cache manager is accessible only in the AcceptorMajority polling mode");
    }

    return cache_active_connections_ud;
  }

  deleted_unique_ptr<struct ibv_cq> &cacheReplyCq() {
    if (!cache_active_connections) {
      throw std::runtime_error(
          "The cache reply CQ is accessible only in the AcceptorMajority polling mode");
    }

    return config->cb->cq(CqViewCacheReply);
  }


  // void emitFailure() {
  //   kernel_notification_controller->emit();
  // }

  void start() {
    membership_thread = std::thread([this]() {poll(); });
    pin_thread_to_core(membership_thread, MembershipManagerCore);
  }

  void enableKernelHeartbeat() { special::enable_heartbeat(config->id); }

  void registerRpcHandlers(RpcServer &rpc_server) {
    rpc_server.attachHandler(heartbeat_counter->rpcHandler());

    if (cache_conn_manager) {
        auto cache_conn_handler = std::make_unique<CacheRpcHandler>(std::move(cache_conn_manager),
                                           dory::membership::RpcKind::RDMA_CACHE_CONNECTION);

      rpc_server.attachHandler(std::move(cache_conn_handler));
    }
  }

  LocalActiveCache<ViewIdType, FullMembership> &localActiveCache() {
    return local_active_cache->get();
  }

  dory::deleted_unique_ptr<MembershipNotifier> membershipNotifier() {
    std::unique_lock lock(notifiers.membership_mtx);
    if (!with_membership_notifier) {
      throw std::runtime_error(
          "Cannot use the membership notifier before enabling it");
    }

    if (notifiers.membership_handed) {
      throw std::runtime_error("Have already returned the membership notifier");
    }

    notifiers.membership_handed = true;
    return dory::deleted_unique_ptr<MembershipNotifier>(
        &membership_notifier, [](MembershipNotifier *) noexcept {});
  }

  FailureNotifier& registerFailureNotifier() {
    return kernel_notification_controller->registerFailureNotifier();
  }

 private:
  void poll() {
    memstore::ProcessAnnouncer announcer;
    auto &full_membership_notifier = full_membership_controller->notifier();

    FullMembership membership;

    while (true) {
      // Fetch the membership from queue (blocking)
      full_membership_notifier.wait_dequeue(membership);

      // Start the majority reader
      if (active_view_majority_controller && !active_view_majority_controller->started()) {
        for (int16_t i = 0; i < membership.cnt; i++) {
          if (membership.members[i].isConsensus()) {
            auto connect_to = membership.members[i].pid;
            auto [ip, port] = announcer.processToHost(connect_to);

            active_view_majority_controller->monitor(connect_to, ip, port);
          }
        }

        active_view_majority_controller->start();
        // std::this_thread::sleep_for(std::chrono::seconds(1));
      }

      // Or start the cache reader
      if (active_view_cache_controller && !active_view_cache_controller->started()) {
        bool connected = false;
        for (int16_t i = 0; i < membership.cnt; i++) {
          if (membership.members[i].isCache()) {
            auto connect_to = membership.members[i].pid;
            auto [ip, port] = announcer.processToHost(connect_to);

            active_view_cache_controller->monitor(connect_to, ip, port);
            connected = true;
          }
        }
        if (connected) {
          active_view_cache_controller->start();
          // std::this_thread::sleep_for(std::chrono::seconds(1));
        } else {
          LOGGER_INFO(logger, "No cache found in current membership, not starting.", membership.toString());
        }
      }

      // Complete the ring
      LOGGER_DEBUG(logger, "I got {}", membership.toString());

      auto [included, inclusion_idx] = membership.includes(config->id);
      if (included) {
        LOGGER_DEBUG(logger, "I am part of the membership");
        auto next = membership.cyclicallyNext(inclusion_idx);
        if (next) {
          if (!heartbeat_poller->monitored() || heartbeat_poller->monitored().value() != next) {
            LOGGER_DEBUG(logger, "Connecting the heartbeat to {}", next.value());

            auto connect_to = next.value();
            auto [ip, port] = announcer.processToHost(connect_to);
            heartbeat_poller->monitor(connect_to, ip, port);
          }
        } else {
          LOGGER_DEBUG(logger, "Nowhere to connect the heartbeat to");
        }
      }
    }
  }

  bool with_membership_notifier;
  bool with_failure_notifier;
  ActiveViewPollingMode polling_mode;

  std::shared_ptr<Config> config;

  // FullView<ViewIdType, FullMembership> full_view;

  Delayed<HeartbeatCounterController> heartbeat_counter;

  std::shared_ptr<FailureHandler> failure_broadcaster;
  Delayed<HeartbeatPollerController> heartbeat_poller;

  Delayed<KernelNotificationController> kernel_notification_controller;

  std::optional<std::reference_wrapper<LocalActiveCache<ViewIdType, FullMembership>>> local_active_cache;
  Delayed<ActiveViewMajorityController> active_view_majority_controller;
  Delayed<ActiveViewCacheController> active_view_cache_controller;
  std::unique_ptr<CacheConnectionManager> cache_conn_manager;
  CacheConnectionManager::ActiveConnection *cache_active_connections = nullptr;
  std::shared_ptr<dory::conn::UnreliableDatagram> cache_active_connections_ud;

  Delayed<FullMembershipController> full_membership_controller;

  MembershipNotifier membership_notifier;
  FailureNotifier failure_notifier;
  static size_t constexpr QueueSize = 128;
  struct Notifiers {
    std::mutex membership_mtx;
    bool membership_handed = false;
  };
  Notifiers notifiers;

  std::thread background_thread;
  std::thread membership_thread;

  LOGGER_DECL(logger);
};

}  // namespace dory::membership::internal

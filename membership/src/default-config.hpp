#pragma once

#ifndef DORY_MEMBERSHIP_CONFIG
#define DORY_MEMBERSHIP_CONFIG
#else
#error "The DORY_MEMBERSHIP_CONFIG define is reserved"
#endif

#include <chrono>
#include <cstdint>
#include <stdexcept>

#include <dory/shared/units.hpp>

#include "internal/full-membership.hpp"

namespace dory::membership {

struct RpcKind {
  enum Kind : uint8_t {
    RDMA_HB_CONNECTION,
    RDMA_AV_CONNECTION,
    RDMA_CACHE_CONNECTION,
    RDMA_HERD_CONNECTION
  };

  static char const *name(Kind k) {
    switch (k) {
      case RDMA_HB_CONNECTION:
        return "HeartbeatConnectionRpcHandler";
      case RDMA_AV_CONNECTION:
        return "ActiveViewConnectionRpcHandler";
      case RDMA_CACHE_CONNECTION:
        return "CacheConnectionRpcHandler";

      // TODO(anon): This does not belong here.
      // Find a different way to initialize
      case RDMA_HERD_CONNECTION:
        return "HerdConnectionRpcHandler";

      default:
        throw std::runtime_error("Unknown RpcKind name!");
    }
  }
};

// Configuration
static constexpr char const *ProtectionDomain = "mc-primary";
static constexpr char const *BufferName = "mc-buf";
static constexpr char const *MemoryRegion = "mc-mr";
static constexpr char const *CqUnused = "mc-cq-unused";
static constexpr char const *CqHeartbeat = "mc-cq-heartbeat";
static constexpr char const *CqPollerBroadcast = "mc-cq-poller-broadcast";
static constexpr char const *CqKernelNotification = "mc-cq-kernel-notif";
static constexpr char const *CqFullMembership = "mc-cq-full-membership";
static constexpr char const *CqView = "mc-cq-view";
static constexpr char const *CqViewCacheReply = "mc-cq-view-cache-reply";

using ProcIdType = uint16_t;
using ViewIdType = uint32_t;
using MembershipRpcKind = RpcKind;

static constexpr std::chrono::milliseconds HbTimeout(10);
static constexpr std::chrono::microseconds HbRereadTimeout(30);

static constexpr size_t MaxMembers = 16;
using FullMembership =
    internal::FullMembershipImpl<ProcIdType, ViewIdType, MaxMembers>;

static constexpr size_t MaxViewCnt = 4096;
static constexpr size_t MaxPaxosAcceptorsCnt = 128;

static constexpr std::chrono::microseconds DeltaMajority(23);
static constexpr std::chrono::microseconds AsyncCoreMajority(
    1);  // 10% of DeltaMajority
static constexpr std::chrono::microseconds DeltaCache(24);
static constexpr std::chrono::microseconds AsyncCoreCache(
    1);  // 10% of DeltaCache

static constexpr size_t AllocationSize = dory::units::mebibytes(10);
static constexpr size_t AllocationAlignment = 64;

// For optimal (i.e. low) synchronization overhead, these cores better be
// hyperthreaded
static constexpr int LeaseCheckingCore = 10;
static constexpr int FdCore = 8;
static constexpr int AcceptorCore =
    LeaseCheckingCore;  // Acceptor nodes don't check the leases
static constexpr int MembershipManagerCore = 14;

}  // namespace dory::membership

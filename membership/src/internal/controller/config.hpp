#pragma once

#ifdef DORY_TIDIER_ON
#include "../../default-config.hpp"
#endif

#include <dory/ctrl/block.hpp>
#include <dory/rpc/server.hpp>
#include <dory/memory/pool/pool-allocator.hpp>
#include <dory/shared/types.hpp>
#include <dory/third-party/sync/spsc.hpp>

namespace dory::membership::internal {
using ControlBlock = ctrl::ControlBlock;
using RpcServer = rpc::RpcServer<MembershipRpcKind::Kind>;
using ArenaAllocator = memory::pool::ArenaPoolAllocator;
using FailureNotifier = third_party::sync::SpscQueue<ProcIdType>;

struct Config {
  ProcIdType id;
  std::shared_ptr<ctrl::ControlBlock> cb;
  Delayed<ArenaAllocator> allocator;
  std::string full_membership_mc_group;
  std::string kernel_fd_mc_group;
};

using MembershipConfig = std::shared_ptr<Config>;
}  // namespace dory::membership::internal

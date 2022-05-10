#pragma once

#ifdef DORY_TIDIER_ON
#include "default-config.hpp"
#endif

#ifndef DORY_MEMBERSHIP_CONFIG
#error \
    "Define the configuration before using the membership. A default configuration is given in <dory/membership/default-config.hpp>"
#endif

#include "internal/controller.hpp"

namespace dory::membership {

using Membership = internal::MembershipController;

using LocalActiveCache = internal::LocalActiveCache<ViewIdType, FullMembership>;
using RpcServer = internal::RpcServer;
using CacheRpcSlot = internal::RpcSlot;
using FailureNotifier = internal::FailureNotifier;
using UnilateralConnectionRpcHandler =
    internal::UnilateralConnectionRpcHandler<ProcIdType,
                                             dory::membership::RpcKind>;

size_t constexpr CacheRpcSlotCnt = internal::RpcSlotCnt;
}  // namespace dory::membership

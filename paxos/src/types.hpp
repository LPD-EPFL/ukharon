#pragma once

#include <cstdint>

#include "internal/acceptor-buffer.hpp"
#include "internal/types.hpp"

namespace dory::paxos {

// We re-expose some internal types
using Instance = internal::Instance;
using RefPoint = internal::RefPoint;

#ifdef DORY_PAXOS_DM
template <bool Inlined, typename RPV>
using DmAcceptorBuffer = internal::DmAcceptorBuffer<Inlined, RPV>;
#endif

template <bool Inlined, typename RPV>
using MmAcceptorBuffer = internal::MmAcceptorBuffer<Inlined, RPV>;

}  // namespace dory::paxos

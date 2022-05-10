#pragma once

#include <cstdint>

namespace dory::paxos::internal {

using ProcId = uint16_t;
using ReqId = uint32_t;
using Proposal = uint16_t;
using ProposalValue = uint16_t;
using Instance = uint16_t;
using RefPoint = ReqId;

}  // namespace dory::paxos::internal

#pragma once

#include "../handlers.hpp"
#include "types.hpp"

namespace dory::paxos::internal {

/**
 * @brief Abstraction capable of handling a Paxos deliver event.
 *
 */
template <typename RealProposalValue>
class Handler {
 public:
  virtual void handle(Instance, RealProposalValue) = 0;
  virtual ~Handler() = default;
};

}  // namespace dory::paxos::internal

#pragma once

#include <ostream>
#include <string>

#include "types.hpp"

namespace dory::paxos::internal {

/**
 * @brief Stores the state of a single instance of Paxos.
 *
 * WARNING: The order of the fields is compiler-specific.
 */
union Consensus {
  struct Data {
    Data() = default;
    Data(Instance instance) : instance{instance} {}

    Instance instance = 0;
    Proposal min_proposal = 0;
    Proposal accepted_proposal = 0;
    ProposalValue accepted_value = 0;
  } data;

  uint64_t packed;

  Consensus() : data{} {}
  Consensus(Instance instance) : data{instance} {}

  bool accepted() const { return data.accepted_proposal != 0; }

  bool operator==(Consensus& other) const { return packed == other.packed; }
};

static std::ostream& operator<<(std::ostream& os, Consensus const& c) {
  return os << "<i: " << std::to_string(static_cast<int>(c.data.instance))
            << ", mp: " << std::to_string(static_cast<int>(c.data.min_proposal))
            << ", ap: "
            << std::to_string(static_cast<int>(c.data.accepted_proposal))
            << ", av: "
            << std::to_string(static_cast<int>(c.data.accepted_value)) << ">";
}

// As IB atomics only work on 64 bits, the whole consensus state should fit
// in 64 bits.
static_assert(sizeof(Consensus) == sizeof(typename Consensus::Data));
static_assert(sizeof(Consensus) == sizeof(uint64_t));

}  // namespace dory::paxos::internal

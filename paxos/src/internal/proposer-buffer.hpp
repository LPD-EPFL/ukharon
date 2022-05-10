#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <map>
#include <vector>

#include <dory/shared/logger.hpp>
#include <dory/shared/types.hpp>

#include "consensus.hpp"
#include "constants.hpp"

namespace dory::paxos::internal {

/**
 * @brief Abstraction responsible for handling the memory layout of a proposers.
 *
 * @tparam ProposalValueInlined whether we will need to store the real value
 *         in an additional buffer.
 * @tparam DecoratedValue the type returned by the Decorator (so that we know
 *         the memory space it takes).
 */
template <bool ProposalValueInlined, typename DecoratedValue>
class ProposerBuffer {
 public:
  /**
   * @brief Simple Buffer constructor.
   *
   * TODO: Split into two constructors:
   *  * Regular memory
   *  * DM
   *
   * @param buffer starts at the beginning of the VM.
   * @param mr_offset starts at the beginning of the MR.
   */
  ProposerBuffer(std::map<ProcId, size_t> const &proc_indices,
                 uintptr_t const buffer, uptrdiff_t const mr_offset,
                 size_t const size, size_t const slots)
      : proc_indices{proc_indices},
        buffer{buffer},
        mr_offset{mr_offset},
        num_nodes{proc_indices.size()},
        slots{slots},
        LOGGER_INIT(logger, "PROPOSER-BUFFER") {
    if (+sizeof(Consensus) * num_nodes       // Destination buffer for CAS reads
            + sizeof(Consensus) * num_nodes  // Dest. buffer for bg deciding
            + sizeof(Consensus)              // Garbage
            + realProposalValueBufferSize()  // Emission buffer for real
                                             // proposals
        > size) {
      LOGGER_ERROR(logger, "Buffer is not big enough.");
      abort();
    }

    // Init polling buffers
    for (auto const &[id, _] : proc_indices) {
      initPolledConsensus(id);
    }
  }

  inline Consensus *casReadPtr(ProcId proc) const {
    return reinterpret_cast<Consensus *>(buffer + casReadOffset(proc));
  }

  inline Consensus casRead(ProcId proc) const { return *casReadPtr(proc); }

  inline void *garbagePtr() const {
    return reinterpret_cast<void *>(buffer + garbageOffset());
  }

  inline Consensus *polledConsensusPtr(ProcId proc) const {
    return reinterpret_cast<Consensus *>(buffer + polledConsensusOffset(proc));
  }

  inline Consensus polledConsensus(ProcId proc) const {
    return *polledConsensusPtr(proc);
  }

  inline void initPolledConsensus(ProcId proc) {
    *polledConsensusPtr(proc) = Consensus();
  }

  inline DecoratedValue *realProposalValuePtr(Instance i) const {
    return reinterpret_cast<DecoratedValue *>(buffer +
                                              realProposalValueOffset(i));
  }

  inline DecoratedValue realProposalValue(Instance i) const {
    return *realProposalValuePtr(i);
  }

  inline void setRealProposalValue(Instance i, DecoratedValue const &dv) const {
    *realProposalValuePtr(i) = dv;
  }

 private:
  inline constexpr uptrdiff_t casReadOffset(ProcId proc) const {
    return sizeof(Consensus) * proc_indices.at(proc);
  }

  inline constexpr uptrdiff_t polledConsensusOffset(ProcId proc) const {
    return sizeof(Consensus) * (num_nodes + proc_indices.at(proc));
  }

  inline constexpr size_t garbageOffset() const {
    return sizeof(Consensus) * (2 * num_nodes);
  }

  inline constexpr size_t realProposalValueBufferSize() {
    if constexpr (ProposalValueInlined) {
      return 0;
    } else {
      return slots * sizeof(DecoratedValue);
    }
  }

  /**
   * @brief Offset to a real proposal value for a given proc, for a given
   * instance.
   *
   * @param proc
   * @param i
   * @return uptrdiff_t
   */
  inline constexpr uptrdiff_t realProposalValueOffset(Instance i) const {
    // The buffer will be right after the polled consensus one.
    auto const before = sizeof(Consensus) * (2 * num_nodes + 1);
    return before + sizeof(DecoratedValue) * (i % slots);
  }

  std::map<ProcId, size_t> const proc_indices;
  uintptr_t const buffer;
  uptrdiff_t const mr_offset;
  size_t const num_nodes;
  size_t const slots;

  LOGGER_DECL(logger);
};

}  // namespace dory::paxos::internal

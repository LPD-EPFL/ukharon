#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <map>
#include <vector>

#include <dory/ctrl/block.hpp>
#include <dory/shared/logger.hpp>
#include <dory/shared/types.hpp>

#include "consensus.hpp"
#include "constants.hpp"

namespace dory::paxos::internal {

/**
 * @brief Abstraction responsible for handling the DM layout of an acceptor.
 *
 * @tparam ProposalValueInlined whether we will need to store the real value
 *         in an additional buffer.
 * @tparam DecoratedValue the type returned by the Decorator (so that we know
 *         the memory space it takes).
 */
template <bool ProposalValueInlined, typename DecoratedValue>
class AcceptorBuffer {
 public:
  /**
   * @brief Simple Buffer constructor.
   *
   * @param buffer starts at the beginning of the VM.
   * @param mr_offset starts at the beginning of the MR.
   */
  AcceptorBuffer(std::map<ProcId, size_t> const& proc_indices,
                 uptrdiff_t const mr_offset, size_t const size,
                 size_t const slots)
      : proc_indices{proc_indices},
        mr_offset{mr_offset},
        num_nodes{proc_indices.size()},
        slots{slots},
        LOGGER_INIT(logger, "ACCEPTOR-BUFFER") {
    if (+sizeof(Consensus) * slots           // Each "simultaneous" consensus
            + realProposalValueBufferSize()  // Destination buffer for real
                                             // proposals
        > size) {
      LOGGER_ERROR(logger, "Buffer is not big enough.");
      abort();
    }
  }

  inline constexpr uptrdiff_t instanceMrOffset(Instance i) const {
    return mr_offset + instanceOffset(i);
  }

  /**
   * @brief Inits a range of consensus slots.
   *
   * @param first
   * @param o_last
   */
  virtual void initConsensus(Instance first,
                             std::optional<Instance> o_last) = 0;

  inline constexpr uptrdiff_t realProposalValueMrOffset(ProcId proc,
                                                        Instance i) const {
    return mr_offset + realProposalValueOffset(proc_indices.at(proc), i);
  }

  virtual inline void setRealProposalValue(ProcId proc, Instance i,
                                           DecoratedValue const& dv) = 0;

  virtual inline DecoratedValue realProposalValue(ProposalValue proc_index,
                                                  Instance i) const = 0;

 protected:
  inline constexpr uptrdiff_t instanceOffset(Instance i) const {
    return sizeof(Consensus) * (i % slots);
  }

  /**
   * @brief Computes the left padding required for the real proposal value
   *        buffer should be cache-aligned.
   *
   * @return size_t
   */
  inline constexpr size_t rpvbLeftPadding() const {
    // The buffer will be right after the polled consensus one.
    auto const before = sizeof(Consensus) * slots;
    // As `buffer` is already aligned, we just have to right-pad `before`.
    return internal::CacheLineSize - (before % internal::CacheLineSize);
  }

  inline constexpr size_t realProposalValueBufferSize() {
    if constexpr (ProposalValueInlined) {
      return 0;
    } else {
      return rpvbLeftPadding() + num_nodes * slots * sizeof(DecoratedValue);
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
  inline constexpr uptrdiff_t realProposalValueOffset(ProposalValue proc_index,
                                                      Instance i) const {
    // The buffer will be right after the polled consensus one.
    auto const before = sizeof(Consensus) * slots;
    auto const start = before + rpvbLeftPadding();
    return start + (sizeof(DecoratedValue) * num_nodes) * (i % slots) +
           sizeof(DecoratedValue) * proc_index;
  }

  std::map<ProcId, size_t> const proc_indices;
  uptrdiff_t const mr_offset;
  size_t const num_nodes;
  size_t const slots;

  LOGGER_DECL(logger);
};

#ifdef DORY_PAXOS_DM
template <bool ProposalValueInlined, typename DecoratedValue>
class DmAcceptorBuffer
    : public AcceptorBuffer<ProposalValueInlined, DecoratedValue> {
  using AB = AcceptorBuffer<ProposalValueInlined, DecoratedValue>;

 public:
  using BufferDescriptor = typename ctrl::ControlBlock::DeviceMemory;
  DmAcceptorBuffer(std::map<ProcId, size_t> const& proc_indices,
                   BufferDescriptor dm, uptrdiff_t const mr_offset,
                   size_t const size, size_t const slots)
      : AB{proc_indices, mr_offset, size, slots},
        dm{std::move(dm)},
        init_buffer(slots) {
    this->initConsensus(0, slots - 1);
  }

  inline void setRealProposalValue(ProcId proc, Instance i,
                                   DecoratedValue const& dv) override {
    dm.copyTo(this->realProposalValueMrOffset(this->proc_indices.at(proc), i),
              dv);
  }

  inline DecoratedValue realProposalValue(ProposalValue proc_index,
                                          Instance i) const override {
    DecoratedValue rpv;
    dm.copyFrom(this->realProposalValueMrOffset(proc_index, i), rpv);
    return rpv;
  }

  void initConsensus(Instance first, std::optional<Instance> o_last) override {
    Instance const last = o_last.value_or(first);
    assert(last >= first);
    std::optional<Instance> last_inited;
    while (!last_inited || *last_inited < last) {
      Instance sub_first = last_inited ? *last_inited + 1 : first;
      Instance sub_last = sub_first;
      while (sub_last < last && (sub_last + 1) % this->slots != 0) {
        sub_last++;
      }
      size_t const nb = 1 + sub_last - sub_first;
      for (size_t i = 0; i < nb; i++) {
        init_buffer[i] = Consensus(sub_first + i);
      }
      dm.copyTo(this->instanceMrOffset(sub_first), &init_buffer[0],
                nb * sizeof(Consensus));
      last_inited = sub_last;
    }
  }

  Consensus instance(Instance const i) {
    Consensus consensus;
    dm.copyFrom(this->instanceMrOffset(i), consensus);
    return consensus;
  }

 private:
  ctrl::ControlBlock::DeviceMemory dm;
  std::vector<Consensus> init_buffer;
};
#endif

template <bool ProposalValueInlined, typename DecoratedValue>
class MmAcceptorBuffer
    : public AcceptorBuffer<ProposalValueInlined, DecoratedValue> {
  using AB = AcceptorBuffer<ProposalValueInlined, DecoratedValue>;

 public:
  using BufferDescriptor = uintptr_t;

  MmAcceptorBuffer(std::map<ProcId, size_t> const& proc_indices,
                   BufferDescriptor const buffer, uptrdiff_t const mr_offset,
                   size_t const size, size_t const slots)
      : AB{proc_indices, mr_offset, size, slots}, buffer{buffer} {
    this->initConsensus(0, slots - 1);
  }

  inline void setRealProposalValue(ProcId proc, Instance i,
                                   DecoratedValue const& dv) override {
    *reinterpret_cast<DecoratedValue*>(
        buffer +
        this->realProposalValueOffset(this->proc_indices.at(proc), i)) = dv;
  }

  inline DecoratedValue realProposalValue(ProposalValue proc_index,
                                          Instance i) const override {
    return *reinterpret_cast<DecoratedValue*>(
        buffer + this->realProposalValueOffset(proc_index, i));
  }

  void initConsensus(Instance first, std::optional<Instance> o_last) override {
    Instance const last = o_last.value_or(first);
    for (Instance i = first; i < last + 1; i++) {
      *reinterpret_cast<Consensus*>(buffer + this->instanceOffset(i)) =
          Consensus(i);
    }
  }

  Consensus instance(Instance const i) {
    return *reinterpret_cast<Consensus*>(buffer + this->instanceOffset(i));
  }

 private:
  uintptr_t const buffer;
};

}  // namespace dory::paxos::internal

#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>

#include "constants.hpp"
#include "types.hpp"

namespace dory::paxos::internal {

/**
 * @brief Static class to decorate/validate/un-decorate proposal values so
 *        that they can be stored over multiple cache lines.
 *
 * As non-inlined proposal values will reuse slots, they have to be
 * decorated with the instance for which they have been written.
 * In addition, because they can span over multiple cache lines, they
 * must be decorated at the beginning of each cache line.
 */
template <typename RealProposalValue>
class RealProposalValueDecorator {
  static const size_t DataPerLine = CacheLineSize - sizeof(Instance);
  static const size_t CacheLines =
      (sizeof(RealProposalValue) - 1) / DataPerLine + 1;

 public:
  static const size_t DecoratedSize = CacheLineSize * CacheLines;

  struct Decorated {
    uint8_t bytes[DecoratedSize];
  };
  static_assert(sizeof(Decorated) == DecoratedSize);
  static_assert(sizeof(Decorated) % CacheLines == 0);

  /**
   * @brief Decorate a proposed value by inserting instance numbers at the
   *        beginning of each cached line.
   *
   * Note: this cannot be made constexpr because of memcpy.
   *
   * @param rpv the proposed real value
   * @param i the instance on which the value is proposed
   * @return Decorated
   */
  static Decorated decorate(RealProposalValue const &rpv, Instance i) {
    Decorated drpv = {};
    // For each cache line, we will write the instance number and the actual
    // data.
    // note: too big for a for-loop.
    auto const *rpv_p = reinterpret_cast<uint8_t const *>(&rpv);
    uint8_t *drpv_p = drpv.bytes;
    size_t remaining = sizeof(rpv);
    while (remaining > 0) {
      // Instance number
      *reinterpret_cast<Instance *>(drpv_p) = i;
      drpv_p += sizeof(Instance);

      // Actual data
      auto to_copy = std::min(remaining, +DataPerLine);
      std::memcpy(drpv_p, rpv_p, to_copy);

      rpv_p += DataPerLine;
      drpv_p += DataPerLine;
      remaining -= to_copy;
    }

    return drpv;
  }

  constexpr static bool validate(Decorated const &drpv, Instance i) {
    uint8_t const *drpv_p = drpv.bytes;
    for (size_t l = 0; l < CacheLines; l++) {
      if (*reinterpret_cast<Instance const *>(drpv_p) != i) {
        return false;
      }
      drpv_p += CacheLineSize;
    }
    return true;
  }

  static RealProposalValue undecorate(Decorated const &drpv) {
    RealProposalValue rpv;
    auto *rpv_p = reinterpret_cast<uint8_t *>(&rpv);
    // Skipping instance number
    uint8_t const *drpv_p = drpv.bytes + sizeof(Instance);
    size_t remaining = sizeof(rpv);
    while (remaining > 0) {
      auto to_copy = std::min(remaining, +DataPerLine);
      std::memcpy(rpv_p, drpv_p, to_copy);

      rpv_p += DataPerLine;
      drpv_p += CacheLineSize;
      remaining -= to_copy;
    }

    return rpv;
  }
};

// // TEST
// {
// using RPVD = RealProposalValueDecorator<uint64_t>;
// paxos::Instance constexpr instance = 1;
// uint64_t constexpr rpv = 5u;
// auto const decorated = RPVD::decorate(rpv, instance);
// auto const validated = RPVD::validate(decorated, instance);
// assert(validated);
// auto const initial = RPVD::undecorate(decorated);
// assert(initial == rpv);
// }
// // TEST

}  // namespace dory::paxos::internal

#pragma once

#include <atomic>
#include <cstdint>
#include <stdexcept>

#ifdef DORY_TIDIER_ON
#include "../../../default-config.hpp"
#endif

namespace dory::membership::internal {

class SimpleHeartbeat {
 public:
  using Type = uint64_t;

  // Used to avoid false sharing in the heartbeat mechanism
  struct Cacheline {
    static size_t constexpr Length = 64;

    union {
      Type *location;
      uint8_t dummy__[Length];
    };
  };

  static_assert(
      sizeof(Cacheline) == Cacheline::Length,
      "The Cacheline data structure is not exactly a cache-line long");

  SimpleHeartbeat(Cacheline const &hb_location)
      : location{static_cast<Type volatile *>(hb_location.location)} {
    if (reinterpret_cast<uintptr_t>(location) % Cacheline::Length != 0) {
      throw std::runtime_error(
          "The provided heartbeat location is not cache-line aligned!");
    }
  }

  void tick() { *location += 1; }

 private:
  uint64_t volatile *location;
};
}  // namespace dory::membership::internal

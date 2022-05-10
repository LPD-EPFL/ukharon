#pragma once

#include <cstddef>

namespace dory::paxos::internal {

static const size_t CacheLineSize = 64;

// Maximum number of queued RefPoints.
// Note: The real number can be a bit higher because of ReaderWritterQueue,
//       but the actual depth if set to 3 is indeed 3.
static const size_t RefPointsDepth = 3;

}  // namespace dory::paxos::internal

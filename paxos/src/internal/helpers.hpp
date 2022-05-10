#pragma once

#include <memory>
#include <vector>

#include <dory/conn/rc.hpp>

#include "types.hpp"

namespace dory::paxos::internal {

/**
 * @brief Helper function to get ids out of a shared RC map.
 *
 * @param rcs
 * @return std::vector<dory::paxos::internal::ProcId>
 */
static std::vector<ProcId> ids(
    const std::shared_ptr<std::map<ProcId, conn::ReliableConnection>>& rcs) {
  std::vector<ProcId> procs(rcs->size());
  for (auto& [proc, _] : *rcs) {
    procs.push_back(proc);
  }
  return procs;
}

/**
 * @brief Helper function to get indices out of a vector.
 *
 * @param rcs
 * @return std::map<dory::paxos::internal::ProcId, size_t>
 */
static std::map<dory::paxos::internal::ProcId, size_t> indices(
    std::vector<ProcId> const& id_v) {
  std::map<dory::paxos::internal::ProcId, size_t> indices_m;
  for (auto id : id_v) {
    indices_m.emplace(id, indices_m.size());
  }
  return indices_m;
}

}  // namespace dory::paxos::internal

#pragma once

#include <iostream>
#include <map>
#include <set>

#include <dory/conn/rc.hpp>
#include <dory/third-party/sync/spsc.hpp>

#include "internal/consensus.hpp"
#include "internal/constants.hpp"
#include "internal/message-kind.hpp"
#include "internal/types.hpp"

namespace dory::paxos {
/**
 * @brief A client that checks remote consensus states.
 *
 */
class MajorityReader {
  using ReqKind = internal::ReqKind;
  using ProcId = internal::ProcId;
  using RefPoint = internal::RefPoint;
  using Packer = conn::Packer<ReqKind, ProcId, RefPoint>;

 public:
  using PolledObj = internal::Consensus;
  using ResolvedRefPointsQueue = third_party::sync::SpscQueue<
      std::pair<internal::RefPoint, internal::Instance>>;

  MajorityReader(size_t slots, deleted_unique_ptr<struct ibv_cq>& send_cq)
      : slots{slots},
        send_cq{send_cq},
        LOGGER_INIT(logger, "MAJORITY-READER") {}

  void addAcceptor(ProcId id, conn::ReliableConnection&& poller,
                   uintptr_t slots_offset, internal::Consensus* local_obj);

  std::optional<RefPoint> insertRefPoint();

  void tick();

  ResolvedRefPointsQueue& resolvedRefPointsQueue() {
    return resolved_ref_points;
  }

 private:
  struct Acceptor {
    Acceptor(conn::ReliableConnection&& poller, uintptr_t slots_offset,
             internal::Consensus* local_obj)
        : poller{std::move(poller)},
          slots_offset{slots_offset},
          local_obj{local_obj} {}

    conn::ReliableConnection poller;
    uintptr_t slots_offset;

    internal::Consensus* local_obj;
    std::optional<RefPoint> latest_resolved_ref_point;
    std::optional<internal::Instance> latest_checked_instance;
    bool onflight = false;

    inline constexpr uptrdiff_t instanceMrOffset(internal::Instance i) const {
      return slots_offset +
             static_cast<uptrdiff_t>(sizeof(internal::Consensus) * i);
    }
  };

  // Paxos
  size_t slots;
  deleted_unique_ptr<struct ibv_cq>& send_cq;
  std::map<ProcId, Acceptor> acceptors;
  std::vector<struct ibv_wc> wce;

  // Ref points
  std::atomic<RefPoint> next_ref_point = 0;
  ResolvedRefPointsQueue resolved_ref_points;
  third_party::sync::SpscQueue<RefPoint> unresolved_ref_points{
      internal::RefPointsDepth};
  std::optional<internal::Instance> highest_accepted;

  std::set<ProcId> failed;
  LOGGER_DECL(logger);
};
}  // namespace dory::paxos

#include "majority-reader.hpp"

#include <dory/ctrl/block.hpp>

namespace dory::paxos {
void MajorityReader::addAcceptor(ProcId id, conn::ReliableConnection &&poller,
                                 uintptr_t slots_offset,
                                 internal::Consensus *local_obj) {
  acceptors.try_emplace(id, std::move(poller), slots_offset, local_obj);
  wce.reserve(acceptors.size());
}

std::optional<internal::RefPoint> MajorityReader::insertRefPoint() {
  RefPoint const ref_point = next_ref_point.load(std::memory_order_relaxed);
  bool const enqueued = unresolved_ref_points.try_enqueue(ref_point);
  if (enqueued) {
    next_ref_point++;
    if (next_ref_point.load(std::memory_order_relaxed) == 0) {
      throw std::runtime_error("Ref point overflowed on insertion");
    }
    return ref_point;
  }
  LOGGER_WARN(logger, "Couldn't enqueue ref point.");

  return {};
}

void MajorityReader::tick() {
  RefPoint const nrp = next_ref_point.load(std::memory_order_relaxed);
  // If no rp has been inserted, no point checking anything.
  if (nrp == 0) {
    return;
  }
  RefPoint const rp_to_issue = nrp - 1;
  auto slot_to_check = static_cast<internal::Instance>(
      highest_accepted ? *highest_accepted + 1 : 0);
  if (slot_to_check > slots) {
    std::runtime_error("Reached last slot");
  }

  for (auto &[id, acceptor] : acceptors) {
    // We skip acceptors for which we failed to send
    if (failed.find(id) != failed.end()) {
      continue;
    }
    // We only issue 1 RDMA read at once for each acceptor.
    if (acceptor.onflight) {
      continue;
    }

    auto ok = acceptor.poller.postSendSingle(
        dory::conn::ReliableConnection::RdmaRead,
        Packer::pack(ReqKind::Poll, id, rp_to_issue), acceptor.local_obj,
        sizeof(internal::Consensus),
        acceptor.poller.remoteBuf() + acceptor.instanceMrOffset(slot_to_check));

    if (!ok) {
      throw std::runtime_error("Failed to post!");
    }
    acceptor.onflight = true;
  }
  wce.resize(acceptors.size());

  if (!dory::ctrl::ControlBlock::pollCqIsOk(send_cq, wce)) {
    throw std::runtime_error("Error polling Cq!");
  }

  // If we didn't poll anything, nothing will be discovered.
  if (wce.empty()) {
    return;
  }

  for (auto &e : wce) {
    auto const [_, proc, ref_point] = Packer::unpackAll(e.wr_id);

    if (e.status != IBV_WC_SUCCESS) {
      LOGGER_WARN(logger, "WR to {} failed ({}), won't poll it again.", proc,
                  e.status);
      failed.insert(proc);
      continue;
    }

    auto acceptor_it = acceptors.find(static_cast<ProcId>(proc));
    if (acceptor_it == acceptors.end()) {
      throw std::runtime_error("Acceptor " + std::to_string(+proc) +
                               " not found.");
    }
    auto &acceptor = acceptor_it->second;
    acceptor.onflight = false;

    if (acceptor.local_obj->accepted()) {
      internal::Instance const ri = acceptor.local_obj->data.instance;
      if (!highest_accepted || *highest_accepted < ri) {
        highest_accepted = ri;
      }
      // std::cout << "Instance " << acceptor.local_obj->data.instance << "
      // accepted at " << proc << std::endl;
    } else {
      // WC FIFO ordering should make this check useless
      if (!acceptor.latest_resolved_ref_point ||
          acceptor.latest_resolved_ref_point < ref_point) {
        acceptor.latest_resolved_ref_point = ref_point;
      }
    }
  }

  auto majority = (acceptors.size() + 1) / 2;
  while (RefPoint *ref_point_p = unresolved_ref_points.peek()) {
    auto ref_point = *ref_point_p;
    size_t above_ref_point = 0;
    for (auto &[_, acceptor] : acceptors) {
      if (acceptor.latest_resolved_ref_point &&
          *acceptor.latest_resolved_ref_point >= ref_point) {
        above_ref_point++;
      }
    }
    if (above_ref_point >= majority) {
      // If the value was only accepted at less than a majority for sure,
      // we could optimize this.
      // See old paxos.hpp.

      // We proved that working_on couldn't have been decided before
      // the first enqueued RefPoint.
      auto lowest_surely_undecided = static_cast<internal::Instance>(
          highest_accepted ? *highest_accepted + 1 : 0);
      resolved_ref_points.enqueue({ref_point, lowest_surely_undecided});
      if (!unresolved_ref_points.pop()) {
        throw std::runtime_error("Couldn't pop.");
      }

      // Let's try to prove it again with the next RefPoint.
      continue;
    }
    // We couldn't prove that highest_accepted + 1 wasn't decided, we'll retry
    // after next polling.
    break;
  }
}
}  // namespace dory::paxos

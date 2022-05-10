#pragma once

#include "types.hpp"

namespace dory::paxos {

/**
 * @brief Abstraction capable of handling reference points events.
 *
 * Our Paxos abstraction exposes one more event compared to a regular consensus
 * abstraction. In that sense, this is a "leaky" consensus abstraction.
 * The other exposed event is the first potentially unreached consensus before a
 * given logical reference point in time.
 *
 * A RefPoint is created by calling Paxos::insertRefPoint. Eventually, the
 * abstraction will yield an event about the first instance on which consensus
 * could not have been reached when insertRefPoint was called.
 * Of course, this could be satisfied by yielding +infinity. A good abstraction
 * will provide very little overestimation.
 *
 * By checking the consensus state in a majority of nodes, we are able to see
 * if one has some accepted proposal. In that case, a consensus could
 * potentially have been reached. In none within a majority has heard of an
 * accepted proposal, then no consensus could have potentially been reached.
 *
 * Because we wait for a majority of polling requests emitted after
 * Paxos::addRefPoint to complete, by causality, we know that the yield value
 * will be correct when the reference point was inserted.
 *
 * This abstraction can be used to build the bubble tag system as an array of
 * tags can locally be associated to a reference point.
 */
class RefPointHandler {
 public:
  virtual void handle(RefPoint, Instance) = 0;
  virtual ~RefPointHandler() = default;
};

/**
 * @brief Abstraction yielding when an instance is accepted.
 *        This doesn't guarantee that the instance has been decided, but as
 *        the previous one has necessarily been decided, it can be used to
 *        measure latency/failover.
 */
class AcceptedHandler {
 public:
  virtual void handle(Instance) = 0;
  virtual ~AcceptedHandler() = default;
};

}  // namespace dory::paxos

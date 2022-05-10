#pragma once

#include <stdexcept>
#include <string>
#include <utility>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <deque>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <set>
#include <thread>
#include <unordered_map>
#include <vector>

#ifndef NDEBUG
#define PRINT_CAS_FAILURES
#endif
#define PRINT_CAS_FAILURES

#ifdef PRINT_CAS_FAILURES
#include <iomanip>
#include <iostream>
#endif

#include <dory/conn/contexted-poller.hpp>
#include <dory/conn/message-identifier.hpp>
#include <dory/conn/rc.hpp>
#include <dory/shared/logger.hpp>
#include <dory/shared/types.hpp>
#include <dory/third-party/sync/spsc.hpp>

#include <dory/extern/ibverbs.hpp>

#include "internal/acceptor-buffer.hpp"
#include "internal/consensus.hpp"
#include "internal/constants.hpp"
#include "internal/decorator.hpp"
#include "internal/handlers.hpp"
#include "internal/helpers.hpp"
#include "internal/message-kind.hpp"
#include "internal/proposer-buffer.hpp"
#include "internal/types.hpp"

#include "handlers.hpp"
#include "types.hpp"

/**
 * Todo:
 * - Let the consensus coordinator notify the clients on success.
 * - Test with more machines.
 * - Make it a bit more generic.
 */

namespace dory::paxos {

/**
 * @brief An RDMA-optimized version of Paxos for very low contention and minimal
 *        latency. It can run multiple instances and achieves GC by reusing old
 *        decided values slots.
 *
 * Uses atomics to not involve the remote CPU (until decision notification).
 * Pseudo-code: https://gist.github.com/SamvitJ/00e84cec539e19419ec3fe503341d746
 * If one can predict the remote values, then everything can be checked with a
 * single CAS. As we expect the contention to be very low, we should be able to
 * predict remote values almost all the time.
 * In our case, we don't care about old decided values and can overwrite them.
 * In the case of SMR, we should provide a mechanism to sync to the almost
 * latest version of the SM as we cannot assume old decided values will be
 * accessible. This is out of the scope for now.
 */
template <typename RealProposalValue,
          template <bool, typename> class AbT = MmAcceptorBuffer>
class Paxos {
  /**
   * Whether proposals can be inlined within the consensus state.
   *
   * Note: When a value cannot be inlined, significantly more memory has to be
   *       allocated to store the proposals and another RDMA request has to be
   *       issued for storing the proposal.
   */
  constexpr static bool ProposalValueInlined =
      sizeof(internal::ProposalValue) == sizeof(RealProposalValue);

  // We alias internal types for better readability.
  using ProcId = internal::ProcId;
  using ReqId = internal::ReqId;
  using Proposal = internal::Proposal;
  using ProposalValue = internal::ProposalValue;
  using Consensus = internal::Consensus;

  using ReqKind = internal::ReqKind;
  using Packer = conn::Packer<ReqKind, ProcId, ReqId>;
  using ContextedPoller = conn::ContextedPoller<Packer>;
  using PollerManager = conn::PollerManager<Packer>;

  using RPVD = internal::RealProposalValueDecorator<RealProposalValue>;
  using DecoratedValue = typename RPVD::Decorated;
  using AcceptorBuffer = AbT<ProposalValueInlined, DecoratedValue>;
  using ProposerBuffer =
      internal::ProposerBuffer<ProposalValueInlined, DecoratedValue>;

  class InstanceState {
   public:
    InstanceState(Paxos<RealProposalValue, AbT> const &paxos, Instance instance)
        : proposal{static_cast<Proposal>(paxos.indexOf(paxos.my_id) + 1)},
          wcs(paxos.rcs->size()) {
      // Initially (unprepared), everyone's memory should be
      // zeroes except the instance number.
      for (auto const &[id, _] : *paxos.rcs) {
        predicted.emplace(id, instance);
      }
    }

    bool isPrepared() { return prepared_for && *prepared_for == proposal; }
    void setPrepared() { prepared_for = proposal; }
    void setUnprepared() { prepared_for = {}; }

    Proposal proposal;
    std::optional<Proposal> prepared_for;
    std::map<ProcId, Consensus> predicted;
    std::vector<struct ibv_wc> wcs;
  };

 public:
  // We expose some specialized internal types that should be accessed by the
  // end user but only through this type.
  using Handler = internal::Handler<RealProposalValue>;

  /**
   * @brief Constructs a new Paxos multi-usage object.
   *
   * The number of simultaneous instances is almost equivalent to the burst of
   * the abstraction.
   *
   * @param buffer_start aligned RNIC-registered buffer of memory
   * @param buffer_offset from the start of the MR
   * @param buffer_size in bytes
   * @param rcs shared remote connections
   * @param handler shared_ptr to the deliver handler
   * @param slots the number of instances to keep in memory
   */
  Paxos(  // View
      ProcId const my_id,
      std::shared_ptr<std::map<ProcId, conn::ReliableConnection>> rcs_arg,
      // Acceptor Buffer
      typename AcceptorBuffer::BufferDescriptor acceptor_buffer_descriptor,
      uptrdiff_t const acceptor_buffer_offset,
      size_t const acceptor_buffer_size,
      // Proposer Buffer
      uintptr_t proposer_buffer_start, uptrdiff_t const proposer_buffer_offset,
      size_t const proposer_buffer_size,
      // Handlers
      std::shared_ptr<Handler> handler,
      // misc.
      deleted_unique_ptr<struct ibv_cq> &cq, size_t const slots = 2,
      std::optional<size_t> const recycle_every = {},
      bool const prepare_in_accept_slack = false)
      : my_id{my_id},
        proc_indices{internal::indices(internal::ids(rcs_arg))},
        acceptor_buffer{proc_indices, acceptor_buffer_descriptor,
                        acceptor_buffer_offset, acceptor_buffer_size, slots},
        proposer_buffer{proc_indices, proposer_buffer_start,
                        proposer_buffer_offset, proposer_buffer_size, slots},
        rcs{std::move(rcs_arg)},
        handler{std::move(handler)},
        slots{slots},
        prepare_in_accept_slack{prepare_in_accept_slack},
        poller_manager{cq},
        majority{rcs->size() / 2 + 1},
        tolerated_failures{rcs->size() - majority},
        latest_inited{static_cast<Instance>(slots - 1)},
        last_to_init{latest_inited},
        recycle_every{recycle_every.value_or(slots)},
        LOGGER_INIT(logger, "PAXOS") {
    assert(slots >= 2);
    // Although we do not signal those writes, errors will still produce
    // WCE. Thus, we have to register the context.
    poller_manager.registerContext(ReqKind::WriteRealValue);
    poller_manager.registerContext(ReqKind::Prepare);
    poller_manager.registerContext(ReqKind::Accept);
    poller_manager.registerContext(ReqKind::Poll);
    poller_manager.endRegistrations(4);
    prepare_poller.emplace(poller_manager.getPoller(ReqKind::Prepare));
    accept_poller.emplace(poller_manager.getPoller(ReqKind::Accept));
    poll_poller.emplace(poller_manager.getPoller(ReqKind::Poll));

    decided.reserve(1000000);

    LOGGER_INFO(logger, "Inlined: {}", ProposalValueInlined);

    // Init outstanding poll request counter.
    for (auto &[proc, _] : *rcs) {
      outstanding_polls.emplace(proc, 0);
    }
  }

  // We prevent copy and only allow some move semantics.
  Paxos &operator=(Paxos const &) = delete;
  Paxos &operator=(Paxos &&) = delete;
  Paxos(Paxos const &) = delete;
  Paxos(Paxos &&) = default;  // NOLINT as adding noexcept breaks clang-6

  /**
   * @brief Proposes a value. Returns once a value has been decided.
   *
   * @param instance on which to propose
   * @param pv the proposed value
   * @return whether the value was decided
   */
  bool propose(Instance instance, RealProposalValue const &rpv) {
    // If the real proposal value can be inlined,
    ProposalValue pv;
    if constexpr (ProposalValueInlined) {
      pv = *reinterpret_cast<ProposalValue *>(&rpv);
    }
    // Otherwise, we have to push it to everyone's memory and we will decide
    // on the sender.
    else {
      pv = static_cast<ProposalValue>(indexOf(my_id));
      auto const dv = RPVD::decorate(rpv, instance);
      // Used to send to acceptors
      proposer_buffer.setRealProposalValue(instance, dv);
      // Used to remove undecoration
      {
        std::lock_guard<std::mutex> const guard(decided_mutex);
        proposed.emplace(instance, rpv);
      }
      for (auto &[id, rc] : *rcs) {
        if (id == my_id) {
          continue;
        }
        if (failed.find(id) != failed.end()) {
          continue;
        }
        bool send_success = rc.postSendSingle(
            conn::ReliableConnection::RdmaWrite,
            Packer::pack(ReqKind::WriteRealValue, id, 0),
            proposer_buffer.realProposalValuePtr(instance),
            sizeof(DecoratedValue),
            rc.remoteBuf() +
                acceptor_buffer.realProposalValueMrOffset(my_id, instance),
            false);
        if (!send_success) {
          throw std::runtime_error("Send error writing real value.");
        }
      }
      // Not theoretically required as the value could be recovered from PB.
      acceptor_buffer.setRealProposalValue(my_id, instance, dv);
    }
    size_t trials = 0;

    // We will only return if a value has been decided for this round.
    while (nextInstance() <= instance) {
      // If there are concurrent proposals, we sleep a bit and update our
      // proposal number.
      constexpr bool BackoffEnabled = false;
      if (BackoffEnabled && trials > 0) {
        sleepExponentialBackoff(trials);
        // Maybe a value has been decided while we were sleeping.
        if (nextInstance() > instance) {
          return false;
        }
      }

      std::lock_guard<std::recursive_mutex> const guard(instance_states_mutex);
      auto &is =
          instance_states.try_emplace(instance, *this, instance).first->second;

      if (is.isPrepared() || prepare(instance, is)) {
        if (accept(instance, is, pv)) {
          return true;
        }
      }
      trials++;
    }
  }

  /**
   * @brief Moves the abstraction forward. Can be called within an endless loop
   *        in its own thread or within a light thread.
   *
   * Brings the abstraction from a coherent state to a new coherent state.
   * Polls remote hosts to detect that a new consensus was reached.
   */
  inline void followerTick() {
    pollFailures();
    auto const working_on = nextInstance();
    auto const instance_slot = working_on % slots;
    // 1. <Potentially todo> poll recv with immediate for decided value.
    // 2. once in a while, check if a consensus has been reached without waiting
    //    for a recv wce as the coordinator could have crashed
    // In burst mode, we may send extra outstanding requests so that we don't
    // have to wait for a new slot.
    bool burst_mode = burst > 0;
    if (burst_mode) {
      --burst;
    }
    for (auto &[id, rc] : *rcs) {
      if (failed.find(id) != failed.end()) {
        continue;
      }
      if (id == my_id) {
        continue;
      }
      if (!burst_mode && outstanding_polls[id] >= 1) {
        // If we already have many outstanding requests to this node, we will
        // not send yet another one.
        continue;
      }
      bool send_success = rc.postSendSingle(
          conn::ReliableConnection::RdmaRead,
          Packer::pack(ReqKind::Poll, id, 0),
          proposer_buffer.polledConsensusPtr(id), sizeof(Consensus),
          rc.remoteBuf() + acceptor_buffer.instanceMrOffset(working_on));
      outstanding_polls[id]++;
      if (!send_success) {
        throw std::runtime_error(
            "Send error while polling for decided values.");
      }
    }
    initConsensusSlots(recycle_every);

    // 3. poll previous reqs.
    // By polling a small number of WCE, we do not wait forever.
    // Indeed, because we use a contexted poller, it won't stop until:
    //  - it polled all values from the QC
    //  - or it polled the requested number of WCE
    // because the WCQ is shared, we could potentially wait forever polling over
    // entries. Regardless of how many requests were sent, we know that we have
    // at least one outstanding request for each node.
    // We still have interferences. We could use another Cq.
    ticker_wc.resize(rcs->size());
    if (!(*poll_poller)(ticker_wc)) {
      throw std::runtime_error("Error while polling for poll wc.");
    }

    // shortcut
    if (ticker_wc.empty() &&
        acceptor_buffer.instance(working_on).data.accepted_proposal == 0) {
      return;
    }

    for (auto const &wce : ticker_wc) {
      auto const proc = Packer::unpackPid(wce.wr_id);
      outstanding_polls[proc]--;
      // todo: use failure handling abstraction
      if (wce.status != IBV_WC_SUCCESS) {
        LOGGER_WARN(logger, "reading from {} failed", std::to_string(proc));
        signalFailure(proc);
      }
    }

    // 4. check for a majority of reads with the same accepted_proposal number
    //    (and thus same accepted_value). If none is found but an instance field
    //    is greater than the one we aimed for, skip the current one (we will
    //    have a hole, but we cannot do any better from reading remote memory
    //    anyway).
    Instance highest_instance = 0;
    std::map<std::pair<Proposal, ProposalValue>, size_t> seen;
    for (auto const &[id, _] : *rcs) {
      auto const pc = id == my_id ? acceptor_buffer.instance(working_on)
                                  : proposer_buffer.polledConsensus(id);
      if (pc.data.instance % slots != instance_slot) {
        // The slot contained data from previous polling and is dismissed.
        continue;
      }
      std::pair<Proposal, ProposalValue> const pp = {pc.data.accepted_proposal,
                                                     pc.data.accepted_value};
      if (pc.data.instance > highest_instance) {
        highest_instance = pc.data.instance;
      }
      if (pc.data.accepted_proposal == 0) {
        // We only consider nodes having accepted some proposal.
        continue;
      }
      if (pc.data.instance == working_on) {
        seen.emplace(pp, 0);  // Init counter to 0 if not seen before.
        seen[pp]++;
      }
    }
    size_t max_seen = 0;
    for (auto const &[pp, counter] : seen) {
      if (counter >= majority) {
        decide(working_on, pp.second);
        // We shouldn't wait too much before polling the next instance.
        burst = 1;
        return;
      }
      max_seen = counter > max_seen ? counter : max_seen;
    }
    // We won't be able to get the consensus output this way, let's close the
    // instance and work on the next one.
    if (highest_instance > working_on) {
      closeInstance(working_on);
      // We shouldn't wait too much before polling the next instance.
      burst = 1;
      return;
    }
  }

  std::optional<RealProposalValue> decidedAtInstance(Instance i) {
    std::lock_guard<std::mutex> const guard(decided_mutex);
    auto const decided_iter = decided.find(i);
    if (decided_iter != decided.end()) {
      return decided_iter->second;
    }
    return {};
  }

  /**
   * @brief Returns the next instance for which we are not aware a consensus has
   *        been reached on (and should thus be available with high probability
   *        under low contention).
   *
   * @return Instance
   */
  Instance nextInstance() {
    std::lock_guard<std::mutex> const guard(next_instance_mutex);
    return next_instance;
  }

  /**
   * @brief Skips all instances that have been decided but not locally as it
   *        would require us to read from a majority.
   *        Speeds up leader change.
   */
  void skipLocallyUndecided() {
    Instance last_undecided = nextInstance();
    while (acceptor_buffer.instance(last_undecided + 1).accepted()) {
      last_undecided++;
    }
    if (last_undecided >= 1) {
      closeInstance(last_undecided - 1);
    }
  }

  /**
   * @brief Optimistically adopt the local values as predictions for the current
   *        (likely decided) and the next (likely prepared) slots.
   *
   * @param instance
   */
  void speedupLeaderChange(Instance instance) {
    std::lock_guard<std::recursive_mutex> const guard(instance_states_mutex);

    Consensus const local_current = acceptor_buffer.instance(instance);
    auto &current_is =
        instance_states.try_emplace(instance, *this, instance).first->second;
    for (auto &[_, ps] : current_is.predicted) {
      ps = local_current;
    }

    Consensus const local_next = acceptor_buffer.instance(instance + 1);
    auto &next_is =
        instance_states.try_emplace(instance + 1, *this, instance + 1)
            .first->second;
    for (auto &[_, ps] : next_is.predicted) {
      ps = local_next;
    }
  }

  /**
   * @brief Signals that a process failed so that no more requests are
   *        sent to it.
   */
  void signalFailure(ProcId proc) { signaled_failures.enqueue(proc); }

  /**
   * @brief Tries to prepare remote consensus slots.
   *
   * @param instance to prepare
   */
  bool prepare(Instance instance) {
    std::lock_guard<std::recursive_mutex> const guard(instance_states_mutex);
    auto &is =
        instance_states.try_emplace(instance, *this, instance).first->second;
    return prepare(instance, is);
  }

  /**
   * @brief Exports the mr offset of decision slots to access them from clients.
   *
   * @return uintptr_t
   */
  uintptr_t slotsMrOffset() const {
    return acceptor_buffer.instanceMrOffset(0);
  }

 private:
  /**
   * @brief Prepares a slot.
   *
   * If prepare is run in a non-blocking, it is optimistically assumed to
   * complete successfully at each replica.
   *
   * @param instance the instance to prepare.
   * @param is local knowledge about it.
   *
   * @tparam Blocking whether we should wait for completion.
   */
  template <bool Blocking = true>
  bool prepare(Instance const instance, InstanceState &is) {
    // Let's update our proposal number. Not required on the first try.
    for (auto const &[_, ps] : is.predicted) {
      while (is.proposal < ps.data.min_proposal) {
        is.proposal = static_cast<Proposal>(is.proposal + rcs->size());
      }
    }

    std::map<ProcId, Consensus> sent_prepared;

    // Id used to identify WCE related to this <instance, proposal>.
    ReqId req_id = next_req_id++;

    // If some acceptor was already prepared before, we skip it.
    // This is safe as it would later fail in the acceptor phase if it had been
    // updated in the meantime.
    // This can happen if we run the prepare phase twice because of unrecycled
    // logs.
    std::set<ProcId> skipped;

    // 1. post atomic prepare
    for (auto &[id, rc] : *rcs) {
      if (failed.find(id) != failed.end()) {
        continue;
      }
      Consensus predicted = is.predicted[id];
      if (predicted.data.min_proposal == is.proposal) {
        skipped.insert(id);
        continue;
      }
      Consensus prepared = predicted;
      prepared.data.min_proposal = is.proposal;
      sent_prepared.emplace(id, prepared);

      bool send_success = rc.postSendSingleCas(
          Packer::pack(ReqKind::Prepare, id, req_id),
          Blocking ? proposer_buffer.casReadPtr(id)
                   : proposer_buffer.garbagePtr(),
          rc.remoteBuf() + acceptor_buffer.instanceMrOffset(instance),
          predicted.packed, prepared.packed, Blocking);
      if (!send_success) {
        throw std::runtime_error("Send error.");
      }
      if constexpr (!Blocking) {
        is.predicted[id] = prepared;
      }
    }
    if constexpr (!Blocking) {
      is.setPrepared();
      return true;
    }

    // 2. wait for a majority to succeed, otherwise update predictions and
    // start over
    // We use a map instead of a set to prealloc.
    std::map<ProcId, bool> updated_predicted;
    for (auto const &[id, _] : *rcs) {
      updated_predicted[id] = false;
    }
    size_t acks = skipped.size();
    size_t nacks = 0;
    auto &wc = is.wcs;
    // We assume an eventually complete FD. Otherwise, use majority.
    while (acks < majority && (acks + nacks < rcs->size() - failed.size())) {
      wc.resize(rcs->size());
      if (!(*prepare_poller)(wc)) {
        throw std::runtime_error("Polling error.");
      }
      for (auto const &wce : wc) {
        // Let's check whether the fetched data matches the expected
        auto const [kind, id, req] = Packer::unpackAll(wce.wr_id);
        if (req != req_id) {
          continue;
        }
        // todo: use failure detector abstraction to handle those cases
        if (wce.status != IBV_WC_SUCCESS) {
          LOGGER_WARN(logger, "{} failed", id);
          signalFailure(id);
          continue;
        }
        auto const read = proposer_buffer.casRead(id);
        if (read == is.predicted[id]) {
          acks++;
        } else {
#ifdef PRINT_CAS_FAILURES
          std::cout << "PREPARE, instance " << instance << ": ";
          std::cout << id << " CAS read didn't match, updating prediction"
                    << std::endl;
          std::cout << "predicted: " << is.predicted[id] << std::endl;
          std::cout << "received: " << read << std::endl;
#endif
          nacks++;
          // Our prediction was wrong. We update our prediction so that it
          // will hopefully match next time.
          // WARN: We only update our predicted state if its instance id
          //       matches the one we are working on! (Otherwise we could
          //       write to nodes not ready for the next consensus.)
          updated_predicted[id] = true;
          if (read.data.instance == instance) {
            is.predicted[id] = read;
          } else {
            // If the node hadn't inited its log entry, then maybe it will have.
            is.predicted[id] = Consensus(instance);
          }
        }
      }

      pollFailures();
    }

    // We can update our predictions for all nodes that haven't been updated
    // yet.
    // Analysis:
    // 1. Best case: no concurrent proposal, all our predictions were right
    //    and they will all move to the value we sent them.
    // 2. Ok case: concurrent proposal but we still win, they will likely
    //    adopt our value.
    // 3. Bad case: We "failed" but some have still adopted our value, so we
    //    might as well "predict" they will have it even if they don't.
    // 4. Worst case: We failed and they haven't adopted our value, either
    //    we could imagine they went for the other value we've seen or that
    //    they went with yet another value. As we don't know, it is still
    //    safe to predict ours but their could be an optimization.
    for (auto const &[id, _] : *rcs) {
      if (!updated_predicted[id] && skipped.find(id) == skipped.end()) {
        is.predicted[id] = sent_prepared[id];
      }
    }

    // Maybe they already switched to the next instance.
    // If so, we may never be able to propose, so let's move to the next
    // instance.
    // todo: try decide before closing the instance?
    for (auto const &[id, _] : *rcs) {
      auto const cr = proposer_buffer.casRead(id);
      if (cr.data.instance % slots != instance % slots) {
        // The slot contained data from a CAS on a previous slot and is
        // dismissed. We could remove this check by only reading after WCE.
        continue;
      }
      if (cr.data.instance > instance) {
        closeInstance(instance);
        return false;
      }
    }

    if (acks < majority) {
#ifndef NDEBUG
      std::cout << "too many nacks, let's try again" << std::endl;
#endif
      return false;
    }
    is.setPrepared();
    return true;
  }

  bool accept(Instance const instance, InstanceState &is, ProposalValue pv) {
    // Pre_preparation can only be used once.
    is.setUnprepared();

    adoptProposalValue(pv, is);

    Consensus accepted(instance);
    accepted.data.min_proposal = is.proposal;
    accepted.data.accepted_proposal = is.proposal;
    accepted.data.accepted_value = pv;

    // Id used to identify WCE related to this <instance, proposal>.
    ReqId req_id = next_req_id++;

    std::set<ProcId> skipped;

    size_t nacks = 0;
    // 1. post atomic accept
    for (auto &[id, rc] : *rcs) {
      if (failed.find(id) != failed.end()) {
        continue;
      }
      Consensus predicted = is.predicted[id];
      if (predicted.data.min_proposal != is.proposal) {
        // It's useless to send an accept request to nodes who didn't like
        // the previous one. Still, a majority of them succeeded if we
        // reached this point.
        nacks++;
        continue;
      }

      if (predicted == accepted) {
        skipped.insert(id);
        continue;
      }

      bool send_success = rc.postSendSingleCas(
          Packer::pack(ReqKind::Accept, id, req_id),
          proposer_buffer.casReadPtr(id),
          rc.remoteBuf() + acceptor_buffer.instanceMrOffset(instance),
          predicted.packed, accepted.packed);
      if (!send_success) {
        throw std::runtime_error("Send error.");
      }
    }

    // 1.5 We have some slack to do operations off the fast-path
    // Let's initialise a consensus slot if there's a queued one.
    if (recycle_every < slots) {  // Only if recycling is possible
      initConsensusSlots();
    }

    // Let's poll new failures signaled by the FD.
    pollFailures();
    // Let's async prepare next slot.
    if (prepare_in_accept_slack) {
      auto const next_instance = instance + 1;
      auto &next_is =
          instance_states.try_emplace(next_instance, *this, next_instance)
              .first->second;
      if (!next_is.isPrepared()) {
        prepare<false>(next_instance, next_is);
      }
    }
    // We use a map instead of a set to prealloc.
    std::map<ProcId, bool> updated_predicted;
    for (auto const &[id, _] : *rcs) {
      updated_predicted[id] = false;
    }

    // 2. wait for a majority to succeed, otherwise update predictions and
    // start over
    size_t acks = skipped.size();
    bool rejected = false;
    auto &wc = is.wcs;
    // We assume an eventually complete FD. Otherwise, use majority.
    while (acks < majority && (acks + nacks < rcs->size() - failed.size())) {
      wc.resize(rcs->size());
      if (!(*accept_poller)(wc)) {
        throw std::runtime_error("Polling error.");
      }
      for (auto const &wce : wc) {
        // Let's check whether the fetched data matches the predicted
        auto const [kind, id, req] = Packer::unpackAll(wce.wr_id);
        if (req != req_id) {
          continue;
        }
        // todo: use failure detector abstraction to handle those cases
        if (wce.status != IBV_WC_SUCCESS) {
          LOGGER_WARN(logger, "{} failed", id);
          signalFailure(id);
          continue;
        }
        updated_predicted[id] = true;
        auto const read = proposer_buffer.casRead(id);
        if (read == is.predicted[id]) {
          acks++;
        } else {
#ifdef PRINT_CAS_FAILURES
          std::cout << "ACCEPT, instance " << instance << ": ";
          std::cout << id << " CAS read didn't match, updating prediction"
                    << std::endl;
          std::cout << "predicted: " << is.predicted[id] << std::endl;
          std::cout << "received: " << read << std::endl;
#endif
          nacks++;
          // Our prediction was wrong. We update our prediction so that it
          // will hopefully match next time.
          // WARN: We only update our predicted state if its instance id
          //       matches the one we are working on! (Otherwise we could
          //       write to nodes not ready for the next consensus.)
          if (read.data.instance == instance) {
            is.predicted[id] = read;
          } else {
            is.predicted[id] = Consensus(instance);
          }
          // If min_proposal > proposal: we got rejected
          // But I think getting a single rejection should not be a problem.
          // It is only one if you only wait for a majority.
          // For now, better safe than sorry.
          // if (read.data.min_proposal > is.proposal) {
          //   rejected = true;
          // }
        }
      }

      pollFailures();
    }

    for (auto const &[id, _] : *rcs) {
      if (!updated_predicted[id] && skipped.find(id) == skipped.end()) {
        is.predicted[id] = accepted;
      }
    }

    if (acks < majority) {  // || rejected) {
#ifndef NDEBUG
      std::cout << "Accept got too many nacks:" << nacks << " nacks vs " << acks
                << "acks" << std::endl;
#endif
      return false;
    }

    // The value has been decided.
    // We could send it to the nodes via RPC.
    // But in our case, it's simpler to let everyone poll from a majority
    // (as this path has to be implemented anyway).
    decide(instance, pv);
    return true;
  }

  void decide(Instance instance, ProposalValue decided_value) {
    std::lock_guard<std::mutex> const guard(decided_mutex);
    if (decided.find(instance) != decided.end()) {
      return;
    }
    closeInstance(instance);
    // We have to translate the ProposalValue to a RealProposalValue.
    RealProposalValue real_decided_value;
    if constexpr (ProposalValueInlined) {
      real_decided_value =
          *reinterpret_cast<RealProposalValue *>(&decided_value);
    } else {
      auto const i_proposed = indexOf(my_id) == decided_value;
      if (i_proposed) {
        real_decided_value = proposed.find(instance)->second;
      } else {
        size_t trials = 0;
        DecoratedValue decorated_decided_value =
            acceptor_buffer.realProposalValue(decided_value, instance);
        while (!RPVD::validate(decorated_decided_value, instance)) {
          trials++;
          decorated_decided_value =
              acceptor_buffer.realProposalValue(decided_value, instance);
          if (trials > 1000000) {
            LOGGER_ERROR(
                logger,
                "The locally stored values didn't contain the correct one");
            // TODO: spin lock + fetch from other acceptors until validated.
            throw std::runtime_error("unimplemented");
            return;
          }
        }
        real_decided_value = RPVD::undecorate(decorated_decided_value);
      }
    }
    LOGGER_DEBUG(logger, "New value decided for instance {}.", instance);
    decided.emplace(instance, real_decided_value);
    handler->handle(instance, real_decided_value);
  }

  void adoptProposalValue(ProposalValue &pv, InstanceState const &is) {
    // Replace our proposal value with the accepted value
    // with highest proposal
    std::optional<std::pair<Proposal, ProposalValue>> hap;
    for (auto const &[_, ps] : is.predicted) {
      if (ps.accepted()) {
        if (!hap || hap->first < ps.data.accepted_proposal) {
          hap = {ps.data.accepted_proposal, ps.data.accepted_value};
        }
      }
    }
    if (hap) {
#ifndef NDEBUG
      std::cout << "Adopting value of highest proposal." << std::endl;
#endif
      pv = hap->second;
    }
  }

  size_t indexOf(ProcId proc) const { return proc_indices.at(proc); }

  void sleepExponentialBackoff(size_t trials) {
    auto const exp =
        static_cast<size_t>(pow(1.4, static_cast<double>(trials + 1)) * 0.4);
    auto const rj = jitter(mt_engine);
    std::this_thread::sleep_for(std::chrono::microseconds(exp + rj));
  }

  void closeInstance(Instance instance) {
    {
      std::lock_guard<std::mutex> const guard(next_instance_mutex);
      if (instance >= next_instance) {
        next_instance = static_cast<Instance>(instance + 1);
      }
      // Let's recycle the slot. We make it so that a buffer of Lag elements is
      // always accessible. In a proper SMR, we should use checkpointing.
      size_t const lag = slots / 2;
      if (instance >= lag) {
        auto const to_init = static_cast<Instance>(instance + slots - lag);
        // We don't want to waste any time on that now.
        if (to_init > last_to_init) {
          last_to_init = to_init;
        }
      }
    }
    {
      // Let's clear the state as it will not be used anymore
      // Note: This may not clear all instances, but this is safe.
      //       We only clear an instance if nobody is using it anymore.
      std::unique_lock<std::recursive_mutex> const lock(instance_states_mutex,
                                                        std::try_to_lock);
      if (lock) {
        instance_states.erase(instance);
      }
    }
  }

  /**
   * @brief Bulk-initializes the consensus state in DM.
   *
   * @param min batch size
   */
  void initConsensusSlots(size_t min = 1) {
    std::unique_lock<std::mutex> const lock(consensus_init_mutex,
                                            std::try_to_lock);
    if (!lock) {
      return;
    }
    Instance const last_to_init_approx = last_to_init;
    if (min == 0 || min > last_to_init_approx - latest_inited) {
      return;
    }

    acceptor_buffer.initConsensus(latest_inited + 1, last_to_init_approx);
    latest_inited = last_to_init_approx;
  }

  /**
   * @brief Adds a process to the set of failed processes so that no more
   *        requests are sent to it.
   */
  void pollFailures() {
    ProcId new_failure;
    while (signaled_failures.try_dequeue(new_failure)) {
      failed.insert(new_failure);
      if (failed.size() > tolerated_failures) {
        throw std::runtime_error("Too many failures.");
      }
    }
  }

  std::map<ProcId, size_t> const proc_indices;
  AcceptorBuffer acceptor_buffer;
  ProposerBuffer proposer_buffer;
  ProcId const my_id;
  std::shared_ptr<std::map<ProcId, conn::ReliableConnection>> const rcs;
  std::shared_ptr<Handler> handler;
  size_t const slots;
  bool const prepare_in_accept_slack;

  PollerManager poller_manager;
  DelayedRef<ContextedPoller> prepare_poller;
  DelayedRef<ContextedPoller> accept_poller;
  DelayedRef<ContextedPoller> poll_poller;

  std::mt19937 mt_engine{std::random_device()()};
  std::uniform_int_distribution<> jitter{0, 60};  // exponential backoff jitter

  std::mutex next_instance_mutex;
  Instance next_instance = 0;
  ReqId next_req_id = 0;

  std::mutex decided_mutex;
  std::unordered_map<Instance, RealProposalValue> proposed;
  std::unordered_map<Instance, RealProposalValue> decided;

  // Follower
  std::vector<struct ibv_wc> ticker_wc;
  std::map<ProcId, size_t> outstanding_polls;
  size_t burst = 0;

  size_t const majority;
  size_t const tolerated_failures;
  std::set<ProcId> failed;
  third_party::sync::SpscQueue<ProcId> signaled_failures{1};

  std::recursive_mutex instance_states_mutex;
  std::map<Instance, InstanceState> instance_states;

  std::mutex consensus_init_mutex;
  Instance latest_inited;
  std::atomic<Instance> last_to_init;

  size_t const recycle_every;

  LOGGER_DECL(logger);
};

}  // namespace dory::paxos

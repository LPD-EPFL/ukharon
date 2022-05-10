#pragma once

#include <iostream>

#include <atomic>
#include <chrono>
#include <mutex>
#include <stdexcept>
#include <thread>

#ifdef DORY_TIDIER_ON
#include "../../../default-config.hpp"
#endif

#include <dory/conn/rc.hpp>
#include <dory/conn/ud.hpp>

#include "../failure-broadcaster.hpp"

namespace dory::membership::internal {
template <typename ProcIdType, typename HeartbeatCounter>
class HeartbeatPoller {
 public:
  HeartbeatPoller(dory::conn::ReliableConnection &rc,
                  HeartbeatCounter *write_to,
                  NotifierQueue<ProcIdType> &notifier,
                  dory::conn::McGroup &broadcast_group)
      : rc{&rc},
        write_to{write_to},
        notifier{notifier},
        broadcast_group{broadcast_group} {
    // Important observation:
    // In order to not corrupt the heartbeat (i.e. data from the past are
    // written to write_to), the class has to be stopped, then reconfigure the
    // connection and then start it again.
  }

  ~HeartbeatPoller() { stop(); }

  bool start(ProcIdType proc_id, uintptr_t read_from) {
    std::unique_lock lock(mtx);

    if (started) {
      return false;
    }

    // TODO (fix):
    // During process startup, initial delays (which we have to identify)
    // delay the heartbeat counter, thus if we start the poller very early
    // we risk considering dead from the beginning
    warmup_iter = 1000;
    this->proc_id = proc_id;
    this->read_from = read_from;
    should_poll = false;
    started = true;
    latest_known_hb.reset();
    last_polled.reset();

    return true;
  }

  bool stop() {
    std::unique_lock lock(mtx);
    if (!started) {
      return false;
    }

    started = false;

    return true;
  }

  void tick() {
    std::unique_lock lock(mtx);
    if (!started) {
      return;
    }

    if (warmup_iter > 0) {
      warmup_iter--;
      return;
    }

    if (should_poll) {
      size_t to_poll = 1;
      bool failed = false;

      // Check if read retuns a value
      wce.resize(to_poll);

      if (!rc->pollCqIsOk(dory::conn::ReliableConnection::SendCq, wce)) {
        throw std::runtime_error("Error polling Cq!");
      }

      do {
        if (wce.empty()) {
          break;
        }

        auto &e = wce[0];

        if (e.wr_id != proc_id) {
          // Ignore old requests with different WRID
          // std::cout << "Ignore old requests with different WRID " <<
          // std::endl;
          break;
        }

        if (e.status != IBV_WC_SUCCESS) {
          // std::cout << "Reporting as failed cause e.status was: " <<
          // +e.status << std::endl;
          failed = true;
          break;
        }

        if (latest_known_hb && *write_to == latest_known_hb.value()) {
          // std::cout << "Reporting as failed cause didn't write since: " <<
          // *latest_known_hb << std::endl;
          failed = true;
          break;
        }

        latest_known_hb = *write_to;
        should_poll = false;
        last_polled = std::chrono::steady_clock::now();
      } while (false);

      if (std::chrono::steady_clock::now() > posted_at + HbTimeout) {
        failed = true;
      }

      if (failed) {
        // std::cout << "rebroadcasting the failure of " << +proc_id <<
        // std::endl;
        broadcastFailure(proc_id);

        if (!notifier.enqueue(proc_id)) {
          throw std::runtime_error(
              "Enqueuing to re-broadcaster failed due to allocation error");
        }

        started = false;
      }
    }

    if (!should_poll) {
      posted_at = std::chrono::steady_clock::now();
      if (!last_polled || posted_at > *last_polled + HbRereadTimeout) {
        auto ok = rc->postSendSingle(dory::conn::ReliableConnection::RdmaRead,
                                     proc_id, write_to,
                                     sizeof(HeartbeatCounter), read_from);

        if (!ok) {
          throw std::runtime_error("Failed to post!");
        }

        should_poll = true;
      }
    }

    if (outstanding_broadcasts > 0) {
      wce.resize(outstanding_broadcasts);
      broadcast_group.ud()->pollCqIsOk<dory::conn::UnreliableDatagram::SendCQ>(
          wce);
      for (auto const &e : wce) {
        outstanding_broadcasts--;
        if (e.status != IBV_WC_SUCCESS) {
          throw std::runtime_error("HbPoller: Broadcast failed.");
        }
      }
    }
  }

  void broadcastFailure(ProcIdType failure) {
    broadcast_group.postSend(0, nullptr, 0, failure);
    outstanding_broadcasts++;
  }

 private:
  dory::conn::ReliableConnection *rc;
  HeartbeatCounter *write_to;
  NotifierQueue<ProcIdType> &notifier;
  dory::conn::McGroup &broadcast_group;
  std::atomic<size_t> outstanding_broadcasts = 0;

  std::optional<HeartbeatCounter> latest_known_hb;
  bool should_poll = false;
  std::chrono::steady_clock::time_point posted_at;
  std::optional<std::chrono::steady_clock::time_point> last_polled;
  int warmup_iter;

  bool started = false;
  ProcIdType proc_id;
  uintptr_t read_from;

  std::vector<struct ibv_wc> wce;
  std::mutex mtx;
};
}  // namespace dory::membership::internal

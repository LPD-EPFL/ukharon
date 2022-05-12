#pragma once

#include <dory/shared/logger.hpp>
#include <dory/shared/unused-suppressor.hpp>

#include <dory/crash-consensus.hpp>

#include "../connection.hpp"
#include "../common.hpp"
#include "../mica.hpp"

struct FreeableMica {
  FreeableMica(int instance_id, uint8_t* log_buffer) {
    mica_init(&kv, instance_id, TEST_NUM_BKTS, TEST_LOG_CAP, log_buffer);

    free_handle = dory::deleted_unique_ptr<struct mica_kv>(
        &kv, [](struct mica_kv *kv) { mica_free(kv); });
  }

  struct mica_kv kv;

 private:
  dory::deleted_unique_ptr<struct mica_kv> free_handle;
};

class MasterSlave {
public:
  struct SlaveInfo {
    SlaveInfo(ProcIdType id, uintptr_t remote_addr)
      : id{id}, remote_addr{remote_addr} {}

    ProcIdType id;
    uintptr_t remote_addr;
  };

  MasterSlave(ProcIdType id, dory::ctrl::ControlBlock &cb) : id{id}, cb{cb}, LOGGER_INIT(logger, "MasterSlave") {
    rc.emplace(cb);
    rc->bindToPd("primary-pd");
    rc->bindToMr("primary-mr");
    rc->associateWithCq("cq", "cq");
    rc->init(dory::ctrl::ControlBlock::LOCAL_READ |
            dory::ctrl::ControlBlock::LOCAL_WRITE |
            dory::ctrl::ControlBlock::REMOTE_READ |
            dory::ctrl::ControlBlock::REMOTE_WRITE);

    ud = std::make_shared<dory::conn::UnreliableDatagram>(
        cb, "primary-pd", "primary-mr", "cq", "cq");
  }

  bool connectTo(ProcIdType connect_to, RpcSlot **slave_fip_at) {
    auto [ip, port, kernel] = announcer.processToHost(connect_to);
    dory::ignore(kernel);

    cli.emplace(ip, port);

    cli->connect();
    auto [cli_ok, cli_info] = cli->handshake<std::pair<dory::uptrdiff_t, size_t>>(
        [this, connect_to, slave_fip_at]() -> std::pair<bool, std::string> {
          auto slave_fip_info = reinterpret_cast<uintptr_t>(slave_fip_at);
          std::string serialized_info =
              rc->remoteInfo().serialize() + " " +
              ud->info().serialize() + " " +
              std::to_string(slave_fip_info);

          return std::make_pair(true, serialized_info);
        },
        [this, connect_to](std::string const &info)
            -> std::pair<bool, std::optional<std::pair<dory::uptrdiff_t, size_t>>> {
          std::cout << "Received " << info << " from " << connect_to << std::endl;
          std::istringstream remote_info_stream(info);
          std::string rc_info;
          std::string ud_info;
          dory::uptrdiff_t offset_info;
          size_t len_info;
          remote_info_stream >> rc_info;
          remote_info_stream >> ud_info;
          remote_info_stream >> offset_info;
          remote_info_stream >> len_info;

          auto remote_rc = dory::conn::RemoteConnection::fromStr(rc_info);
          rc->reset();
          rc->reinit();
          rc->connect(remote_rc, connect_to);

          udc = std::make_unique<dory::conn::UnreliableDatagramConnection>(
              cb, "primary-pd", ud, ud_info);

          return std::make_pair(true, std::make_pair(offset_info, len_info));
        },
        id, dory::membership::RpcKind::RDMA_HERD_CONNECTION);

    if (!cli_ok) {
      LOGGER_WARN(logger, "Could not connect to process {}", connect_to);
      return false;
    }

    auto [offset, len] = cli_info.value();
    auto remote_addr = rc->remoteBuf() + offset;

    slave_info.emplace(connect_to, remote_addr);
    slave_circular_buffer_image.emplace(reinterpret_cast<void *>(remote_addr), len, false);
    *slave_fip_at = slave_circular_buffer_image->firstNew().value();
    this->slave_fip_at = slave_fip_at;

    return true;
  }

  bool replicate(RpcSlot &slot) {
    auto *slave_fip = *reinterpret_cast<RpcSlot * volatile *>(slave_fip_at);

    while (auto fip = slave_circular_buffer_image->firstInProcess()) {
      if (fip.value() == slave_fip) {
        break;
      }

      slave_circular_buffer_image->advanceInProcess();
    }

    auto fn = slave_circular_buffer_image->firstNew();
    if (!fn) {
      return false;
    }

    auto *slot_addr = fn.value();
    auto ok = rc->postSendSingle(dory::conn::ReliableConnection::RdmaWrite,
                                  reinterpret_cast<uint64_t>(&slot),
                                  &slot.op,
                                  sizeof(slot.op),
                                  reinterpret_cast<uintptr_t>(slot_addr) + offsetof(RpcSlot, op));

    if (!ok) {
      throw std::runtime_error("Failed to post RDMA Write for replication to the slave");
    }

    slave_circular_buffer_image->advanceNew();

    return true;
  }

private:
  ProcIdType id;
  dory::ctrl::ControlBlock &cb;

  dory::Delayed<dory::conn::ReliableConnection> rc;
  std::shared_ptr<dory::conn::UnreliableDatagram> ud;
  std::unique_ptr<dory::conn::UnreliableDatagramConnection> udc;
  dory::Delayed<dory::rpc::conn::UniversalConnectionRpcClient<ProcIdType, dory::membership::RpcKind::Kind>> cli;

  dory::Delayed<CircularBuffer<RpcSlot>> slave_circular_buffer_image;
  RpcSlot **slave_fip_at;

  std::optional<SlaveInfo> slave_info;
  dory::memstore::ProcessAnnouncer announcer;

  LOGGER_DECL(logger);
};

class Service {
public:
  Service(ProcIdType id,
  dory::ctrl::ControlBlock &cb,
  dory::deleted_unique_ptr<struct ibv_cq> &cq,
  FreeableMica &mica,
  dory::Consensus &consensus,
  bool skip_proposal) : id{id}, cb{cb}, cq{cq}, mica{mica}, consensus{consensus}, skip_proposal{skip_proposal} {
    auto &m = this->mica;
    consensus.commitHandler([&m]([[maybe_unused]] bool leader,
                              [[maybe_unused]] uint8_t* buf,
                              [[maybe_unused]] size_t len) {
                                if (!leader) {
                                  struct mica_resp resp;
                                  auto *op = reinterpret_cast<struct mica_op *>(buf);

                                  // mica_print_op(op);
                                  mica_single_op(&m.kv, op, &resp);
                                }
                              });

    std::cout << "Wait some time to make the consensus engine ready" << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(5));
  }

  template <typename T>
  void leaderTick(std::vector<T> *connection_list) {
    // Poll for completions
    wce.resize(outstanding_responses);
    if (!dory::ctrl::ControlBlock::pollCqIsOk(cq, wce)) {
      throw std::runtime_error("Error polling Cq!");
    }

    for (auto &e : wce) {
      if (e.status != IBV_WC_SUCCESS) {
        throw std::runtime_error("RDMA polling error for slot");
      }

      outstanding_responses -= 1;

      // When I switch from slave to leader, because I use the same
      // completion queue, I may receive WC from when I was writing my
      // slave_fip to the master. These writes always have as WRID = 0
      // We skip these when we become a leader.
      if (e.wr_id == 0) {
        continue;
      }

      auto &slot = *reinterpret_cast<RpcSlot *>(e.wr_id);

      auto &circular_buffer = *slot.circular_buffer;

      if (is_replicating(slot)) {
        arm_slot(slot);
        make_ready_to_send(slot);
      }

      if (is_final(slot)) {
        make_new(slot);

        while (auto fip = circular_buffer.firstInProcess()) {
          if (!is_new(*fip.value())) {
            break;
          }

          circular_buffer.advanceInProcess();
        }
      }
    }

    // auto [active, changed] = local_active_cache.isActiveOrViewChanged(view_id);

    // if (changed || !active) {
    //   return;
    // }

    for (auto &[proc_id, c] : *connection_list) {
      if (*c.active) {
        auto &circular_buffer = *c.circular_buffer;

        while (auto fn = circular_buffer.firstNew()) {
          auto &slot = *fn.value();

          if (slot_written(slot)) {
            if (is_new(slot)) {
              // std::cout << "Proposing ";
              // mica_print_op(&slot.op);

              auto *payload = reinterpret_cast<uint8_t *>(&slot.op);

              // Encode process doing the proposal
              dory::ProposeError err;
	      if (skip_proposal) {
                err = dory::ProposeError::NoError;
              } else {
                err = consensus.propose(payload, sizeof(slot.op));
              }

              if (err != dory::ProposeError::NoError) {
                // std::cout << "FAILED" << std::endl;

                switch (err) {
                  case dory::ProposeError::FastPath:
                  case dory::ProposeError::FastPathRecyclingTriggered:
                  case dory::ProposeError::SlowPathCatchFUO:
                  case dory::ProposeError::SlowPathUpdateFollowers:
                  case dory::ProposeError::SlowPathCatchProposal:
                  case dory::ProposeError::SlowPathUpdateProposal:
                  case dory::ProposeError::SlowPathReadRemoteLogs:
                  case dory::ProposeError::SlowPathWriteAdoptedValue:
                  case dory::ProposeError::SlowPathWriteNewValue:
                    std::cout << "Error: in leader mode. Code: "
                              << static_cast<int>(err) << std::endl;
                    break;

                  case dory::ProposeError::SlowPathLogRecycled:
                    std::cout << "Log recycled, waiting a bit..." << std::endl;
                    std::this_thread::sleep_for(std::chrono::seconds(1));
                    break;

                  case dory::ProposeError::MutexUnavailable:
                  case dory::ProposeError::FollowerMode:
                    // std::cout << "Error: in follower mode. Potential leader: "
                    //           << consensus.potentialLeader() << std::endl;
                    make_failed(slot);
                    break;

                  default:
                    std::cout << "Bug in code. You should only handle errors here"
                              << std::endl;
                }
              } else {
                // No error: I am leader and replicated successfully

                // std::cout << "Storing to Mica" << std::endl;
                mica_single_op(&mica.kv, &slot.op, &slot.resp);
              }

              // Reply to client (notifying for success or failure)
              arm_slot(slot);
              make_ready_to_send(slot);
              circular_buffer.advanceNew();

              // std::cout << "Advancing new" << std::endl;
            } else {
              throw std::runtime_error("Should be unreachable");
            }
          } else {
            break;
          }
        }

        while (auto fip = circular_buffer.firstInProcess()) {
          auto &slot = *fip.value();
          if (is_ready_to_send(slot)) {
            if (outstanding_responses > OutstandingResponsesMax) {
              break;
            }

            // std::cout << "Posting" << std::endl;
            auto ok = c.ud->postSend(reinterpret_cast<uintptr_t>(&slot),
                        slot.resp.val_ptr, slot.resp.val_len,
                        (static_cast<uint32_t>(slot.failed) << 31) |
                        static_cast<uint32_t>(circular_buffer.toIdx(&slot)));
            if (!ok) {
              throw std::runtime_error("RDMA UD error");
            }

            outstanding_responses += 1;
            make_final(slot);
            circular_buffer.advanceInProcess();
            // std::cout << "Advancing inProcess" << std::endl;
          } else {
            break;
          }
        }

      }
    } // for: execute requests and start replication
  }

private:
  ProcIdType id;
  dory::ctrl::ControlBlock &cb;
  dory::deleted_unique_ptr<struct ibv_cq> &cq;
  FreeableMica &mica;
  dory::Consensus &consensus;
  bool skip_proposal;

  std::vector<struct ibv_wc> wce;

  size_t outstanding_responses = 0;

  static size_t constexpr OutstandingResponsesMax = 127;
};

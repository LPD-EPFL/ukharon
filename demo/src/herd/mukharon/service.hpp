#pragma once

#include <dory/shared/logger.hpp>
#include <dory/shared/unused-suppressor.hpp>

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
  ProcIdType master_id,
  std::optional<ProcIdType> slave_id,
  RpcSlot **slave_fip_at,
  dory::membership::LocalActiveCache &local_active_cache
) : id{id}, cb{cb}, cq{cq},
    master_slave{id, cb}, mica{mica},
    master_id{master_id}, slave_id{slave_id},
    slave_fip_at{slave_fip_at},
    local_active_cache{local_active_cache} { }

  void forgetSlave() {
    slave_id.reset();
  }

  bool tryConnectToSlave(dory::membership::FullMembership &membership) {
    auto const [includes_me, my_idx] = membership.includes(id);

    if (includes_me) {
      if (!membership.members[my_idx].isApp()) {
        throw std::runtime_error("Master must be an app node");
      }

      auto const [includes_slave, slave_idx] = membership.includes(slave_id.value());
      if (includes_slave) {
        if (!membership.members[slave_idx].isApp()) {
          throw std::runtime_error("Slave must be an app node");
        }

        master_slave.connectTo(slave_id.value(), slave_fip_at);
        return true;
      }
    }

    return false;
  }

  template <typename T>
  void leaderTick(ViewIdType view_id, std::vector<T> *connection_list) {
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

    auto [active, changed] = local_active_cache.isActiveOrViewChanged(view_id);

    if (changed || !active) {
      return;
    }

    for (auto &[proc_id, c] : *connection_list) {
      if (*c.active) {
        auto &circular_buffer = *c.circular_buffer;

        while (auto fn = circular_buffer.firstNew()) {
          auto &slot = *fn.value();

          if (slot_written(slot)) {
            if (is_new(slot)) {
              if (is_get(slot)) {
                // mica_print_op(&slot.op);
                mica_single_op(&mica.kv, &slot.op, &slot.resp);

                arm_slot(slot);
                make_ready_to_send(slot);
                circular_buffer.advanceNew();
              } else if (is_put(slot)) {
                if (slave_id) {
                  if (master_slave.replicate(slot)) {
                    outstanding_responses += 1;
                    // mica_print_op(&slot.op);
                    mica_single_op(&mica.kv, &slot.op, &slot.resp);
                    make_replicating(slot);
                    circular_buffer.advanceNew();
                  } else {
                    break;
                  }
                } else {
                  // mica_print_op(&slot.op);
                  mica_single_op(&mica.kv, &slot.op, &slot.resp);

                  arm_slot(slot);
                  make_ready_to_send(slot);
                  circular_buffer.advanceNew();
                }
              } else {
                throw std::runtime_error("Should be unreachable");
              }
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

            auto ok = c.ud->postSend(reinterpret_cast<uintptr_t>(&slot),
                        slot.resp.val_ptr, slot.resp.val_len, static_cast<uint32_t>(circular_buffer.toIdx(&slot)));
            if (!ok) {
              throw std::runtime_error("RDMA UD error");
            }

            outstanding_responses += 1;
            make_final(slot);
            circular_buffer.advanceInProcess();
          } else {
            break;
          }
        }

      }
    } // for: execute requests and start replication
  }

  template <typename T>
  void slaveTick(std::vector<T> *connection_list) {
    for (auto &[proc_id, c] : *connection_list) {
      if (proc_id != master_id) {
        continue;
      }

      if (!*c.active) {
        continue;
      }

      auto &circular_buffer = *c.circular_buffer;

      wce.resize(outstanding_responses);
      if (!dory::ctrl::ControlBlock::pollCqIsOk(cq, wce)) {
        throw std::runtime_error("Error polling Cq!");
      }

      for (auto &e : wce) {
        outstanding_responses -= 1;

        if (e.status != IBV_WC_SUCCESS) {
          throw std::runtime_error("RDMA RC error from slave!");
        }
      }

      if (outstanding_responses > OutstandingResponsesMax) {
        std::cout << "Slave ran out of RC outstanding requests" << std::endl;
        break;
      }

      for (int i = 0; i < 32; i++) {
        if (auto fn = circular_buffer.firstNew()) {
          auto &slot = *fn.value();

          if (slot_written(slot)) {
            if (is_new(slot)) {
              if (is_put(slot)) {
                // mica_print_op(&slot.op);
                mica_single_op(&mica.kv, &slot.op, &slot.resp);

                arm_slot(slot);
                make_new(slot);
                circular_buffer.advanceNew();
                circular_buffer.advanceInProcess();

                *slave_fip_at = circular_buffer.firstNew().value();

              } else {
                throw std::runtime_error("Should be unreachable");
              }
            } else {
              throw std::runtime_error("Should be unreachable");
            }
          }
        }
      }

      auto ok = c.rc->postSendSingle(dory::conn::ReliableConnection::RdmaWrite,
              0,  // wr_id not used
              slave_fip_at, sizeof(*slave_fip_at), reinterpret_cast<uintptr_t>(c.slave_fip_at));
      if (!ok) {
        throw std::runtime_error("RDMA UD error");
      }

      outstanding_responses ++;
    } // for: execute requests and start replication
  }

  template <typename T>
  void depleteMasterBuffer(ViewIdType view_id, std::vector<T> *connection_list, CircularBuffer<RpcSlot> &circular_buffer) {
    for (auto &[proc_id, c] : *connection_list) {
      if (proc_id == master_id) {
        // std::cout << "Making master's connection inactive" << std::endl;
        *c.active = false;
        break;
      }
    }

    // This assumes only a single view change from master to slave
    // If anything else happens, this will break correctness
    while (!local_active_cache.isActiveOrViewChanged(view_id).first) {}

    // std::cout << "Depleting leader's buffer" << std::endl;

    while (auto fn = circular_buffer.firstNew()) {
      auto &slot = *fn.value();

      if (slot_written(slot)) {
        if (is_new(slot)) {
          if (is_put(slot)) {
            // mica_print_op(&slot.op);
            mica_single_op(&mica.kv, &slot.op, &slot.resp);

            circular_buffer.advanceNew();

          } else {
            throw std::runtime_error("Should be unreachable");
          }
        } else {
          throw std::runtime_error("Should be unreachable");
        }
      } else {
        break;
      }
    }

  }

private:
  ProcIdType id;
  dory::ctrl::ControlBlock &cb;
  dory::deleted_unique_ptr<struct ibv_cq> &cq;
  MasterSlave master_slave;
  FreeableMica &mica;
  ProcIdType master_id;
  std::optional<ProcIdType> slave_id;
  RpcSlot **slave_fip_at;

  dory::membership::LocalActiveCache &local_active_cache;
  std::vector<struct ibv_wc> wce;

  size_t outstanding_responses = 0;

  static size_t constexpr OutstandingResponsesMax = 127;
};

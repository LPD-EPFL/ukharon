#pragma once

#include <dory/ctrl/block.hpp>

#include <dory/conn/rc.hpp>
#include <dory/conn/ud.hpp>

#include <dory/conn/manager/uniform-rc.hpp>
#include <dory/conn/manager/uniform-ud.hpp>

#include <dory/shared/logger.hpp>
#include <dory/shared/units.hpp>

#include <dory/memory/pool/pool-allocator.hpp>

#include <dory/rpc/conn/universal-connection.hpp>

#include <dory/membership/default-config.hpp>

#include "common.hpp"
#include "dynamic-connections.hpp"
#include "rpc-slot.hpp"
#include "buffer.hpp"

struct Connection {
  dory::conn::ReliableConnection *rc{nullptr};
  dory::conn::UnreliableDatagramConnection *ud{nullptr};

  // Use a shared pointer, otherwise the value is not preserved when altering
  // connections. Because the DynamicConnections have two ping-pong set of
  // connections, if the data of this struct is not a pointer, during copying
  // (from ping to pong set of connections) the latest value may be lost.
  std::shared_ptr<bool> active{std::make_shared<bool>(true)};

  void *raw_buffer;
  std::shared_ptr<CircularBuffer<RpcSlot>> circular_buffer;

  // This is the address of the remote part of the connection where
  // this process can write the RpcSlot ptr (of the circular buffer)
  // that has last been processed.
  RpcSlot **slave_fip_at;
};

using Handler =
    dory::rpc::conn::UniversalConnectionRpcHandler<ProcIdType, dory::membership::MembershipRpcKind>;

class Manager : public Handler::AbstractManager {
 private:
  using ProcIdType = Handler::AbstractManager::ProcIdType;
  using RcConnMgr = dory::conn::manager::UniformRcConnectionManager<ProcIdType>;
  using UdConnMgr = dory::conn::manager::UniformUdConnectionManager<ProcIdType>;
  using ConnMap = std::unordered_map<ProcIdType, Connection>;
  using ConnIterator = std::unordered_map<ProcIdType, Connection>::iterator;

public:
  static size_t constexpr CircularBufferSlotCnt = 128;
  static size_t constexpr MaxClientCnt = 128;
  static size_t constexpr Alignment = CircularBuffer<RpcSlot>::Alignment;

private:
  static size_t constexpr circ_buf_sz = CircularBuffer<RpcSlot>::estimateSize(CircularBufferSlotCnt);
  using PoolType = CircularBuffer<RpcSlot>::RawBuffer<circ_buf_sz>;

 public:
  Manager(dory::ctrl::ControlBlock &cb, std::optional<ProcIdType> keep_conn = {})
      : cb{cb}, keep_conn{keep_conn}, LOGGER_INIT(logger, "ConnectionManager") {

    LOGGER_INFO(logger, "Need {} bytes for {} RPC slots for each client", circ_buf_sz,
                CircularBufferSlotCnt);

    auto pool_size =
        dory::memory::pool::PoolAllocator<PoolType>::alignedSpaceRequirement(
            Alignment, MaxClientCnt);
    LOGGER_INFO(logger, "Need {} bytes for {} clients", pool_size, MaxClientCnt);

    auto slave_fip_size = std::max(sizeof(RpcSlot *), 2 * Alignment);

    cb.allocateBuffer("primary-buf", pool_size + slave_fip_size, Alignment);
    cb.registerMr("primary-mr", "primary-pd", "primary-buf",
                  dory::ctrl::ControlBlock::LOCAL_READ |
                      dory::ctrl::ControlBlock::LOCAL_WRITE |
                      dory::ctrl::ControlBlock::REMOTE_READ |
                      dory::ctrl::ControlBlock::REMOTE_WRITE);

    pool = std::make_shared<dory::memory::pool::PoolAllocator<PoolType>>(
        reinterpret_cast<void *>(cb.mr("primary-mr").addr), pool_size,
        MaxClientCnt, reinterpret_cast<void *>(cb.mr("primary-mr").addr));

    slave_fip_at = reinterpret_cast<RpcSlot **>(roundUp(cb.mr("primary-mr").addr + pool_size, Alignment));

    rc_manager = std::make_unique<RcConnMgr>(cb);
    rc_manager->usePd("primary-pd");
    rc_manager->useMr("primary-mr");
    rc_manager->useSendCq("cq");
    rc_manager->useRecvCq("cq");
    rc_manager->setNewConnectionRights(dory::ctrl::ControlBlock::LOCAL_READ |
                                       dory::ctrl::ControlBlock::LOCAL_WRITE |
                                       dory::ctrl::ControlBlock::REMOTE_READ |
                                       dory::ctrl::ControlBlock::REMOTE_WRITE);

    ud = std::make_shared<dory::conn::UnreliableDatagram>(
        cb, "primary-pd", "mica-mr", "cq", "cq");
    ud_manager = std::make_unique<UdConnMgr>(cb, ud);
    ud_manager->usePd("primary-pd");
  }

  RpcSlot **slaveWritesFipAtAddr() const {
    return slave_fip_at;
  }

  std::pair<bool, std::string> handleStep1(
      ProcIdType proc_id,
      Handler::AbstractManager::Parser const &parser) override {
    std::istringstream remote_info(parser.connectionInfo());
    std::string rc_info;
    std::string ud_info;
    uintptr_t write_slave_fip_to_info;
    remote_info >> rc_info;
    remote_info >> ud_info;
    remote_info >> write_slave_fip_to_info;

    LOGGER_INFO(logger, "Process {} sent ReliableConnection info: {}", proc_id,
                rc_info);

    LOGGER_INFO(logger, "Process {} sent UnreliableDatagram info: {}", proc_id,
                ud_info);

    LOGGER_INFO(logger, "Process {} say that signaling occurs at address: {}", proc_id,
                write_slave_fip_to_info);

    auto &rc = rc_manager->newConnection(proc_id, rc_info);
    auto rc_info_for_remote = rc.remoteInfo().serialize();

    auto &ud = ud_manager->newConnection(proc_id, ud_info);
    auto ud_info_for_remote = ud_manager->remoteInfo().serialize();

    // Store connection
    Connection conn_data;
    conn_data.rc = &rc;
    conn_data.ud = &ud;
    auto *raw_buffer = pool->create();

    if (raw_buffer == nullptr) {
      // Connection data will be cleared when the client disconnects
      LOGGER_WARN(logger,
                  "Pool allocator is exhausted when trying to allocate for {}",
                  proc_id);
      std::make_pair(false, "");
    }

    conn_data.raw_buffer = raw_buffer;
    conn_data.circular_buffer = std::make_shared<CircularBuffer<RpcSlot>>(raw_buffer, sizeof(PoolType), true);

    if (proc_id == keep_conn) {
      keep_conn_buffer = conn_data.circular_buffer;
    }

    auto offset = pool->offset(raw_buffer);
    auto len = sizeof(PoolType);
    conn_data.slave_fip_at = reinterpret_cast<RpcSlot **>(write_slave_fip_to_info);

    conns.insert({proc_id, conn_data});

    LOGGER_INFO(logger, "Replying to process {}", proc_id);
    std::string local_serialized_info = rc_info_for_remote + " " +
                                        ud_info_for_remote + " " +
                                        std::to_string(offset) + " " +
                                        std::to_string(len);

    return std::make_pair(true, local_serialized_info);
  }

  bool handleStep2(ProcIdType proc_id,
                   Handler::AbstractManager::Parser const &parser) override {
    dory::ignore(proc_id);
    dory::ignore(parser);
    dc.alterConnections(conns.begin(), conns.end());
    return true;
  }

  void remove(ProcIdType proc_id) override {

    rc_manager->removeConnection(proc_id);
    ud_manager->removeConnection(proc_id);
    if (keep_conn != proc_id) {
      pool->destroy(reinterpret_cast<PoolType *>(conns[proc_id].raw_buffer));
    }
    conns.erase(proc_id);
  }

  DynamicConnections<ConnIterator> *connections() { return &dc; }

  std::shared_ptr<CircularBuffer<RpcSlot>> &keepConnBuffer() {
    return keep_conn_buffer;
  }

  std::vector<ProcIdType> collectInactive() override {
    std::vector<ProcIdType> inactive_vec;
    auto *inactive = dc.alterConnections(conns.begin(), conns.end());

    for (auto &[proc_id, c] : *inactive) {
      if (!*c.active) {
        inactive_vec.push_back(proc_id);
      }
    }

    return inactive_vec;
  }

  void markInactive(ProcIdType proc_id) override {
    *conns[proc_id].active = false;
    dc.alterConnections(conns.begin(), conns.end());
  }

 private:
  dory::ctrl::ControlBlock &cb;

  std::unique_ptr<RcConnMgr> rc_manager;
  std::shared_ptr<dory::conn::UnreliableDatagram> ud;
  std::unique_ptr<UdConnMgr> ud_manager;
  std::shared_ptr<dory::memory::pool::PoolAllocator<PoolType>> pool;
  RpcSlot **slave_fip_at;

  ConnMap conns;
  DynamicConnections<ConnIterator> dc;

  std::optional<ProcIdType> keep_conn;
  std::shared_ptr<CircularBuffer<RpcSlot>> keep_conn_buffer;

  LOGGER_DECL(logger);
};

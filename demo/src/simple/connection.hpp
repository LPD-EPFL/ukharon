#pragma once

#include <memory>
#include <sstream>
#include <string>

#include <dory/ctrl/block.hpp>

#include <dory/conn/manager/uniform-rc.hpp>
#include <dory/conn/rc.hpp>

#include <dory/shared/logger.hpp>
#include <dory/shared/unused-suppressor.hpp>

#include <dory/rpc/conn/universal-connection.hpp>

#include "common.hpp"

using Handler =
    dory::rpc::conn::UniversalConnectionRpcHandler<ProcIdType, RpcKind>;

class Manager : public Handler::AbstractManager {
 private:
  using ProcIdType = Handler::AbstractManager::ProcIdType;
  using RcConnMgr = dory::conn::manager::UniformRcConnectionManager<ProcIdType>;

  dory::ctrl::ControlBlock &cb;

  std::unique_ptr<RcConnMgr> rc_manager;

 public:
  struct MrMemoryLocation {
    void *location;
    uintptr_t mr_offset;
  };

  Manager(dory::ctrl::ControlBlock &cb, MrMemoryLocation const &hb_location)
      : cb{cb},
        location{hb_location},
        LOGGER_INIT(logger, "ConnectionManager") {
    rc_manager = std::make_unique<RcConnMgr>(cb);
    rc_manager->usePd("primary-pd");
    rc_manager->useMr("primary-mr");
    rc_manager->useSendCq("cq-used");
    rc_manager->useRecvCq("cq-unused");
    rc_manager->setNewConnectionRights(dory::ctrl::ControlBlock::LOCAL_READ |
                                       dory::ctrl::ControlBlock::LOCAL_WRITE |
                                       dory::ctrl::ControlBlock::REMOTE_READ |
                                       dory::ctrl::ControlBlock::REMOTE_WRITE);
  }

  std::pair<bool, std::string> handleStep1(
      ProcIdType proc_id,
      Handler::AbstractManager::Parser const &parser) override {
    std::istringstream remote_info(parser.connectionInfo());
    std::string rc_info;
    remote_info >> rc_info;

    LOGGER_INFO(logger, "Process {} sent ReliableConnection info: {}", proc_id,
                rc_info);

    auto &rc = rc_manager->newConnection(proc_id, rc_info);
    auto rc_info_for_remote = rc.remoteInfo().serialize();

    auto offset = location.mr_offset;

    std::string local_serialized_info =
        rc_info_for_remote + " " + std::to_string(offset);

    return std::make_pair(true, local_serialized_info);
  }

  bool handleStep2(ProcIdType proc_id,
                   Handler::AbstractManager::Parser const &parser) override {
    dory::ignore(proc_id);
    dory::ignore(parser);
    return true;
  }

  void remove(ProcIdType proc_id) override {
    rc_manager->removeConnection(proc_id);
  }

  std::vector<ProcIdType> collectInactive() override { return {}; }

  void markInactive(ProcIdType proc_id) override { dory::ignore(proc_id); }

 private:
  MrMemoryLocation location;

  LOGGER_DECL(logger);
};

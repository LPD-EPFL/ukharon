#include <functional>
#include <iostream>

#include <signal.h>
#include <unistd.h>  // for getpid()

#include <dory/rpc/server.hpp>

#include <lyra/lyra.hpp>

#include <dory/ctrl/block.hpp>
#include <dory/ctrl/device.hpp>

#include <dory/shared/logger.hpp>
#include <dory/shared/units.hpp>
#include <dory/shared/unused-suppressor.hpp>

#include <dory/memstore/store.hpp>

// Define the membership config before the membership
#include <dory/membership/default-config.hpp>

#include <dory/membership/membership.hpp>

#include "membership.h"

static auto main_logger = dory::std_out_logger("Member");
using ProcIdType = dory::membership::ProcIdType;

std::optional<std::reference_wrapper<dory::membership::LocalActiveCache>>
    local_active_cache;

struct Context {
  dory::ctrl::OpenDevice open_device;
  std::optional<dory::ctrl::ResolvedPort> resolved_port;
  std::optional<dory::ctrl::ControlBlock> cb;
  std::optional<dory::membership::Membership> mc;
  std::optional<dory::membership::RpcServer> rpc_server;

  void start(ProcIdType id, std::string serialized_membership_mc_group,
             std::string serialized_kernel_mc_group) {
    std::cout << "PID" << getpid() << "PID" << std::endl;

    LOGGER_INFO(main_logger, "Process has ID {}", id);

    //// Setup RDMA ////
    LOGGER_INFO(main_logger, "Opening RDMA device ...");
    open_device = std::move(dory::ctrl::Devices().list().back());
    LOGGER_INFO(main_logger, "Device: {} / {}, {}, {}", open_device.name(),
                open_device.devName(),
                dory::ctrl::OpenDevice::typeStr(open_device.nodeType()),
                dory::ctrl::OpenDevice::typeStr(open_device.transportType()));

    size_t binding_port = 0;
    LOGGER_INFO(main_logger, "Binding to port {} of opened device {} ...",
                binding_port, open_device.name());
    resolved_port.emplace(open_device);
    auto binded = resolved_port->bindTo(binding_port);
    if (!binded) {
      throw std::runtime_error("Couldn't bind the device.");
    }
    LOGGER_INFO(main_logger,
                "Binded successfully (port_id, port_lid) = ({}, {})",
                +resolved_port->portId(), +resolved_port->portLid());

    LOGGER_INFO(main_logger, "Configuring the control block");
    cb.emplace(*resolved_port);

    //// Setup Membership Controller /////
    LOGGER_INFO(main_logger, "Setting up the Membership Controller");
    mc.emplace(*cb, id, true, dory::membership::Membership::Cache,
               serialized_membership_mc_group, serialized_kernel_mc_group);

    mc->initializeControlBlock();

    mc->startBackgroundThread();

    LOGGER_INFO(main_logger, "Setting up the RPC server");
    int initial_rpc_port = 7000;
    rpc_server.emplace("0.0.0.0", initial_rpc_port);

    mc->registerRpcHandlers(*rpc_server);

    LOGGER_INFO(main_logger, "Starting the RPC server");
    rpc_server->startOrChangePort();

    LOGGER_INFO(main_logger, "Announcing the process is online");
    dory::memstore::ProcessAnnouncer announcer;
    announcer.announceProcess(id, rpc_server->port());

    mc->enableKernelHeartbeat();

    mc->start();

    // auto membership_notifier = mc.membershipNotifier();
    local_active_cache = mc->localActiveCache();
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // dory::pin_main_to_core(dory::membership::LeaseCheckingCore);

    // dory::membership::FullMembership new_membership;
    // membership_notifier->try_dequeue(new_membership);
    // auto view_id = new_membership.id;
    // auto [active, changed] = dv.isActiveOrViewChanged(view_id);
  }
};

std::unique_ptr<Context> ctx;

extern "C" {
void start_membership_c() {
#ifndef MEMBERSHIP_NOP_LIB
  ctx = std::make_unique<Context>();
  ctx->start(10, "ff12:601b:ffff::1:ff28:cf2a/0xc003",
             "ff12:401b:ffff::1/0xc003");
#endif
}

activity_t is_active(int view_id) {
#ifndef MEMBERSHIP_NOP_LIB
  auto [a, c] = local_active_cache->get().isActiveOrViewChanged(
      static_cast<dory::membership::ViewIdType>(view_id));

  return activity_t{a, c};
#else
  reinterpret_cast<void *>(view_id);
  return activity_t{true, false};
#endif
}
}

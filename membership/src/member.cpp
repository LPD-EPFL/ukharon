#include <iostream>

#include <unistd.h>  // for getpid()
#include <csignal>

#include <dory/rpc/server.hpp>

#include <lyra/lyra.hpp>

#include <dory/ctrl/block.hpp>
#include <dory/ctrl/device.hpp>

#include <dory/shared/logger.hpp>
#include <dory/shared/units.hpp>
#include <dory/shared/unused-suppressor.hpp>

#include <dory/memstore/store.hpp>

// Define the membership config before the membership
#include "default-config.hpp"

#include "membership.hpp"

static auto main_logger = dory::std_out_logger("Member");
using ProcIdType = dory::membership::ProcIdType;

int main(int argc, char *argv[]) {
  std::cout << "PID" << getpid() << "PID" << std::endl;

  ProcIdType id;
  bool no_deadbeat = false;
  std::string serialized_membership_mc_group;
  std::string serialized_kernel_mc_group;

  //// Parse Arguments ////
  lyra::cli cli;
  bool get_help = false;

  cli.add_argument(lyra::help(get_help))
      .add_argument(
          lyra::opt(id, "id").required().name("-p").name("--pid").help(
              "ID of the process"))
      .add_argument(
          lyra::opt(no_deadbeat)
              .name("--no-deadbeat")
              .help("Disables the deadbeat to force FD to be heartbeat-based."))
      .add_argument(
          lyra::opt(serialized_membership_mc_group,
                    "serialized_membership_mc_group")
              .required()
              .name("-m")
              .name("--membership-mc-group")
              .help("Serialized multicast group used for new memberships"))
      .add_argument(
          lyra::opt(serialized_kernel_mc_group, "serialized_kernel_mc_group")
              .required()
              .name("-k")
              .name("--kernel-mc-group")
              .help("Serialized multicast group used for kernel failures"));

  // Parse the program arguments.
  auto result = cli.parse({argc, argv});

  if (get_help) {
    std::cout << cli;
    return 0;
  }

  // Check that the arguments where valid.
  if (!result) {
    std::cerr << "Error in command line: " << result.errorMessage()
              << std::endl;
    return 1;
  }

  LOGGER_INFO(main_logger, "Process has ID {}", id);

  //// Setup RDMA ////
  LOGGER_INFO(main_logger, "Opening RDMA device ...");
  auto open_device = std::move(dory::ctrl::Devices().list().back());
  LOGGER_INFO(main_logger, "Device: {} / {}, {}, {}", open_device.name(),
              open_device.devName(),
              dory::ctrl::OpenDevice::typeStr(open_device.nodeType()),
              dory::ctrl::OpenDevice::typeStr(open_device.transportType()));

  size_t binding_port = 0;
  LOGGER_INFO(main_logger, "Binding to port {} of opened device {} ...",
              binding_port, open_device.name());
  dory::ctrl::ResolvedPort resolved_port(open_device);
  auto binded = resolved_port.bindTo(binding_port);
  if (!binded) {
    throw std::runtime_error("Couldn't bind the device.");
  }
  LOGGER_INFO(main_logger, "Binded successfully (port_id, port_lid) = ({}, {})",
              +resolved_port.portId(), +resolved_port.portLid());

  LOGGER_INFO(main_logger, "Configuring the control block");
  dory::ctrl::ControlBlock cb(resolved_port);

  //// Setup Membership Controller /////
  LOGGER_INFO(main_logger, "Setting up the Membership Controller");
  dory::membership::Membership mc(
      cb, id, true, dory::membership::Membership::Cache,
      serialized_membership_mc_group, serialized_kernel_mc_group);

  mc.initializeControlBlock();

  mc.startBackgroundThread();

  LOGGER_INFO(main_logger, "Setting up the RPC server");
  int initial_rpc_port = 7000;
  dory::membership::RpcServer rpc_server("0.0.0.0", initial_rpc_port);

  mc.registerRpcHandlers(rpc_server);

  LOGGER_INFO(main_logger, "Starting the RPC server");
  rpc_server.startOrChangePort();

  LOGGER_INFO(main_logger, "Announcing the process is online");
  dory::memstore::ProcessAnnouncer announcer;
  announcer.announceProcess(id, rpc_server.port());

  if (!no_deadbeat) {
    mc.enableKernelHeartbeat();
  }

  mc.start();

  auto membership_notifier = mc.membershipNotifier();
  auto &dv = mc.localActiveCache();

  dory::pin_main_to_core(dory::membership::LeaseCheckingCore);

  std::this_thread::sleep_for(std::chrono::seconds(1));

  dory::membership::FullMembership new_membership;

  while (true) {
    if (membership_notifier->try_dequeue(new_membership)) {
      while (true) {
        // {char _; std::cin >> _;}

        // auto view_id = new_membership.id;
        // auto [active, changed] = dv.isActiveOrViewChanged(view_id);

        // std::cout << "View: " << view_id << ", Active?: " << active << ",
        // Changed?: " << changed << std::endl;

        // if (changed) {
        //   break;
        // }
      }
    }
  }

  LOGGER_ERROR(main_logger, "Unreachable");

  return 0;
}

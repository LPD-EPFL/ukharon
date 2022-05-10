#include <iostream>

#include <unistd.h> // for getpid()

#include <lyra/lyra.hpp>

#include <dory/ctrl/block.hpp>
#include <dory/ctrl/device.hpp>

#include <dory/memstore/store.hpp>

#include <dory/conn/manager/uniform-rc.hpp>
#include <dory/memory/pool/pool-allocator.hpp>

#include <dory/shared/logger.hpp>
#include <dory/shared/units.hpp>

#include <dory/rpc/server.hpp>

#include "common.hpp"
#include "connection.hpp"

static auto main_logger = dory::std_out_logger("Member");

int main(int argc, char *argv[]) {
  std::cout << "PID" << getpid() << "PID" << std::endl;

  ProcIdType id;

  //// Parse Arguments ////
  lyra::cli cli;
  bool get_help = false;

  cli.add_argument(lyra::help(get_help))
      .add_argument(
          lyra::opt(id, "id").required().name("-p").name("--pid").help(
              "ID of the process"));

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
  LOGGER_INFO(main_logger, "Device: {} / {}, {}, {}\n", open_device.name(),
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
  LOGGER_INFO(main_logger,
              "Binded successfully (port_id, port_lid) = ({}, {})\n",
              +resolved_port.portId(), +resolved_port.portLid());

  LOGGER_INFO(main_logger, "Configuring the control block\n");
  dory::ctrl::ControlBlock cb(resolved_port);

  //// Setup allocator ////
  cb.registerPd("primary-pd");
  cb.allocateBuffer("primary-buf", dory::units::mebibytes(1), 64);
  cb.registerMr("primary-mr", "primary-pd", "primary-buf",
                dory::ctrl::ControlBlock::LOCAL_READ |
                    dory::ctrl::ControlBlock::LOCAL_WRITE |
                    dory::ctrl::ControlBlock::REMOTE_READ);

  auto *buf = reinterpret_cast<void *>(cb.mr("primary-mr").addr);
  dory::memory::pool::ArenaPoolAllocator arena_allocator(
      buf, dory::units::mebibytes(1), buf);

  //// Setup Connection Manager ////
  using PoolObj = uint64_t;
  size_t pool_obj_alignment = 64;
  size_t pool_obj_cnt = 1;
  auto pool =
      arena_allocator.createPool<PoolObj>(pool_obj_cnt, pool_obj_alignment);

  auto *obj = pool->create();

  cb.registerCq("cq-unused");
  cb.registerCq("cq-used");

  using RpcHandlerMemLoc = Manager::MrMemoryLocation;
  RpcHandlerMemLoc mem_location;
  mem_location.location = obj;
  mem_location.mr_offset = pool->offset(obj);

  auto manager = std::make_unique<Manager>(cb, mem_location);
  auto rpc_handler = std::make_unique<Handler>(
      std::move(manager), RpcKind::RDMA_ACTIVE_VIEW_CONNECTION);

  //// Setup the rpc server ////
  LOGGER_INFO(main_logger, "Setting up the RPC server");
  int initial_rpc_port = 7000;
  dory::rpc::RpcServer<RpcKind::Kind> rpc_server("0.0.0.0", initial_rpc_port);

  rpc_server.attachHandler(std::move(rpc_handler));

  LOGGER_INFO(main_logger, "Starting the RPC server\n");
  rpc_server.startOrChangePort();

  LOGGER_INFO(main_logger, "Announcing the process is online");
  dory::memstore::ProcessAnnouncer announcer;
  announcer.announceProcess(id, rpc_server.port());

  LOGGER_INFO(main_logger, "I modify the object at offset {}",
              mem_location.mr_offset);
  while (true) {
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
    auto *v = reinterpret_cast<PoolObj volatile *>(obj);

    *v += 1;
    std::cout << "Wrote " << *v << std::endl;
  }

  return 0;
}

#include <chrono>
#include <iostream>
#include <thread>

#include <unistd.h> // for getpid()

#include <lyra/lyra.hpp>

#include <dory/ctrl/block.hpp>
#include <dory/ctrl/device.hpp>

#include <dory/memstore/store.hpp>

#include <dory/memory/pool/pool-allocator.hpp>

#include <dory/shared/logger.hpp>
#include <dory/shared/units.hpp>
#include <dory/shared/unused-suppressor.hpp>

#include <dory/conn/rc.hpp>
#include <dory/rpc/conn/universal-connector.hpp>

#include "common.hpp"

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
  size_t device_idx = 1;
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

  //// Setup Connection ////
  using PoolObj = uint64_t;
  size_t pool_obj_alignment = 64;
  size_t pool_obj_cnt = 1;
  auto pool =
      arena_allocator.createPool<PoolObj>(pool_obj_cnt, pool_obj_alignment);

  auto *obj = pool->create();

  dory::conn::ReliableConnection poller(cb);
  poller.bindToPd("primary-pd");
  poller.bindToMr("primary-mr");
  cb.registerCq("cq-used");
  cb.registerCq("cq-unused");
  poller.associateWithCq("cq-used", "cq-unused");
  poller.init(dory::ctrl::ControlBlock::LOCAL_READ |
              dory::ctrl::ControlBlock::LOCAL_WRITE |
              dory::ctrl::ControlBlock::REMOTE_READ);

  //// Read memory ////

  dory::memstore::ProcessAnnouncer announcer;

  while (true) {
    std::cout << "Give ID to connect to: ";
    int connect_to;
    std::cin >> connect_to;

    auto [ip, port, kernel] = announcer.processToHost(connect_to);
    dory::ignore(kernel);
    dory::rpc::conn::UniversalConnectionRpcClient<ProcIdType, RpcKind::Kind>
        cli(ip, port);

    cli.connect();
    auto [cli_ok, cli_offset_info] = cli.handshake<dory::uptrdiff_t>(
        [&poller]() -> std::pair<bool, std::string> {
          std::string serialized_info = poller.remoteInfo().serialize();

          return std::make_pair(true, serialized_info);
        },
        [&poller, connect_to](std::string const &info)
            -> std::pair<bool, std::optional<dory::uptrdiff_t>> {
          std::istringstream remote_info_stream(info);
          std::string rc_info;
          dory::uptrdiff_t offset_info;
          remote_info_stream >> rc_info;
          remote_info_stream >> offset_info;

          auto remote_rc = dory::conn::RemoteConnection::fromStr(rc_info);
          poller.reset();
          poller.reinit();
          poller.connect(remote_rc, connect_to);

          return std::make_pair(true,
                                std::optional<dory::uptrdiff_t>(offset_info));
        },
        id, RpcKind::RDMA_ACTIVE_VIEW_CONNECTION);

    if (!cli_ok) {
      LOGGER_WARN(main_logger, "Could not connect to process {}", connect_to);
      continue;
    }

    std::vector<struct ibv_wc> wce;
    char continue_loop;

    do {
      auto ok = poller.postSendSingle(
          dory::conn::ReliableConnection::RdmaRead,
          static_cast<ProcIdType>(connect_to), obj, sizeof(PoolObj),
          poller.remoteBuf() + cli_offset_info.value());

      if (!ok) {
        throw std::runtime_error("Failed to post!");
      }

      std::this_thread::sleep_for(std::chrono::milliseconds(100));

      size_t to_poll = 1;
      while (true) {
        wce.resize(to_poll);

        if (!poller.pollCqIsOk(dory::conn::ReliableConnection::SendCq, wce)) {
          throw std::runtime_error("Error polling Cq!");
        }

        if (wce.empty()) {
          continue;
        }

        auto &e = wce[0];

        if (e.wr_id != static_cast<ProcIdType>(connect_to)) {
          // Ignore old requests with different WRID
          continue;
        }

        // The WRID matches
        if (e.status != IBV_WC_SUCCESS) {
          throw std::runtime_error("RDMA read failed!");
        }

        std::cout << "Got " << *obj << " when reading offset "
                  << cli_offset_info.value() << " from " << connect_to
                  << std::endl;
        break;
      }

      do {
        std::cout << "Continue [y/n]? ";
        std::cin >> continue_loop;
      } while (continue_loop != 'y' && continue_loop != 'n');

    } while (continue_loop == 'y');
  }

  return 0;
}

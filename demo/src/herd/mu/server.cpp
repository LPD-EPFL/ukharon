#include <chrono>
#include <thread>
#include <bitset>

#include <lyra/lyra.hpp>

#include <dory/ctrl/block.hpp>
#include <dory/ctrl/device.hpp>

#include <dory/shared/logger.hpp>
#include <dory/shared/pinning.hpp>
#include <dory/shared/units.hpp>

#include <dory/memstore/store.hpp>

#include <dory/memory/pool/pool-allocator.hpp>

#include <dory/rpc/conn/universal-connection.hpp>
#include <dory/rpc/server.hpp>

#include <unistd.h>  // for getpid()

#include <dory/crash-consensus.hpp>

#include "../common.hpp"
#include "../connection.hpp"
#include "../rpc-slot.hpp"

#include "service.hpp"

struct LeaderState {
  bool became = false;
  bool leader = false;
};

static auto main_logger = dory::std_out_logger("HERD");

int main(int argc, char *argv[]) {
  std::cout << "PID" << getpid() << "PID" << std::endl;

  ProcIdType id;
  std::vector<ProcIdType> consensus_nodes;
  bool skip_proposal = false;

  //// Parse Arguments ////
  lyra::cli cli;
  bool get_help = false;

  cli.add_argument(lyra::help(get_help))
      .add_argument(
          lyra::opt(id, "id").required().name("-p").name("--pid").help(
              "ID of the process"))
      .add_argument(lyra::opt(consensus_nodes, "id")
                        .required()
                        .name("-c")
                        .name("--consensus-node")
                        .help("IDs of all consensus nodes"))
      .add_argument(
          lyra::opt(skip_proposal).optional().name("--skip-proposal").help(
              "Leader directly replies to client without proposing to consensus"));

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

  //// Setup Herd ////
  cb.registerPd("primary-pd");
  cb.allocateBuffer("mica-log", dory::units::bytes(M_1024), 64);
  // cb.allocateBuffer("primary-buf", dory::units::mebibytes(1), 64);
  cb.registerMr("mica-mr", "primary-pd", "mica-log",
                dory::ctrl::ControlBlock::LOCAL_READ |
                dory::ctrl::ControlBlock::REMOTE_READ);
  cb.registerCq("cq");

  LOGGER_INFO(main_logger, "Initializing MICA");
  FreeableMica mica(0, reinterpret_cast<uint8_t*>(cb.mr("mica-mr").addr));
  // Warm-up
  auto keys = dory::deleted_unique_ptr<uint128>(mica_gen_keys(TEST_NUM_KEYS),
                                                [](uint128 *k) { free(k); });
  auto seed = static_cast<uint64_t>(id);
  permute_for(keys.get(), &seed);

  for (uint64_t k = 0; k < TEST_NUM_KEYS; k++) {
    struct mica_op op;
    struct mica_resp resp;

    prepare_put_request(op, keys.get() + k);
    mica_single_op(&mica.kv, &op, &resp);
  }

  auto manager = std::make_unique<Manager>(cb);
  auto *active_connections = manager->connections();
  auto handler = std::make_unique<Handler>(std::move(manager),
                                           dory::membership::RpcKind::RDMA_HERD_CONNECTION);

  //// Main ////
  LOGGER_INFO(main_logger, "Setting up the RPC server");
  int initial_rpc_port = 7000;
  dory::membership::RpcServer rpc_server("0.0.0.0", initial_rpc_port);

  rpc_server.attachHandler(std::move(handler));

  LOGGER_INFO(main_logger, "Starting the RPC server");
  rpc_server.startOrChangePort();

  LOGGER_INFO(main_logger, "Announcing the process is online");
  dory::memstore::ProcessAnnouncer announcer;
  announcer.announceProcess(id, rpc_server.port());

  int core = 14;
  LOGGER_INFO(main_logger, "Pinning polling thread to core {}", core);
  dory::pin_main_to_core(core);

  LOGGER_INFO(
      main_logger,
      "My PID is {}. Run `chrt -f -p 99 {} to switch me to a realtime task",
      getpid(), getpid());

  size_t outstanding_responses = 0;
  size_t constexpr OutstandingResponsesMax = 127;

  std::vector<struct ibv_wc> wce;
  auto &cq = cb.cq("cq");

  std::vector<int> remote_ids;
  for (auto pid : consensus_nodes) {
    if (id == pid) {
      continue;
    }

    remote_ids.push_back(pid);
  }

  // Zero outstanding requests means no speculation.
  // For every post the consensus engine waits for a majority of replies.
  int outstanding_req = 0;

  dory::Consensus consensus(id, remote_ids, outstanding_req);

  Service service(id, cb, cq, mica, consensus, skip_proposal);

  while (true) { // Main loop
    auto *connection_list = active_connections->connections();
    service.leaderTick(connection_list);
  }
}

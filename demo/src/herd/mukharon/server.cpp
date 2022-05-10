#include <chrono>
#include <thread>
#include <bitset>
#include <string>

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
  std::string serialized_membership_mc_group;
  std::string serialized_kernel_mc_group;

  ProcIdType master_id;
  std::optional<ProcIdType> slave_id;
  std::string access_majority_or_cache;

  //// Parse Arguments ////
  lyra::cli cli;
  bool get_help = false;

  cli.add_argument(lyra::help(get_help))
      .add_argument(
          lyra::opt(id, "id").required().name("-p").name("--pid").help(
              "ID of the process"))
      .add_argument(
          lyra::opt(master_id, "id").required().name("-m").name("--master-pid").help(
              "ID of the master process"))
      .add_argument(
          lyra::opt(slave_id, "id").name("-s").name("--slave-pid").help(
              "ID of the slave process"))
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
              .help("Serialized multicast group used for kernel failures"))
      .add_argument(
          lyra::opt(access_majority_or_cache, "majority_or_cache").required().name("-a").name("--access").choices("majority", "cache").help(
              "Have uKharon-Core access a majority of acceptors or a cache"));

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


  //// Setup Membership Controller /////
  LOGGER_INFO(main_logger, "Setting up the Membership Controller");
  auto access_kind = dory::membership::Membership::AcceptorMajority;
  if (access_majority_or_cache == "cache") {
    access_kind = dory::membership::Membership::Cache;
  }
  dory::membership::Membership mc(cb, id, true, access_kind,
                                  serialized_membership_mc_group,
                                  serialized_kernel_mc_group);

  mc.initializeControlBlock();
  mc.startBackgroundThread();


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

  auto manager = std::make_unique<Manager>(cb, master_id);
  auto **slave_fip_at = manager->slaveWritesFipAtAddr();
  auto &master_conn_buffer = manager->keepConnBuffer();
  auto *active_connections = manager->connections();
  auto handler = std::make_unique<Handler>(std::move(manager),
                                           dory::membership::RpcKind::RDMA_HERD_CONNECTION);

  //// Main ////
  LOGGER_INFO(main_logger, "Setting up the RPC server");
  int initial_rpc_port = 7000;
  dory::membership::RpcServer rpc_server("0.0.0.0", initial_rpc_port);

  mc.registerRpcHandlers(rpc_server);
  rpc_server.attachHandler(std::move(handler));

  LOGGER_INFO(main_logger, "Starting the RPC server");
  rpc_server.startOrChangePort();

  LOGGER_INFO(main_logger, "Announcing the process is online");
  dory::memstore::ProcessAnnouncer announcer;
  announcer.announceProcess(id, rpc_server.port());

  mc.enableKernelHeartbeat();
  mc.start();

  LOGGER_INFO(main_logger, "Pinning polling thread to core {}",
    dory::membership::LeaseCheckingCore);
  dory::pin_main_to_core(dory::membership::LeaseCheckingCore);

  LOGGER_INFO(
      main_logger,
      "My PID is {}. Run `chrt -f -p 99 {} to switch me to a realtime task",
      getpid(), getpid());

  size_t outstanding_responses = 0;
  size_t constexpr OutstandingResponsesMax = 127;

  std::vector<struct ibv_wc> wce;
  auto &cq = cb.cq("cq");

  auto membership_notifier = mc.membershipNotifier();
  auto &dv = mc.localActiveCache();

  Service service(id, cb, cq, mica, master_id, slave_id, slave_fip_at, dv);

  dory::membership::FullMembership new_membership;
  while (id == master_id && slave_id) {
    membership_notifier->wait_dequeue(new_membership);
    if (service.tryConnectToSlave(new_membership)) {
      break;
    }
  }

  if (id == master_id) {
    if (slave_id) {
      LOGGER_INFO(main_logger, "I am connected to slave (id {})", slave_id.value());
    } else {
      LOGGER_INFO(main_logger, "I am running without a slave");
    }
  }

  LeaderState leader_state;

  leader_state.leader = (id == master_id);

  while (true) { // Main loop
    membership_notifier->try_dequeue(new_membership);
    auto *connection_list = active_connections->connections();

    if (!new_membership.includes(id).first) {
      continue;
    }

    if (id == slave_id && !leader_state.leader) {
      // Note: This assumes that the slave has to see the master in the first membership it receives
      auto const includes_master = new_membership.includes(master_id).first;
      if (!includes_master) {
        // LOGGER_INFO(main_logger, "Became leader");
        leader_state.became = true;
        leader_state.leader = true;
      }
    }

    if (leader_state.leader) {
      auto view_id = new_membership.id;

      if (id == slave_id) {
        if (leader_state.became) {
          // LOGGER_INFO(main_logger, "Forgetting slave");
          leader_state.became = false;

          // Scan the master's buffer once
          service.depleteMasterBuffer(view_id, connection_list, *master_conn_buffer);

          service.forgetSlave();
        }
      }

      service.leaderTick(view_id, connection_list);

    } else {
      // If master is out, it doesn't try to become a slave
      if (id == slave_id) {
        service.slaveTick(connection_list);
      }
    }
  }
}

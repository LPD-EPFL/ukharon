#include <algorithm>
#include <iostream>
#include <queue>

#include <unistd.h> // for getpid()

#include <dory/ctrl/block.hpp>
#include <dory/ctrl/device.hpp>

#include <dory/conn/rc-exchanger.hpp>
#include <dory/conn/rc.hpp>
#include <dory/conn/ud.hpp>

#include <dory/shared/logger.hpp>
#include <dory/shared/pinning.hpp>
#include <dory/shared/units.hpp>
#include <dory/shared/unused-suppressor.hpp>

#include <dory/paxos/paxos.hpp>

#include <dory/memstore/store.hpp>

#include <dory/rpc/server.hpp>

#include <lyra/lyra.hpp>

// Define the membership config before the membership
#include "default-config.hpp"

#include "membership.hpp"

using FullMembership = dory::membership::FullMembership;
using Paxos = dory::paxos::Paxos<FullMembership>;

enum CliCommand {
  AddConsensus,
  AddCache,
  AddApp,
  Remove,
  AcceptorFailed,
};

static void wait_completion(
    const std::shared_ptr<dory::conn::UnreliableDatagram>& ud) {
  std::vector<struct ibv_wc> wce;
  while (true) {
    wce.resize(1);
    if (!ud->pollCqIsOk<dory::conn::UnreliableDatagram::SendCQ>(wce)) {
      throw std::runtime_error("Polling error.");
    }
    if (wce.empty()) {
      continue;
    }
    if (wce[0].status != IBV_WC_SUCCESS) {
      throw std::runtime_error("WC not successful.");
    }
    break;
  }
}

static void broadcast_membership(dory::conn::McGroup& mb, FullMembership* mp) {
  auto send_len = static_cast<uint32_t>(mp->sizeOf());
  // std::cout << "Broadcasting membership " << mp->id << ", " << send_len
  //           << " bytes." << std::endl;
  if (!mb.postSend(0, mp, send_len)) {
    throw std::runtime_error("Multicast error.");
  }
  wait_completion(mb.ud());
  // std::cout << "Broadcast done." << std::endl;
}

class DecisionHandler : public Paxos::Handler {
 public:
  DecisionHandler(dory::third_party::sync::SpscQueue<FullMembership>&
                      decision_queue) noexcept
      : decision_queue{decision_queue} {}
  void handle(dory::paxos::Instance i, FullMembership ms) override {
    dory::ignore(i);
    decision_queue.enqueue(ms);
  }

 private:
  dory::third_party::sync::SpscQueue<FullMembership>& decision_queue;
};

static auto main_logger = dory::std_out_logger("Acceptor");
static int constexpr PaxosCore = 24;
static constexpr dory::ctrl::ControlBlock::MemoryRights FullRights =
    dory::ctrl::ControlBlock::LOCAL_READ |
    dory::ctrl::ControlBlock::LOCAL_WRITE |
    dory::ctrl::ControlBlock::REMOTE_READ |
    dory::ctrl::ControlBlock::REMOTE_WRITE |
    dory::ctrl::ControlBlock::REMOTE_ATOMIC;
static constexpr size_t AcceptorBufferSize = dory::units::mebibytes(100);
static constexpr size_t ProposerBufferSize = dory::units::mebibytes(10);
static constexpr size_t BroadcastBufferSize = sizeof(FullMembership);
static constexpr size_t Alignment = 64;

int main(int argc, char* argv[]) {
  std::cout << "PID" << getpid() << "PID" << std::endl;

  using ProcIdType = uint16_t;

  ProcIdType id;
  std::vector<ProcIdType> acceptors;
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
      .add_argument(lyra::opt(acceptors, "acceptors")
                        .required()
                        .name("-a")
                        .name("--acceptor")
                        .help("IDs of all acceptors"))
      .add_argument(
          lyra::opt(no_deadbeat)
            .name("--no-deadbeat")
            .help("Disables the deadbeat to force FD to be heartbeat-based."))
      .add_argument(lyra::opt(serialized_membership_mc_group,
                              "serialized_membership_mc_group")
                        .required()
                        .name("-m")
                        .name("--mc-group")
                        .help("Serialized multicast group info"))
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

  if (std::find(acceptors.begin(), acceptors.end(), id) == acceptors.end()) {
    std::cerr << "Own id not found in acceptors list" << std::endl;
    return 1;
  }

  LOGGER_INFO(main_logger, "Using multicast group {}",
              serialized_membership_mc_group);

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

  LOGGER_INFO(main_logger,
              "Registering the protection domain in the control block");
  cb.registerPd("primary-pd");

  LOGGER_INFO(main_logger,
              "Registering the completion queue in the control block");
  cb.registerCq("cq-broadcast");
  cb.registerCq("cq-unused");

  //// Allocate RDMA Memory ////
  LOGGER_INFO(main_logger, "Allocating {} bytes for broadcasting memberships",
              BroadcastBufferSize);
  cb.allocateBuffer("broadcast-buf", BroadcastBufferSize, Alignment);
  cb.registerMr("broadcast-mr", "primary-pd", "broadcast-buf",
                dory::ctrl::ControlBlock::LOCAL_READ);

  uintptr_t const broadcast_start = cb.mr("broadcast-mr").addr;
  FullMembership* broadcast_slot =
      reinterpret_cast<FullMembership*>(broadcast_start);

  // For membership controller
  LOGGER_INFO(main_logger, "Setting up the Membership Controller");
  dory::membership::Membership mc(cb, id, false, dory::membership::Membership::None,
                                  serialized_membership_mc_group,
                                  serialized_kernel_mc_group);

  // For Paxos
  cb.allocateBuffer("acceptor-buf", AcceptorBufferSize, Alignment);
  cb.registerMr("acceptor-mr", "primary-pd", "acceptor-buf", FullRights);
  cb.allocateBuffer("proposer-buf", ProposerBufferSize, Alignment);
  cb.registerMr("proposer-mr", "primary-pd", "proposer-buf", FullRights);
  cb.registerCq("paxos-cq");

  // For membership controller
  mc.initializeControlBlock();

  //// Init Paxos ////
  auto& store = dory::memstore::MemoryStore::getInstance();

  dory::conn::RcConnectionExchanger<ProcIdType> proposer_ce(id, acceptors, cb);
  dory::conn::RcConnectionExchanger<ProcIdType> acceptor_ce(id, acceptors, cb);

  proposer_ce.configureAll("primary-pd", "proposer-mr", "paxos-cq", "paxos-cq");
  acceptor_ce.configureAll("primary-pd", "acceptor-mr", "paxos-cq", "paxos-cq");
  proposer_ce.announceAll(store, "proposer");
  acceptor_ce.announceAll(store, "acceptor");

  proposer_ce.announceReady(store, "qp", "prepared");
  proposer_ce.waitReadyAll(store, "qp", "prepared");

  proposer_ce.connectAll(store, "acceptor", FullRights);
  acceptor_ce.connectAll(store, "proposer", FullRights);

  // The ConnectionExchanger gives us a reference to a connection map.
  // We wrap it artificially inside a shared_ptr.
  // We do not own the connections hand have to rely on the fact that the CE
  // will live long enough.
  std::shared_ptr<std::map<ProcIdType, dory::conn::ReliableConnection>> rcs(
      &proposer_ce.connections(),
      [](std::map<ProcIdType, dory::conn::ReliableConnection>* /*unused*/) {});

  dory::third_party::sync::SpscQueue<FullMembership> decision_queue;
  auto decision_handler = std::make_shared<DecisionHandler>(decision_queue);

  auto paxos = std::make_shared<Paxos>(
      // View
      id, rcs,
      // Acceptor Buffer
      cb.mr("acceptor-mr").addr, 0, AcceptorBufferSize,
      // Proposer Buffer
      cb.mr("proposer-mr").addr, 0, ProposerBufferSize,
      // Handlers
      decision_handler,
      // misc.
      cb.cq("paxos-cq"), dory::membership::MaxViewCnt);

  //// RDMA Connections ////
  // For heartbeat counter
  LOGGER_INFO(main_logger,
              "Creating RC connection manager for the active view provider");
  dory::conn::manager::UniformRcConnectionManager<ProcIdType>
      activeViewConnManager(cb);
  activeViewConnManager.usePd("primary-pd");
  activeViewConnManager.useMr("acceptor-mr");
  activeViewConnManager.useSendCq("cq-unused");
  activeViewConnManager.useRecvCq("cq-unused");
  activeViewConnManager.setNewConnectionRights(
      dory::ctrl::ControlBlock::LOCAL_READ |
      dory::ctrl::ControlBlock::LOCAL_WRITE |
      dory::ctrl::ControlBlock::REMOTE_READ);

  //// Main ////
  LOGGER_INFO(main_logger, "Starting membership controller");
  mc.startBackgroundThread();

  LOGGER_INFO(main_logger, "Setting up the RPC server");
  int initial_rpc_port = 7000;

  dory::rpc::RpcServer<dory::membership::RpcKind::Kind> rpcServer(
      "0.0.0.0", initial_rpc_port);

  mc.registerRpcHandlers(rpcServer);

  using ActiveViewConnectionRpcHandler = dory::membership::UnilateralConnectionRpcHandler;
  ActiveViewConnectionRpcHandler::MrMemoryLocation av_handler_mem;
  av_handler_mem.mr_offset = paxos->slotsMrOffset();

  auto handler = std::make_unique<ActiveViewConnectionRpcHandler>(
      av_handler_mem, &activeViewConnManager,
      dory::membership::RpcKind::RDMA_AV_CONNECTION);
  rpcServer.attachHandler(std::move(handler));

  LOGGER_INFO(main_logger, "Starting the RPC server");
  rpcServer.startOrChangePort();

  dory::memstore::ProcessAnnouncer announcer;
  announcer.announceProcess(id, rpcServer.port());

  proposer_ce.announceReady(store, "paxos", "running");
  proposer_ce.waitReadyAll(store, "paxos", "running");

  if (!no_deadbeat) {
    mc.enableKernelHeartbeat();
  }

  mc.start();

  auto ud = std::make_shared<dory::conn::UnreliableDatagram>(
      cb, "primary-pd", "broadcast-mr", "cq-broadcast", "cq-broadcast");

  dory::conn::McGroup membership_broadcaster(cb, "primary-pd", ud,
                                             serialized_membership_mc_group);

  //// Setup CLI ////
  dory::third_party::sync::SpscQueue<std::pair<CliCommand, ProcIdType>>
      command_queue;

  std::thread cli_thread([&command_queue]() {
    std::cout << "CLI:\n"
                 "- Add (Paxos/Cache/App) Member (Leader only): (P/C/A)ID\n"
                 "- Remove Member (Leader only): -ID\n"
                 "- Acceptor Failed: ~ID\n"
              << std::endl;

    while (true) {
      std::string line;
      std::cin >> line;
      if (line.size() < 2) {
        std::cout << "Invalid command \"" << line << "\"" << std::endl;
        continue;
      }
      char command = line[0];
      line.erase(0, 1);
      try {
        auto target = static_cast<ProcIdType>(std::stoi(line));
        switch (command) {
          case 'P':  // P = Paxos
            command_queue.enqueue({CliCommand::AddConsensus, target});
            break;
          case 'C':
            command_queue.enqueue({CliCommand::AddCache, target});
            break;
          case 'A':
            command_queue.enqueue({CliCommand::AddApp, target});
            break;
          case '-':
            command_queue.enqueue({CliCommand::Remove, target});
            break;
          case '~':
            command_queue.enqueue({CliCommand::AcceptorFailed, target});
            break;
          default:
            std::cout << "Unknown command '" << command << "'." << std::endl;
        }
      } catch (...) {
        std::cout << "Invalid proc id \"" << line << "\"" << std::endl;
      }
    }
  });

  dory::pin_main_to_core(dory::membership::AcceptorCore);

  dory::set_thread_name(cli_thread, "cli");
  cli_thread.detach();

  FullMembership initial_membership;
  for (auto const id : acceptors) {
    initial_membership.addConsensus(id);
  }

  //// Paxos proposer + follower logic ////
  std::set<ProcIdType> alive_acceptors(acceptors.begin(), acceptors.end());
  std::optional<FullMembership> latest_membership;
  dory::membership::FailureNotifier &failure_notifier = mc.registerFailureNotifier();
  std::queue<ProcIdType> failures_heard_as_follower;

  while (true) {
    // Leader if lowest id among local live acceptor list.
    bool leader =
        *std::min_element(alive_acceptors.begin(), alive_acceptors.end()) == id;
    // As a paxos leader:
    if (leader) {
      // Special actions on leader change.
      bool leader_change =
          *std::min_element(acceptors.begin(), acceptors.end()) != id;
      if (leader_change) {
        FullMembership unqueued_membership;
        while (decision_queue.try_dequeue(unqueued_membership)) {
          latest_membership = unqueued_membership;
        }
        paxos->speedupLeaderChange(paxos->nextInstance());
      }
      LOGGER_INFO(main_logger, "Became leader, can P/C/A/- members.");
      // Decide and broadcast the initial membership.
      if (!latest_membership) {
        latest_membership = initial_membership;
        if (!paxos->propose(0, *latest_membership)) {
          throw std::runtime_error("Another proposer proposed.");
        }
        *broadcast_slot = *latest_membership;
        broadcast_membership(membership_broadcaster, broadcast_slot);
      }
      // Issue a new membership - all failures heard as a follower
      if (!failures_heard_as_follower.empty()) {
        bool removed = false;
        while (!failures_heard_as_follower.empty()) {
          if (latest_membership->remove(failures_heard_as_follower.front())) {
            removed = true;
          }
          failures_heard_as_follower.pop();
        }
        if (removed) {
          latest_membership->id++;
          if (!paxos->propose(static_cast<dory::paxos::Instance>(latest_membership->id), *latest_membership)) {
            throw std::runtime_error("Another proposer proposed.");
          }
          *broadcast_slot = *latest_membership;
          broadcast_membership(membership_broadcaster, broadcast_slot);
        }
      }
      // Check 99% of the time for a failure and 1% for commands.
      while (true) {
        // Check for failures.
        for (auto i = 0; i < 99; i++) {
          ProcIdType failure;
          if (failure_notifier.try_dequeue(failure)) {
            if (latest_membership->includes(failure).first) {
#ifdef VERBOSE_ACCEPTOR
              std::cout << "Removing " << failure << " from the membership." << std::endl;
#endif
              // The failed node is part of the latest membership.
              latest_membership->remove(failure);
              latest_membership->id++;
#ifdef VERBOSE_ACCEPTOR
              auto start = std::chrono::steady_clock::now();
#endif
              if (!paxos->propose(
                      static_cast<dory::paxos::Instance>(latest_membership->id),
                      *latest_membership)) {
                throw std::runtime_error("Another proposer proposed.");
              }
#ifdef VERBOSE_ACCEPTOR
              auto middle = std::chrono::steady_clock::now();
#endif
              *broadcast_slot = *latest_membership;
              broadcast_membership(membership_broadcaster, broadcast_slot);
              auto end = std::chrono::steady_clock::now();

#ifdef VERBOSE_ACCEPTOR
              auto diff1 = std::chrono::duration_cast<std::chrono::microseconds>(middle - start);
              std::cout << "Time to propose " << diff1.count() << std::endl;
              auto diff2 = std::chrono::duration_cast<std::chrono::microseconds>(end - middle);
              std::cout << "Time to broadcast " << diff2.count() << std::endl;
#endif
            } else {
              LOGGER_WARN(main_logger, "Received a failure notification for {}, but it isn't part of the latest membership.", failure);
            }
            if (std::find(alive_acceptors.begin(), alive_acceptors.end(), failure) != alive_acceptors.end()) {
              // The failed node is part of the acceptors.
#ifdef VERBOSE_ACCEPTOR
              std::cout << "removing " << +failure << " from acceptors" << std::endl;
#endif
              alive_acceptors.erase(failure);
              paxos->signalFailure(failure);
            }
          }
        }
        // Check for a command.
        std::pair<CliCommand, ProcIdType> command;
        if (command_queue.try_dequeue(command)) {
          switch (command.first) {
            case CliCommand::AcceptorFailed: {
              alive_acceptors.erase(command.second);
              paxos->signalFailure(command.second);
              break;
            }
            case CliCommand::AddConsensus:
            case CliCommand::AddCache:
            case CliCommand::AddApp:
            case CliCommand::Remove: {
              switch (command.first) {
                case CliCommand::AddConsensus:
                  latest_membership->addConsensus(command.second);
                  break;
                case CliCommand::AddCache:
                  latest_membership->addCache(command.second);
                  break;
                case CliCommand::AddApp:
                  latest_membership->addApp(command.second);
                  break;
                case CliCommand::Remove:
                  latest_membership->remove(command.second);
                  break;
                default:
                  throw std::runtime_error("Cannot default.");
              }
              latest_membership->id++;
              if (!paxos->propose(
                      static_cast<dory::paxos::Instance>(latest_membership->id),
                      *latest_membership)) {
                throw std::runtime_error("Another proposer proposed.");
              }
              *broadcast_slot = *latest_membership;
              broadcast_membership(membership_broadcaster, broadcast_slot);
              paxos->prepare(paxos->nextInstance());
              break;
            }
            default:
              throw std::runtime_error("Unknown command, shouldn't happen.");
          }
        }
      }
    // As a Paxos follower:
    } else {
      while (true) {
        // Move Paxos forward.
        paxos->followerTick();
        // Check if there have been changes in the acceptors.
        std::optional<ProcIdType> to_remove;
        // Check for notified failed acceptors.
        {
          ProcIdType failure;
          if (failure_notifier.try_dequeue(failure)) {
            to_remove = failure;
          }
        }
        // Check for manual Acceptor removal.
        if (!to_remove) {
          std::pair<CliCommand, ProcIdType> command;
          if (command_queue.try_dequeue(command)) {
            if (command.first == CliCommand::AcceptorFailed) {
              to_remove = command.second;
            } else {
              LOGGER_WARN(main_logger, "Follower can only execute ~ commands.");
            }
          }
        }
        // Check that the node is part of the acceptors.
        if (to_remove) {
          failures_heard_as_follower.emplace(*to_remove);
          if (std::find(alive_acceptors.begin(), alive_acceptors.end(), *to_remove) != alive_acceptors.end()) {
            // The failed node is part of the acceptors.
            alive_acceptors.erase(*to_remove);
            paxos->signalFailure(*to_remove);
            break;
          }
        }
      }
    }
  }

  return 0;
}

#include <optional>

#include <signal.h>

#include <unistd.h>

#include <dory/conn/ud.hpp>

#include <dory/shared/logger.hpp>
#include <dory/shared/pinning.hpp>

#include <lyra/lyra.hpp>

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
      throw std::runtime_error("WC not successful: " +
                               std::to_string(+wce[0].status));
    }
    break;
  }
}

static void wait_signal(void* grh_dest, dory::conn::McGroup& mb) {
  if (!mb.ud()->postRecv(0, grh_dest, 0)) {
    throw std::runtime_error("Post RECV signal error.");
  }
  wait_completion(mb.ud());
}

static auto main_logger = dory::std_out_logger("Sync killer");

int main(int argc, char* argv[]) {
  std::cout << "PID" << getpid() << "PID" << std::endl;

  std::optional<int> pid_to_kill;
  std::string serialized_sync_kill_mc_group;
  std::optional<int> pin_to_core;

  //// Parse Arguments ////
  lyra::cli cli;

  bool get_help = false;

  cli.add_argument(lyra::help(get_help))
      .add_argument(lyra::opt(pid_to_kill, "pid")
                        .name("-p")
                        .name("--pid-to-kill")
                        .help("PID to kill"))
      .add_argument(lyra::opt(pin_to_core, "id")
                        .name("-c")
                        .name("--core")
                        .help("Pin to core"))
      .add_argument(lyra::opt(serialized_sync_kill_mc_group,
                              "serialized_sync_kill_mc_group")
                        .required()
                        .name("-m")
                        .name("--mc-group")
                        .help("Serialized multicast group info"));

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

  if (pin_to_core) {
    dory::pin_main_to_core(*pin_to_core);
    LOGGER_INFO(main_logger, "Pinning to core {}", *pin_to_core);
  } else {
    LOGGER_INFO(main_logger, "Not pinning to any specific core");
  }


  LOGGER_INFO(main_logger, "Using multicast group {}",
              serialized_sync_kill_mc_group);

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

  dory::ctrl::ControlBlock cb(resolved_port);

  cb.registerPd("primary-pd");
  cb.registerCq("cq-signal");

  //// Allocate RDMA Memory ////
  cb.allocateBuffer("grh-buf", 40, 64);
  cb.registerMr("grh-mr", "primary-pd", "grh-buf",
                dory::ctrl::ControlBlock::LOCAL_READ |
                    dory::ctrl::ControlBlock::LOCAL_WRITE |
                    dory::ctrl::ControlBlock::REMOTE_WRITE );
  
  //// Main ////
  auto ud = std::make_shared<dory::conn::UnreliableDatagram>(
      cb, "primary-pd", "grh-mr", "cq-signal", "cq-signal");

  dory::conn::McGroup sync_kill_mc_group(cb, "primary-pd", ud,
                                         serialized_sync_kill_mc_group);

  if (pid_to_kill) {
    LOGGER_INFO(main_logger, "Waiting for signal to kill {}.", *pid_to_kill);
    wait_signal(reinterpret_cast<void*>(cb.mr("grh-mr").addr),
                sync_kill_mc_group);
    if (kill(*pid_to_kill, SIGKILL) != 0) {
      throw std::runtime_error("Failed to kill.");
    }
    LOGGER_INFO(main_logger, "Killed.");
  } else {
    LOGGER_INFO(main_logger, "No PID given, sending kill signal.");
    dory::pin_main_to_core(4);
    sync_kill_mc_group.postSend(0, 0, 0);
    wait_completion(sync_kill_mc_group.ud());
    LOGGER_INFO(main_logger, "Signal sent.");
  }
  return 0;
}

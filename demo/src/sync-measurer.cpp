#include <algorithm>
#include <chrono>
#include <iostream>
#include <optional>
#include <thread>

#include <signal.h>

#include <unistd.h>

#include <dory/conn/rc-exchanger.hpp>
#include <dory/conn/rc.hpp>
#include <dory/ctrl/block.hpp>
#include <dory/memstore/store.hpp>

#include <dory/shared/logger.hpp>
#include <dory/shared/pinning.hpp>

#include <lyra/lyra.hpp>

static void wait_completion(dory::conn::ReliableConnection& rc) {
  std::vector<struct ibv_wc> wce;
  while (true) {
    wce.resize(1);
    if (!rc.pollCqIsOk(dory::conn::ReliableConnection::Cq::SendCq, wce)) {
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

using ProcIdType = int;

static auto main_logger = dory::std_out_logger("meas measurer");

int main(int argc, char* argv[]) {
  std::cout << "PID" << getpid() << "PID" << std::endl;

  bool measurer = false;
  std::optional<int> pin_to_core;

  //// Parse Arguments ////
  lyra::cli cli;

  bool get_help = false;

  cli.add_argument(lyra::help(get_help))
      .add_argument(lyra::opt(measurer)
                        .name("-m")
                        .name("--measurer")
                        .help("If set, measures. Otherwise, reply."))
      .add_argument(lyra::opt(pin_to_core, "id")
                        .name("-c")
                        .name("--core")
                        .help("Pin to core"));

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

  // We configure the control block.
  dory::ctrl::ControlBlock cb(resolved_port);
  cb.registerPd("primary");
  cb.allocateBuffer("shared-buf", sizeof(int) * 2, 64);
  cb.registerMr("shared-mr", "primary", "shared-buf",
                dory::ctrl::ControlBlock::LOCAL_READ |
                    dory::ctrl::ControlBlock::LOCAL_WRITE |
                    dory::ctrl::ControlBlock::REMOTE_READ |
                    dory::ctrl::ControlBlock::REMOTE_WRITE);
  cb.registerCq("cq");

  // 3. We write our message (our id) to our shared memory.
  auto* written_slot = reinterpret_cast<int*>(cb.mr("shared-mr").addr);
  *written_slot = 0;
  auto* payload_slot = written_slot + 1;
  *payload_slot = 1;

  // 4. We establish reliable connections.
  auto& store = dory::memstore::MemoryStore::getInstance();

  ProcIdType my_id = measurer ? 1 : 2;
  std::vector<ProcIdType> remote_ids{3 - my_id};

  dory::conn::RcConnectionExchanger<ProcIdType> ce(my_id, remote_ids, cb);
  ce.configureAll("primary", "shared-mr", "cq", "cq");
  ce.announceAll(store, "qp");

  ce.announceReady(store, "qp", "prepared");
  ce.waitReadyAll(store, "qp", "prepared");

  ce.connectAll(store, "qp",
                dory::ctrl::ControlBlock::LOCAL_READ |
                    dory::ctrl::ControlBlock::LOCAL_WRITE |
                    dory::ctrl::ControlBlock::REMOTE_READ |
                    dory::ctrl::ControlBlock::REMOTE_WRITE);

  if (measurer) {
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }

  size_t constexpr Iterations = 1000;
  std::chrono::nanoseconds constexpr Expected(1000000000);

  std::vector<std::chrono::nanoseconds> ping_vec, meas_vec;
  // LatencyProfiler ping_profiler, meas_profiler;
  ping_vec.reserve(Iterations);
  meas_vec.reserve(Iterations);

  auto& rc = ce.connections().begin()->second;

  for (size_t i = 0; i < Iterations * 2; i++) {
    auto const meas = (i % 2) == 1;
    if (measurer) {
      std::this_thread::sleep_for(std::chrono::microseconds(50));
      *written_slot = 0;
      std::chrono::steady_clock::time_point start =
          std::chrono::steady_clock::now();
      rc.postSendSingle(dory::conn::ReliableConnection::RdmaWrite, 0,
                        payload_slot, sizeof(int), rc.remoteBuf());
      while (!*reinterpret_cast<int volatile*>(written_slot))
        ;
      std::chrono::steady_clock::time_point end =
          std::chrono::steady_clock::now();
      *written_slot = 0;
      if (meas) {
        // meas_profiler.addMeasurement(end - start);
        meas_vec.emplace_back(end - start);
      } else {
        // ping_profiler.addMeasurement(end - start);
        ping_vec.emplace_back(end - start);
      }
      wait_completion(rc);
    } else {
      while (!*reinterpret_cast<int volatile*>(written_slot))
        ;
      std::chrono::steady_clock::time_point start =
          std::chrono::steady_clock::now();
      *written_slot = 0;
      if (meas) {
        while (std::chrono::steady_clock::now() < start + Expected)
          ;
      }
      rc.postSendSingle(dory::conn::ReliableConnection::RdmaWrite, 0,
                        payload_slot, sizeof(int), rc.remoteBuf());
      wait_completion(rc);
    }
    if (meas) {
      std::cout << i / 2 + 1 << "/" << Iterations << std::endl;
    }
  }

  // ping_profiler.reportBuckets();
  // ping_profiler.report();
  // meas_profiler.reportBuckets();
  // meas_profiler.report();

  if (measurer) {
    for (auto const& e : ping_vec) {
      std::cout << "ping: " << e.count() << "ns" << std::endl;
    }

    for (auto const& e : meas_vec) {
      std::cout << "meas: " << e.count() << "ns" << std::endl;
    }

    auto const [min_ping, max_ping] =
        std::minmax_element(ping_vec.begin(), ping_vec.end());

    std::cout << "[min-max ping]: " << min_ping->count() << "ns, "
              << max_ping->count() << "ns" << std::endl;

    auto const [min_meas, max_meas] =
        std::minmax_element(meas_vec.begin(), meas_vec.end());

    std::cout << "[min-max measurement]: " << min_meas->count() << "ns, "
              << max_meas->count() << "ns" << std::endl;

    auto min_async = *min_meas - Expected - *min_ping;
    auto max_async = *max_meas - Expected - *min_ping;

    std::cout << "Clock diff: ["
              << std::chrono::duration_cast<std::chrono::nanoseconds>(min_async)
                     .count()
              << "ns, "
              << std::chrono::duration_cast<std::chrono::nanoseconds>(max_async)
                     .count()
              << "ns] every "
              << std::chrono::duration_cast<std::chrono::nanoseconds>(Expected)
                     .count()
              << "ns." << std::endl;

    std::cout << "Relative: ["
              << static_cast<double>(min_async.count()) /
                     static_cast<double>(Expected.count()) * 100.
              << "%, "
              << static_cast<double>(max_async.count()) /
                     static_cast<double>(Expected.count()) * 100.
              << "%]" << std::endl;
  }
  return 0;
}

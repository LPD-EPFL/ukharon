#include <chrono>
#include <iostream>
#include <sstream>
#include <thread>
#include <type_traits>

#include <unistd.h> // for getpid()

#include <lyra/lyra.hpp>

#include <dory/third-party/mica/mica.h>
#include <dory/conn/rc.hpp>
#include <dory/conn/ud.hpp>
#include <dory/ctrl/block.hpp>
#include <dory/ctrl/device.hpp>
#include <dory/shared/latency.hpp>
#include <dory/shared/pinning.hpp>
#include <dory/shared/units.hpp>

#include <dory/memstore/store.hpp>

#include <dory/memory/pool/pool-allocator.hpp>

#include <dory/rpc/conn/universal-connector.hpp>

#include "../common.hpp"
#include "../buffer.hpp"
#include "../rpc-slot.hpp"
#include "../mica.hpp"

#include "client.hpp"

static auto main_logger = dory::std_out_logger("HERD");

int main(int argc, char *argv[]) {
  std::cout << "PID" << getpid() << "PID" << std::endl;

  ProcIdType id;
  ProcIdType master_id;
  std::optional<ProcIdType> slave_id;
  std::string put_or_get;
  bool test_failover = false;

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
          lyra::opt(put_or_get, "put_or_get").required().name("-l").name("--load").choices("put", "get").help(
              "PUT or GET load"))
      .add_argument(
          lyra::opt(test_failover).optional().name("-f").name("--failover").help(
              "Simply measure latency or run the failover test"));

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



  dory::LatencyProfiler profiler_for_master(0);
  Client client_to_master(id, cb, "primary-pd", "primary-mr", "cq-master", arena_allocator, profiler_for_master);

  dory::LatencyProfiler profiler_for_slave(0);
  Client client_to_slave(id, cb, "primary-pd", "primary-mr", "cq-slave", arena_allocator, profiler_for_slave);

  dory::pin_main_to_core(dory::membership::LeaseCheckingCore);

  // Generate the access keys.
  auto keys = dory::deleted_unique_ptr<uint128>(mica_gen_keys(TEST_NUM_KEYS),
                                                [](uint128 *k) { free(k); });
  auto seed = static_cast<uint64_t>(id);
  permute_for(keys.get(), &seed);

  //// Main ////

  client_to_master.connect(master_id);
  client_to_slave.connect(slave_id.value());

  std::optional<std::chrono::steady_clock::time_point> last_on_master;
  std::optional<std::chrono::steady_clock::time_point> first_on_slave;

  auto master_in = true;
  auto slave_in = true;

  std::this_thread::sleep_for(std::chrono::seconds(3));
  std::cout << "Starting issuing " << put_or_get << " requests" << std::endl;

  using ReqType = std::add_pointer<void(struct mica_op &, uint128 *)>::type;
  ReqType req_type = nullptr;

  if (put_or_get == "put") {
    req_type = prepare_put_request;
  } else {
    req_type = prepare_get_request;
  }

  uint64_t i = 0;
  while (true) { // Main loop
    if (!master_in && !slave_in) {
      if (last_on_master && first_on_slave) {
        break;
      }

      continue;
    }

    if (i > 10000000UL) {
      break;
    }

    auto key_idx = hrd_fastrand(&seed) % TEST_NUM_KEYS;

    auto leader_id = master_in ? master_id : slave_id.value();

    if (leader_id == master_id) {
      auto poll_res = client_to_master.poll();

      auto failed = std::get<0>(poll_res);
      auto last_on_master_maybe = std::get<2>(poll_res);
      if (last_on_master_maybe) {
        last_on_master = last_on_master_maybe;
      }

      if (failed) {
        master_in = false;
      } else {
        auto accepted = client_to_master.post(req_type, keys.get() + key_idx);

        if (accepted) {
          i++;
        }
      }
    }

    if (leader_id == slave_id.value()) {
      auto poll_res = client_to_slave.poll();

      // Indicates the the request failed, i.e. the slave is still in follower mode
      auto failed = std::get<0>(poll_res);

      if (!failed) {
        auto first_on_slave_maybe = std::get<1>(poll_res);
        if (first_on_slave_maybe && !first_on_slave) {
          first_on_slave = first_on_slave_maybe;
        }
      }

      auto accepted = client_to_slave.post(req_type, keys.get() + key_idx);
      if (accepted) {
        i++;
      }
    }

    // {char x; std::cin >> x;}
  }

  if (!test_failover) {
    profiler_for_master.report();
    std::cout << "Failed gets " << client_to_master.failedGets() << std::endl;
  } else {
    // std::cout << "Is last_on_master broken? (us) " << std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - *last_on_master).count() << std::endl;
    // std::cout << "Is first_on_slave broken? (us) " << std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - *first_on_slave).count() << std::endl;
    std::cout << "Time to switch (us) " << std::chrono::duration_cast<std::chrono::microseconds>(*first_on_slave - *last_on_master).count() << std::endl;
  
    // profiler_for_master.reportBuckets();
    profiler_for_master.report();
    std::cout << "Failed gets " << client_to_master.failedGets() << std::endl;
  
    // profiler_for_slave.reportBuckets();
    profiler_for_slave.report();
    std::cout << "Failed gets " << client_to_slave.failedGets() << std::endl;
  }

  return 0;
}

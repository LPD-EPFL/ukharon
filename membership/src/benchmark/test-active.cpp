#include <iostream>
#include <optional>

#include <signal.h>
#include <unistd.h>  // for getpid()

#include <dory/rpc/server.hpp>

#include <lyra/lyra.hpp>

#include <dory/ctrl/block.hpp>
#include <dory/ctrl/device.hpp>

#include <dory/shared/latency.hpp>
#include <dory/shared/logger.hpp>
#include <dory/shared/units.hpp>
#include <dory/shared/unused-suppressor.hpp>

#include <dory/memstore/store.hpp>

// Define the membership config before the membership
#include "../default-config.hpp"

#include "../membership.hpp"

#include "inactivity.hpp"

static std::string exec(std::string const &cmd) {
  std::array<char, 128> buffer;
  std::string result;
  std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd.c_str(), "r"),
                                                pclose);
  if (!pipe) {
    throw std::runtime_error("popen() failed!");
  }
  while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
    result += buffer.data();
  }
  return result;
}

#ifdef MAJORITY_POLLING
#define POLLING_MODE (dory::membership::Membership::AcceptorMajority)
#define POLLING_MODE_STR "[MAJORITY] "
#endif

#ifdef CACHE_POLLING
#define POLLING_MODE (dory::membership::Membership::Cache)
#define POLLING_MODE_STR "[CACHE] "
#endif

#ifdef NO_POLLING
#define POLLING_MODE this_should_generate_an_error_if_used_in_code
#define POLLING_MODE_STR "this_should_generate_an_error_if_used_in_code"
#endif

#ifndef POLLING_MODE
#error "Must define POLLING_MODE"
#endif

#ifndef POLLING_MODE_STR
#error "Must define POLLING_MODE_STR"
#endif

#ifdef TEST_STEADY_CLK_OVERHEAD
static auto main_logger = dory::std_out_logger("TEST_STEADY_CLK_OVERHEAD");
#endif

#ifdef TEST_IS_ACTIVE_OVERHEAD
static auto main_logger =
    dory::std_out_logger(POLLING_MODE_STR "TEST_IS_ACTIVE_OVERHEAD");
#endif

#ifdef TEST_MEMBERSHIP_CHANGE_TIME
static auto main_logger =
    dory::std_out_logger(POLLING_MODE_STR "TEST_MEMBERSHIP_CHANGE_TIME");
#endif

#ifdef TEST_INACTIVITY_IN_STABLE_VIEW
static auto main_logger =
    dory::std_out_logger(POLLING_MODE_STR "TEST_INACTIVITY_IN_STABLE_VIEW");
#endif

#ifdef TEST_CRASH_TIME_TO_NEW_VIEW
static auto main_logger =
    dory::std_out_logger(POLLING_MODE_STR "TEST_CRASH_TIME_TO_NEW_VIEW");
#endif

#ifdef TEST_ASK_REMOVE
static auto main_logger =
    dory::std_out_logger(POLLING_MODE_STR "TEST_ASK_REMOVE");
#endif

#ifdef TEST_COORDINATED_FAILURES
static auto main_logger =
    dory::std_out_logger(POLLING_MODE_STR "TEST_COORDINATED_FAILURES");
#endif

using ProcIdType = dory::membership::ProcIdType;

int main(int argc, char *argv[]) {
  std::cout << "PID" << getpid() << "PID" << std::endl;

/* This test computes the overhead of the call to
 * `std::chrono::steady_clock::now()`. It takes a steady_clock timestamp in a
 * tight loop. It computes the time it took for the loop (which is timestamp +
 * profiler) and produces a profile of these timestamps.
 */
#ifdef TEST_STEADY_CLK_OVERHEAD
  int cpu_id;
  size_t iterations;

  //// Parse Arguments ////
  lyra::cli cli;
  bool get_help = false;

  cli.add_argument(lyra::help(get_help))
      .add_argument(lyra::opt(cpu_id, "cpu")
                        .required()
                        .name("-c")
                        .name("--cpu")
                        .help("CPU to pin"))
      .add_argument(lyra::opt(iterations, "iterations")
                        .required()
                        .name("-i")
                        .name("--iterations")
                        .help("How many iterations to test"));

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

  LOGGER_INFO(main_logger, "CPU: {}, Iterations: {}", cpu_id, iterations);

  dory::pin_main_to_core(cpu_id);
  std::this_thread::sleep_for(std::chrono::seconds(1));
  dory::LatencyProfiler profiler(1000000UL);

  auto old_ts = std::chrono::steady_clock::now();
  for (size_t i = 0; i < iterations; i++) {
    auto new_ts = std::chrono::steady_clock::now();
    profiler.addMeasurement(new_ts - old_ts);
    old_ts = new_ts;
  }

  profiler.reportBuckets();
  profiler.report();

  return 0;
#else
  /* The rest of the tests assume a deployment with 3 consensus nodes
   * and a member node.
   */

  ProcIdType id;
  std::string serialized_membership_mc_group;
  std::string serialized_kernel_mc_group;

#ifdef TEST_COORDINATED_FAILURES
  std::string serialized_sync_kill_mc_group;
  size_t wait_for;
#endif

  //// Parse Arguments ////
  lyra::cli cli;
  bool get_help = false;

  cli.add_argument(lyra::help(get_help))
      .add_argument(
          lyra::opt(id, "id").required().name("-p").name("--pid").help(
              "ID of the process"))
#ifdef TEST_COORDINATED_FAILURES
      .add_argument(
          lyra::opt(serialized_sync_kill_mc_group,
                    "serialized_sync_kill_mc_group")
              .name("-s")
              .name("--sync-killer-mc-group")
              .required()
              .help("Serialized multicast group used for sync killer"))
      .add_argument(lyra::opt(wait_for, "wait_for")
                        .name("-w")
                        .name("--wait-for")
                        .required()
                        .help("Number of new views to wait for"))
#endif
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
  dory::membership::Membership mc(cb, id, true, POLLING_MODE,
                                  serialized_membership_mc_group,
                                  serialized_kernel_mc_group);

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

  // mc.enableKernelHeartbeat();

  mc.start();

  auto membership_notifier = mc.membershipNotifier();
  auto &dv = mc.localActiveCache();

  dory::pin_main_to_core(dory::membership::LeaseCheckingCore);

  std::optional<std::pair<uint64_t, std::chrono::steady_clock::time_point> >
      view_data_inactive, view_data_before_kill;

  std::this_thread::sleep_for(std::chrono::seconds(1));

/* This test computes the latency overhead of using isActive before replying to
 * an application It involves taking a timestamp, using the profiler to store
 * the timestamp and also making a call to isActive.
 */
#ifdef TEST_IS_ACTIVE_OVERHEAD
  dory::LatencyProfiler profiler(1000000UL);

  bool started = false;

  while (true) {
    dory::membership::FullMembership new_membership;
    if (!membership_notifier->try_dequeue(new_membership)) {
      continue;
    }

    if (started) {
      profiler.reportBuckets();
      profiler.report();

      return 0;
    }

    if (!new_membership.includes(id).first) {
      continue;
    }

    started = true;

    auto view_id = new_membership.id;
    LOGGER_INFO(main_logger,
                "I got view that includes me, start profiling. I will stop "
                "profiling when view changes");

    auto old_ts = std::chrono::steady_clock::now();
    while (true) {
      auto now = std::chrono::steady_clock::now();
      auto [active, changed] = dv.isActiveOrViewChanged(view_id);
      profiler.addMeasurement(now - old_ts);
      old_ts = now;

      if (changed) {
        break;
      }
    }
  }
#endif

#ifdef TEST_MEMBERSHIP_CHANGE_TIME
  std::optional<std::pair<uint64_t, std::chrono::steady_clock::time_point> >
      view_data_last_active;

  while (true) {
    dory::membership::FullMembership new_membership;
    if (!membership_notifier->try_dequeue(new_membership)) {
      continue;
    }

    auto view_id = new_membership.id;
    while (true) {
      auto now = std::chrono::steady_clock::now();
      auto [active, changed] = dv.isActiveOrViewChanged(view_id);

      if (active && view_data_last_active) {
        auto [old_view_id, last_active] = view_data_last_active.value();
        if (view_id != old_view_id) {
          auto diff = std::chrono::duration_cast<std::chrono::microseconds>(
              now - last_active);
          std::cout << "From view " << old_view_id << " to view " << view_id
                    << " it took " << diff.count() << " us" << std::endl;
        }
      }

      if (active) {
        view_data_last_active = std::make_pair(view_id, now);
      }

      if (changed) {
        break;
      }
    }
  }
#endif

#ifdef TEST_INACTIVITY_IN_STABLE_VIEW
  std::optional<InactivityProfiler> profiler;
  profiler.emplace(1000000UL);

  bool started = false;

  while (true) {
    dory::membership::FullMembership new_membership;
    if (!membership_notifier->try_dequeue(new_membership)) {
      continue;
    }

    if (started) {
      profiler->reportBuckets();
      profiler->report();
      profiler.emplace(1000000UL);
      started = false;
    }

    if (!new_membership.includes(id).first) {
      continue;
    }

    started = true;

    auto view_id = new_membership.id;
    LOGGER_INFO(main_logger,
                "I got view that includes me, start profiling. I will stop "
                "profiling when view changes");

    std::cout << "View which I am checking is " << view_id << std::endl;

    while (true) {
      auto [active, changed] = dv.isActiveOrViewChanged(view_id);

      if (changed) {
        break;
      }

      profiler->feedEvent(active);
    }
  }
#endif

// For this test to work (and not block to the way it has been coded) do the
// following depending on whether there is a cache or not. If not cache exists
// (majority mode from member), start the test before starting paxos. If cache
// exits (cache mode from member), start the cache, then start paxos and add the
// cache. Finally, start the test and add it in the paxos leader.
#if defined(TEST_CRASH_TIME_TO_NEW_VIEW) || defined(TEST_ASK_REMOVE)
#ifdef TEST_ASK_REMOVE
  cb.registerCq("cq-ask-remove");
  auto ud_ask_remove = std::make_shared<dory::conn::UnreliableDatagram>(
      cb, dory::membership::ProtectionDomain, dory::membership::MemoryRegion,
      "cq-ask-remove", "cq-ask-remove");
  dory::conn::McGroup mc_ask_remove(cb, dory::membership::ProtectionDomain,
                                    ud_ask_remove, serialized_kernel_mc_group);

  auto &cq_ask_remove = cb.cq("cq-ask-remove");
  std::vector<struct ibv_wc> wce;
#endif

  std::optional<std::chrono::steady_clock::time_point> timestamp_before_kill;
  dory::LatencyProfiler profiler(0);
  size_t samples = 20;
  while (true) {
    dory::membership::FullMembership new_membership;
    if (!membership_notifier->try_dequeue(new_membership)) {
      continue;
    }

    if (samples-- == 0) {
      profiler.reportBuckets();
      profiler.report();
    }

    auto view_id = new_membership.id;

    auto includes_me = new_membership.includes(id).first;
    if (includes_me) {
#ifdef TEST_CRASH_TIME_TO_NEW_VIEW
      auto forked_pid =
          std::stoi(exec("./test_prctl_proxy -p " + std::to_string(+id)));
#endif
      std::this_thread::sleep_for(std::chrono::seconds(3));
      auto [active, changed] = dv.isActiveOrViewChanged(view_id);

      if (!active || changed) {
        LOGGER_INFO(main_logger, "Something is wrong");
        exit(1);
      }

      timestamp_before_kill = std::chrono::steady_clock::now();

#ifdef TEST_CRASH_TIME_TO_NEW_VIEW
      if (kill(forked_pid, SIGKILL) != 0) {
        throw std::runtime_error("Could not kill " +
                                 std::to_string(forked_pid));
      }
#else
      mc_ask_remove.postSend(0, nullptr, 0, static_cast<uint32_t>(id));

      do {
        wce.resize(1);
        cb.pollCqIsOk(cq_ask_remove, wce);
      } while (wce.empty());
#endif

      while (!dv.isActiveOrViewChanged(view_id).second) {
      }
    } else {
      while (!dv.isActiveOrViewChanged(view_id).first) {
      }

      if (timestamp_before_kill) {
        auto now = std::chrono::steady_clock::now();
        profiler.addMeasurement(now - timestamp_before_kill.value());
      }

      while (!dv.isActiveOrViewChanged(view_id).second) {
      }
    }
  }
#endif

#ifdef TEST_COORDINATED_FAILURES
  // Init
  cb.registerCq("cq-signal");
  auto ud = std::make_shared<dory::conn::UnreliableDatagram>(
      cb, dory::membership::ProtectionDomain, dory::membership::MemoryRegion,
      "cq-signal", "cq-signal");

  dory::conn::McGroup sync_kill_mc_group(cb, dory::membership::ProtectionDomain,
                                         ud, serialized_sync_kill_mc_group);

  auto &cq_signal = cb.cq("cq-signal");
  std::vector<struct ibv_wc> wce;
  std::vector<dory::membership::FullMembership> memberships;
  std::vector<std::chrono::steady_clock::time_point> timestamps;
  memberships.reserve(wait_for + 1);

  // Wait for the first membership to include us.
  dory::membership::FullMembership membership;
  while (!membership.includes(id).first) {
    membership_notifier->try_dequeue(membership);
  }

  std::cout << "First membership that includes me: " << membership.toString()
            << std::endl;

  size_t initial_cnt = membership.cnt;
  memberships.push_back(membership);

  std::this_thread::sleep_for(std::chrono::seconds(2));
  auto [active, changed] = dv.isActiveOrViewChanged(membership.id);
  if (!active || changed) {
    LOGGER_INFO(main_logger, "Something is wrong, active: {}, changed: {}",
                active, changed);
    exit(1);
  }

  auto view_to_check = membership.id;

  // Send kill signal.
  auto before_kill = std::chrono::steady_clock::now();
  sync_kill_mc_group.postSend(0, nullptr, 0);

  do {
    wce.resize(1);
    cb.pollCqIsOk(cq_signal, wce);
  } while (wce.empty());

  if (wce[0].status != IBV_WC_SUCCESS) {
    throw std::runtime_error("Posting signal failed.");
  }

  // Measure the last active.
  std::optional<std::chrono::steady_clock::time_point> last_active;
  // bool compatible_views = true;

#if 0
  std::optional<std::chrono::steady_clock::time_point> first_active4, first_active5;
  std::chrono::steady_clock::time_point last_active4;

  while (true) {
    auto [active4, changed4] = dv.isActiveOrViewChanged(4);
    auto [active5, changed5] = dv.isActiveOrViewChanged(4);

    auto now = std::chrono::steady_clock::now();
    if (active4) {
      if (!first_active4) {
        first_active4 = now;
      }

      last_active4 = now;
    }

    if (active5) {
      if (!first_active5) {
        first_active5 = now;
        break;
      }
    }
  }

  std::cout << "a->a: "
    << std::chrono::duration_cast<std::chrono::nanoseconds>(new_active - *last_active).count()
    << " ns, s->a: "
    << std::chrono::duration_cast<std::chrono::nanoseconds>(new_active - before_kill).count()
    << " ns" << std::endl;
#else
  bool guard = true;
  while (true) {
    // Wait for `wait_for` views and check if views are still compatible
    // with the initial view
    if (membership.cnt + wait_for > initial_cnt) {
      if (membership_notifier->try_dequeue(membership)) {
        // compatible_views &= membership.isCompatible(memberships.back());
        memberships.push_back(membership);
        timestamps.push_back(std::chrono::steady_clock::now());
      }
    }

    if (guard) {
      // We assume correlated failures, thus we will go from the initial view
      // to `wait_for` views ahead.
      auto const [active, changed] = dv.isActiveOrViewChanged(view_to_check);
      if (active) {
        last_active = std::chrono::steady_clock::now();
      }

      if (changed) {
        guard = false;
      }
    }

    if (membership.cnt + wait_for <= initial_cnt) {
      if (dv.isActiveOrViewChanged(memberships.back().id).first) {
        // if (compatible_views || !active) {
        break;
      }
    }

    // if (membership.cnt + wait_for <= initial_cnt) {
    //   if (dv.isActiveOrViewChanged(memberships.back().id).first) {
    //   // if (compatible_views || !active) {
    //     break;
    //   }
    // }
  }

  if (!last_active) {
    throw std::runtime_error(
        "View changed right after sending kill, very unlikely");
  }

  // Wait for the target view to become active
  // while (!dv.isActiveOrViewChanged(memberships.back().id).first) {}

  std::chrono::steady_clock::time_point new_active =
      std::chrono::steady_clock::now();
  std::cout << "a->a: "
            << std::chrono::duration_cast<std::chrono::nanoseconds>(
                   new_active - *last_active)
                   .count()
            << " ns, s->a: "
            << std::chrono::duration_cast<std::chrono::nanoseconds>(new_active -
                                                                    before_kill)
                   .count()
            << " ns" << std::endl;

  for (size_t i = 0; i < memberships.size() - 1; i++) {
    std::cout << "Going from " << memberships[i].toString() << " to "
              << memberships[i + 1].toString() << ", compatible: "
              << memberships[i].isCompatible(memberships[i + 1]) << std::endl;
  }

  if (!timestamps.empty()) {
    std::cout << "last membership received/active diff "
              << std::chrono::duration_cast<std::chrono::nanoseconds>(
                     new_active - timestamps.back())
                     .count()
              << std::endl;
    std::cout << "last membership received from kill "
              << std::chrono::duration_cast<std::chrono::nanoseconds>(
                     timestamps.back() - before_kill)
                     .count()
              << std::endl;
  } else {
    std::cout << "The last membership became active before receiving its full "
                 "membership"
              << std::endl;
  }
#endif

  return 0;
#endif
#endif
}

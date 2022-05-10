/* This program checks if two cores have their steady clocks in sync.
 * It has two threads A and B, each pinned on a different core.
 * Thread A takes a timestamp (called start) and sends a signal (using a queue)
 * to thread B Thread B takes another timestamp (called middle) and sends it
 * back to thread A. Finally, when thread A receives the timestamp from thread
 * B, it takes a last timestampt (called end). Thread A measures the time
 * difference end - start and checks that start < middle < end. The check must
 * be always true for every pair of threads, otherwise the clocks of the cores
 * are out of sync. From the end - start difference we get the worst case clock
 * difference between the two cores.
 */

#include <atomic>
#include <cstdlib>
#include <iostream>
#include <thread>
#include <vector>

#include <dory/shared/latency.hpp>
#include <dory/shared/logger.hpp>
#include <dory/shared/pinning.hpp>
#include <dory/shared/unused-suppressor.hpp>

#include <dory/third-party/sync/spsc.hpp>

#include <lyra/lyra.hpp>

using Timestamp = std::chrono::steady_clock::time_point;

static bool isInSync(Timestamp const &start, Timestamp const &middle,
                     Timestamp const &end) {
  return start < middle && middle < end;
}

struct Measurement {
  Timestamp start;
  Timestamp middle;
  Timestamp end;

  Measurement(Timestamp const &s, Timestamp const &m,
              Timestamp const &e) noexcept
      : start{s}, middle{m}, end{e} {}

  bool isValid() const { return isInSync(start, middle, end); }

  auto diff() const { return end - start; }

  int64_t diffCount() const {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(diff()).count();
  }
};

static auto main_logger = dory::std_out_logger("TEST_TSC_SYNC");

int main(int argc, char *argv[]) {
  int cpu_id_a;
  int cpu_id_b;
  size_t iterations;

  //// Parse Arguments ////
  lyra::cli cli;
  bool get_help = false;

  cli.add_argument(lyra::help(get_help))
      .add_argument(lyra::opt(cpu_id_a, "cpu-a")
                        .required()
                        .name("-a")
                        .name("--cpu-a")
                        .help("Where to pin thread a"))
      .add_argument(lyra::opt(cpu_id_b, "cpu-b")
                        .required()
                        .name("-b")
                        .name("--cpu-b")
                        .help("Where to pin thread b"))
      .add_argument(lyra::opt(iterations, "iteartions")
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

  LOGGER_INFO(main_logger, "Cpu A: {}, Cpu B: {}, Iterations: {}", cpu_id_a,
              cpu_id_b, iterations);

  dory::LatencyProfiler profiler(0);

  int constexpr QueueSize = 256 * 256;
  dory::third_party::sync::SpscQueue<bool> a_to_b(QueueSize);
  dory::third_party::sync::SpscQueue<Timestamp> b_to_a(QueueSize);

  std::atomic<bool> started_b(false);

  std::thread thread_b([&]() noexcept {
    started_b.store(true);

    for (size_t i = 0; i < iterations; i++) {
      bool signal_received;
      while (!a_to_b.try_dequeue(signal_received)) {
      }

      auto middle = std::chrono::steady_clock::now();
      b_to_a.try_enqueue(middle);
    }
  });

  // Pin and wait to start
  dory::pin_thread_to_core(thread_b, cpu_id_b);
  while (!started_b.load()) {
  }

  std::thread thread_a([&]() noexcept {
    for (size_t i = 0; i < iterations; i++) {
      auto start = std::chrono::steady_clock::now();
      a_to_b.try_enqueue(true);

      Timestamp middle;
      while (!b_to_a.try_dequeue(middle)) {
      }

      auto end = std::chrono::steady_clock::now();

      if (!isInSync(start, middle, end)) {
        LOGGER_ERROR(main_logger, "Oops");
        exit(1);
      }

      profiler.addMeasurement(end - start);
    }
  });

  dory::pin_thread_to_core(thread_a, cpu_id_a);

  thread_a.join();
  thread_b.join();

  profiler.reportBuckets();
  profiler.report();

  return 0;
}

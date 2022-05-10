
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <ctime>

#include <sched.h>
#include <sys/prctl.h>
#include <unistd.h>

#include <sys/ipc.h>
#include <sys/shm.h>

#include <dory/shared/pinning.hpp>
#include <dory/special/heartbeat.hpp>

#include "size.h"

int main() {
  pid_t pid = getpid();
  printf("Starting process with PID=%u\n", pid);

  dory::pin_main_to_core(4);
  printf("Benchmark runs on core %d\n", sched_getcpu());

  printf("Enable heartbeat notification upon exit\n");

  dory::special::enable_heartbeat(10);

  int id = shmget(static_cast<int>(SHM_MEM_KEY), size, 0);

  if (id == -1) {
    perror("shmget");
    return 1;
  }
  auto *ptr = reinterpret_cast<uint64_t *>(shmat(id, nullptr, 0));
  if (reinterpret_cast<void *>(ptr) == reinterpret_cast<void *>(-1)) {
    perror("shmat");
    return 1;
  }

  printf("Simulate process execution with sleep\n");
  sleep(10);

  struct timespec start_ts;
  if (clock_gettime(CLOCK_MONOTONIC, &start_ts) == -1) {
    perror("clock_gettime");
    return 1;
  }

  *ptr = static_cast<uint64_t>(start_ts.tv_sec) *
             static_cast<uint64_t>(1000000000) +
         static_cast<uint64_t>(start_ts.tv_nsec);

  if (kill(pid, SIGKILL) == -1) {
    perror("kill");
  }
  return 0;
}

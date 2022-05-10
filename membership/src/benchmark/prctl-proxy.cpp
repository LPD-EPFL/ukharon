#include <iostream>
#include <thread>
#include <chrono>
#include <cstdlib>
#include <cstring>

#include <sys/stat.h>
#include <unistd.h>
#include <sched.h>

#include <dory/special/heartbeat.hpp>
#include <dory/shared/pinning.hpp>

#include <lyra/lyra.hpp>

int main(int argc, char* argv[]) {
  dory::reset_main_pinning();
  using ProcIdType = uint16_t;

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

  // Fork off the parent process
  auto pid = fork();
  if (pid < 0) {
    exit(EXIT_FAILURE);
  }

  // If we got a good PID, then we can exit the parent process.
  if (pid > 0) {
    std::cout << pid << std::endl;
    exit(EXIT_SUCCESS);
  }

  // Change the file mode mask
  umask(0);

  // Change the current working directory
  if ((chdir("/")) < 0) {
    exit(EXIT_FAILURE);
  }

  // Close out the standard file descriptors
  close(STDIN_FILENO);
  close(STDOUT_FILENO);
  close(STDERR_FILENO);

  // We have observed that a process that runs with RT scheduler is cleaned
  // up faster than other processes.
  // Thus, when using the kernel heartbeat module, it is better to spawn in
  // in a thread that runs in real-time fifo scheduler.
  // If you uncomment the following, you will get worse failover time.

  // struct sched_param param = {};
  // param.sched_priority = 0;

  // if (sched_setscheduler(getpid(), SCHED_OTHER, &param) != 0) {
  //   throw std::runtime_error("sched_setscheduler failed: " + std::string(std::strerror(errno)));
  // }


  dory::special::enable_heartbeat(id);

  // If sleeping for long periods (e.g. 100s), then the kernel
  // is very slow (e.g. 40us instead of 10us) to kill the process.
  // With 1s it works and doesn't consume cpu in our setup.
  // Alternatively, do a busy wait
  while (true) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  return 0;
}

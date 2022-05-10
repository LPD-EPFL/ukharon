#include <cstdint>
#include <cstring>
#include <fstream>
#include <limits>
#include <sstream>
#include <stdexcept>

#define _POSIX_C_SOURCE 200809L  // NOLINT
#include <unistd.h>              // sysconf

#include <lyra/lyra.hpp>

struct PagemapEntry {
  uint64_t pfn : 55;
  uint8_t soft_dirty : 1;
  uint8_t file_page : 1;
  uint8_t swapped : 1;
  uint8_t present : 1;
};

uintptr_t virt_to_phys_user(pid_t pid, uintptr_t vaddr);
void start_rtcore(uintptr_t physical_address);
void stop_rtcore();

static PagemapEntry pagemap_get_entry(std::ifstream &process_pagemap,
                                      uintptr_t vaddr, uintptr_t pagesize) {
  size_t nread;
  ssize_t ret;
  uint64_t data = 0;

  auto vpn = static_cast<size_t>(vaddr / pagesize);
  auto offset = vpn * sizeof(data);

  auto max_seek_offset = std::numeric_limits<std::streamoff>::max();

  auto reminder_offset = offset % static_cast<size_t>(max_seek_offset);
  auto times_max_offset = offset / static_cast<size_t>(max_seek_offset);
  bool large_offset = false;

  for (size_t i = 0; i < times_max_offset; i++) {
    if (i == 0) {
      process_pagemap.seekg(max_seek_offset, std::ifstream::beg);
    } else {
      process_pagemap.seekg(max_seek_offset, std::ifstream::cur);
      large_offset = true;
    }
  }

  if (reminder_offset > 0) {
    process_pagemap.seekg(
        static_cast<std::streamoff>(reminder_offset),
        large_offset ? std::ifstream::cur : std::ifstream::beg);
  }

  if (!process_pagemap.good()) {
    throw std::runtime_error("Could not seek at the specified pagemap offset");
  }

  process_pagemap.read(reinterpret_cast<char *>(&data), sizeof(data));

  if (!process_pagemap.good()) {
    throw std::runtime_error("Could not read at the specified pagemap offset");
  }

  PagemapEntry entry;
  entry.pfn = data & ((uint64_t(1) << 55) - 1);
  entry.soft_dirty = uint8_t(data >> 55) & 1;
  entry.file_page = uint8_t(data >> 61) & 1;
  entry.swapped = uint8_t(data >> 62) & 1;
  entry.present = uint8_t(data >> 63) & 1;

  return entry;
}

uintptr_t virt_to_phys_user(pid_t pid, uintptr_t vaddr) {
  long pagesize_s = sysconf(_SC_PAGE_SIZE);
  if (pagesize_s == -1) {
    throw std::runtime_error("Could not query the virtual page size: " +
                             std::string(std::strerror(errno)));
  }

  auto pagesize = static_cast<uintptr_t>(pagesize_s);

  std::stringstream ss;
  ss << "/proc/" << pid << "/pagemap";

  std::ifstream pagemap;
  pagemap.rdbuf()->pubsetbuf(nullptr, 0);
  pagemap.open(ss.str());

  PagemapEntry entry = pagemap_get_entry(pagemap, vaddr, pagesize);

  pagemap.close();

  return (entry.pfn * pagesize) + (vaddr % pagesize);
}

void stop_rtcore() {
  std::ofstream sysfs;
  sysfs.open("/sys/kernel/rt_core");

  sysfs << "0 0x0"
        << "\n";

  if (!sysfs.good()) {
    throw std::runtime_error("Could not stop the RT core");
  }

  sysfs.close();
}

void start_rtcore(uintptr_t physical_address = 0) {
  std::ofstream sysfs;
  sysfs.open("/sys/kernel/rt_core");
  sysfs << "1 "
        << "0x" << std::hex << physical_address << "\n";

  if (!sysfs.good()) {
    throw std::runtime_error("Could not start the RT core");
  }

  sysfs.close();
}

int main(int argc, char **argv) {
  bool start_dry = false;
  bool stop = false;

  bool resolve = false;
  pid_t pid;
  std::string vaddress;
  bool start = false;

  // Did the user ask for help?
  bool get_help = false;

  lyra::cli cli;
  cli.add_argument(lyra::help(get_help))
      .add_argument(lyra::group().add_argument(
          lyra::opt(start_dry)
              .required()
              .name("-n")
              .name("--dry-start")
              .help(
                  "Start the RT Core without incrementing a memory location.")))
      .add_argument(lyra::group().add_argument(
          lyra::opt(stop).required().name("-s").name("--stop").help(
              "Stop the RT Core.")))
      .add_argument(
          lyra::group(
              [&](const lyra::group & /*unused*/) noexcept { resolve = true; })
              .add_argument(lyra::opt(pid, "pid")
                                .required()
                                .name("-p")
                                .name("--pid")
                                .help("PID of the process"))
              .add_argument(lyra::opt(vaddress, "vaddress")
                                .required()
                                .name("-a")
                                .name("--address")
                                .help("Virtual address to "
                                      "resolve to physical address"))
              .add_argument(lyra::opt(start).name("-s").name("--start").help(
                  "Start the RT Core with "
                  "incrementing the memory location")));

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

  if (int(start_dry) + int(stop) + int(resolve) != 1) {
    std::cerr << "Specify exactly one group\n\n";
    std::cout << cli;
    return 1;
  }

  if (start_dry) {
    start_rtcore();
    return 0;
  }

  if (stop) {
    stop_rtcore();
    return 0;
  }

  // Resolving the address
  unsigned long long addr = 0;
  char *end;

  addr = strtoull(vaddress.c_str(), &end, 0);

  if (errno == ERANGE) {
    throw std::runtime_error("Could not parse the virtual address: " +
                             std::string(std::strerror(errno)));
  }

  auto paddr = virt_to_phys_user(pid, addr);
  std::cout << "0x" << std::hex << paddr << std::endl;

  start_rtcore(paddr);

  return 0;
}

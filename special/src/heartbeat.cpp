#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <string>

#include <sys/prctl.h>
#include <sys/utsname.h>

#include <dory/shared/logger.hpp>

#include "heartbeat.hpp"

#ifndef PR_SET_HEARTBEAT
#define PR_SET_HEARTBEAT 57
#endif

namespace dory::special {

auto logger = dory::std_out_logger("SPECIAL");

void enable_heartbeat(int data) {
  auto info = heartbeat::kernel_version();
  auto found = heartbeat::is_kernel_compatible(info, true);

  if (!found) {
    LOGGER_WARN(
        logger,
        "The current kernel is not compatible with any of the supported "
        "kernels. The heartbeat mechanism is not supported");
    return;
  }

  // Try to enable the heartbeat mechanism
  auto ret_prctl = prctl(PR_SET_HEARTBEAT, data, 0, 0, 0);
  if (ret_prctl != 0) {
    throw std::runtime_error(
        "Could not use prctl to enable the heartbeat for this process (" +
        std::to_string(errno) + "): " + std::string(std::strerror(errno)));
  }
}

namespace heartbeat {
std::optional<KernelInfo> from_serialized_kernel_info(std::string const& info) {
  size_t release_size;
  size_t version_size;
  size_t arch_size;

  std::stringstream ss(info);

  ss >> release_size;
  ss.ignore();

  ss >> version_size;
  ss.ignore();

  ss >> arch_size;
  ss.ignore();

  std::string release(release_size, '\0');
  ss.read(&release[0], static_cast<std::streamsize>(release_size));

  std::string version(version_size, '\0');
  ss.read(&version[0], static_cast<std::streamsize>(version_size));

  std::string arch;
  ss >> arch;

  return KernelInfo(release, version, arch);
}

KernelInfo kernel_version() {
  struct utsname buf;

  // Check the kernel version
  auto ret_uname = uname(&buf);
  if (ret_uname == -1) {
    throw std::runtime_error("Could not read the kernel version (" +
                             std::to_string(errno) +
                             "): " + std::string(std::strerror(errno)));
  }

  return KernelInfo(buf.release, buf.version, buf.machine);
}

bool is_kernel_compatible(KernelInfo const& info, bool relaxed) {
  for (const auto& current : compatible_kernels) {
    if (info.release != current.release) {
      continue;
    }

    if (info.arch != current.arch) {
      continue;
    }

    if (relaxed) {
      if (info.version.find(current.version) != std::string::npos) {
        return true;
      }
    } else {
      if (info.version == current.version) {
        return true;
      }
    }
  }

  return false;
}

}  // namespace heartbeat
}  // namespace dory::special

#pragma once

#include <cstdint>
#include <optional>
#include <sstream>
#include <string>
#include <utility>

namespace dory::special {
void enable_heartbeat(int data);

namespace heartbeat {
struct KernelInfo {
  KernelInfo(std::string release, std::string version, std::string arch)
      : release{std::move(release)},
        version{std::move(version)},
        arch{std::move(arch)} {}

  std::string const release;
  std::string const version;
  std::string const arch;

  std::string serialize() const {
    std::ostringstream s;
    s << release.size() << "," << version.size() << "," << arch.size() << ","
      << release << version << arch;

    return s.str();
  }
};

std::optional<KernelInfo> from_serialized_kernel_info(std::string const &info);

static KernelInfo const compatible_kernels[] = {
    KernelInfo{"5.4.0-74-custom", "#83+rtcore+heartbeat+nohzfull", "x86_64"}};

std::size_t constexpr CompatibleKernelsNum =
    sizeof(compatible_kernels) / sizeof(KernelInfo);

KernelInfo kernel_version();
bool is_kernel_compatible(KernelInfo const &info, bool relaxed = false);
}  // namespace heartbeat
}  // namespace dory::special

#include <memory>

#include "internal/logger.hpp"

namespace dory {

static auto constexpr FormatStrDefault = "[%n:%^%l%$] %v";
static auto constexpr FormatStrWithSource = "[%n:%^%l%$:%@] %v";

using logger = std::shared_ptr<spdlog::logger>;
logger std_out_logger(std::string const &prefix);
}  // namespace dory

namespace dory {

logger std_out_logger(std::string const &prefix) {
  auto logger = spdlog::get(prefix);

  if (logger == nullptr) {
    logger = spdlog::stdout_color_mt(prefix);

    logger->set_pattern(FormatStrDefault);
  }

  spdlog::cfg::load_env_levels();

  return logger;
}
}  // namespace dory

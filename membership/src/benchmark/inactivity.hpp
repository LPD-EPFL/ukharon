#pragma once

#include <chrono>

#include <dory/shared/latency.hpp>

class InactivityProfiler : public dory::LatencyProfiler {
 public:
  InactivityProfiler(size_t skip = 0) : dory::LatencyProfiler(skip) {}

  void feedEvent(bool active) {
    events_total++;
    if (active) {
      if (!first_active) {
        first_active = events_total;
      }

      last_active = events_total;

      active_total++;
      at_least_once_active = true;
      auto now = std::chrono::steady_clock::now();

      if (falling_edge) {
        auto became_inactive = falling_edge.value();
        addMeasurement(now - became_inactive);
        falling_edge.reset();
      }
    } else if (at_least_once_active && !falling_edge) {
      inactive_periods_cnt++;
      // Start capturing periods of inactivity only
      // after there has been some active events.
      // Just transitioning to inactive
      auto now = std::chrono::steady_clock::now();
      falling_edge = now;
    }
  }

  void report() {
    std::cout << "Number of samples: " << events_total << "\n"
              << "Number of active samples: " << active_total << "\n"
              << "Number of inactive samples: " << events_total - active_total
              << "\n"
              << "Number of periods of inactivity: " << inactive_periods_cnt
              << "\n"
              << "First active sample: " << first_active.value() << "\n"
              << "Last active sample: " << last_active.value() << "\n"
              << "Distribution of periods of inactivity:"
              << "\n"
              << std::endl;
    dory::LatencyProfiler::report();
  }

 private:
  uint64_t events_total = 0;  // Counts all calls
  uint64_t active_total =
      0;  // Starts counting after it becomes active for the first time
  uint64_t inactive_periods_cnt =
      0;  // Starts counting after it becomes active for the first time

  std::optional<uint64_t> first_active;
  std::optional<uint64_t> last_active;

  std::optional<std::chrono::steady_clock::time_point>
      falling_edge;  // from active to inactive
  bool at_least_once_active = false;
};

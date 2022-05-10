#pragma once

#include <algorithm>
#include <chrono>
#include <memory>
#include <optional>
#include <set>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>
#include <mutex>
#include <shared_mutex>
#include <atomic>

#ifdef DORY_TIDIER_ON
#include "../../default-config.hpp"
#endif


// When using the shared_mutex, threads that call isActiveOrChangeView
// and need to update their cache can do it in parallel.
// This, however increases the latency.
#ifdef DORY_MEMBERSHIP_SHARED_MTX
#undef DORY_MEMBERSHIP_SHARED_MTX
#endif

namespace dory::membership::internal {

// // V is the view ID
// // M is the View
// template <typename V, typename M>
// class FullView {
//  public:
//   void storeView(M const &membership) {
//     if (membership.id <= latest_view_id) {
//       return;
//     }

//     std::unique_lock lock(mtx);
//     full_membership = membership;
//   }

//   std::optional<M> getNewerView(V view_id) {
//     std::shared_lock lock(mtx);
//     if (full_membership.id <= view_id) {
//       return std::nullopt;
//     }

//     return std::optional<M>(full_membership);
//   }

//  private:
//   std::shared_mutex mtx;

//   M full_membership;
//   V latest_view_id = 0;
// };

// M is the type of the membership ID
// U is the actual container storing the membership
template <typename ViewId, typename View>
class LocalActiveCache {
 private:
  struct ViewData {
    ViewId view_id;

    // When this view becomes active
    std::chrono::steady_clock::time_point start;

    // Until when is this view active
    std::chrono::steady_clock::time_point until;
  };

 public:
  using OptView = std::optional<ViewData>;

  LocalActiveCache(std::chrono::microseconds const &delta,
                   std::chrono::microseconds const &async)
      : delta{delta}, core_async{async} {}

  std::pair<bool, bool> isActiveOrViewChanged(ViewId id) {
    auto now = std::chrono::steady_clock::now();

    if (!cached_data || cached_data->view_id != id ||
        cached_data->start > now ||  // Probably not required
        cached_data->until < now) {
      if (cached_data && cached_data->view_id < id) {
        if (max_renewed_view_id < id) {
          return std::make_pair(false, false);
        }
      }

#ifdef DORY_MEMBERSHIP_SHARED_MTX
      std::shared_lock lock(mtx);
#else
      std::unique_lock lock(mtx);
#endif
      auto data_matches_id = dataFor(id);

      if (data_matches_id &&
          data_matches_id->get().start < now &&  // Probably not required
          data_matches_id->get().until > now) {
        cached_data = data_matches_id->get();
      } else if (max_view_id) {
        cached_data = dataFor(max_view_id.value())->get();
      }
    }

    if (!cached_data) {
      return std::make_pair(false, false);
    }

    if (cached_data->view_id != id) {
      return std::make_pair(false, true);
    }

    if (cached_data->start > now) {
      return std::make_pair(false, false);
    }

    if (now > cached_data->until) {
      return std::make_pair(false, false);
    }

    if (cached_data->start == cached_data->until) {
      return std::make_pair(false, false);
    }

    return std::make_pair(true, false);
  }

  void discovered(View const &membership) {
    std::unique_lock lock(mtx);
    if (latest_discovered && latest_discovered->first.id >= membership.id) {
      return;
    }
    auto const &previous_view = latest_discovered->first;
    auto compatible = latest_discovered &&
                      latest_discovered->first.id + 1 == membership.id &&
                      latest_discovered->first.isCompatible(membership);
    // if (!compatible) {
    //   std::cout << "not compatible: " << latest_discovered.has_value() <<
    //   std::endl; if (latest_discovered.has_value()) {
    //     std::cout << "previous:" << latest_discovered->first.toString() <<
    //     std::endl; std::cout << "new:" << membership.toString() << std::endl;
    //   }
    // }
    latest_discovered = {membership, compatible};
  }

  void barrier(ViewId id,
               std::optional<std::chrono::steady_clock::time_point> when = {}) {
    if (!when) {
      when = std::chrono::steady_clock::now();
    }

    std::unique_lock lock(mtx);

    if (id <= max_view_id) {
      throw std::runtime_error("Barriered views increase monotonically!");
    }

    max_view_id = id;

    auto &data = dataFor(id, false)->get();

    data.view_id = id;
    // Bypass the delta if the new view is compatible
    if (latest_discovered && latest_discovered->first.id == id &&
        latest_discovered->second && dataFor(id - 1)) {
      auto &prev_data = dataFor(id - 1)->get();
      data.start = prev_data.start;
      data.until = prev_data.until;
      // std::cout << "COULD leverage compatible views from view " << id-1 << "
      // to " << id << std::endl;
    } else {
      // std::cout << "could not leverage compatible from view " << id-1 << " to
      // " << id << std::endl;
      data.start = *when + delta + core_async;
      data.until = data.start;
    }
  }

  // Extend the lease
  void renew(ViewId id, std::chrono::steady_clock::time_point when) {
    std::unique_lock lock(mtx);

    auto max_renewed = max_renewed_view_id.load(std::memory_order_relaxed);
    if (id > max_renewed) {
      max_renewed_view_id.store(id);
    }

    auto ref = dataFor(id);

    if (!ref) {
      return;
    }

    auto &data = ref->get();

    if (when + delta - core_async > data.until) {
      data.until = when + delta - core_async;
    }
  }

  std::optional<std::reference_wrapper<ViewData>> dataFor(ViewId view_id,
                                                          bool check = true) {
    auto idx = view_id % MaxElem;
    if (!check || live_data[idx].view_id == view_id) {
      return live_data[idx];
    }

    return std::nullopt;
  }

 private:
  std::chrono::microseconds delta;
  std::chrono::microseconds core_async;

  static size_t constexpr MaxElem = 2;
  static thread_local OptView cached_data;

  std::optional<ViewId> max_view_id;
  std::optional<std::pair<View, bool>> latest_discovered;
  std::atomic<ViewIdType> max_renewed_view_id{0};
  ViewData live_data[MaxElem] = {};

#ifdef DORY_MEMBERSHIP_SHARED_MTX
  std::shared_mutex mtx;
#else
  std::mutex mtx;
#endif
};

template <typename ViewId, typename View>
thread_local typename LocalActiveCache<ViewId, View>::OptView
    LocalActiveCache<ViewId, View>::cached_data;

}  // namespace dory::membership::internal

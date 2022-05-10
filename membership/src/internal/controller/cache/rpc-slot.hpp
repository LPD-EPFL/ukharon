#pragma once

#include <cstdint>
#include <cstring>

#ifdef DORY_TIDIER_ON
#include "../../../default-config.hpp"
#endif

namespace dory::membership::internal {
// Used by the membership cache
/*
// Union-based implementation in which the app member
// asks the cache if a specific view is active
struct RpcSlot {
  static size_t constexpr bits = sizeof(uint64_t) * 8 - 1;

  using ViewIdType = dory::membership::ViewIdType;

  // Ask if the view with `view_id` is active or not
  union {
    struct {
      ViewIdType padding__ : bits;
      bool served : 1;
    } unpack;

    ViewIdType view_id;
  } req;

  static_assert(sizeof(req) <= sizeof(uint64_t),
    "Req in RpcSlot does not fit in an atomic unit of memory operation!");

  // Reply if the requested view is active or not
  struct {
    bool active;
  } resp;
};
*/

struct RpcSlot {
  using ViewIdType = dory::membership::ViewIdType;

  // Query for the latest active view
  struct Request {
    enum ReqState : uint8_t { Query = 1, NoQuery };

    ReqState state;
  } req;

  static_assert(
      sizeof(req) <= sizeof(uint64_t),
      "Req in RpcSlot does not fit in an atomic unit of memory operation!");

  // Reply if the requested view is active or not
  struct Response {
    ViewIdType view_id;
  } resp;

  inline bool written() const { return req.state == RpcSlot::Request::Query; }

  inline void arm() { req.state = RpcSlot::Request::NoQuery; }
};
}  // namespace dory::membership::internal

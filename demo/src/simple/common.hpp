#pragma once

#include <cstdint>
#include <stdexcept>

using ProcIdType = uint32_t;

struct RpcKind {
  enum Kind : uint8_t {
    RDMA_ACTIVE_VIEW_CONNECTION,
  };

  static char const* name(Kind k) {
    switch (k) {
      case RDMA_ACTIVE_VIEW_CONNECTION:
        return "ActiveViewConnectionRpcHandler";
      default:
        throw std::runtime_error("Unknown RpcKind name!");
    }
  }
};

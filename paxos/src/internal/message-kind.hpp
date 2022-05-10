#pragma once

#include <dory/conn/message-identifier.hpp>

namespace dory::paxos::internal {

class ReqKind : public dory::conn::BaseKind<ReqKind, uint64_t> {
 public:
  enum Value : uint64_t {
    WriteRealValue,
    Prepare,
    Accept,
    Poll,
    MAX_KIND_VALUE__ = 3
  };

  constexpr ReqKind(Value v) { value = v; }

  constexpr char const *toStr() const {
    switch (value) {
      case WriteRealValue:
        return "ReqKind::WriteRealValue";
      case Prepare:
        return "ReqKind::Prepare";
      case Accept:
        return "ReqKind::Accept";
      case Poll:
        return "ReqKind::Poll";
      default:
        return "Out of range";
    }
  }
};

}  // namespace dory::paxos::internal

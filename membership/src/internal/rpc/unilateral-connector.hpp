#pragma once

#include <sstream>
#include <string>
#include <utility>

#include <dory/conn/rc.hpp>
#include <dory/rpc/conn/rpc-request.hpp>
#include <dory/shared/types.hpp>

namespace dory::membership::internal {

template <typename ProcIdType, typename RpcKind>
class UnilateralConnectionRpcClient
    : public dory::rpc::conn::ConnectionRpcClient<ProcIdType, RpcKind> {
 public:
  UnilateralConnectionRpcClient(std::string const &ip, int port)
      : dory::rpc::conn::ConnectionRpcClient<ProcIdType, RpcKind>(ip, port) {}

  std::pair<bool, dory::uptrdiff_t> handshake(
      dory::conn::ReliableConnection &rc, ProcIdType my_id,
      ProcIdType remote_id, RpcKind kind) {
    if (!this->sendRpc(kind)) {
      return std::make_pair(false, 0);
    }

    if (!this->sendClientId(my_id)) {
      return std::make_pair(false, 0);
    }

    std::string serialized_info = rc.remoteInfo().serialize();
    if (!this->sendConnectionInfo(serialized_info)) {
      return std::make_pair(false, 0);
    }

    std::string remote_info;
    if (!this->recvConnectionInfo(remote_info)) {
      return std::make_pair(false, 0);
    }

    std::istringstream remote_info_stream(remote_info);
    std::string rc_info;
    dory::uptrdiff_t offset_info;
    remote_info_stream >> rc_info;
    remote_info_stream >> offset_info;

    auto remote_rc = dory::conn::RemoteConnection::fromStr(rc_info);
    rc.reset();
    rc.reinit();
    rc.connect(remote_rc, remote_id);

    if (!this->sendDone()) {
      return std::make_pair(false, 0);
    }

    if (!this->recvOk()) {
      return std::make_pair(false, 0);
    }

    return std::make_pair(true, offset_info);
  }
};
}  // namespace dory::membership::internal

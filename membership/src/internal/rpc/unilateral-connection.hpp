#pragma once

#include <cstdint>
#include <optional>
#include <unordered_map>
#include <utility>

#include <sstream>

#include <dory/conn/manager/uniform-rc.hpp>
#include <dory/rpc/abstract-handler.hpp>
#include <dory/rpc/conn/rpc-parser.hpp>

namespace dory::membership::internal {

template <typename ProcIdType, typename K>
class UnilateralConnectionRpcHandler
    : public dory::rpc::AbstractRpcHandler<typename K::Kind> {
 private:
  using EnumKind = typename K::Kind;

 public:
  struct MrMemoryLocation {
    void *location;
    uintptr_t mr_offset;
  };

  using Manager = dory::conn::manager::UniformRcConnectionManager<ProcIdType>;
  using Parser = dory::rpc::conn::ConnectionRpcHandlerParser<ProcIdType>;

  UnilateralConnectionRpcHandler(MrMemoryLocation const &hb_location,
                                 Manager *manager, EnumKind k)
      : location{hb_location},
        manager{manager},
        enum_kind{k},
        LOGGER_INIT(logger, K::name(k)) {}

  EnumKind kind() const override { return enum_kind; }

  void feed(uv_stream_t *client, ssize_t nread, char const *buf) override {
    // Create new parser or get existing one
    auto &connection = sessions[reinterpret_cast<uintptr_t>(client)];
    auto &parser = connection.parser;
    auto &proc_has_id = connection.proc_id;

    if (proc_has_id) {
      LOGGER_DEBUG(logger, "Using parser for client with id: {}",
                   proc_has_id.value());
    } else {
      LOGGER_DEBUG(logger, "Using parser for client with ptr: {}",
                   reinterpret_cast<uintptr_t>(client));
    }

    parser.feed(nread, buf);

    std::optional<typename Parser::Step> has_more;
    while ((has_more = parser.parse())) {
      auto step = has_more.value();
      switch (step) {
        case Parser::Step1: {
          auto proc_id = static_cast<ProcIdType>(parser.clientId());

          proc_has_id = proc_id;

          LOGGER_DEBUG(logger, "Process {} sent a connection request", proc_id);

          std::istringstream remote_info(parser.connectionInfo());
          std::string rc_info;
          remote_info >> rc_info;

          LOGGER_DEBUG(logger, "Process {} sent ReliableConnection info: {}",
                       proc_id, rc_info);
          auto &rc = manager->newConnection(proc_id, rc_info);
          connection.rc = &rc;
          auto rc_info_for_remote = rc.remoteInfo().serialize();

          LOGGER_DEBUG(logger, "Replying to process {}", proc_id);

          auto offset = location.mr_offset;

          LOGGER_DEBUG(logger, "Replying to process {}", proc_id);
          std::string local_serialized_info =
              rc_info_for_remote + " " + std::to_string(offset);

          auto len = static_cast<uint32_t>(local_serialized_info.size());
          this->write(client, sizeof(len), &len);
          this->write(client, local_serialized_info.size(),
                      local_serialized_info.data());

          break;
        }
        case Parser::Step2: {
          auto proc_id = static_cast<ProcIdType>(parser.clientId());
          LOGGER_DEBUG(logger, "Process {} sent DONE", proc_id);

          std::string tmp("OK");
          this->write(client, tmp.size(), tmp.data());

          break;
        }
        default:
          break;
      }
    }
  }

  void disconnected(uv_stream_t *client) override {
    auto connection_it = sessions.find(reinterpret_cast<uintptr_t>(client));

    if (connection_it != sessions.end()) {
      auto &connection = connection_it->second;
      auto &proc_has_id = connection.proc_id;

      if (proc_has_id) {
        LOGGER_DEBUG(
            logger,
            "Client with id {} disconnected. Destroying its RC connection",
            proc_has_id.value());
        if (connection.rc) {
          manager->removeConnection(proc_has_id.value());
        }
      } else {
        LOGGER_DEBUG(logger, "Client with ptr {} disconnected",
                     reinterpret_cast<uintptr_t>(client));
      }
    } else {
      LOGGER_DEBUG(logger, "Client with ptr {} disconnected",
                   reinterpret_cast<uintptr_t>(client));
    }

    if (sessions.erase(reinterpret_cast<uintptr_t>(client)) == 0) {
      LOGGER_WARN(logger, "Client {} did not have a session",
                  reinterpret_cast<uintptr_t>(client));
    }
  }

 private:
  struct Connection {
    Parser parser;
    std::optional<ProcIdType> proc_id;
    dory::conn::ReliableConnection *rc = nullptr;
  };

  std::unordered_map<uintptr_t, Connection> sessions;
  MrMemoryLocation location;
  Manager *manager;
  EnumKind enum_kind;

  LOGGER_DECL(logger);
};
}  // namespace dory::membership::internal

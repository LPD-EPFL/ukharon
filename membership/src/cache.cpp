#include <bitset>
#include <iostream>
#include <numeric>

#include <signal.h>
#include <unistd.h>  // for getpid()

#include <dory/rpc/server.hpp>

#include <lyra/lyra.hpp>

#include <dory/ctrl/block.hpp>
#include <dory/ctrl/device.hpp>

#include <dory/shared/logger.hpp>
#include <dory/shared/units.hpp>
#include <dory/shared/unused-suppressor.hpp>

#include <dory/memstore/store.hpp>

// Define the membership config before the membership
#include "default-config.hpp"

#include "membership.hpp"

static auto main_logger = dory::std_out_logger("Member");
using ProcIdType = dory::membership::ProcIdType;

int main(int argc, char *argv[]) {
  std::cout << "PID" << getpid() << "PID" << std::endl;

  ProcIdType id;
  bool no_deadbeat = false;
  std::string serialized_membership_mc_group;
  std::string serialized_kernel_mc_group;

  //// Parse Arguments ////
  lyra::cli cli;
  bool get_help = false;

  cli.add_argument(lyra::help(get_help))
      .add_argument(
          lyra::opt(id, "id").required().name("-p").name("--pid").help(
              "ID of the process"))
      .add_argument(
          lyra::opt(no_deadbeat)
              .name("--no-deadbeat")
              .help("Disables the deadbeat to force FD to be heartbeat-based."))
      .add_argument(
          lyra::opt(serialized_membership_mc_group,
                    "serialized_membership_mc_group")
              .required()
              .name("-m")
              .name("--membership-mc-group")
              .help("Serialized multicast group used for new memberships"))
      .add_argument(
          lyra::opt(serialized_kernel_mc_group, "serialized_kernel_mc_group")
              .required()
              .name("-k")
              .name("--kernel-mc-group")
              .help("Serialized multicast group used for kernel failures"));

  // Parse the program arguments.
  auto result = cli.parse({argc, argv});

  if (get_help) {
    std::cout << cli;
    return 0;
  }

  // Check that the arguments where valid.
  if (!result) {
    std::cerr << "Error in command line: " << result.errorMessage()
              << std::endl;
    return 1;
  }

  LOGGER_INFO(main_logger, "Process has ID {}", id);

  //// Setup RDMA ////
  LOGGER_INFO(main_logger, "Opening RDMA device ...");
  auto open_device = std::move(dory::ctrl::Devices().list().back());
  LOGGER_INFO(main_logger, "Device: {} / {}, {}, {}", open_device.name(),
              open_device.devName(),
              dory::ctrl::OpenDevice::typeStr(open_device.nodeType()),
              dory::ctrl::OpenDevice::typeStr(open_device.transportType()));

  size_t binding_port = 0;
  LOGGER_INFO(main_logger, "Binding to port {} of opened device {} ...",
              binding_port, open_device.name());
  dory::ctrl::ResolvedPort resolved_port(open_device);
  auto binded = resolved_port.bindTo(binding_port);
  if (!binded) {
    throw std::runtime_error("Couldn't bind the device.");
  }
  LOGGER_INFO(main_logger, "Binded successfully (port_id, port_lid) = ({}, {})",
              +resolved_port.portId(), +resolved_port.portLid());

  LOGGER_INFO(main_logger, "Configuring the control block");
  dory::ctrl::ControlBlock cb(resolved_port);

  //// Setup Membership Controller /////
  LOGGER_INFO(main_logger, "Setting up the Membership Controller");
  dory::membership::Membership mc(
      cb, id, true, dory::membership::Membership::AcceptorMajority,
      serialized_membership_mc_group, serialized_kernel_mc_group);

  mc.initializeControlBlock();

  //// Setup Connection Manager for App Members ////
  auto *cache_active_connections = mc.cacheActiveConnections();
  auto cache_active_connections_ud = mc.underlyingCacheActiveConnectionsUd();

  mc.startBackgroundThread();

  LOGGER_INFO(main_logger, "Setting up the RPC server");
  int initial_rpc_port = 7000;
  dory::membership::RpcServer rpc_server("0.0.0.0", initial_rpc_port);

  mc.registerRpcHandlers(rpc_server);

  LOGGER_INFO(main_logger, "Starting the RPC server");
  rpc_server.startOrChangePort();

  LOGGER_INFO(main_logger, "Announcing the process is online");
  dory::memstore::ProcessAnnouncer announcer;
  announcer.announceProcess(id, rpc_server.port());

  if (!no_deadbeat) {
    mc.enableKernelHeartbeat();
  }

  mc.start();

  dory::pin_main_to_core(dory::membership::LeaseCheckingCore);

  auto membership_notifier = mc.membershipNotifier();
  auto &dv = mc.localActiveCache();

  dory::membership::FullMembership new_membership;
  std::optional<dory::membership::FullMembership> next_membership;

  size_t outstanding_responses = 0;
  size_t constexpr OutstandingResponsesMax = 127;

  std::vector<struct ibv_wc> wce;
  auto &cq = mc.cacheReplyCq();

  std::array<dory::conn::UdBatch<OutstandingResponsesMax>,
             OutstandingResponsesMax>
      batched_responses;
  std::vector<size_t> available_batched_responses(OutstandingResponsesMax);
  std::iota(available_batched_responses.begin(),
            available_batched_responses.end(), 0);

  while (true) {  // Main loop
    if (next_membership || membership_notifier->try_dequeue(new_membership)) {
      // std::cout << new_membership.toString() << std::endl;

      if (next_membership) {
        new_membership = *next_membership;
        next_membership.reset();
      }

      auto const [includes_me, _] = new_membership.includes(id);
      while (includes_me) {
        auto view_id = new_membership.id;
        auto *connection_list = cache_active_connections->connections();
        std::bitset<dory::membership::CacheRpcSlotCnt> requests;

        wce.resize(outstanding_responses);
        if (!dory::ctrl::ControlBlock::pollCqIsOk(cq, wce)) {
          throw std::runtime_error("Error polling Cq!");
        }

        for (auto &e : wce) {
          auto batched_resp_idx = e.wr_id;

          if (e.status != IBV_WC_SUCCESS) {
            throw std::runtime_error("RDMA UD error!");
          }

          outstanding_responses -= batched_responses[batched_resp_idx].size();

          batched_responses[batched_resp_idx].reset();
          available_batched_responses.push_back(batched_resp_idx);
        }

        size_t index = 0;
        for (auto &[proc_id, c] : *connection_list) {
          if (*c.active) {
            auto &slot =
                *reinterpret_cast<dory::membership::CacheRpcSlot *>(c.slot);
            if (slot.written()) {
              requests[index] = true;
            }
          }
          index++;
        }  // for: mark requests

        auto [active, changed] = dv.isActiveOrViewChanged(view_id);

        if (!active) {
          if (changed) {
            break;  // Try fetching a new membership
          }

          continue;  // Wait for this membership to become active
        }

        if (!next_membership) {
          if (membership_notifier->try_dequeue(next_membership)) {
            if (next_membership->isCompatible(new_membership)) {
              break;
            }
          }
        }

        if (available_batched_responses.empty()) {
          continue;
        }

        if (requests.none()) {
          continue;
        }

        if (outstanding_responses > OutstandingResponsesMax) {
          continue;
        }

        auto next_avail_batch = available_batched_responses.back();
        available_batched_responses.pop_back();

        auto &batch = batched_responses[next_avail_batch];

        index = 0;
        for (auto &[proc_id, c] : *connection_list) {
          if (*c.active && requests[index]) {
            auto &slot =
                *reinterpret_cast<dory::membership::CacheRpcSlot *>(c.slot);

            if (outstanding_responses > OutstandingResponsesMax) {
              break;
            }

            // Reset the slot for the next request
            slot.arm();

            auto &resp = slot.resp;
            resp.view_id = view_id;

            // Send the response
            c.ud->append(batch, next_avail_batch, &resp,
                         sizeof(dory::membership::CacheRpcSlot::Response), id);

            outstanding_responses += 1;
          }
          index++;
        }  // for: post responses

        cache_active_connections_ud->postSend(batch);
      }  // while: i am part of the membership
    }    // while: for a given membership
  }      // while: main loop
  LOGGER_ERROR(main_logger, "Unreachable");

  return 0;
}

#include <iostream>
#include <stdexcept>
#include <vector>

#include <lyra/lyra.hpp>

#include <dory/conn/ud-exchanger.hpp>
#include <dory/conn/ud.hpp>
#include <dory/ctrl/block.hpp>
#include <dory/ctrl/device.hpp>
#include <dory/memstore/store.hpp>
#include <dory/shared/units.hpp>

using ProcId = uint32_t;
using ConnectionExchanger = dory::conn::UdConnectionExchanger<ProcId>;

using namespace dory;
using namespace conn;

void wait_completion(size_t to_poll,
                     const std::shared_ptr<UnreliableDatagram>& ud);
void wait_completion(size_t to_poll,
                     const std::shared_ptr<UnreliableDatagram>& ud) {
  std::vector<struct ibv_wc> wce;
  while (to_poll) {
    wce.resize(to_poll);
    // Note: we don't use the "correct" Cq all the time, but they should be the
    // same.
    if (!ud->pollCqIsOk<UnreliableDatagram::SendCQ>(wce)) {
      throw std::runtime_error("Polling error.");
    }
    if (!wce.empty()) {
      std::cout << "polled " << wce.size() << std::endl;
    }
    for (auto& wc : wce) {
      if (wc.status != IBV_WC_SUCCESS) {
        throw std::runtime_error("WC not successful.");
      }
    }
    to_poll -= wce.size();
  }
}

int main(int argc, char* argv[]) {
  bool is_sender = false;
  size_t num_msg = 1;
  bool exchanger_enabled = false;
  ProcId proc_id = 0;
  ProcId num_proc = 0;
  std::vector<std::string> serialized_receivers;
  std::vector<std::string> serialized_mc_groups;

  auto cli =
      lyra::cli() |
      lyra::opt(is_sender)["-s"]["--sender"]("Whether I should send.") |
      lyra::opt(num_msg, "num_msg")["-n"]["--num_msg"](
          "Number of messages to send/recv.") |
      lyra::opt(serialized_receivers, "receivers")["-r"]["--receiver"].help(
          "lid/qpn/qkey to which send the msgs.") |
      lyra::opt(serialized_mc_groups, "mc_groups")["-m"]["--mc_group"].help(
          "gid/lid of an mc group to attach.") |
      lyra::opt(exchanger_enabled)["-x"]["--exchanger"].help(
          "activate the exchanger") |
      lyra::opt(proc_id, "proc_id")["-i"]["--id"].help(
          "receiver_id of this process.") |
      lyra::opt(num_proc, "num_proc")["-p"]["--num_proc"].help(
          "receiver_id of this process.");

  auto result = cli.parse({argc, argv});

  if (!result) {
    std::cerr << "Error in command line: " << result.errorMessage()
              << std::endl;
    return 1;
  }

  using namespace units;
  size_t allocated_size = 1_GiB;
  size_t alignment = 64;

  ctrl::Devices d;
  ctrl::OpenDevice od;

  // Get the last device
  {
    for (auto& dev : d.list()) {
      od = std::move(dev);
    }
  }

  std::cout << od.name() << " " << od.devName() << " "
            << ctrl::OpenDevice::typeStr(od.nodeType()) << " "
            << ctrl::OpenDevice::typeStr(od.transportType()) << std::endl;

  ctrl::ResolvedPort rp(od);
  rp.bindTo(0);

  // Configure the control block
  ctrl::ControlBlock cb(rp);
  cb.registerPd("primary");
  cb.allocateBuffer("shared-buf", allocated_size, alignment);
  cb.registerMr(
      "shared-mr", "primary", "shared-buf",
      ctrl::ControlBlock::LOCAL_READ | ctrl::ControlBlock::LOCAL_WRITE |
          ctrl::ControlBlock::REMOTE_READ | ctrl::ControlBlock::REMOTE_WRITE);
  cb.registerCq("cq");

  auto ud = std::make_shared<UnreliableDatagram>(cb, "primary", "shared-mr",
                                                 "cq", "cq");

  std::cout << "UD QP serialized as " << ud->info().serialize() << std::endl;

  std::vector<McGroup> mc_groups;
  mc_groups.reserve(serialized_mc_groups.size());

  for (auto& serialized_mc_group : serialized_mc_groups) {
    mc_groups.emplace_back(cb, "primary", ud, serialized_mc_group);
  }

  auto mr = cb.mr("shared-mr");

  // Sender
  if (is_sender) {
    // Unicast from list
    std::vector<UnreliableDatagramConnection> udcs;
    udcs.reserve(serialized_receivers.size());

    for (auto& serialized_receiver : serialized_receivers) {
      udcs.emplace_back(cb, "primary", ud, serialized_receiver);
    }
    for (auto& udc : udcs) {
      for (auto i = 0U; i < num_msg; i++) {
        if (!udc.postSend(0, reinterpret_cast<void*>(mr.addr), 60)) {
          throw std::runtime_error("Unicasting error.");
        }
      }
    }
    wait_completion(num_msg * udcs.size(), ud);
    // Unicast from exchanger
    if (exchanger_enabled) {
      ConnectionExchanger exchanger(memstore::MemoryStore::getInstance(), cb,
                                    "primary", ud);
      std::vector<ProcId> remote_ids;
      for (ProcId id = 0; id < num_proc; id++) {
        if (id == proc_id) {
          continue;
        }
        remote_ids.push_back(id);
      }
      exchanger.waitReadyAll(remote_ids, "test-ud", "listening");
      exchanger.connectAll(remote_ids, "test-ud");
      for (auto& [id, udc] : exchanger.connections()) {
        for (auto i = 0U; i < num_msg; i++) {
          if (!udc.postSend(0, reinterpret_cast<void*>(mr.addr), 60)) {
            throw std::runtime_error("Unicast error.");
          }
        }
      }
      wait_completion(num_msg * exchanger.connections().size(), ud);
    }
    // Multicast
    for (auto& mc_group : mc_groups) {
      for (auto i = 0U; i < num_msg; i++) {
        if (!mc_group.postSend(0, reinterpret_cast<void*>(mr.addr), 60)) {
          throw std::runtime_error("Multicast error.");
        }
      }
    }
    wait_completion(num_msg * mc_groups.size(), ud);

    // Receiver
  } else {
    if (exchanger_enabled) {
      ConnectionExchanger exchanger(memstore::MemoryStore::getInstance(), cb,
                                    "primary", ud);
      exchanger.announce(proc_id, "test-ud");
      exchanger.announceReady(proc_id, "test-ud", "listening");
    }
    size_t to_poll = num_msg;
    if (!mc_groups.empty()) {
      to_poll *= mc_groups.size();
    }
    for (auto i = 0U; i < to_poll; i++) {
      ud->postRecv(0, reinterpret_cast<void*>(mr.addr), 60);
    }
    wait_completion(to_poll, ud);
  }

  return 0;
}

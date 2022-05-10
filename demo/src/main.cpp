#include <iostream>
#include <stdexcept>
#include <thread>
#include <vector>

#include <dory/conn/rc-exchanger.hpp>
#include <dory/conn/rc.hpp>
#include <dory/ctrl/block.hpp>
#include <dory/ctrl/device.hpp>
#include <dory/memstore/store.hpp>
#include <dory/shared/units.hpp>
#include <dory/shared/unused-suppressor.hpp>

using namespace dory;

using ProcIdType = unsigned;
using RcConnectionExchanger = conn::RcConnectionExchanger<ProcIdType>;

int main(int argc, char* argv[]) {
  if (argc < 2) {
    throw std::runtime_error("Provide the id of the process as argument");
  }

  int constexpr NrProcs = 2;
  int constexpr MinimumId = 1;
  ProcIdType id = 0;
  switch (argv[1][0]) {
    case '1':
      id = 1;
      break;
    case '2':
      id = 2;
      break;
    case '3':
      id = 3;
      break;
    case '4':
      id = 4;
      break;
    case '5':
      id = 5;
      break;
    default:
      throw std::runtime_error("Invalid id");
  }

  using namespace units;
  size_t allocated_size = 1_GiB;

  size_t alignment = 64;

  // Build the list of remote ids
  std::vector<ProcIdType> remote_ids;
  for (ProcIdType i = 0, min_id = MinimumId; i < NrProcs; i++, min_id++) {
    if (min_id == id) {
      continue;
    }
    remote_ids.push_back(min_id);
  }

  std::vector<ProcIdType> ids(remote_ids);
  ids.push_back(id);

  // Exchange info using memcached
  auto& store = memstore::MemoryStore::getInstance();

  ctrl::Devices d;
  ctrl::OpenDevice od;

  // Get the last device
  {
    // TODO(anon): The copy constructor is invoked here if we use auto and then
    // iterate on the dev_lst
    // auto dev_lst = d.list();
    for (auto& dev : d.list()) {
      od = std::move(dev);
    }
  }

  std::cout << od.name() << " " << od.devName() << " "
            << ctrl::OpenDevice::typeStr(od.nodeType()) << " "
            << ctrl::OpenDevice::typeStr(od.transportType()) << std::endl;

  ctrl::ResolvedPort rp(od);
  auto binded = rp.bindTo(0);
  std::cout << "Binded successful? " << binded << std::endl;
  std::cout << "(port_id, port_lid) = (" << +rp.portId() << ", "
            << +rp.portLid() << ")" << std::endl;

  // Configure the control block
  ctrl::ControlBlock cb(rp);
  cb.registerPd("primary");
  cb.allocateBuffer("shared-buf", allocated_size, alignment);
  cb.registerMr(
      "shared-mr", "primary", "shared-buf",
      ctrl::ControlBlock::LOCAL_READ | ctrl::ControlBlock::LOCAL_WRITE |
          ctrl::ControlBlock::REMOTE_READ | ctrl::ControlBlock::REMOTE_WRITE);
  cb.registerCq("cq");

  RcConnectionExchanger ce(id, remote_ids, cb);
  ce.configureAll("primary", "shared-mr", "cq", "cq");
  ce.announceAll(store, "qp");

  std::cout << "Waiting (10 sec) for all processes to fetch the connections"
            << std::endl;
  std::this_thread::sleep_for(std::chrono::seconds(10));

  ce.connectAll(
      store, "qp",
      ctrl::ControlBlock::LOCAL_READ | ctrl::ControlBlock::LOCAL_WRITE |
          ctrl::ControlBlock::REMOTE_READ | ctrl::ControlBlock::REMOTE_WRITE);

  return 0;
}

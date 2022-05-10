#pragma once

#include <chrono>
#include <iostream>
#include <sstream>
#include <thread>

#include <lyra/lyra.hpp>

#include <dory/third-party/mica/mica.h>
#include <dory/conn/rc.hpp>
#include <dory/conn/ud.hpp>
#include <dory/ctrl/block.hpp>
#include <dory/ctrl/device.hpp>
#include <dory/shared/latency.hpp>
#include <dory/shared/pinning.hpp>
#include <dory/shared/units.hpp>

#include <dory/memstore/store.hpp>

#include <dory/memory/pool/pool-allocator.hpp>

#include <dory/rpc/conn/universal-connector.hpp>

#include "../buffer.hpp"
#include "../rpc-slot.hpp"

template <typename T>
class IndexedContext {
public:
    IndexedContext(size_t max_cnt) : context_(max_cnt) {

    }

    std::optional<T> &getRef(size_t idx) {
        return context_.at(idx);
    }

private:
    std::vector<std::optional<T>> context_;
};

struct HerdResponse {
  uint8_t data[MICA_MAX_VALUE];
};

using Request = struct mica_op;
using Response = dory::conn::UdReceiveSlot<HerdResponse>;

struct Context {
  Context(Request *r) : req{r}, start{std::chrono::steady_clock::now()} {}

  Request *req;
  std::chrono::steady_clock::time_point start;
};

static void handle_response(Request &req, HerdResponse &resp, size_t resp_len) {
  std::cout << "Request completed:\n"
    << "bkt: " << req.key.bkt << "\n"
    << "server: " << req.key.server << "\n"
    << "tag: " << req.key.tag << "\n"
    << "type: " << (req.opcode == MICA_OP_GET ? "get" : "put") << "\n";

  if (req.opcode == MICA_OP_GET) {
    size_t length = resp_len - dory::conn::UnreliableDatagram::UdGrhLength;

    std::cout << "found: " << (length != 0) << "\n"
      << "received: ";
    for (size_t i = 0; i < length; i++) {
      std::cout << static_cast<int>(resp.data[i]) << " ";
    }
    std::cout << std::endl;
  } else {
    std::cout << "inserted!" << std::endl;
  }
}

class Client {
public:
  Client(ProcIdType id, dory::ctrl::ControlBlock &cb, std::string const &pd, std::string const &mr, std::string const &cq_name,
         dory::memory::pool::ArenaPoolAllocator &allocator, dory::LatencyProfiler &profiler )
      : id{id}, cb{cb}, rc{cb}, profiler{profiler}, LOGGER_INIT(logger, "Client") {

    //// Setup Pools ////
    size_t pool_req_alignment = 64;
    size_t pool_req_cnt = 256;
    pool_req = allocator.createPool<Request>(pool_req_cnt, pool_req_alignment);

    size_t pool_resp_alignment = 64;
    size_t pool_resp_cnt = 256;
    pool_resp = allocator.createPool<Response>(pool_resp_cnt, pool_resp_alignment);

    //// Setup Connections ////
    cb.registerCq(cq_name);
    rc.bindToPd(pd);
    rc.bindToMr(mr);
    rc.associateWithCq(cq_name, cq_name);
    rc.init(dory::ctrl::ControlBlock::LOCAL_READ |
            dory::ctrl::ControlBlock::LOCAL_WRITE |
            dory::ctrl::ControlBlock::REMOTE_READ |
            dory::ctrl::ControlBlock::REMOTE_WRITE);

    ud = std::make_shared<dory::conn::UnreliableDatagram>(
        cb, pd, mr, cq_name, cq_name);

    cq.emplace(cb.cq(cq_name));
  }

  void connect(ProcIdType connect_to) {
    auto [ip, port] = announcer.processToHost(connect_to);
    cli.emplace(ip, port);

    cli->connect();
    auto [cli_ok, cli_info] = cli->handshake<std::pair<dory::uptrdiff_t, size_t>>(
        [this]() -> std::pair<bool, std::string> {
          std::string serialized_info =
              rc.remoteInfo().serialize() + " " +
              ud->info().serialize() + " " +
              std::to_string(0xdeadbeefc0cac01aUL); // Only the master uses this field

          return std::make_pair(true, serialized_info);
        },
        [this, connect_to](std::string const &info)
            -> std::pair<bool, std::optional<std::pair<dory::uptrdiff_t, size_t>>> {
          std::istringstream remote_info_stream(info);
          std::string rc_info;
          std::string ud_info;
          dory::uptrdiff_t offset_info;
          size_t len_info;
          remote_info_stream >> rc_info;
          remote_info_stream >> ud_info;
          remote_info_stream >> offset_info;
          remote_info_stream >> len_info;

          auto remote_rc = dory::conn::RemoteConnection::fromStr(rc_info);
          rc.reset();
          rc.reinit();
          rc.connect(remote_rc, connect_to);

          udc = std::make_unique<dory::conn::UnreliableDatagramConnection>(
              cb, "primary-pd", ud, ud_info);

          return std::make_pair(true,
                                std::make_pair(offset_info, len_info));
        },
        id, dory::membership::RpcKind::RDMA_HERD_CONNECTION);

    if (!cli_ok) {
      LOGGER_WARN(logger, "Could not connect to process {}", connect_to);
      return;
    }

    auto [offset, len] = cli_info.value();
    auto remote_addr = rc.remoteBuf() + offset;

    circular_buffer_image.emplace(reinterpret_cast<void *>(remote_addr), len, false);
    contexts.emplace(circular_buffer_image->maxElements());

    std::cout << "Circular buffer size: " << circular_buffer_image->maxElements() << std::endl;
  }

  std::tuple<bool, std::optional<std::chrono::steady_clock::time_point>, std::optional<std::chrono::steady_clock::time_point>> poll() {
    std::optional<std::chrono::steady_clock::time_point> first_received;
    std::optional<std::chrono::steady_clock::time_point> last_received;

    bool failed = false;
    wce.resize(outstanding_requests);
    if (!dory::ctrl::ControlBlock::pollCqIsOk(cq.value().get(), wce)) {
      throw std::runtime_error("Error polling Cq!");
    }

    for (auto &wc : wce) {
      if (wc.status != IBV_WC_SUCCESS) {
        throw std::runtime_error("RDMA RC/UD error!" + std::to_string(+wc.status));
      }

      outstanding_requests --;

      if (wc.wr_id == 0) {
        // This is a wc for the RC write
        continue;
      }

      failed = (wc.imm_data & (static_cast<uint32_t>(1) << 31)) != 0;
      wc.imm_data = (wc.imm_data << 1) >> 1;
      auto circular_buffer_idx = static_cast<size_t>(wc.imm_data);
      // std::cout << "From circ buf idx " << circular_buffer_idx << std::endl;
      auto &context = contexts->getRef(circular_buffer_idx);
      auto &req = *context.value().req;
      auto &resp_padded = *reinterpret_cast<Response *>(wc.wr_id);
      auto &resp = resp_padded.resp;
      // handle_response(req, resp, wc.byte_len);

      last_received = std::chrono::steady_clock::now();
      if (req.opcode == MICA_OP_PUT || wc.byte_len > dory::conn::UnreliableDatagram::UdGrhLength) {
        profiler.addMeasurement(*last_received - context->start);
      } else if (req.opcode == MICA_OP_GET) {
        failed_get ++;
      }

      if (!first_received) {
        first_received = *last_received;
      }

      pool_req->destroy(&req);
      pool_resp->destroy(&resp_padded);

      // This is a ud reply by the server
      // auto *rpc_slot_image = circular_buffer_image.fromIdx(circular_buffer_idx);
      // circular_buffer_image.markAsFree(rpc_slot_image);
      context.reset();
    }

    return {failed, first_received, last_received};
  }

  bool post(void (*put_or_get)(struct mica_op &, uint128 *), uint128 *key) {
    if (outstanding_requests + 2> OutstandingRequestsMax) {
      // std::cout << "OutstandingRequestsMax exceeded (" << outstanding_requests << ")" << std::endl;
      return false;
    }

    // circular_buffer_image.advanceInProcessConsideringFree();
    while (auto fip = circular_buffer_image->firstInProcess()) {
      auto idx = circular_buffer_image->toIdx(fip.value());
      if (!contexts->getRef(idx)) {
        circular_buffer_image->advanceInProcess();
      } else {
        break;
      }
    }

    auto fn = circular_buffer_image->firstNew();
    if (!fn) {
      // std::cout << "Circular buffer is full" << std::endl;
      return false;
    }

    auto *req = pool_req->create();
    if (req == nullptr) {
      // std::cout << "Request pool is full" << std::endl;
      return false;
    }

    auto *resp = pool_resp->create();
    if (resp == nullptr) {
      // std::cout << "Response pool is full" << std::endl;
      pool_resp->destroy(resp);
      return false;
    }

    put_or_get(*req, key);
    // mica_print_op(req);

    contexts->getRef(circular_buffer_image->toIdx(fn.value())).emplace(req);

    ud->postRecv(reinterpret_cast<uintptr_t>(resp),  // WRID
                  resp, sizeof(std::remove_reference_t<decltype(*resp)>::inner_type));

    auto ok = rc.postSendSingle(dory::conn::ReliableConnection::RdmaWrite,
                                0,  // WRID, special value
                                req, sizeof(*req),
                                reinterpret_cast<uintptr_t>(fn.value()) + offsetof(RpcSlot, op));
    // circular_buffer_image.markAsUsed(fn.value());
    circular_buffer_image->advanceNew();
    outstanding_requests += 2;

    if (!ok) {
      throw std::runtime_error("Failed to post and RDMA Write");
    }

    return true;
  }

  uint64_t failedGets() { return failed_get; }

private:
ProcIdType id;
dory::ctrl::ControlBlock &cb;
dory::conn::ReliableConnection rc;
std::shared_ptr<dory::conn::UnreliableDatagram> ud;
std::unique_ptr<dory::conn::UnreliableDatagramConnection> udc;
dory::Delayed<dory::rpc::conn::UniversalConnectionRpcClient<ProcIdType, dory::membership::RpcKind::Kind>> cli;
dory::Delayed<CircularBuffer<RpcSlot>> circular_buffer_image;
dory::Delayed<IndexedContext<Context>> contexts;
dory::LatencyProfiler &profiler;

std::unique_ptr<dory::memory::pool::PoolAllocator<Request>> pool_req;
std::unique_ptr<dory::memory::pool::PoolAllocator<Response>> pool_resp;

size_t outstanding_requests = 0;
static size_t constexpr OutstandingRequestsMax = 2*1;

dory::DelayedRef<dory::deleted_unique_ptr<struct ibv_cq>> cq;
std::vector<struct ibv_wc> wce;

dory::memstore::ProcessAnnouncer announcer;

uint64_t failed_get = 0;

LOGGER_DECL(logger);
};

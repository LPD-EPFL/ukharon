#pragma once

#include <dory/third-party/mica/mica.h>
#include <cstdint>
#include <cstring>

template<typename T>
class CircularBuffer;

// Used by the server
struct RpcSlot {
  enum ReplicationState {
    New = 1,
    Replicating,
    ReadyToSend,
    Final
  };

  RpcSlot(CircularBuffer<RpcSlot> * cb) : circular_buffer{cb} {
    op = {};
    resp = {};
    state = New;
    failed = false;
  }
  // The mica_op fits exactly in one cache line.
  // Thus, when polling for the last byte, the whole operation must be there
  struct mica_op op;
  struct mica_resp resp;

  alignas(64) ReplicationState state;
  CircularBuffer<RpcSlot> *circular_buffer;
  bool failed;
};

static inline bool is_get(RpcSlot &slot) {
  return slot.op.opcode == MICA_OP_GET;
}

static inline bool is_put(RpcSlot &slot) {
  return slot.op.opcode == MICA_OP_PUT;
}

static inline bool is_new(RpcSlot &slot) {
  return slot.state == RpcSlot::New;
}

static inline bool is_replicating(RpcSlot &slot) {
  return slot.state == RpcSlot::Replicating;
}

static inline bool is_ready_to_send(RpcSlot &slot) {
  return slot.state == RpcSlot::ReadyToSend;
}

static inline bool is_final(RpcSlot &slot) {
  return slot.state == RpcSlot::Final;
}

static inline void make_new(RpcSlot &slot) {
  slot.state = RpcSlot::New;
  slot.failed = false;
}

static inline void make_failed(RpcSlot &slot) {
  slot.failed = true;
}

static inline void make_replicating(RpcSlot &slot) {
  slot.state = RpcSlot::Replicating;
}

static inline void make_ready_to_send(RpcSlot &slot) {
  slot.state = RpcSlot::ReadyToSend;
}

static inline void make_final(RpcSlot &slot) {
  slot.state = RpcSlot::Final;
}

static inline bool slot_written(RpcSlot &slot) {
  // If a GET appears in memory, the key must be there,
  // because it is in the same cache line
  if (is_get(slot)) {
    return true;
  }

  // If a PUT appears, wait for the val_len to appear
  // and wait for the entire value to appear.
  // By convention, the next byte after the value is the
  // canary byte.
  if (is_put(slot)) {
    if (slot.op.val_len > 0) {
      if (slot.op.value[slot.op.val_len] == 0xff) {
        return true;
      }
    }
  }

  return false;
}

static inline void arm_slot(RpcSlot &slot) {
  if (is_get(slot)) {
    slot.op.opcode = 0;
  }

  if (is_put(slot)) {
    slot.op.opcode = 0;
    std::memset(slot.op.value, 0, slot.op.val_len + 1);
    slot.op.val_len = 0;
  }
}
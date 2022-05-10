#pragma once

#include <dory/third-party/mica/mica.h>

#define TEST_VAL_LEN 32
#define TEST_NUM_BKTS M_2
#define TEST_LOG_CAP M_1024
#define TEST_NUM_KEYS (TEST_NUM_BKTS * 4) /* 50% load */

static void permute_for(uint128 *key_arr, uint64_t *seed) {
  size_t i;

  size_t j;
  uint128 temp;

  /* Shuffle */
  for (i = TEST_NUM_KEYS - 1; i >= 1; i--) {
    j = hrd_fastrand(seed) % (i + 1);
    temp = key_arr[i];
    key_arr[i] = key_arr[j];
    key_arr[j] = temp;
  }
}

static inline void prepare_put_request(struct mica_op &op, uint128 *key) {
  auto *op_key_put = reinterpret_cast<unsigned long long *>(&op.key);
  op_key_put[0] = key->first;
  op_key_put[1] = key->second;

  op.opcode = MICA_OP_PUT;

  op.val_len = TEST_VAL_LEN;
  memset(op.value, static_cast<uint8_t>(op.key.tag), op.val_len);
  op.value[TEST_VAL_LEN] = 0xff;
}



static inline void prepare_get_request(struct mica_op &op, uint128 *key) {
  auto *op_key_get = reinterpret_cast<unsigned long long *>(&op.key);
  op_key_get[0] = key->first;
  op_key_get[1] = key->second;

  op.opcode = MICA_OP_GET;
  op.val_len = 0;
}
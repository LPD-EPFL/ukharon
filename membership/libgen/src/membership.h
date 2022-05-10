#pragma once

#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  bool active;
  bool changed;
} activity_t;

void start_membership_c();

activity_t is_active(int view_id);

#ifdef __cplusplus
}
#endif

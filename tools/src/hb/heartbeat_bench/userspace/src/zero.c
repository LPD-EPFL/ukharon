#include <stdint.h>
#include <stdio.h>

#include <sys/ipc.h>
#include <sys/shm.h>

#include "size.h"

int main() {
  int id = shmget((int)SHM_MEM_KEY, size, 0);

  if (id == -1) {
    perror("shmget");
    return 1;
  }

  uint64_t *ptr = (uint64_t *)shmat(id, NULL, 0);
  if ((void *)ptr == (void *)-1) {
    perror("shmat");
    return 1;
  }

  *ptr = 0UL;

  return 0;
}

#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include <sys/ipc.h>
#include <sys/shm.h>

#include "size.h"

int main(int argc, char *argv[]) {
  if (argc != 2) {
    fprintf(stderr, "Provide the end timestamp (in ns) as argument\n");
    return 1;
  }

  uint64_t end_ts = strtoul(argv[1], NULL, 0);
  if (end_ts == 0UL) {
    fprintf(stderr, "Could not convert the argument to an integer\n");
    return 1;
  }

  if (end_ts == ULONG_MAX) {
    perror("strtoul");
    return 1;
  }

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

  uint64_t start_ts = *ptr;

  if (start_ts > end_ts) {
    fprintf(stderr, "Starting timestamp is larger that ending timestamp\n");
    return 1;
  }

  printf("%lu ns\n", end_ts - start_ts);

  return 0;
}

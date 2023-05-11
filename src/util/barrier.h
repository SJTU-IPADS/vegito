#ifndef _UTILS_BARRIER_H_
#define _UTILS_BARRIER_H_

#include <atomic>
#include <pthread.h>
#include "framework/utils/macros.h"

/**
 * a simple barrier wrapper over pthread barrier
 */
class Barrier {
  pthread_barrier_t barrier_;

public:
  explicit Barrier(int num) {
    pthread_barrier_init(&barrier_, nullptr, num);
  }

  ~Barrier() { pthread_barrier_destroy(&barrier_); }

  void wait() {
    pthread_barrier_wait(&barrier_);
  }

  DISABLE_COPY_AND_ASSIGN(Barrier);
};

#endif


/*
 * The code is a part of our project called VEGITO, which retrofits
 * high availability mechanism to tame hybrid transaction/analytical
 * processing.
 *
 * Copyright (c) 2021 Shanghai Jiao Tong University.
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS
 *  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied.  See the License for the specific language
 *  governing permissions and limitations under the License.
 *
 * For more about this software visit:
 *
 *      http://ipads.se.sjtu.edu.cn/projects/vegito
 *
 */

#ifndef NOCC_UTIL_H
#define NOCC_UTIL_H

/* This util file is imported from DrTM+ */

#include <sched.h>
#include <time.h>
#include <assert.h>

#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>    /* sort  */
#include <math.h>       /* floor */

#include <unistd.h>

#include <sys/mman.h>

#define ALWAYS_INLINE __attribute__((always_inline))
inline ALWAYS_INLINE uint64_t rdtsc(void) { uint32_t hi, lo; __asm volatile("rdtsc" : "=a"(lo), "=d"(hi)); return ((uint64_t)lo)|(((uint64_t)hi)<<32); }
#define unlikely(x) __builtin_expect(!!(x), 0)

namespace nocc {

  namespace util {

    int BindToCore (int thread_id);
    int CorePerSocket();
    int TotalCores();
    inline int getCpuListener()  { return TotalCores() - 1; }
    inline int getCpuGC()        { return TotalCores() - 1; }
    inline int getCpuLogList()   { return TotalCores() - 1; }
    inline int getCpuClientTxn() { return TotalCores() - 2; }
    inline int getCpuClientQry() { return TotalCores() - 2; }
    inline int getCpuTxn()       { return 0; }  
    // the rest is used for Txn, Cleaner, Qry
    inline int getCpuQry()       { return TotalCores() - 3; }
    int choose_nic(int thread_id);

    template <class Num> inline ALWAYS_INLINE  // Round "a" according to "b"
      Num Round (Num a, Num b) { return (a + b - a % b);  }
    int  DiffTimespec(const struct timespec &end, const struct timespec &start);

    // wrappers for parsing configure file
    // !! notice that even a failed parsing will results for cursor advancing
    bool NextInt(std::istream &ist,int &res);
    bool NextLine(std::istream &ist,std::string &res);
    bool NextString(std::istream &ist,std::string &res);
    void BypassLine(std::istream &ist);

    inline uint64_t TimeToMs(struct timespec &t) { return t.tv_sec * 1000 + t.tv_nsec / 1000000;}

    std::pair<uint64_t, uint64_t> get_system_memory_info(); // instruction's memory comsuption
    void *malloc_huge_pages(size_t size,uint64_t huge_page_sz,bool flag = true);
    inline double get_memory_size_g(uint64_t bytes) { static double G = 1024.0 * 1024 * 1024; return bytes / G; }

    void print_stacktrace(FILE *out = stderr, unsigned int max_frames = 63);
    
    inline bool CAS(uint32_t *ptr, uint32_t oldval, uint32_t newval) {
      return __sync_bool_compare_and_swap(ptr, oldval, newval);
    }

    inline bool CAS(volatile uint64_t *ptr, uint64_t oldval, uint64_t newval) {
      return __sync_bool_compare_and_swap(ptr, oldval, newval);
    }

    // fetch and add
    inline uint64_t FAA(uint64_t *ptr, uint64_t add_val) {
      return __sync_fetch_and_add(ptr, add_val);
    }

    inline uint64_t FAA(volatile uint64_t *ptr, uint64_t add_val) {
      return __sync_fetch_and_add(ptr, add_val);
    }

    inline void lock32(uint32_t *lock_ptr) {
      while (!CAS(lock_ptr, 0, 1)) ;
    }

    inline void unlock32(uint32_t *lock_ptr) {
      *lock_ptr = 0;
    }

  } // namespace util
}   // namespace nocc

// other related utils
#include "timer.h"     // timer helper
#endif


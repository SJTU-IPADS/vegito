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

/*
 * This file implements a timestamp manager for SI
 * The timestamp is distributed accoring to the paper "The End of a Myth: Distributed Transactions Can Scale". 
 * 
 */

#ifndef NOCC_TX_SI_TS_
#define NOCC_TX_SI_TS_

#include <functional>
#include <vector>
#include <stdint.h> 

#include "rdmaio.h"
#include "all.h"

typedef std::function<void *(void *)> ts_manage_func_t;

namespace nocc {
  namespace db {

    /* there would be exactly one of this class */
    class TSManager {
    public:
      /* The timestamp is a vector timestamp, one per server */
      void get_timestamp(char *buffer,int tid);

      /* Ts manaer thread is used to update the true commit timestamp */
      void *ts_monitor(void *);
      void *timestamp_poller(void *);
      TSManager(rdmaio::RdmaCtrl *q,uint64_t time_addr,int id,int master_id,int worker_id );

      inline uint64_t commit_ts() {
        return __sync_fetch_and_add(&local_timestamp_,1);
      }

      static void print_ts(char *ts_ptr) {
        static uint64_t tv_size = 16;
        for(uint printed = 0; printed < sizeof(uint64_t) * tv_size; printed += sizeof(uint64_t)) {
          fprintf(stdout,"%lu\t",*((uint64_t *)(ts_ptr + printed)));
        }
        fprintf(stdout,"\n");
      }

      void post_commit_ts(uint64_t ts,int tid);
      void thread_local_init();
    
      uint64_t last_ts_;
      int      total_partition;

      static void initilize_meta_data(char *buffer,int partitions) {
        uint64_t *arr = (uint64_t *)buffer;
        for(uint i = 0;i < partitions;++i) {
          /*FIXME i know 2 is a magic number. The existence is due to a historical reason */
          arr[i] = 2;
        }
      }

      rdmaio::RdmaCtrl *cm_;
      /* the master server which stores the timestamp */
      int master_id_;
      uint64_t ts_addr_;
    private:
      uint64_t local_timestamp_;
      /* the rdma handler i should use */
      int worker_id_;
      /* an offset of where to read for the timestamp */
      char *fetched_ts_buffer_;
      /* the current server's timestamp */
      /* the current server's id */
      int id_;
      int tv_size_;
    } __attribute__ ((aligned(CACHE_LINE_SZ))) ;
  
  };
};
#endif



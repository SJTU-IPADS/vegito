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

#ifndef _TX_ONE_REMOTE
#define _TX_ONE_REMOTE

// This file implements remote fetch operations using one-sided operator

#include "framework/rdma_sched.h"

#include "memstore/memdb.h" // for db handler

#include "rdmaio.h"         // for rdma operators
#include "db_statistics_helper.h"

#include <vector>

namespace nocc {

  using namespace oltp;
  namespace db {

    class RRWSet {

      struct RemoteSetItem {
        int   pid;
        int   len;
        int   tableid;

        uint64_t key;
        uint64_t off;
        char *val;

        bool ro;
      };

    public:

      struct Meta { // meta data format
        uint64_t lock;
        uint64_t seq;
      };

      RRWSet(rdmaio::RdmaCtrl *cm,RDMA_sched *sched,MemDB *db,int tid,int cid,int meta);

      int  add(int pid,int tableid,uint64_t key,int len); // fetch the remote key to the cache
      // the data off is written after meta off has been written

      bool validate_reads(yield_func_t &yield);
      int  write_all_back(int meta_off,int data_off,char *buffer);

      // len does not include the meta data of the record
      inline void clear() { elems_ = 0; read_num_ = 0; }
      inline int  numItems() { return elems_; }
      inline void promote_to_write(int idx) { kvs_[idx].ro = false;read_num_ -= 1; }

      void report() { REPORT(post); }

      RemoteSetItem *kvs_;

    private:
      int elems_;
      int read_num_;

      RDMA_sched *sched_;

      std::vector<rdmaio::Qp *> qps_; // Qp in-used for one-sided operations
      MemDB    *db_;
      int       tid_; // thread id
      int       cor_id_; // coroutine id
      int       meta_len_;

      LAT_VARS(post);
    };

  }; // namespace db
};   // namespace nocc

#endif

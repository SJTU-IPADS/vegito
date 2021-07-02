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

/* An rpc handler for distributed transactions */

#ifndef NOCC_OLTP_BENCH_WORKER_H
#define NOCC_OLTP_BENCH_WORKER_H

#include <vector>
#include <string>
#include <cstdint>

#include "all.h"
#include "config.h"
#include "./utils/macros.h"
#include "./utils/thread.h"

#include "rpc.h"
#include "rdma_sched.h"
#include "routine.h"
#include "view_manager.h"

#include "memstore/memdb.h"
#include "backup_store/backup_db.h"
#include "db/txs/tx_handler.h"
#include "db/db_statistics_helper.h"

using namespace nocc::db;

/* global config constants */
extern size_t nthreads;            // total of worker in used
extern size_t coroutine_num;       // number of concurrent request per worker
 
extern size_t backup_nthreads;               // number of log cleaner
extern size_t scale_factor;                  // the scale of the database
extern size_t total_partition;               // total of machines in used
extern size_t current_partition;             // current worker id
 
extern size_t distributed_ratio; // the distributed transaction's ratio
 
extern size_t txn_nthreads;
extern size_t query_nthreads;    // number of query worker
extern double sync_seconds;

namespace nocc {

extern __thread coroutine_func_t *routines_;
extern __thread int          *reply_counts_;    // for RPC
extern __thread int          *pending_counts_;  // for RDMA_sche

namespace oltp {

extern RdmaCtrl *cm;
extern char *store_buffer;
extern uint64_t ring_padding;
extern uint64_t ringsz;

} // end namespace oltp
} // end namespace nocc

#endif

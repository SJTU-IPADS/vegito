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

#include "all.h"
#include "config.h"

#include "util/util.h"
#include "util/spinlock.h"
#include "util/mapped_log.h"

#include "db/req_buf_allocator.h"
#include "db/txs/tx_handler.h"

#include "framework.h"
#include "routine.h"
#include "backup_worker.h"
#include "view_manager.h"
#include "bench_query.h"
#include "log_listener.h"

// rdma related libs
#include "rdmaio.h"
#include "ralloc.h"
#include "ring_msg.h"
#include "ud_msg.h"

// System or library headers
#include <iostream>
#include <queue>
#include <unistd.h>
#include <stdio.h>
#include <signal.h>

#include <atomic>
#include <chrono> // for print current system time in a nicer way

#include <boost/bind.hpp>

using namespace std;
using namespace rdmaio;
using namespace rdmaio::ringmsg;
using namespace nocc::util;

/* global config constants */
size_t nthreads = 1;                      // total of worker in used
size_t coroutine_num = 1;                 // number of concurrent request per worker
size_t backup_nthreads = 0;                   // number of log cleaner
size_t scale_factor = 0;                  // the scale of the database
size_t total_partition = 1;               // total of machines in used
size_t current_partition = 0;             // current worker id
size_t distributed_ratio = 1; // the distributed transaction's ratio
size_t txn_nthreads = 0;
size_t query_nthreads = 0;    // number of query worker

// per-thread log handler
__thread MappedLog local_log;

namespace nocc {
#ifdef RPC_TIMEOUT_FLAG
__thread struct  timeval   *routine_timeouts_;
#endif

#ifdef RPC_VERBOSE
extern __thread rpc_info *rpc_infos_;
#endif

namespace oltp {

RdmaCtrl *cm = NULL;
char *store_buffer = NULL; // the buffer used to store memstore
uint64_t ring_padding = MAX_MSG_SIZE;
uint64_t ringsz;

}// end namespace oltp
} // end namespace nocc

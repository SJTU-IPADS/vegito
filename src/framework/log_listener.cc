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

#include "log_listener.h"
#include "backup_worker.h"
#include "framework_cfg.h"

// RDMA related
#include "ralloc.h"
#include "ring_msg.h"
#include "ud_msg.h"

#include "rpc.h"
#include "routine.h"

using namespace std;
using namespace rdmaio;
using namespace rdmaio::ringmsg;
using namespace nocc::util;
using namespace nocc::framework;

/* global config constants */
extern size_t current_partition;
double sync_seconds = 0.0;

namespace {
  const int RDMA_TID = 25;
}

namespace nocc {
namespace oltp {

extern RdmaCtrl *cm;

LogListener::LogListener()
    : RWorker(cm, getCpuLogList(), -1, RDMA_TID, 1),
      master_node_(0),
      SECOND_CYCLE_(util::Breakdown_Timer::get_one_second_cycle()),
      qps_(config.getNumServers())
{ }

void LogListener::user_init_() {
  for (int i = 0; i < qps_.size(); ++i) {
    qps_[i] = cm->get_rc_qp(RDMA_TID, i, 0);
  }

  // routines_[1] = coroutine_func_t(bind(&LogListener::slave_func, this, placeholders::_1, 1));
}

void LogListener::run_body_(yield_func_t &yield) {
  printf("[Log Listener]: Monitor epoch running\n");
  const uint64_t sync_interval = SECOND_CYCLE_ * sync_seconds;
  uint64_t begin = rdtsc(), end;
  uint64_t *epoch = (uint64_t *) Rmalloc(sizeof(uint64_t));
  *epoch = 1;
  uint64_t *local_epoch = (uint64_t *) (cm->conn_buf_ + M1);
  *local_epoch = 1;

  while(1) {
    if (config.getEpochType() == 1) continue;
#if 0
#if USE_UD_MSG == 1
    msg_handler_->poll_comps();
#else
    rpc_handler_->poll_comps();
#endif
    // yield_next(yield);
#endif

    // master timing
    if(current_partition == master_node_) {

      end = rdtsc();
      if (end - begin > sync_interval) {
        // printf("sync %lf sec\n", sync_seconds);
        ++(*epoch);
        for (Qp *qp : qps_) {
          Qp::IOStatus rc = 
            qp->rc_post_send(IBV_WR_RDMA_WRITE, (char *) epoch, 
                             sizeof(uint64_t), M1, IBV_SEND_SIGNALED);
          qp->poll_completion();
          assert(rc == 0);
          assert(*local_epoch == *epoch);
        }
  
        begin = end;
      }

    } // end master timing
  }
}

void LogListener::slave_func(yield_func_t &yield, int cor_id) {
  const uint64_t sync_interval = SECOND_CYCLE_ * sync_seconds;
  uint64_t begin = rdtsc(), end;

  while(1) {
    end = rdtsc();
    if (end - begin > sync_interval) {
      printf("sync %lf sec\n", sync_seconds);
      begin = end;
    }
    yield_next(yield);
  }
}

}  // namespace oltp
}  // namespace nocc


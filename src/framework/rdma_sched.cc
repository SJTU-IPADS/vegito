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

#include "rdma_sched.h"
#include "routine.h" // poll_comps will add_routine to scheduler

#include "util/util.h"  // for rdtsc()


extern size_t nthreads;
extern size_t current_partition;
extern int coroutine_num;

using rdmaio::Qp;
using namespace std;


namespace nocc {

  extern __thread int          *reply_counts_;
  __thread int                 *pending_counts_; // number of pending qps per thread

  namespace oltp {

    RDMA_sched::RDMA_sched(Routine *r):
      routine_(r),
      pre_total_costs_(0),total_costs_(0),
      pre_poll_costs_(0),poll_costs_(0),
      counts_(0),pre_counts_(0)
    {
    }

    RDMA_sched::~RDMA_sched() {
    }

    void RDMA_sched::thread_local_init() {
      pending_counts_ = new int[coroutine_num + 1];
      for(uint i = 0;i <= coroutine_num;++i)
        pending_counts_[i] = 0;
    }

    void RDMA_sched::add_pending(int cor_id, Qp* qp) {
      //if(pending_qps_.find(qp) == pending_qps_.end()) {
      //        pending_qps_.insert(std::make_pair(qp,1));
      //}
      //else
      //        pending_qps_[qp] += 1;
      pending_qps_.push_back(qp);
      pending_counts_[cor_id] += 1;
    }

    void RDMA_sched::poll_comps() {
#if 0
      assert(false);
      // old version, not used
      for(auto it =  pending_qps_.begin();it != pending_qps_.end();) {

        int num = it->second;
        //assert(num < 1024);

        Qp *qp  = it->first;

        //auto startp = rdtsc();
        while( num > 0) {

          auto poll_result = ibv_poll_cq(qp->send_cq,1,&wc_);

          if(poll_result == 0) break;

          auto cor_id = wc_.wr_id;
          assert(pending_counts_[cor_id] > 0);

          if(cor_id == 0) continue;  // ignore null completion

          pending_counts_[cor_id] -= 1;
          if(pending_counts_[cor_id] == 0) add_to_routine_list(cor_id);

          num -= 1;
        }

        // cleaning pending qp list
        if(num == 0) it = pending_qps_.erase(it);
        else { it->second = num;it++;}
      }
#else
      for(auto it = pending_qps_.begin();it != pending_qps_.end();) {

        Qp *qp = *it;
        auto poll_result = ibv_poll_cq(qp->send_cq,1,&wc_);

        if(poll_result == 0) {
          it++;
          continue;
        }

        auto cor_id = wc_.wr_id;
        if(cor_id == 0) continue;  // ignore null completion

        assert(pending_counts_[cor_id] > 0);

        pending_counts_[cor_id] -= 1;
        if(pending_counts_[cor_id] == 0) {
          if (routine_)
            routine_->add_to_routine_list(cor_id);
        }

        // update the iterator
        it = pending_qps_.erase(it);
      }
#endif
    }

    void RDMA_sched::report() {
    } // end report
  } // namespace db
}   // namespace nocc

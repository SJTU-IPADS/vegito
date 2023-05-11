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

#ifndef DB_RDMA_SCHED
#define DB_RDMA_SCHED

#include <deque>

#include "config.h"
#include "rdmaio.h"

#include "routine.h"

namespace nocc {
  namespace oltp {

    class RDMA_sched {
    public:
      RDMA_sched(Routine *r = nullptr);
      ~RDMA_sched();

      // add pending qp to corresponding coroutine
      void add_pending(int cor_id,rdmaio::Qp *qp);

      // poll all the pending qps of the thread and schedule
      void poll_comps();

      void thread_local_init();

      void report();
    private:
      Routine * const routine_;
      //std::map<rdmaio::Qp *,int> pending_qps_;
      std::deque<rdmaio::Qp *> pending_qps_;

      //struct ibv_wc wcs_[1024];
      struct ibv_wc wc_;

      /* Some performance counting statistics ********************************/
      uint64_t total_costs_;
      uint64_t pre_total_costs_;

      uint64_t poll_costs_;
      uint64_t pre_poll_costs_;

      uint64_t counts_;
      uint64_t pre_counts_;
    };

  }; // namespace db

};   // namespace nocc

#endif // DB_RDMA_SCHED

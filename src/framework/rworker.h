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

#ifndef RWORKER_H_
#define RWORKER_H_

#include "utils/thread.h"
#include "rdmaio.h"
#include "ralloc.h"
#include "rpc.h"
#include "rdma_sched.h"
// #include "msg_handler.h"
#include "routine.h"

namespace nocc {
namespace framework {

using namespace nocc::oltp;

// Base class for worker thread using RDMA and bind cpu core
class RWorker : public ndb_thread {
 public:
  RWorker(rdmaio::RdmaCtrl *cm, int cpu_id, int nic_id, int rdma_tid, 
          int num_slave_routines);
  virtual void run() override final;
  void indirect_yield(yield_func_t &yield);  // remote_set.cc
  void indirect_must_yield(yield_func_t &yield);
  void yield_next(yield_func_t &yield);

  int get_cor_id() const { return cor_id_; }
 
 protected:

  int get_cpu_id() const { return cpu_id_; }

  void register_callbacks(oltp::rpc_func_t callback, int rpc_id);
  inline void link_rc_qps_(int qp_id) {
    rdmaCtrl_->
      link_connect_qps(rdma_tid_, dev_id_, port_idx_, qp_id, IBV_QPT_RC);
  }
  
  virtual void register_rpc_() = 0;
  virtual void user_init_() = 0;
  virtual void run_body_(yield_func_t &yield) = 0;
  virtual int get_qp_num() { return 0; }
  virtual void change_ctx_() { }

  // coroutines
  void master_routine(yield_func_t &yield);
  void set_routine_fn(int id, coroutine_func_t &&fn) {
    routine_.set_routine_fn(id, (coroutine_func_t &&) fn);
  }

 private:
  void thread_local_init_();
  void init_rpc_();
  void create_rc_qps_(int num);
  void init_coroutines_();

  // for rdma
  rdmaio::RdmaCtrl *const rdmaCtrl_;
  const int cpu_id_;  // NUMA0(0,...,11), NUMA1(12,...,23) 
  const int nic_id_;  // NIC0 is near to NUMA1, NIC1 - NUMA0
  const int rdma_tid_;
  const int num_sroutines_;  // #slave routines

  int dev_id_;
  int port_idx_;

 protected:     // TODO
  // for coroutines
  int cor_id_;  // routine id
  Routine routine_;
  oltp::Rpc *rpc_handler_;
  oltp::RDMA_sched rdma_sched_;  // TODO: not used!
  MsgHandler *msg_handler_;
};

}  // namesapce framework
}  // namespace nocc

#endif

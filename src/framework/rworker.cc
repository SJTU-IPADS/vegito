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

#include "rworker.h"
#include "framework.h"
#include "util/util.h"
#include "ring_imm_msg.h"
#include "ud_msg.h"
#include <cstdio>

using namespace rdmaio;
using namespace nocc::util;
using namespace std;

namespace nocc {
namespace framework {

RWorker::RWorker(RdmaCtrl *cm, int cpu_id, int nic_id, 
                 int rdma_tid, int num_slave_routines)
  : rdmaCtrl_(cm), cpu_id_(cpu_id), 
    nic_id_(nic_id == -1? choose_nic(cpu_id) : nic_id), 
    rdma_tid_(rdma_tid),
    num_sroutines_(num_slave_routines), cor_id_(0), routine_(1 + num_sroutines_),
    rdma_sched_(&routine_) 
{
  int c = CorePerSocket();
  assert(cm != NULL);
  if (cpu_id == -1) return;

  assert(cpu_id >= 0 && cpu_id < 2 * c);  // XXX: hard code 
  // assert(nic_id == 0 || nic_id == 1);
  assert(num_sroutines_ >= 0);
  if (nic_id_ != choose_nic(cpu_id_))
    printf("*** WARNING: CPU and NIC are not matched ***\n");
}

void RWorker::fake_run() {
  if (cpu_id_ != -1) BindToCore(cpu_id_);
  RThreadLocalInit(); // inital ralloc (Rmalloc...)
  thread_local_init_();
  create_rc_qps_(get_qp_num());
  init_rpc_();
  // init_coroutines_();
  user_init_simple_();
}

void RWorker::run() {
  if (cpu_id_ != -1) BindToCore(cpu_id_);
  RThreadLocalInit(); // inital ralloc (Rmalloc...)
  thread_local_init_();
  create_rc_qps_(get_qp_num());
  init_rpc_();
  init_coroutines_();
  user_init_();

  routine_.start();
}

void RWorker::thread_local_init_() {
  dev_id_ = rdmaCtrl_->get_active_dev(nic_id_);
  port_idx_ = rdmaCtrl_->get_active_port(nic_id_);
  rdmaCtrl_->thread_local_init();
  rdmaCtrl_->open_device(dev_id_);
  rdmaCtrl_->register_connect_mr(dev_id_);
}

void RWorker::init_rpc_() {
#if USE_UD_MSG == 1
  // rdmaio::udmsg::bootstrap_ud_qps(cm,tid, 1, dev_id,port_idx,1);
  using namespace rdmaio::udmsg;
  using namespace nocc::oltp;

  int send_qp_num = 1;
  int total_threads = 1;
  rpc_handler_ = new Rpc(NULL, rdma_tid_, &routine_);
  // int max_recv_num = 2048; // 2048 for app, 64 for micro benchmarks
  int max_recv_num = MAX_SERVER_TO_SENT;
  msg_handler_ = new UDMsg(rdmaCtrl_, rdma_tid_, total_threads, max_recv_num,
                           std::bind(&Rpc::poll_comp_callback,rpc_handler_,
                                     std::placeholders::_1,
                                     std::placeholders::_2),
                           dev_id_, port_idx_, send_qp_num);

  rpc_handler_->message_handler_ = msg_handler_; // reset the msg handler
  assert(rpc_handler_);
#else
  assert(false);
  // msg_handler_ = new RingMessage(ringsz,ring_padding,worker_id_,cm,rdma_buffer + HUGE_PAGE_SZ);
  // rpc_handler_ = new Rpc(msg_handler_,worker_id_);
#endif
  
  // init rpc meta data
  rpc_handler_->init();
  rpc_handler_->thread_local_init();
  rdma_sched_.thread_local_init();
  
  register_rpc_();
}

void RWorker::create_rc_qps_(int num) {
  for (int i = 0; i < num; ++i) {
    rdmaCtrl_->link_connect_qps(rdma_tid_, dev_id_, port_idx_, i, IBV_QPT_RC);
  }
}

void RWorker::register_callbacks(rpc_func_t callback, int rpc_id) {
  rpc_handler_->register_callback(callback, rpc_id);
}

void RWorker::init_coroutines_() {
  routine_.set_routine_fn(0, coroutine_func_t(bind(
        &RWorker::master_routine, this, std::placeholders::_1))); 
}

void RWorker::master_routine(yield_func_t &yield) {
  yield_next(yield);
  run_body_(yield);
}

// class nocc_worker
// inline ALWAYS_INLINE
void RWorker::indirect_yield(yield_func_t &yield) {
  // make a simple check to avoid yield, if possible
  // TODO: put reply_counts_ in `RPC` and `pending_counts` in `RDMA_sche`
  if(unlikely(reply_counts_[cor_id_] == 0 && pending_counts_[cor_id_] == 0))
    return;
  
  indirect_must_yield(yield);
}

// inline ALWAYS_INLINE
void RWorker::indirect_must_yield(yield_func_t &yield) {
  routine_.yield_from_routine_list(yield);
  cor_id_ = routine_.get_cur_id();
  change_ctx_();
}

// inline ALWAYS_INLINE
void RWorker::yield_next(yield_func_t &yield) {
  routine_.yield_to_next(yield);
  cor_id_ = routine_.get_cur_id();
  change_ctx_();
}

}  // namespace framework
}  // namespace nocc

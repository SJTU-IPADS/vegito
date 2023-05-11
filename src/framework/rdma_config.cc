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

#include "rdma_config.h"
#include "framework_cfg.h"
#include "util/util.h"
#include "db/db_logger.h"

#include "config.h"

#include "ring_imm_msg.h"
#include "ralloc.h"

#include <vector>

using namespace std;
using namespace rdmaio;
using namespace nocc::util;
using namespace nocc::db;

namespace nocc {
namespace framework {

RdmaConfig::RdmaConfig()
  : init_(false) { }

void RdmaConfig::init_rdma(uint64_t log_area_sz) {
  if (init_) return;

  // 2MB | ring area | log area | store buffer | free (rdma heap)
  set_ring_area_sz_();  // set ringsz_per_worker_ and ring_area_sz_
  log_area_sz_ = (log_area_sz + M2 - 1) / M2 * M2;
  total_sz_ = M2 + ring_area_sz_ + log_area_sz_;

  uint64_t store_buf_off = total_sz_;
  uint64_t store_sz = ONE_SIDED? (STORE_SIZE * 1024ll * 1024ll) : 0;
  total_sz_ += store_sz;
  uint64_t r_buffer_sz = total_sz_ + r_heap_sz_;

  printf("[MEM] Total log buf area : %fG\n",get_memory_size_g(log_area_sz_));
  printf("[MEM] Total msg buf area : %fG\n",get_memory_size_g(ring_area_sz_));
  printf("[MEM] Total store buffer : %fG\n",get_memory_size_g(store_sz));
  printf("[MEM] Total buffer area  : %fG\n",get_memory_size_g(total_sz_));
  printf("[MEM] Total allocate area: %fG\n",get_memory_size_g(r_buffer_sz));

  // rdma buffer
  char *buf = (char *) malloc_huge_pages(r_buffer_sz, HUGE_PAGE_SZ, 1);
  assert(buf);
  memset(buf, 0, r_buffer_sz);

#ifdef WITH_RDMA
  // start creating RDMA
  const vector<string> &net_def = config.getServerHosts();
  RdmaCtrl *ctrl = new RdmaCtrl(config.getServerID(), net_def, r_port, false);
  if(ctrl == nullptr && net_def.size() != 1) {
    printf("Distributed transactions needs RDMA support!\n");
    assert(false);
  }

  ctrl->set_connect_mr(buf, r_buffer_sz); // register the buffer
  cm_ = ctrl;

  store_buf_ = (char *) cm_->conn_buf_ + store_buf_off;
  free_buffer_ = (char *) cm_->conn_buf_ + total_sz_;
#else
  store_buf_ = buf + store_buf_off;
  free_buffer_ = buf + total_sz_;
#endif

  // Init rmalloc
  // use the free buffer as the local RDMA heap
  uint64_t real_alloced = RInit(free_buffer_, r_heap_sz_);
  assert(real_alloced != 0);
  RThreadLocalInit();

  init_ = true;
}

// set ringsz_per_worker_ and ring_area_sz_
void RdmaConfig::set_ring_area_sz_() {
  if (USE_UD_MSG) {
    ringsz_per_worker_ = 0;
    ring_area_sz_ = 0;
    return;
  }

  int coroutine_num = config.getNumRoutines();
  int nthreads = config.getNumWorkerThreads();
  using rdmaio::ring_imm_msg::MSG_META_SZ;
  // Calculating message size
  uint64_t total_ring_sz =
    // MAX_MSG_SIZE * coroutine_num * 32 + ring_padding_ + MSG_META_SZ; // used for applications
    MAX_MSG_SIZE * coroutine_num * 4 + ring_padding_ + MSG_META_SZ;    // used for micro benchmarks

  ringsz_per_worker_ = total_ring_sz - ring_padding_ - MSG_META_SZ;

  ring_area_sz_ = (total_ring_sz * config.getNumServers()) * nthreads;
}

uint64_t RdmaConfig::getLogBase() const {
  // Set logger's global offset
  printf("  [Mem] Total logger area %fG\n",get_memory_size_g(log_area_sz_));
  return (ring_area_sz_ + M2);
}

void RdmaConfig::check_connect(int worker_id) {
  std::vector<Qp*> qp_vec;
  for(uint i = 0;i < cm_->get_num_nodes();++i) {
    qp_vec.push_back(cm_->get_rc_qp(worker_id,i));
    assert(qp_vec[i]);
  }

  uint64_t *send_buffer = (uint64_t *)(Rmalloc(sizeof(uint64_t)));
  uint64_t *recv_buffer = (uint64_t *)(Rmalloc(sizeof(uint64_t)));

  int server_id = config.getServerID();
  int nthreads = config.getNumTxnThreads();
  for(uint i = 0;i < cm_->get_num_nodes();++i) {

    uint64_t val = 73 + server_id + i;
    uint64_t off = 0 + (server_id + 1) * nthreads * sizeof(uint64_t);

    *send_buffer = val;

    auto ret = qp_vec[i]->rc_post_send(IBV_WR_RDMA_READ,(char *)recv_buffer,sizeof(uint64_t),
                                       0,
                                       IBV_SEND_SIGNALED);
    assert(ret == Qp::IO_SUCC);
    ret = qp_vec[i]->poll_completion();
    assert(ret == Qp::IO_SUCC);
    if(73 + i != *recv_buffer){ // make sure the destination is the destination
      fprintf(stdout,"failed val %lu, at prob %d\n",*recv_buffer,i);
      assert(false);
    }

    ret = qp_vec[i]->rc_post_send(IBV_WR_RDMA_WRITE,(char *)send_buffer,sizeof(uint64_t),
                            off + sizeof(uint64_t) * worker_id,
                            IBV_SEND_SIGNALED);
    assert(ret == Qp::IO_SUCC);
    ret = qp_vec[i]->poll_completion();
    assert(ret == Qp::IO_SUCC);

    ret = qp_vec[i]->rc_post_send(IBV_WR_RDMA_READ,(char *)recv_buffer,sizeof(uint64_t),
                                  off + sizeof(uint64_t) * worker_id,
                                  IBV_SEND_SIGNALED);
    assert(ret == Qp::IO_SUCC);
    ret = qp_vec[i]->poll_completion();
    assert(ret == Qp::IO_SUCC);

    if(*recv_buffer != val) {
      fprintf(stdout,"failed val %lu, required %lu @%d\n",*recv_buffer,val,worker_id);
      assert(false);
    }
    //fprintf(stdout,"check %d %d pass\n",worker_id,i);
  }
  Rfree((void *)send_buffer);
  Rfree((void *)recv_buffer);
}

RdmaConfig rdmaConfig;

}  // namespace framework
}  // namespace nocc

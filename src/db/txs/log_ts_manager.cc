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

#include "log_ts_manager.h"
#include "rdmaio.h"
#include "ralloc.h"
#include "util/util.h"

#include <mutex>

extern size_t current_partition;

using namespace rdmaio;
using namespace nocc::util;

namespace nocc {
namespace db {

uint64_t *LogTSManager::global_base_;
uint64_t LogTSManager::num_macs_;

uint64_t LogTSManager::local_ts_ = 0;
uint64_t LogTSManager::local_ts_lock_ = 0;

LogTSManager::LogTSManager(RdmaCtrl *cm, int worker_id)
    : cm_(cm), worker_id_(worker_id), vec_ts_(NULL) {
  assert(cm_ != NULL);
  num_macs_ = cm_->get_num_nodes();
  qps_ = new rdmaio::Qp *[num_macs_];
}

void LogTSManager::initialize_meta_data(char *buffer, int num_macs) {
  uint64_t *arr = (uint64_t *) (buffer + M1);
  for (uint i = 0; i < num_macs; ++i) {
    assert(arr[i] == 0);
  } 
  LogTSManager::global_base_ = arr;
}

void LogTSManager::initialize_connect() {
  master_qp_ = cm_->get_rc_qp(worker_id_, MASTER_ID, 3);
  for (int i = 0; i < num_macs_; ++i) {
    if (i == MASTER_ID) {
      qps_[i] = master_qp_;
    } else {
      qps_[i] = cm_->get_rc_qp(worker_id_, i, 3);
    }
  }
  vec_ts_ = (uint64_t *) Rmalloc(sizeof(uint64_t) * num_macs_); 
}

// global timestamp
uint64_t LogTSManager::get_global_ts() {
  char *local_buf = (char *) &vec_ts_[current_partition];
  auto ret = master_qp_->
      rc_post_fetch_and_add(local_buf, M1, 1, IBV_SEND_SIGNALED);
  assert(ret == Qp::IO_SUCC);
  master_qp_->poll_completion();
  return vec_ts_[current_partition];
}

// vector timestamp
uint64_t LogTSManager::get_vec_ts() {
  char *local_buf = (char *) &vec_ts_[current_partition];
  uint64_t remote_off = M1 + current_partition * sizeof(uint64_t);

  auto ret = master_qp_->
      rc_post_fetch_and_add(local_buf, remote_off, 1, IBV_SEND_SIGNALED);
  assert(ret == Qp::IO_SUCC);
  master_qp_->poll_completion();

#if 0
  uint64_t ts = vec_ts_[current_partition] + 1;
  local_buf = (char *) vec_ts_;
  uint64_t tv_size = num_macs_ * sizeof(uint64_t);
  auto ret2 = master_qp_->
      rc_post_send(IBV_WR_RDMA_READ, local_buf, tv_size, M1, 
                   IBV_SEND_SIGNALED);
  assert(ret2 == Qp::IO_SUCC);
  master_qp_->poll_completion();
  vec_ts_[current_partition] = ts;
#endif
  return vec_ts_[current_partition];
}

// local timestamp
uint64_t LogTSManager::get_local_ts() {
  uint64_t res;
  while (!CAS(&local_ts_lock_, 0, 1));
  res = local_ts_++;
  // printf("get local_ts %ld\n", local_ts_);
  local_ts_lock_ = 0;
  return res;
}

uint64_t LogTSManager::get_epoch(int server_id) {
  assert(server_id != current_partition);
  char *buf = (char *) &vec_ts_[server_id];
  auto ret = qps_[server_id]->
      rc_post_send(IBV_WR_RDMA_READ, buf, sizeof(uint64_t), M1, IBV_SEND_SIGNALED);
  assert(ret == Qp::IO_SUCC);
  qps_[server_id]->poll_completion();
  return vec_ts_[server_id];
}

uint64_t LogTSManager::get_epoch_buffer(int server_id) {
  return vec_ts_[server_id];
}

void LogTSManager::sync_epoch(int server_id, uint64_t epoch) {
  assert(server_id != current_partition);
  vec_ts_[server_id] = epoch;
  char *buf = (char *) &vec_ts_[server_id];
  auto ret = qps_[server_id]->
      rc_post_send(IBV_WR_RDMA_WRITE, buf, sizeof(uint64_t), M1, IBV_SEND_SIGNALED);
  assert(ret == Qp::IO_SUCC);
  qps_[server_id]->poll_completion();
}

}  // namesapce db
}  // namesapce nocc

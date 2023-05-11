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

#ifndef LOG_TS_MANAGER_H_
#define LOG_TS_MANAGER_H_

#include "util/util.h"
#include "rdmaio.h"

namespace nocc {
namespace db {

class LogTSManager {

 public:

  LogTSManager(rdmaio::RdmaCtrl *cm, int worker_id);
  // initialize [1M, 2M] from the rdma_buffer to use as log TS region
  static void initialize_meta_data(char *buffer, int num_macs);
  void initialize_connect();

  uint64_t get_global_ts();
  uint64_t get_vec_ts();
  uint64_t get_local_ts();
  uint64_t get_epoch(int server_id);
  uint64_t get_epoch_buffer(int server_id);
  void sync_epoch(int server_id, uint64_t epoch);

 private:
  
  static const uint64_t M1 = 1024 * 1024ll;// HUGE_PAGE_SIZE / 2
  const int MASTER_ID = 0;

  rdmaio::RdmaCtrl *cm_;
  rdmaio::Qp *master_qp_;
  rdmaio::Qp **qps_;

  int worker_id_;
  uint64_t *vec_ts_;
  static uint64_t num_macs_;
  static uint64_t *global_base_;

  static uint64_t local_ts_;
  static uint64_t local_ts_lock_;
};

}  // namesapce db
}  // namesapce nocc

#endif

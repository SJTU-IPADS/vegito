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

#ifndef RDMA_CONFIG_H_
#define RDMA_CONFIG_H_

#include "all.h"
#include "custom_config.h"
#include "rdmaio.h"

namespace nocc {
namespace framework {

class RdmaConfig {
 public:
  RdmaConfig();
  void init_rdma(uint64_t log_area_sz);
  void check_connect(int worker_id);

  inline rdmaio::RdmaCtrl *getRdmaCtrl() const { return cm_; }
  inline char *getStoreBuf() const { return store_buf_; }
  inline char *getFreeBuf() const { return free_buffer_; }
  inline uint64_t getRingPadding() const { return ring_padding_; }
  inline uint64_t getRingSize() const { return ringsz_per_worker_; }

  uint64_t getLogBase() const;
  
 private:

  // set ringsz_per_worker_ and ring_area_sz_
  void set_ring_area_sz_();

  static const uint64_t r_heap_sz_ = 2 * 1024ll * 1024ll * 1024ll;
  static const int r_port = 23333;
  static const uint64_t ring_padding_ = MAX_MSG_SIZE;
  static const uint64_t M2 = HUGE_PAGE_SZ;

  bool init_;
  rdmaio::RdmaCtrl *cm_;
  char *store_buf_;
  uint64_t ringsz_per_worker_;
  uint64_t ring_area_sz_;
  uint64_t log_area_sz_;
  uint64_t total_sz_;

  char *free_buffer_; 
};

extern RdmaConfig rdmaConfig;

}  // namespace framework
}  // namespace nocc

#endif  // RDMA_CONFIG_H_

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

#include "log_area.h"
#include <cassert>

namespace nocc {
namespace framework {

void LogArea::init(int num_node, int num_thread, int num_log, 
                   uint32_t log_area_k) {
  assert(log_area_k < 4 * 1024 * 1024);  // log_area must smaller than 4G
  num_node_ = num_node;
  num_thread_ = num_thread;
  num_log_ = num_log;

  log_area_sz_ = log_area_k * 1024;
  log_meta_sz_ = sizeof(uint64_t) * num_node;
  log_padding_sz_ = 32 * 1024;
  assert(log_area_sz_ > log_meta_sz_ + log_padding_sz_);
  log_buf_sz_ = log_area_sz_ - log_meta_sz_ - log_padding_sz_;

  thread_area_sz_ = num_log_ * uint64_t(log_area_sz_);
  node_area_sz_ = num_thread_ * thread_area_sz_;
  sz_ = num_node_ * node_area_sz_;
}

LogArea logArea;

}  // namesapce framework
}  // namesapce nocc

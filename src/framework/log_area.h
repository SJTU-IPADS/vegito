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

#ifndef LOG_AREA_H_
#define LOG_AREA_H_

#include <cstdint>

//                        logger memory structure
//  |--------------------------------node_area---------------------------------|
//  ||--------------------------thread_area---------------------------|        |
//  |||-----------log_area-----------|                                |        |
//  ||||-META-||---BUF---||-PADDING-||                                |        |
//  {<([      ][         ][         ])([      ][         ][         ])>  <....>} {....}


namespace nocc {
namespace framework {

class LogArea {
 public:
  LogArea() { }
  void init(int num_node, int num_thread, int num_log, 
            uint32_t log_area_k);
  void setBase(uint64_t base_off) { base_off_ = base_off; }

  inline uint64_t size() const { return sz_; }
  inline int num_log() const { return num_log_; }

  // inline uint64_t getNodeAreaSize() const { return node_area_sz_; }

  inline uint32_t getLogAreaSize() const { return log_area_sz_; }
  inline uint32_t getLogBufferSize() const { return log_buf_sz_; }
  inline uint32_t getLogPadding() const { return log_padding_sz_; }

  inline uint64_t getBase() const { return base_off_; }

  inline uint64_t getMetaBase(int node_id, int thread_id, int log_id) const {
    return (base_off_ + node_id * node_area_sz_ + 
            thread_id * thread_area_sz_ + log_id * log_area_sz_);
  }

  inline uint64_t getBufferBase(int node_id, int thread_id, int log_id) const {
    return (getMetaBase(node_id, thread_id, log_id) + log_meta_sz_);
  }


 private:

  int num_node_;
  int num_thread_;
  int num_log_;

  uint32_t log_area_sz_;
  uint32_t log_meta_sz_;
  uint32_t log_padding_sz_;
  uint32_t log_buf_sz_;

  uint64_t thread_area_sz_;
  uint64_t node_area_sz_;
  uint64_t sz_;

  uint64_t base_off_;
};

extern LogArea logArea;

}  // namesapce framework
}  // namesapce nocc

#endif  // LOG_AREA_H_

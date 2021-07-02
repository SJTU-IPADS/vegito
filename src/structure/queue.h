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

#include <vector>

template <class T>
class Queue {
 public:
  Queue()
    : header_(0), tailer_(0) { }

  inline bool enqueue(const T &ele) {
    if((MAX_SZ - (header_ - tailer_)) == 0)
      return false;

    buf_[tailer_ % MAX_SZ] = ele;
    asm volatile("" ::: "memory");
    ++tailer_;
    return true;
  }

  inline bool dequeue(T &ele) {
    if (header_ == tailer_) return false; 

    ele = buf_[header_ % MAX_SZ];
    ++header_;
    return true;
  }
  
  // TODO: 2 stage enqueue and dequeue
  inline T *pre_enqueue() {
    asm volatile("" ::: "memory");
    if((MAX_SZ - (header_ - tailer_)) == 0)
      return nullptr;

    return &buf_[tailer_ % MAX_SZ];
  }

  inline void post_enqueue() {
    asm volatile("" ::: "memory");
    ++tailer_;
  }

  inline const T *pre_dequeue() const {
    asm volatile("" ::: "memory");
    if (header_ == tailer_) {
      return nullptr; 
    }
    return &buf_[header_ % MAX_SZ];
  }

  inline void post_dequeue() {
    ++header_;
  }

  inline uint64_t size() const { return tailer_ - header_; }

 private:
  static const size_t MAX_SZ = 512;
  T buf_[MAX_SZ];
  volatile uint64_t header_;
  volatile uint64_t tailer_;
} __attribute__ ((aligned (CACHE_LINE_SZ)));

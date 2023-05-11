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

#ifndef UTIL_TEMP_LOGGER_H
#define UTIL_TEMP_LOGGER_H

// #include <unordered_set>

#include "ralloc.h"
#include "config.h"

#define unlikely(x) __builtin_expect(!!(x), 0)
#define likely(x)   __builtin_expect(!!(x), 1)

// #define TEMP_LOG_DEBUGGING

const int ROUND_UP_BASE = 8;

class TempLog {
 public:
  std::vector<std::vector<uint64_t>> log_off;
  std::vector<std::vector<uint64_t>> ack_off;

  TempLog(int num_nodes, int num_logs, 
          uint32_t reply_buf_size = 0, uint32_t reserved_size = 0)
    : reserved_size_(reserved_size),
      in_use_(false),
      mem_start_(nullptr),
      start_(nullptr),
      current_(nullptr),
      end_(nullptr),
      alloced_size_(0),
      reply_buf_(reply_buf_size, 0),
      log_off(num_nodes, std::vector<uint64_t> (num_logs, 0)), 
      ack_off(num_nodes, std::vector<uint64_t> (num_logs, 0)) 
  { }

  inline static int decode_mac_id(uint64_t mac_log) {
    return int(mac_log >> 32);
  }

  inline static int decode_log_id(uint64_t mac_log) {
    return int(mac_log << 32 >> 32);
  }

  inline char *start() const { return start_; }
  inline char* current() const { return current_; }

  inline uint64_t total_size() const {
    return (uint64_t)(end_ - start_);
  }

  inline uint64_t log_size() const {
    return (uint64_t)(end_ - start_ - sizeof(uint64_t));
  }

  inline const std::set<uint64_t> &mac_log_ids() const { return mac_log_; }

  inline bool empty() const { return mac_log_.empty(); }

  inline std::vector<int> mac_vec() const {
    return std::vector<int> (mac_.begin(), mac_.end());
  }

  inline char *reply_buf() { return reply_buf_.data(); }

  inline void add_dest(int mac_id, int log_id) {
    uint64_t code = encode_mac_log_(mac_id, log_id);
    mac_log_.insert(code);
    mac_.insert(mac_id);
  }

  void open() {
    assert(in_use_ == false);
    alloced_size_ = base_alloc_size_;
    
    mem_start_ = (char*) Rmalloc(alloced_size_);
    assert(mem_start_ != nullptr);
    assert(((uint64_t)mem_start_) % ROUND_UP_BASE == 0);

    std::fill(reply_buf_.begin(), reply_buf_.end(), 0);

    start_ = mem_start_ + reserved_size_;
    end_ = current_ = start_ + sizeof(uint64_t);
    print_();
    in_use_ = true;
  }

  void close() {
    assert(in_use_ = true);
    Rfree(mem_start_);
    mem_start_ = start_ = current_ = end_ = nullptr;

    mac_.clear();
    mac_log_.clear();

    in_use_ = false;
  }

  char* append_entry(int size) {
    assert(current_ == end_);
    resize_(size);
    end_ += size;
    print_();
    return current_;
  }

  void close_entry() {
    current_ = end_;
    print_();
  }

  void log_size_round_up() {
    assert(current_ == end_);
    uint64_t log_size = total_size();
    if(log_size % ROUND_UP_BASE != 0) {
      uint64_t delta = ROUND_UP_BASE - log_size % ROUND_UP_BASE;
      if(unlikely(log_size + delta > alloced_size_)){
        resize_(delta);
      }
      current_ = end_ += delta;
    }
    print_();
  }
 
 private:
  inline static uint64_t encode_mac_log_(int mac_id, int log_id) {
    uint64_t code = uint64_t(mac_id) << 32 | uint64_t(log_id);
    return code;
  }

  inline uint64_t mem_size_() const {
    return (uint64_t)(end_ - mem_start_);
  }

  inline void resize_(int size) {
    bool need_resize = false;
    while(mem_size_() + size > alloced_size_){
      alloced_size_ *= 2;
      need_resize = true;
    }
    if(need_resize){
      char *new_start = (char*)Rmalloc(alloced_size_);
      uint64_t old_size = mem_size_();
      memcpy(new_start, mem_start_, old_size);
      Rfree(mem_start_);
      mem_start_ = new_start;
      start_ = mem_start_ + reserved_size_;
      end_ = current_ = mem_start_ + old_size;
    }
  }
  
  inline void print_() const {
#ifdef TEMP_LOG_DEBUGGING
    printf("templog: %p %p %p\n", start_, current_,end_);
    printf("total_size: %lu\n", total_size());
    printf("alloced_size_: %d\n", alloced_size_);
#endif
  }

  static const int base_alloc_size_ = 512;
  const uint32_t reserved_size_;

  bool in_use_;

  // start of the memory of temp log
  char *mem_start_;
  char *start_, *current_, *end_;

  uint32_t alloced_size_;
  
  std::set<int> mac_;  // 
  std::set<uint64_t> mac_log_;  // mac_id << 32 | log_id
  
  std::vector<char> reply_buf_;
};

#endif

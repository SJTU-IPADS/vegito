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

#ifndef NOCC_DB_ROUTINE_H
#define NOCC_DB_ROUTINE_H

#include "all.h"
#include "utils/macros.h"

namespace nocc {
namespace oltp {

// functions exposed to the upper layer
// -----------------------------------
struct RoutineMeta {
  RoutineMeta()
    : active(false), next(nullptr), prev(nullptr) { }

  int  id;
  bool active;  // active if in the chain
  coroutine_func_t fn;
  RoutineMeta *next;
  RoutineMeta *prev; // for find prev when delete meta from list
};

class Routine {
 public:
  Routine(int num_routines);
  
  void print_chain() const;

  int get_cur_id() const { return cur_->id; }
  
  void set_routine_fn(int id, coroutine_func_t &&fn);

  void start();

  void inline ALWAYS_INLINE add_to_routine_list(int id) {
    assert(id != 0);

    RoutineMeta *meta = &list_[id];
    if(meta->active) return; //skip add to the routine chain

    assert(tailer_->next == header_);
    tailer_->next = meta;
    meta->next = header_;
    meta->prev = tailer_;
    tailer_ = meta;
    meta->active = true;
  }
  
  // release `cur_` from the routine list
  void inline ALWAYS_INLINE yield_from_routine_list(yield_func_t &yield) {
    assert(cur_->active && cur_ != header_);
    RoutineMeta *next = cur_->next;
    cur_->prev->next = next;
    next->prev  = cur_->prev;
    if(tailer_ == cur_)
      tailer_ = cur_->prev;
    cur_->prev = cur_->next = nullptr;
    cur_->active = false;

    cur_ = next;
    yield(cur_->fn);
  }

  void inline ALWAYS_INLINE yield_to_next(yield_func_t &yield) {
    RoutineMeta *next = cur_->next;
    for ( ; next != cur_ && !next->active; next = next->next) ; 

    if (cur_ == next) return;

    cur_ = next;
    assert(cur_->active && cur_->fn);
    yield(cur_->fn);
  } 
  
 private:
  const int num_routines_;
  // expose some of the data structures
  RoutineMeta * const list_;
  RoutineMeta *tailer_;
  RoutineMeta *header_;
  RoutineMeta *cur_;
};

#if 0
inline __attribute__ ((always_inline))
RoutineMeta *get_routine_meta(int id) {
  return list_ + id;
}
#endif
}  // namespace oltp
}  // namesapce nocc



#endif

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

#include "routine.h"

#include <boost/bind.hpp>

namespace nocc {
namespace oltp {
Routine::Routine(int num) 
  : num_routines_(num >= 0? num : 0), 
    list_(new RoutineMeta[num_routines_]) {

  if (num == 0) return;
  assert(num > 0);
 
  for(int i = 0;i < num_routines_; ++i) {
    RoutineMeta &meta = list_[i];
    meta.id = i;
    meta.next = &list_[i + 1];
    meta.prev = &list_[i - 1];
  }

  header_ = &list_[0];
  tailer_ = &list_[num_routines_ - 1];

  // set head routine's chain
  header_->prev = header_;  // loop in the header

  // set tail routine's chain
  tailer_->next = header_; //loop back

  cur_ = header_;
}

void Routine::set_routine_fn(int id, coroutine_func_t &&fn) {
  assert(id >=0 && id < num_routines_);
  list_[id].fn = std::move(fn);
  list_[id].active = true;
}

void Routine::start() {
  assert(cur_->active);
  cur_->fn();
}

void Routine::print_chain() const {
  // for debugging
  RoutineMeta *cur = header_;
  for(uint counter = 0; counter < 10; ++counter) {
    fprintf(stdout,"%d\t",cur->id);
    cur = cur->next;
    if(cur == tailer_)
      break;
  }
  fprintf(stdout,"%d\n",cur->id);
}
  
}  // namespace oltp
}  // namespace nocc

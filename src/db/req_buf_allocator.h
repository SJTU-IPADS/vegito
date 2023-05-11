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

#ifndef NOCC_DB_MEM_ALLOCATOR
#define NOCC_DB_MEM_ALLOCATOR

#include "all.h"
#include "ralloc.h"
#include "config.h"

#include <stdint.h>
#include <queue>

namespace nocc {

  namespace db {

    // RPC memory allocator
    class RPCMemAllocator {
    public:

      // must be created after RThreadlocalinit
      RPCMemAllocator() {
        for(int i = 0;i < MAX_INFLIGHT_REQS;++i) {
#if 1
          buf_pools_[i] = (char *)Rmalloc(MAX_MSG_SIZE);
          //assert(buf_pools_[i] != NULL);
          // check for alignment
          assert( ((uint64_t)(buf_pools_[i])) % 8 == 0);
          if(buf_pools_[i] != NULL)
            memset(buf_pools_[i],0,MAX_MSG_SIZE);
          else
            assert(false);
#endif
        }
        current_buf_slot_ = 0;
      }

      inline char * operator[] (int id) const{
        return buf_pools_[id];
      }

      inline char * get_req_buf() {

        uint16_t buf_idx = (current_buf_slot_++) % MAX_INFLIGHT_REQS;
        assert(buf_idx >= 0 && buf_idx < MAX_INFLIGHT_REQS);

        // fetch the buf
        char *res = buf_pools_[buf_idx];
        return res;
      }

      inline void post_buf(int num = 1) {
        current_freed_slot_ += num;
      }

    private:
      char *buf_pools_[MAX_INFLIGHT_REQS];
      uint64_t current_buf_slot_;
      uint64_t current_freed_slot_;
    };
  } // namespace db
};  // namespace nocc

#endif

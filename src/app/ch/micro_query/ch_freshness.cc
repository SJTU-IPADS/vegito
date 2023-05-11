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

#include "app/ch/ch_query.h"

#include <thread>

using namespace std;
using namespace nocc::oltp::ch;

namespace nocc {
namespace oltp {
namespace ch {

// Tranverse stock table and sum the unchanged field 'ol_quantity'
bool ChQueryWorker::micro_freshness(yield_func_t &yield) {
  if(current_partition != 2) return true;

  // struct timeval tv;
  // gettimeofday(&tv, nullptr);
  // uint64_t now_us = (uint64_t(tv.tv_sec) * 1000000 + tv.tv_usec);
  uint64_t ol_tbl_sz = ol_tbl->getOffsetHeader();
  uint64_t ver = get_read_ver_();
  Keys ol_keys = ol_tbl->getKeyCol();
  assert(ol_keys.size() >= ol_tbl_sz);
  uint64_t ol_key = ol_keys[ol_tbl_sz - 1];
  assert(ol_key != 0);
  // uint64_t *time = (uint64_t *) db_->Get(ORLI, ol_key, OL_DELIVERY_D, ver);
  uint64_t *time = nullptr;
  time = (uint64_t *) db_->GetByOffset(ORLI, ol_tbl_sz - 1, OL_DELIVERY_D, ver);
  while (!time) {
    ver = get_read_ver_();
    time = (uint64_t *) db_->GetByOffset(ORLI, ol_tbl_sz - 1, OL_DELIVERY_D, ver);
  }
  assert(time);
  // printf("offset %lu\n", ol_tbl_sz - 1);
#if 0
  printf("now %ld sec, %ld usec, time %lu usec\n", tv.tv_sec, tv.tv_usec, *time);
  if (*time) {
    printf("epoch: %d ms, freshness: %lf ms\n",
           int(config.getEpochSeconds() * 1000), (now_us - *time) / 1000.0);
  }
#endif

#if 1
  RpcReply *reply = (RpcReply *) payload_ptr_;
  reply->size = sizeof(uint64_t);
  uint64_t *buf = (uint64_t*) (payload_ptr_ + sizeof(RpcReply));
  *buf = *time;
#endif
  return true;
}

}  // namespace ch
}  // namesapce oltp
}  // namespace nocc

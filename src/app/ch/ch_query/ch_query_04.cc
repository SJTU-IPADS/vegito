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

#include <vector>
using namespace std;
using namespace nocc::oltp::ch;

namespace {

const int NUM_ORLI = 15;

struct Ctx : public BaseCtx {
  // Query data
  vector<uint64_t> sub_result;

  Ctx(BackupDB &d, uint64_t v, uint64_t b, uint64_t e) 
    : BaseCtx(d, v, b, e), sub_result(NUM_ORLI, 0) { }

} __attribute__ ((aligned (CACHE_LINE_SZ)));

void query(void *arg);

} // namespace anonymous

namespace nocc {
namespace oltp {
namespace ch {


bool ChQueryWorker::query04(yield_func_t &yield) {

  uint64_t ver = get_read_ver_();
  uint64_t cnt = 0;

  // Result data
  vector<uint64_t> result_table(NUM_ORLI, 0);

  /**************** Parallel Part Begin ********************/
  vector<Ctx> ctxs;
  ctxs.reserve(num_thread_);

  // Calculate workload for each thread
  uint64_t ol_tbl_sz = ol_tbl->getOffsetHeader();
  uint64_t o_tbl_sz = o_tbl->getOffsetHeader();
  db_cnt_ = ol_tbl_sz;
  vector<vector<uint64_t>> workloads = part_workloads(o_tbl_sz);

  for (const auto &v : workloads) {
    uint64_t off_begin = v[0],
             off_end   = v[1];
    ctxs.emplace_back(*db_, ver, off_begin, off_end);
  }
  
  parallel_process(ctxs, query);

  // Collect data from all slave threads
  for (const Ctx &ctx : ctxs) {
    for (int j = 0; j < NUM_ORLI; j ++) {
      result_table[j] += ctx.sub_result[j];
    }

    cnt += ctx.cnt;
    walk_cnt_ += ctx.walk_cnt;
  }
  // printf("count = %d\n");

  /**************** Parallel Part End ********************/

 if (!q_verbose_) return true;

  uint64_t res_cnt = 0;

  printf("Result of query 4:\n");
  printf("o_ol_cnt order_count \n");
  for (int i = 0; i < NUM_ORLI; i++) {
    if (result_table[i] > 0) {
      res_cnt++;
      printf("%-8d %-12ld\n", i + 1, result_table[i]);
    }
  }

  printf("Result: %lu tuples, total record cnt: %lu\n",
         res_cnt, cnt);
  return true;

}
}  // namespace ch
}  // namespace oltp
}  // namespace nocc

namespace {

void query(void *arg) {
  Ctx &ctx = * (Ctx *) arg;
  uint64_t cnt = 0;

  Keys o_keys = o_tbl->getKeyCol();

  for (uint64_t o_i = ctx.off_begin; o_i < ctx.off_end; ++o_i) {
    int64_t o_key = (int64_t) o_keys[o_i];
    int32_t o_w_id = orderKeyToWare(o_key);
    int32_t o_d_id = orderKeyToDistrict(o_key);
    int32_t o_o_id = orderKeyToOrder(o_key);

    if (o_entry_ds[o_i] > 0) {
      int8_t o_ol_cnt = o_ol_cnts[o_i];
      uint32_t o_entry_d = o_entry_ds[o_i];

      uint64_t ol_key_begin = 
        (uint64_t) makeOrderLineKey(o_w_id, o_d_id, o_o_id, 1);
      uint64_t ol_key_end = 
        (uint64_t) makeOrderLineKey(o_w_id, o_d_id, o_o_id, o_ol_cnt);
      assert(o_ol_cnt > 0);

      for (uint64_t ol_key = ol_key_begin; ol_key <= ol_key_end; ++ol_key) {
        uint32_t *ol_delivery_d = (uint32_t *) ctx.db.Get(ORLI, ol_key, OL_DELIVERY_D, ctx.ver, &ctx.walk_cnt);
        if (ol_delivery_d == nullptr) continue;

        if (*ol_delivery_d >= o_entry_d) {
          ctx.sub_result[o_ol_cnt - 1]++;
          cnt++; // access orderline record(including both valid and invalid record)
        }
      }
    }
  }
  ctx.cnt = cnt;
}

} // namespace anonymous


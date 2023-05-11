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

#define TIMER 0

#include "ch_query_timer.h"

namespace {

struct Ctx : public BaseCtx {
  // Query data
  AMOUNT revenue;

  Ctx(BackupDB &d, uint64_t v, uint64_t b, uint64_t e) 
    : BaseCtx(d, v, b, e), revenue(0) { }
} __attribute__ ((aligned (CACHE_LINE_SZ)));

void query(void *arg);

} // namespace anonymous


namespace nocc {
namespace oltp {
namespace ch {

bool ChQueryWorker::query06(yield_func_t &yield) {

  uint64_t ver = get_read_ver_();
  uint64_t cnt = 0;

  // Result data
  AMOUNT revenue = 0;

  /**************** Parallel Part Begin ********************/
  vector<Ctx> ctxs;
  ctxs.reserve(num_thread_);

  // Calculate workload for each thread
  uint64_t ol_tbl_sz = ol_tbl->getOffsetHeader();
  db_cnt_ = ol_tbl_sz;
  
  vector<vector<uint64_t>> workloads = part_workloads(ol_tbl_sz);

  for (const auto &v : workloads) {
    uint64_t off_begin = v[0],
             off_end   = v[1];
    ctxs.emplace_back(*db_, ver, off_begin, off_end);
  }
    
  parallel_process(ctxs, query);

  /**************** Parallel Part End ********************/

  // Collect data from all slave threads
  for (const Ctx &ctx : ctxs) {
    revenue += ctx.revenue;
    cnt += ctx.cnt;
  }

  if (!q_verbose_) return true;

  printf("Result of query 6:\n");
  printf("revenue     \n");
  printf("%-12.2f\n", revenue);

  printf("Result: %lu tuples, selected %lu tuples\n", 1ul, cnt);

  return true;

}
}  // namespace ch
}  // namespace oltp
}  // namespace nocc

namespace {

void query(void *arg) {
  Ctx &ctx = *(Ctx *) arg;

  uint64_t cnt = 0;

  unique_ptr<BackupStore::ColCursor> 
    del_cur = ol_tbl->getColCursor(OL_DELIVERY_D, ctx.ver);
  assert(del_cur.get());
  del_cur->seekOffset(ctx.off_begin, ctx.off_end);
  ol_tbl->locateCol(OL_DELIVERY_D, sizeof(uint32_t));

  while (del_cur->nextRow()) {
    uint32_t ol_delivery_d = *(uint32_t *) del_cur->value();
    uint64_t ol_i = del_cur->cur();
    int8_t ol_quantity = ol_quantities[ol_i];
    if (ol_delivery_d > 0 && ol_delivery_d <= (uint32_t)-1 
        && ol_quantity >= 1 && ol_quantity <= 100000) {
      ctx.revenue += ol_amounts[ol_i];
      ++cnt;
    }
  }

  ctx.cnt = cnt;
}

} // namespace anonymous


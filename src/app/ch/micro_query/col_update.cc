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

namespace {

struct Ctx : public BaseCtx {
  // Query data
  int64_t s_quantity;
  uint64_t *read_op;

  Ctx(BackupDB &d, uint64_t v, uint64_t b, uint64_t e, uint64_t *p) 
    : BaseCtx(d, v, b, e), s_quantity(0), read_op(p) { }
} __attribute__ ((aligned (CACHE_LINE_SZ))) ;

void query(void *arg) {
  Ctx &ctx = *(Ctx *) arg;
 
  register uint64_t cnt = 0;
  register uint64_t s_quantity = 0;

  uint64_t ver = ctx.ver;

  for (int i = 0; ; ++i) {
    unique_ptr<BackupStore::RowCursor> row_cur = s_tbl->getRowCursor(ver);
    if (row_cur.get() != nullptr) {
      // row s_tbl
      uint64_t off = s_tbl->locateCol(S_QUANTITY, sizeof(int16_t)); 
      row_cur->seekOffset(ctx.off_begin, ctx.off_end);
      while (row_cur->nextRow()) {
        const char *val = row_cur->value(); 
        int32_t order_cnt = *(int16_t *) (val + off);
        s_quantity += order_cnt;
        ++cnt ;
        ++(*ctx.read_op);
        if (cnt == 32000000) goto outer;
      } 
    } else {
      unique_ptr<BackupStore::ColCursor> col_cur = s_tbl->getColCursor(S_QUANTITY, ctx.ver);
      assert(col_cur.get() != nullptr);
      uint64_t off = 0;
      // off = s_tbl->locateCol(S_QUANTITY, sizeof(int16_t));
      assert(off == 0); 
      col_cur->seekOffset(ctx.off_begin, ctx.off_end);
      while (col_cur->nextRow(&ctx.walk_cnt)) {
        const char *val = col_cur->value(); 
        int32_t order_cnt = *(int16_t *) (val + off);
        s_quantity += order_cnt;
        ++cnt ;
        ++(*ctx.read_op);
        // if (cnt == 32000000) break;
        if (cnt == 32000000) goto outer;
      }
    }
  }
outer:
  ctx.s_quantity = s_quantity;
  ctx.cnt = cnt;
  return;
}

}

namespace nocc {
namespace oltp {
namespace ch {

// Tranverse stock table and sum the unchanged field 's_quantity'
bool ChQueryWorker::micro_col_update(yield_func_t &yield) {
  uint64_t ver = get_read_ver_();
  // uint64_t ver = get_read_ver_() - 2;
  uint64_t cnt = 0;
  // Result data
  int64_t sum_s_quantity = 0;

  /**************** Parallel Part Begin ********************/

  // Thread information
  vector<Ctx> ctxs;
  ctxs.reserve(num_thread_);

  // Calculate workload for each thread
  db_cnt_ = s_tbl_sz;

  vector<vector<uint64_t>> workloads = part_workloads(s_tbl_sz);

  for (const auto &v : workloads) {
    uint64_t off_begin = v[0],
             off_end   = v[1];
    ctxs.emplace_back(*db_, ver, off_begin, off_end, &prof_.read_op);
  }
 
  uint64_t begin = rdtsc();
  parallel_process(ctxs, query);

#if 0
  uint64_t workload_per_thread = (s_tbl_sz + num_thread_ - 1) / num_thread_;

  for (int i = 0; i < num_thread_; ++i) {
    uint64_t off_begin = workload_per_thread * i;
    uint64_t off_end = workload_per_thread * (i + 1);
    if (off_end >= s_tbl_sz)
      off_end = s_tbl_sz;
    ctxs.emplace_back(db_, ver, off_begin, off_end);
  }

  // start query
  for (int i = 1; i < subs_.size(); ++i) {
    subs_[i]->set_task(query, (void *) &ctxs[i]);
  }

  query((void *) &ctxs[0]);

  for (int i = 1; i < subs_.size(); ++i) 
    subs_[i]->clear();
#endif

  // Collect data from all slave threads
  walk_cnt_ = 0;
  for (const Ctx &ctx : ctxs) {
    sum_s_quantity += ctx.s_quantity;
    cnt += ctx.cnt;
    walk_cnt_ += ctx.walk_cnt;
  }

  uint64_t end = rdtsc();
  float seconds = float(end - begin) / util::Breakdown_Timer::get_one_second_cycle();
  printf("sum_s_quantity = %ld, cnt = %lu, walk = %lu\n", sum_s_quantity, cnt, walk_cnt_);
  printf("  %f secs\n", seconds);

  /**************** Parallel Part End ********************/

#if 0
#if SHOW_QUERY_RESULT
  printf("sum_s_quantity\n");
  printf("%-lu\n", sum_s_quantity);
#endif

  float seconds = float(end - begin) / SECOND_CYCLE_;
  printf("Query configuration: use %s and %s\n",
          stock_types[BACKUP_STORE_TYPE].c_str(),
          query_methods[QUERY_METHOD].c_str());
  printf("Total record cnt: %lu, result size: %d, %f secs\n",
          cnt, 1, seconds);
#endif
  return true;
}

}  // namespace ch
}  // namesapce oltp
}  // namespace nocc

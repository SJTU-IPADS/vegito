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

struct Ctx {
  // Store data
  BackupDB *db;
  uint64_t ver;

  // Partition information
  uint64_t c_begin;
  uint64_t c_end;

  // Query data
  int64_t checksum;

  // Evaluation data
  uint64_t cnt;

  Ctx(BackupDB *d, uint64_t v, uint64_t b, uint64_t e) 
    : db(d), ver(v), c_begin(b), c_end(e), cnt(0), checksum(0) { }

} __attribute__ ((aligned (CACHE_LINE_SZ))) ;

void query(void *arg) {
  Ctx &ctx = *(Ctx *) arg;
  register uint64_t cnt = 0;
  register uint64_t checksum = 0;

  int merge_cols = 4;

  BackupStore *store = ctx.db->getStore(CUST);
  unique_ptr<BackupStore::RowCursor> row_cur = store->getRowCursor(ctx.ver);
  std::vector<int> cust_col = { C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DELIVERY_CNT };
  cust_col.resize(merge_cols);
  if (row_cur.get() != nullptr) {
    // row store
    uint64_t off = store->locateCol(C_BALANCE, sizeof(float)); 
    row_cur->seekOffset(ctx.c_begin, ctx.c_end);
    while (row_cur->nextRow()) {
      store->update(ctx.c_begin+cnt, cust_col, row_cur->value(), ctx.ver, ctx.ver);
      ++cnt ;
    } 
  } else {
    bool test_flex_col = true;
    if(test_flex_col) {
      std::cout << "use flex col store!" << std::endl;
      unique_ptr<BackupStore::ColCursor> col_cur = store->getColCursor(0, ctx.ver);
      assert(col_cur.get() != nullptr);

      uint64_t off = store->locateCol(0, sizeof(float) * merge_cols);
      assert(off == 0);

      col_cur->seekOffset(ctx.c_begin, ctx.c_end);
      while (col_cur->nextRow()) {
        store->update(ctx.c_begin+cnt, 0, col_cur->value(), ctx.ver);
        ++cnt ;
      }
    } else {
      std::cout << "use col store!" << std::endl;
      std::vector<unique_ptr<BackupStore::ColCursor>> col_cursors(merge_cols);
      for(int i = 0; i < merge_cols; i++) {
        col_cursors[i] = store->getColCursor(i, ctx.ver);
        assert(col_cursors[i].get() != nullptr);
        uint64_t off = store->locateCol(i, sizeof(float));
        assert(off == 0);
        col_cursors[i]->seekOffset(ctx.c_begin, ctx.c_end);
      }

      while (col_cursors[0]->nextRow()) {
        for(int i = 1; i < merge_cols; i++) {
          assert(col_cursors[i]->nextRow());
        }

        // calculate checksum
        for(int i = 0; i < merge_cols; i++) {
          store->update(ctx.c_begin+cnt, i, col_cursors[i]->value(), ctx.ver);
        }
        ++cnt ;
      }
    }
  }

  ctx.checksum = checksum;
  ctx.cnt = cnt;
}

}

namespace nocc {
namespace oltp {
namespace ch {

// Tranverse customer table and calculate the combined column 'valid_cols'
bool ChQueryWorker::micro_flex_prop_write(yield_func_t &yield) {
  uint64_t ver = get_read_ver_();
  uint64_t cnt = 0;
  // Result data
  int64_t checksum = 0;

  /**************** Parallel Part Begin ********************/

  // Thread information
  vector<Ctx> ctxs;
  ctxs.reserve(num_thread_);

  // Calculate workload for each thread
  uint64_t c_tbl_sz = db_->getStore(CUST)->getOffsetHeader();
  uint64_t workload_per_thread = (c_tbl_sz + num_thread_ - 1) / num_thread_;

  for (int i = 0; i < num_thread_; ++i) {
    uint64_t c_begin = workload_per_thread * i;
    uint64_t c_end = workload_per_thread * (i + 1);
    if (c_end >= c_tbl_sz)
      c_end = c_tbl_sz;
    ctxs.emplace_back(db_, ver, c_begin, c_end);
  }

  uint64_t begin = rdtsc();
  // start query
  parallel_process(ctxs, query);

  // Collect data from all slave threads
  for (const Ctx &ctx : ctxs) {
    checksum += ctx.checksum;
    cnt += ctx.cnt;
  }

  uint64_t end = rdtsc();

  float seconds = float(end - begin) / util::Breakdown_Timer::get_one_second_cycle();
  printf("Total record cnt: %lu, result size: %d, %f secs, write thpt %f (record/S)\n", 
          cnt, 1, seconds, cnt / (seconds));

  return true;
}

}  // namespace ch
}  // namesapce oltp
}  // namespace nocc

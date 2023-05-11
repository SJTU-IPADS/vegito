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

using namespace std;
using namespace nocc::oltp::ch;

#define TIMER 0

#include "ch_query_timer.h"

namespace {

const int MAX_ORLI = 15;

struct Foo : public BaseCtx {
  // Evaluation data
  uint64_t filter_cnt;

  // Query data
  vector<uint64_t> sum_qty_vec;
  
  Foo(BackupDB &d, uint64_t v, uint64_t b, uint64_t e) 
    : BaseCtx(d, v, b, e),
      filter_cnt(0), sum_qty_vec(MAX_ORLI, 0) 
  { }
};

struct Ctx : public BaseCtx {
  // Evaluation data
  uint64_t filter_cnt;

  // Query data
  vector<uint64_t> sum_qty_vec;
  vector<AMOUNT>   sum_amount_vec;
  vector<uint64_t> count_order_vec;

  // XXX: order is important
  Ctx(BackupDB &d, uint64_t v, uint64_t b, uint64_t e) 
    : BaseCtx(d, v, b, e),
      filter_cnt(0), sum_qty_vec(MAX_ORLI, 0), 
      sum_amount_vec(MAX_ORLI, 0.0), count_order_vec(MAX_ORLI, 0) { }
} /* __attribute__ ((aligned (CACHE_LINE_SZ))) */ ;

void foo(void *arg) { }

void query(void *arg) {

  assert(arg != nullptr);
  Ctx &ctx = *(Ctx *) arg;
  declare_timer(timer);

  uint64_t cnt = 0;
  uint64_t filter_cnt = 0;
  // float mem_ms = 0.0f;
  
  unique_ptr<BackupStore::RowCursor> row_cur = ol_tbl->getRowCursor(ctx.ver);
  if (row_cur.get() != nullptr) {
    // row store
    row_cur->seekOffset(ctx.off_begin, ctx.off_end);
    while (row_cur->nextRow()) {
      order_line::value *v =
        reinterpret_cast<order_line::value *> (row_cur->value());
#if 1
      if (v->ol_delivery_d != 0) {
        int64_t key = row_cur->key();
        int32_t ol_number = orderLineKeyToNumber(key) - 1;

        ctx.sum_qty_vec[ol_number] += v->ol_quantity;
        ctx.sum_amount_vec[ol_number] += v->ol_amount;
        ctx.count_order_vec[ol_number]++;
        ++filter_cnt;
      }
      ++cnt; // access orderline record
#else
      cnt += v->ol_delivery_d;
#endif
    }
  } else {
    // column store
    timer_start(timer);
    unique_ptr<BackupStore::ColCursor> 
      del_cur = ol_tbl->getColCursor(OL_DELIVERY_D, ctx.ver);
    assert(del_cur.get());
    del_cur->seekOffset(ctx.off_begin, ctx.off_end);
    // timer_end(timer, ctx, 0);
    
    ol_tbl->locateCol(OL_DELIVERY_D, sizeof(uint32_t));

    // while (del_cur->nextRow()) 
    while (1)
    {
      timer_start(timer);
      bool has = del_cur->nextRow();
      // ++cnt; // access orderline record
      timer_end(timer, ctx, 1);
      if (!has) break;
      timer_start(timer);
      uint32_t ol_delivery_d = *(uint32_t *) del_cur->value();
      timer_end(timer, ctx, 2);
      if (ol_delivery_d != 0) {
        timer_start(timer);
        int64_t key = del_cur->key();
        timer_end(timer, ctx, 3);
        int32_t ol_number = orderLineKeyToNumber(key) - 1;
        if (!(ol_number >= 0 && ol_number < MAX_ORLI)) {
          printf("error ol_number: %d, key: %ld\n", ol_number, key);
          assert(false);
        }
        timer_start(timer);
        uint64_t cur = del_cur->cur();
        ctx.sum_qty_vec[ol_number] += ol_quantities[cur];
        ctx.sum_amount_vec[ol_number] += ol_amounts[cur];
        ctx.count_order_vec[ol_number]++;
        timer_end(timer, ctx, 4);
        ++filter_cnt;
      }
      ++cnt; // access orderline record
    }
  }

  ctx.cnt = cnt;
  ctx.filter_cnt = filter_cnt;
  // ctx.mem_ms = mem_ms;
}

}

namespace nocc {
namespace oltp {
namespace ch {

bool ChQueryWorker::query01(yield_func_t &yield) {
#if 0  // debug
  char *buf = (char *) malloc(1024);
  for (int i = 0; i < 1024; ++i) {
    buf[i] = i;
  }
  free(buf);
  return true;
#endif

  uint64_t ver = get_read_ver_();
  uint64_t cnt = 0, filter_cnt = 0;

  // Result data
  vector<uint64_t> sum_qty_vec(MAX_ORLI, 0);
  vector<AMOUNT>   sum_amount_vec(MAX_ORLI, 0.0);
  vector<int8_t>   avg_qty_vec(MAX_ORLI, 0);
  vector<AMOUNT>   avg_amount_vec(MAX_ORLI, 0.0);
  vector<uint64_t> count_order_vec(MAX_ORLI, 0);

  /**************** Parallel Part Begin ********************/

  // Thread information
  vector<Ctx> ctxs;
  // vector<Foo> tmp;
  // tmp.emplace_back(*db_, ver, 0, 0);
  // ctxs.emplace_back(*db_, ver, 0, 0);
  // return true;
  ctxs.reserve(num_thread_);

  // Calculate workload for each thread
  uint64_t ol_tbl_sz = db_->getStore(ORLI)->getOffsetHeader();
  db_cnt_ = ol_tbl_sz;

  vector<vector<uint64_t>> workloads = part_workloads(ol_tbl_sz);

  for (const auto &v : workloads) {
    uint64_t off_begin = v[0],
             off_end   = v[1];
    ctxs.emplace_back(*db_, ver, off_begin, off_end);
  }
 
  parallel_process(ctxs, query);
  // parallel_process(ctxs, foo);
  
  // Collect data from all slave threads
  for (int i = 0; i < MAX_ORLI; ++i) {
    for (const Ctx &ctx : ctxs) {
      sum_qty_vec[i] += ctx.sum_qty_vec[i];
      sum_amount_vec[i] += ctx.sum_amount_vec[i];
      count_order_vec[i] += ctx.count_order_vec[i];
    }

    if(count_order_vec[i]) {
      avg_qty_vec[i] = static_cast<int8_t>(sum_qty_vec[i] / count_order_vec[i]);
      avg_amount_vec[i] = sum_amount_vec[i] / count_order_vec[i];
    }
  }

  for (const Ctx &ctx : ctxs) {
    cnt += ctx.cnt;
    filter_cnt += ctx.filter_cnt;
  }
 
  print_timer(ctxs); 

  /**************** Parallel Part End ********************/

  // No need to sort
  if (!q_verbose_) return true;

  printf("Result of query 1:\n");
  printf("ol_number        sum_qty          sum_amount       avg_qty         avg_amount       count_order      \n");
  for(int i = 0; i < MAX_ORLI; ++i) {
    // Only show the existing result_table
    if(count_order_vec[i]) {
      printf("%-16d %-16lu %-16.2f %-16d %-16.2f %-16lu \n", i + 1,
             sum_qty_vec[i], sum_amount_vec[i], avg_qty_vec[i],
             avg_amount_vec[i], count_order_vec[i]);
    }
  }

  printf("Total record cnt: %lu, filter size: %lu, ver %lu\n", 
         cnt, filter_cnt, ver);
  return true;
}

}  // namespace ch
}  // namesapce oltp
}  // namespace nocc


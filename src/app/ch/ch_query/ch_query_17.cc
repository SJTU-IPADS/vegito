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

using T = map<int32_t, int8_t>;  // i_id -> avg(ol_quantity) as a

const char CONST_I_DATA_CHAR_TEMPLATE = 'b';

struct Ctx : public BaseCtx {
  // for step 1
  map<int32_t, int32_t> ol_quantities;  // ol_i_id -> ol_quantity
  map<int32_t, int32_t> ol_cnt;         // ol_i_id -> count

  uint64_t cnt_1;

  // for step 2
  const T &t;
  AMOUNT ol_amount;

  Ctx(BackupDB &d, uint64_t v, uint64_t b, uint64_t e, const T &t) 
    : BaseCtx(d, v, b, e),  
      cnt_1(0), t(t), ol_amount(0) { }

} __attribute__ ((aligned (CACHE_LINE_SZ))) ;

void query_step_1(void *arg) {
  assert(arg != nullptr);
  Ctx &ctx = *(Ctx *) arg;
  uint64_t cnt_1 = 0;

  unique_ptr<BackupStore::ColCursor> 
    ol_quantity_cur = ol_tbl->getColCursor(OL_QUANTITY, ctx.ver);
  unique_ptr<BackupStore::ColCursor> 
    ol_i_id_cur = ol_tbl->getColCursor(OL_I_ID, ctx.ver);
 
  ol_tbl->locateCol(OL_QUANTITY, sizeof(int8_t));
  ol_tbl->locateCol(OL_I_ID, sizeof(int32_t));
  auto ol_quantities = (const int8_t *) ol_quantity_cur->base();
  auto ol_i_ids = (const int32_t *) ol_i_id_cur->base();

  for (uint64_t ol_i = ctx.off_begin; ol_i < ctx.off_end; ++ol_i) {
    int32_t ol_i_id = ol_i_ids[ol_i];
    uint64_t i_i = ctx.db.getOffset(ITEM, ol_i_id);
    const inline_str_8<50> *i_data = &i_datas[i_i];
    char last = i_data->data()[i_data->size() - 1];

    if(last == CONST_I_DATA_CHAR_TEMPLATE) {
      int8_t ol_quantity = ol_quantities[ol_i];
      if (ctx.ol_quantities.find(ol_i_id) == ctx.ol_quantities.end()) {
        ctx.ol_quantities[ol_i_id] = ol_quantity;
        ctx.ol_cnt[ol_i_id] = 1;
      } else {
        ctx.ol_quantities[ol_i_id] += ol_quantity;
        ctx.ol_cnt[ol_i_id] += 1;
      }
      ++cnt_1;
    }
  }

  ctx.cnt_1 = cnt_1;

}

void query_step_2(void *arg) {
  assert(arg != nullptr);
  Ctx &ctx = *(Ctx *) arg;
  uint64_t cnt = 0;

  assert(!ctx.t.empty());
  
  for (uint64_t ol_i = ctx.off_begin; ol_i < ctx.off_end; ++ol_i) {
    int32_t ol_i_id = ol_i_ids[ol_i];
    auto it = ctx.t.find(ol_i_id);
    if (it != ctx.t.end()) {
      if(ol_quantities[ol_i] < it->second)  {
        ctx.ol_amount += ol_amounts[ol_i];
        ++cnt;
      }
    }
  }

  ctx.cnt = cnt;
  
}

}

namespace nocc {
namespace oltp {
namespace ch {

bool ChQueryWorker::query17(yield_func_t &yield) {
  uint64_t ver = get_read_ver_();
  uint64_t cnt_1 = 0;
  uint64_t cnt_2 = 0;
  declare_timer(timer);

  T t;  // i_id -> avg(ol_quantity) as a
  AMOUNT ol_amount;

  /**************** Parallel Part Begin ********************/

  // Thread information
  vector<Ctx> ctxs;
  ctxs.reserve(num_thread_);

  // Calculate workload for each thread
  uint64_t ol_tbl_sz = ol_tbl->getOffsetHeader();
  db_cnt_ = ol_tbl_sz;

  vector<vector<uint64_t>> workloads = part_workloads(ol_tbl_sz);
  for (const auto &v : workloads) {
    uint64_t off_begin = v[0],
             off_end   = v[1];
    ctxs.emplace_back(*db_, ver, off_begin, off_end, t);
  }
 
  timer_start(timer);
  parallel_process(ctxs, query_step_1);
  timer_end_print(timer, "Parallel 1");
  
  timer_start(timer);
  
  map<int32_t, int32_t> ol_quantities;  // ol_i_id -> ol_quantity
  map<int32_t, int32_t> ol_cnt;         // ol_i_id -> count

  for (const Ctx &ctx : ctxs) {
    for (auto &p : ctx.ol_quantities) {
      if (ol_quantities.find(p.first) == ol_quantities.end()) {
        ol_quantities[p.first] = p.second;
        ol_cnt[p.first] = ctx.ol_cnt.find(p.first)->second;
      } else {
        ol_quantities[p.first] += p.second;
        ol_cnt[p.first] += ctx.ol_cnt.find(p.first)->second;
      }
    }
    cnt_1 += ctx.cnt_1;
  }

  for (auto &p : ol_quantities) {
    int8_t avg = p.second / ol_cnt.find(p.first)->second;
    t[p.first] = avg;
  }
  timer_end_print(timer, "Middle");
  
  // start query_step_2
  timer_start(timer);
  parallel_process(ctxs, query_step_2);
  timer_end_print(timer, "Parallel 2");
  print_timer(ctxs); 

  /**************** Parallel Part End ********************/

  for (const Ctx &ctx : ctxs) {
    ol_amount += ctx.ol_amount;
    cnt_2 += ctx.cnt;
  }
  ol_amount /= 2.0;

  if (!q_verbose_) return true;

  printf("Result of query 17:\n");
  printf("%-16s\n", "avg_yearly");
  printf("%-16.2f\n", ol_amount);
  printf("Result: %lu tuples, sub %lu tuples, selected %lu tuples\n", 
         1ul, cnt_1, cnt_2);
  return true;
}

}  // namespace ch
}  // namesapce oltp
}  // namespace nocc


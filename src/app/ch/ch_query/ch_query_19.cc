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

const char CHAR_1 = 'a';
const char CHAR_2 = 'b';
const char CHAR_3 = 'c';

struct Ctx : public BaseCtx {
  // Query data
  const uint32_t ware_begin;
  AMOUNT revenue;

  Ctx(BackupDB &d, uint64_t v, uint64_t b, uint64_t e, uint32_t w) 
    : BaseCtx(d, v, b, e),
      ware_begin(w), revenue(0) { } 
} __attribute__ ((aligned (CACHE_LINE_SZ)));

void query(void *arg);

} // namespace anonymous


namespace nocc {
namespace oltp {
namespace ch {

bool ChQueryWorker::query19(yield_func_t &yield) {

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
    ctxs.emplace_back(*db_, ver, off_begin, off_end, start_w_);
  }
 
  parallel_process(ctxs, query);

  /**************** Parallel Part End ********************/

  // Collect data from all slave threads
  for (const Ctx &ctx : ctxs) {
    revenue += ctx.revenue;
    cnt += ctx.cnt;
  }

  if (!q_verbose_) return true;

  printf("Result of query 19:\n");
  printf("%-12s\n", "revenue");
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

  Keys ol_keys = ol_tbl->getKeyCol();

  for (uint64_t ol_i = ctx.off_begin; ol_i < ctx.off_end; ++ol_i) {
    uint64_t ol_key = ol_keys[ol_i];
    int32_t ol_w_id = orderLineKeyToWare(ol_key);
    int32_t ol_i_id = ol_i_ids[ol_i];
    int8_t ol_quantity = ol_quantities[ol_i];
    AMOUNT ol_amount = ol_amounts[ol_i];

    int32_t w_idx = ol_w_id - ctx.ware_begin + 1;
    if (!(ol_quantity >= 1 && ol_quantity <= 10)) continue;
    if (!(w_idx >= 1 && w_idx <= 5)) 
      continue;
    
    uint64_t i_i = ctx.db.getOffset(ITEM, ol_i_id);
    assert(i_i != -1);
    
    AMOUNT i_price = i_prices[i_i];
    if (!(i_price >= 1 && i_price <= 400000)) continue;
    
    const inline_str_8<50> *i_data = &i_datas[i_i];
    char last = i_data->c_str()[i_data->size() - 1];
    if (last != CHAR_1 && last != CHAR_2 && last != CHAR_3) continue;

#if 0
    ctx.revenue += ol_amount;
    ++cnt;
#else


#if 0
    bool cond1 = ((w_idx == 1 || w_idx == 2 || w_idx == 3) && last == CHAR_1);
    bool cond2 = ((w_idx == 1 || w_idx == 2 || w_idx == 4) && last == CHAR_2);
    bool cond3 = ((w_idx == 1 || w_idx == 5 || w_idx == 3) && last == CHAR_3);

    bool cond = cond1 || cond2 || cond3;

    if (!cond) continue;

    ctx.revenue += ol_amount;
    ++cnt;

#else
    if ((w_idx == 1 || w_idx == 2 || w_idx == 3) && last == CHAR_1) {
      ctx.revenue += ol_amount;
      ++cnt;
    } else if ((w_idx == 1 || w_idx == 2 || w_idx == 4) && last == CHAR_2) {
      ctx.revenue += ol_amount;
      ++cnt;
    } else if ((w_idx == 1 || w_idx == 5 || w_idx == 3) && last == CHAR_3) {
      ctx.revenue += ol_amount;
      ++cnt;
    }
#endif

#endif
  }
  ctx.cnt = cnt;
}

} // namespace anonymous


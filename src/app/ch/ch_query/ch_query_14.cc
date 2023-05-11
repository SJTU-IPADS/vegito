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

const char* I_DATA_STR = "PR";

using KeyVec = vector<uint64_t>;

struct Ctx : public BaseCtx {
  const KeyVec &m_i_keys;

  // Query data
  AMOUNT promo_revenue;
  AMOUNT sum_ol_amount;

  Ctx(BackupDB &d, uint64_t v, uint64_t b, uint64_t e, const KeyVec &keys) 
    : BaseCtx(d, v, b, e), 
      m_i_keys(keys),
      promo_revenue(0), sum_ol_amount(0) { }
} __attribute__ ((aligned (CACHE_LINE_SZ)));

void query(void *arg);

} // namespace anonymous


namespace nocc {
namespace oltp {
namespace ch {

bool ChQueryWorker::query14(yield_func_t &yield) {
  uint64_t ver = get_read_ver_();
  uint64_t cnt = 0;

  // middle result
  vector<uint64_t> m_i_keys;

  // Result data
  AMOUNT promo_revenue = 0;
  AMOUNT sum_ol_amount = 0;

  Keys i_keys = i_tbl->getKeyCol();
  
  for (int i_i = 0; i_i < i_tbl_sz; ++i_i) {
    const char *str = i_datas[i_i].data();
    if (str[0] == 'P' && str[1] == 'R') {
      m_i_keys.push_back(i_keys[i_i]);
    }
  }
  // printf("m_i_keys size: %lu\n", m_i_keys.size());

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
    ctxs.emplace_back(*db_, ver, off_begin, off_end, m_i_keys);
  }
 
  parallel_process(ctxs, query);

  /**************** Parallel Part End ********************/

  // Collect data from all slave threads
  for (const Ctx &ctx : ctxs) {
    promo_revenue += ctx.promo_revenue;
    sum_ol_amount += ctx.sum_ol_amount;

    cnt += ctx.cnt;
  }

  promo_revenue = 100.0 * promo_revenue / 1 + sum_ol_amount;

  if (!q_verbose_) return true;

  printf("Result of query 14:\n");
  printf("%-12s\n", "promo_revenue");
  printf("%-12.2f\n", promo_revenue);
  
  printf("Result: %lu tuples, selected %lu tuples\n", 1lu, cnt);

  return true;

}
}  // namespace ch
}  // namespace oltp
}  // namespace nocc

namespace {

void query(void *arg) {
  Ctx &ctx = *(Ctx *) arg;
  uint64_t cnt = 0;


  // Order line
  unique_ptr<BackupStore::ColCursor> 
      del_cur = ol_tbl->getColCursor(OL_DELIVERY_D, ctx.ver);
  assert(del_cur.get());
  del_cur->seekOffset(ctx.off_begin, ctx.off_end);

  while (del_cur->nextRow()) {
    uint32_t *delivery = (uint32_t *) del_cur->value();
    assert(delivery);

    if (*delivery > 0) {
      uint64_t ol_i = del_cur->cur();
      uint64_t i_key = ol_i_ids[ol_i];
      // auto i_data = &i_datas[i_i];
      // assert(i_data);
      
      AMOUNT ol_amount = ol_amounts[ol_i];
      ctx.promo_revenue += ol_amount;
      ++cnt;
      
      bool find = false;
      for (uint64_t k : ctx.m_i_keys) {
        if (i_key == k) {
          find = true;
          break;
        }
      }
      if (!find) {
        ctx.promo_revenue -= ol_amount;
        --cnt;
      }


    }
  }

  ctx.cnt = cnt;
}

} // namespace anonymous


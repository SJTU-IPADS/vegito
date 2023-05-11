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

#define Q15_STABLE_SORT 0

#include "ch_query_timer.h"

namespace {

struct ResultRow {
  uint64_t su_suppkey;
  const char* su_name;
  const char* su_address;
  const char* su_phone;
  AMOUNT total_revenue;

  ResultRow(uint64_t su_suppkey, const char* su_name, const char* su_address, 
         const char* su_phone, AMOUNT total_revenue)
    : su_suppkey(su_suppkey), su_name(su_name), su_address(su_address), 
      su_phone(su_phone), total_revenue(total_revenue) { }

  static bool Compare(const ResultRow &a, const ResultRow &b) {
    return a.su_suppkey < b.su_suppkey;
  }
  
  static void print_header() {
    printf("%-12s %-20s %-40s %-15s %-16s\n", 
           "su_suppkey", "su_name", "su_address", "su_phone", "total_revenue");
  }

  void print() const {
    printf("%-12lu %-.20s %-40s %-.15s %-16.2f\n", 
           su_suppkey, su_name, su_address, su_phone, total_revenue);
  }
};

using ResultMap = unordered_map<uint64_t, AMOUNT>;

struct Ctx : public BaseCtx {
  // Query data
  ResultMap &sub_results; // table (supplier_no, total_revenue)

  Ctx(BackupDB &d, uint64_t v, uint64_t b, uint64_t e, ResultMap &rm) 
    : BaseCtx(d, v, b, e), sub_results(rm) { } 
} __attribute__ ((aligned (CACHE_LINE_SZ)));

void query(void *arg);

thread_local vector<ResultMap> sub_res_vec_;
  
thread_local vector<ResultRow> result_vec;

} // namespace anonymous


namespace nocc {
namespace oltp {
namespace ch {

void ChQueryWorker::query15_init() {
  sub_res_vec_.resize(num_thread_);
  for (int i = 0; i < num_thread_; ++i) {
    sub_res_vec_[i].reserve(10001);
  }
  result_vec.reserve(10001);
}

bool ChQueryWorker::query15(yield_func_t &yield) {
  uint64_t ver = get_read_ver_();
  uint64_t cnt = 0;
  declare_timer(timer);

  result_vec.clear();
  timer_start(timer);
  // ResultRow data
  ResultMap subquery_table; // table (supplier_no, total_revenuel)

  /**************** Parallel Part Begin ********************/
  // Thread information
  vector<Ctx> ctxs;
  ctxs.reserve(num_thread_);

  // Calculate workload for each thread
  uint64_t ol_tbl_sz = ol_tbl->getOffsetHeader();
  db_cnt_ = ol_tbl_sz;
  
  vector<vector<uint64_t>> workloads = part_workloads(ol_tbl_sz);
  for (int i = 0; i < num_thread_; ++i) {
    const auto &v = workloads[i];
    uint64_t off_begin = v[0],
             off_end   = v[1];
    sub_res_vec_[i].clear();
    ctxs.emplace_back(*db_, ver, off_begin, off_end, sub_res_vec_[i]);
  }
  timer_end_print(timer, "Before parallel");
  timer_start(timer);

  parallel_process(ctxs, query);

  timer_end_print(timer, "Parallel");
  /**************** Parallel Part End ********************/
  // Collect data from all slave threads
  timer_start(timer);
  for (const Ctx &ctx : ctxs) {
    for (const auto &pa : ctx.sub_results) {
      subquery_table[pa.first] += pa.second;
    }
    cnt += ctx.cnt;
  }

  // Outer query
  vector<uint64_t> supplier_nos;
  AMOUNT max_total_revenue = 0;
  for (pair<const uint64_t, AMOUNT> &pa : subquery_table) {
    if (pa.second > max_total_revenue) {
      supplier_nos.clear();
      supplier_nos.push_back(pa.first);
      max_total_revenue = pa.second;
    } else if (pa.second == max_total_revenue) {
      supplier_nos.push_back(pa.first);
    }
  }

  for (uint64_t su_key : supplier_nos) {
    uint64_t su_i = db_->getOffset(SUPP, su_key);
    assert(su_i != -1);

    const inline_str_fixed<20> *su_name = &su_names[su_i];
    const inline_str_8<40> *su_address = &su_addresses[su_i];
    const inline_str_fixed<15> *su_phone = &su_phones[su_i];

    result_vec.emplace_back(su_key, su_name->data(), su_address->c_str(), 
                              su_phone->data(), max_total_revenue);
  }

#if Q15_STABLE_SORT
  stable_sort(result_vec.begin(), result_vec.end(), ResultRow::Compare);
#else
  sort(result_vec.begin(), result_vec.end(), ResultRow::Compare);
#endif
  timer_end_print(timer, "After parallel");
  
  print_timer(ctxs); 

  if (!q_verbose_) return true;

  printf("Result of query 15:\n");
  ResultRow::print_header();
  for (const ResultRow &row : result_vec)
    row.print();

  printf("Result: %lu tuples, selected %lu tuples\n", result_vec.size(), cnt);
  
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
  while (del_cur->nextRow()) {
    uint32_t *delivery = (uint32_t *) del_cur->value();
    assert(delivery);

    if (*delivery == 0) continue;

    uint64_t ol_i = del_cur->cur();
    int32_t ol_supply_w_id = ol_supply_w_ids[ol_i];
    int32_t ol_i_id = ol_i_ids[ol_i];
    
#if 1  // XXX: performance bottleneck
    uint64_t s_key = makeStockKey(ol_supply_w_id, ol_i_id);
    uint64_t s_i = ctx.db.getOffset(STOC, s_key);
    assert(s_i != -1);
#endif

    AMOUNT ol_amount = ol_amounts[ol_i];
    uint64_t supplier_no = (ol_supply_w_id * ol_i_id) % 10000 + 1;
    ctx.sub_results[supplier_no] += ol_amount;

    ++cnt;
  }

  ctx.cnt = cnt;
}

} // namespace anonymous


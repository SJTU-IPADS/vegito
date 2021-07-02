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

#define Q18_STABLE_SORT 0

namespace {

struct ResultRow {
  const char* c_last;
  int32_t c_id;
  int32_t o_id;
  uint32_t o_entry_d;
  int8_t o_ol_cnt;
  AMOUNT sum_ol_amount;

  ResultRow(const char* c_last, int32_t c_id, int32_t o_id, 
            uint32_t o_entry_d, int8_t o_ol_cnt, AMOUNT sum_ol_amount)
    : c_last(c_last), c_id(c_id), o_id(o_id), o_entry_d(o_entry_d), 
      o_ol_cnt(o_ol_cnt), sum_ol_amount(sum_ol_amount) { }

  static bool Compare(const ResultRow &a, const ResultRow &b) {
    if (a.sum_ol_amount == b.sum_ol_amount)
      return a.o_entry_d < b.o_entry_d;
    else
      return a.sum_ol_amount >= b.sum_ol_amount;
  }

  static void print_header() {
    printf("%-16s %-8s %-8s %-12s %-12s %-14s\n", 
           "c_last", "c_id", "o_id", "o_entry_d", "o_ol_cnt", "sum_ol_amount");
  }

  void print() const {
    printf("%-16s %-8d %-8d %-12u %-12d %-14.2f\n", 
           c_last, c_id, o_id, o_entry_d, o_ol_cnt, sum_ol_amount);
  }

};

struct Ctx : public BaseCtx {
  // Query data
  vector<ResultRow> &sub_results;

  Ctx(BackupDB &d, uint64_t v, uint64_t b, uint64_t e, vector<ResultRow> &sr) 
    : BaseCtx(d, v, b, e), sub_results(sr) { 
    sr.clear();  
  } 
} __attribute__ ((aligned (CACHE_LINE_SZ)));

void query(void *arg);

thread_local vector<vector<ResultRow>> sub_res_vec;
thread_local vector<ResultRow> result_vec;

} // namespace anonymous

namespace nocc {
namespace oltp {
namespace ch {

void ChQueryWorker::query18_init() {
  sub_res_vec.resize(num_thread_);
  for (int i = 0; i < num_thread_; ++i) {
    sub_res_vec[i].reserve(75000);
  }
  result_vec.reserve(75000);
}

bool ChQueryWorker::query18(yield_func_t &yield) {
  uint64_t ver = get_read_ver_();
  uint64_t cnt = 0;

  // ResultRow data
  result_vec.clear();

  /**************** Parallel Part Begin ********************/
  vector<Ctx> ctxs;
  ctxs.reserve(num_thread_);
  
  // Calculate workload for each thread
  uint64_t o_tbl_sz = o_tbl->getOffsetHeader();
  db_cnt_ = o_tbl_sz;
  
  vector<vector<uint64_t>> workloads = part_workloads(o_tbl_sz);
  for (int i = 0; i < num_thread_; ++i) {
    const auto &v = workloads[i];
    uint64_t off_begin = v[0],
             off_end   = v[1];
    ctxs.emplace_back(*db_, ver, off_begin, off_end, sub_res_vec[i]);
  }
 
  parallel_process(ctxs, query);

  /**************** Parallel Part End ********************/

  // Collect data from all slave threads
  for (const Ctx &ctx : ctxs) {
   result_vec.insert(result_vec.end(), 
                     ctx.sub_results.begin(), ctx.sub_results.end());
    cnt += ctx.cnt;
  }

#if Q18_STABLE_SORT
  stable_sort(result_vec.begin(), result_vec.end(), ResultRow::Compare);
#else
  sort(result_vec.begin(), result_vec.end(), ResultRow::Compare);
#endif
  
  if (!q_verbose_) return true;

  printf("Result of query 18:\n");
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

  Keys o_keys = o_tbl->getKeyCol();
  
  // Nested-loop
  bool end = false;
  for (uint64_t o_i = ctx.off_begin; o_i < ctx.off_end && !end; ++o_i) {
    int64_t o_key = int64_t(o_keys[o_i]);
    int32_t o_o_id = orderKeyToOrder(o_key);
    int32_t o_w_id = orderKeyToWare(o_key);
    int32_t o_d_id = orderKeyToDistrict(o_key);
    int32_t o_c_id = o_c_ids[o_i];
    int8_t o_ol_cnt = o_ol_cnts[o_i];
    uint32_t o_entry_d = o_entry_ds[o_i];

    uint64_t c_key = makeCustomerKey(o_w_id, o_d_id, o_c_id);
    uint64_t c_i = ctx.db.getOffset(CUST, c_key);
    assert(c_i != -1);
    const inline_str_8<16>* c_last = &c_lasts[c_i];

    uint64_t start_ol_key = makeOrderLineKey(o_w_id, o_d_id, o_o_id, 1);
    uint64_t end_ol_key = makeOrderLineKey(o_w_id, o_d_id, o_o_id, o_ol_cnt);
    assert(o_ol_cnt > 0);

    // Nested-loop
    AMOUNT sum_ol_amount = 0;
    for (uint64_t ol_key = start_ol_key; ol_key <= end_ol_key; ++ol_key) {
      uint64_t ol_i = ctx.db.getOffset(ORLI, ol_key, ctx.ver);
      if (ol_i == -1) {
        end = true;
        break;
      }

      sum_ol_amount += ol_amounts[ol_i];
      cnt++; // access orderline record
    }

    if (sum_ol_amount > 200) {
      ctx.sub_results.emplace_back(c_last->c_str(), o_c_id, o_o_id, o_entry_d, o_ol_cnt, sum_ol_amount);
    }
  }

  ctx.cnt = cnt;
}

} // namespace anonymous


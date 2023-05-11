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

#define Q13_STABLE_SORT 0

namespace {

struct ResultRow {
  uint64_t c_count;
  uint64_t custdist;

  ResultRow(uint64_t c_count, uint64_t custdist)
    : c_count(c_count), custdist(custdist) {}

  static bool Compare(const ResultRow &a, const ResultRow &b) {
    if (a.custdist == b.custdist)
      return a.c_count >= b.c_count;
    else
      return a.custdist >= b.custdist;
  }
  
  static void print_header() {
    printf("%-12s %-12s\n", "c_count", "custdist");
  }

  void print() const {
    printf("%-12lu %-12lu\n", c_count, custdist);
  }
};

struct Ctx : public BaseCtx {
  const uint64_t c_begin;
  const uint64_t c_end;

  // Query data
  vector<uint64_t> c_orders; // derived table (c_id, c_count)

  Ctx(BackupDB &d, uint64_t v, uint64_t b1, uint64_t e1, uint64_t b2, uint64_t e2) 
    : BaseCtx(d, v, b1, e1), 
      c_begin(b2), c_end(e2), c_orders(NumCustomersPerDistrict(), 0) { }
} __attribute__ ((aligned (CACHE_LINE_SZ)));

void query(void *arg);

} // namespace anonymous

namespace nocc {
namespace oltp {
namespace ch {

bool ChQueryWorker::query13(yield_func_t &yield) {
  uint64_t ver = get_read_ver_();
  uint64_t cnt = 0;

  // ResultRow data
  vector<uint64_t> c_orders(NumCustomersPerDistrict(), 0); // derived table (c_id, c_count)
  unordered_map<uint64_t, uint64_t> result_map; // result table (c_count, custdist)
  vector<ResultRow> result_vec;

  /**************** Parallel Part Begin ********************/
  vector<Ctx> ctxs;
  ctxs.reserve(num_thread_);
  
  // Calculate workload for each thread
  uint64_t o_tbl_sz = o_tbl->getOffsetHeader();
  db_cnt_ = o_tbl_sz;
  
  vector<vector<uint64_t>> workloads0 = part_workloads(o_tbl_sz);
  vector<vector<uint64_t>> workloads1 = part_workloads(c_tbl_sz);

  for (int i = 0; i < workloads0.size(); ++i) {
    uint64_t off_begin = workloads0[i][0],
             off_end   = workloads0[i][1],
             c_begin   = workloads1[i][0],
             c_end     = workloads1[i][1];
    ctxs.emplace_back(*db_, ver, off_begin, off_end, c_begin, c_end);
  }
 
  parallel_process(ctxs, query);

  /**************** Parallel Part End ********************/

  // Collect data from all slave threads
  for (const Ctx &ctx : ctxs) {
   for (int j = 0; j < NumCustomersPerDistrict(); ++j) {
      c_orders[j] += ctx.c_orders[j];
    }
    cnt += ctx.cnt;
  }

  // Outer query
  for (uint64_t c_count : c_orders) {
    ++result_map[c_count];
  }

  for (const auto &p : result_map) {
    result_vec.emplace_back(p.first, p.second);
  }

#if Q13_STABLE_SORT
  stable_sort(result_vec.begin(), result_vec.end(), ResultRow::Compare);
#else
  sort(result_vec.begin(), result_vec.end(), ResultRow::Compare);
#endif
  
  if (!q_verbose_) return true;

  printf("Result of query 13:\n");
  ResultRow::print_header();
  for (const auto &row : result_vec)
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

  // Order
  Keys o_keys = o_tbl->getKeyCol();

  unique_ptr<BackupStore::ColCursor>
    o_carrier_id_cur = o_tbl->getColCursor(O_CARRIER_ID, ctx.ver);
  assert(o_carrier_id_cur.get());
  o_carrier_id_cur->seekOffset(ctx.off_begin, ctx.off_end);
  o_tbl->locateCol(O_CARRIER_ID, sizeof(int32_t));

  bool end = false;
  while (o_carrier_id_cur->nextRow() && !end) {
    uint64_t o_i = o_carrier_id_cur->cur(); 
    int64_t o_key = int64_t(o_keys[o_i]);
    int32_t o_w_id = orderKeyToWare(o_key);
    int32_t o_d_id = orderKeyToDistrict(o_key);
    int32_t o_c_id = o_c_ids[o_i];
    int32_t o_carrier_id = *(int32_t *) o_carrier_id_cur->value();

    uint64_t c_key = makeCustomerKey(o_w_id, o_d_id, o_c_id);
    if (o_carrier_id > 8) {
      uint64_t c_i = ctx.db.getOffset(CUST, c_key);
      assert(c_i != -1);
      ctx.c_orders[o_c_id - 1]++;
      ++cnt; // access customer record
    }
  }

#if 0
  // Left outer join
  // Customer
  BackupStore *c_tbl = ctx.db.getStore(CUST);
  Keys c_keys = c_tbl->getKeyCol();

  for (uint64_t c_i = ctx.c_begin; c_i < ctx.c_end; ++c_i) {
    uint64_t c_key = c_keys[c_i];
    int32_t c_id = customerKeyToCustomer(c_key);
    // Fake left join
    if (ctx.c_orders[c_id - 1] == 0) {
      ctx.c_orders[c_id - 1] = 0;
    }
  }
#endif

  ctx.cnt = cnt;
}

} // namespace anonymous


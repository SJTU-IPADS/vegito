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

#define Q21_STABLE_SORT 1

namespace {

const char* N_NAME_STR = "GERMANY";

struct ResultRow {
  const char* su_name;
  uint64_t numwait;

  ResultRow(const char* su_name, uint64_t numwait)
    : su_name(su_name), numwait(numwait) {}

  static bool Compare(const ResultRow &a, const ResultRow &b) {
    if (a.numwait != b.numwait)
      return a.numwait >= b.numwait;
    else
      return strncmp(a.su_name, b.su_name, 20) < 0;
  }
  
  static void print_header() {
    printf("%-20s %-12s\n", "su_name", "num_wait");
  }
 
  void print() const { 
    printf("%-.20s %-12lu \n", su_name, numwait);
  }
};

using ResultMap = unordered_map<const char*, uint64_t>;

struct Ctx : public BaseCtx {
  // Query data
  const uint64_t n_nationkey;
  ResultMap sub_results; // table (su_name, numwait)

  Ctx(BackupDB &d, uint64_t v, uint64_t b, uint64_t e, uint64_t n) 
    : BaseCtx(d, v, b, e), 
      n_nationkey(n) { }
} __attribute__ ((aligned (CACHE_LINE_SZ)));

void query(void *arg);

} // namespace anonymous


namespace nocc {
namespace oltp {
namespace ch {

bool ChQueryWorker::query21(yield_func_t &yield) {
  uint64_t ver = get_read_ver_();
  uint64_t cnt = 0;

  // Result data
  uint64_t n_nationkey = 0;
  ResultMap result_map;
  vector<ResultRow> result_vec;

  // Nation
  Keys n_keys = n_tbl->getKeyCol();
  for (uint64_t n_i = 0; n_i < n_keys.size(); ++n_i) {
    const char* n_name = n_names[n_i].c_str();
    if (strncmp(n_name, N_NAME_STR, strlen(N_NAME_STR)) == 0) {
      n_nationkey = n_keys[n_i];
    }
  }
  assert(n_nationkey != 0);

  /**************** Parallel Part Begin ********************/
  vector<Ctx> ctxs;
  ctxs.reserve(num_thread_);
  
  // Calculate workload for each thread
  uint64_t o_tbl_sz = o_tbl->getOffsetHeader();
  db_cnt_ = o_tbl_sz;
  
  vector<vector<uint64_t>> workloads = part_workloads(o_tbl_sz);
  for (const auto &v : workloads) {
    uint64_t off_begin = v[0],
             off_end   = v[1];
    ctxs.emplace_back(*db_, ver, off_begin, off_end, n_nationkey);
  }
 
  parallel_process(ctxs, query);
 
  /**************** Parallel Part End ********************/

  // Collect data from all slave threads
  for (const Ctx &ctx : ctxs) {
    for (const auto &pa : ctx.sub_results)
      result_map[pa.first] += pa.second;

    cnt += ctx.cnt;
  }

  for (const auto &pa : result_map)
    result_vec.emplace_back(pa.first, pa.second);

#if Q21_STABLE_SORT
  stable_sort(result_vec.begin(), result_vec.end(), ResultRow::Compare);
#else
  sort(result_vec.begin(), result_vec.end(), ResultRow::Compare);
#endif

  if (!q_verbose_) return true;

  printf("Result of query 21:\n");
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

  bool end = false;
  for (uint64_t o_i = ctx.off_begin; o_i < ctx.off_end && !end; ++o_i) {
    int64_t o_key = int64_t(o_keys[o_i]);
    int32_t o_w_id = orderKeyToWare(o_key);
    int32_t o_d_id = orderKeyToDistrict(o_key);
    int32_t o_o_id = orderKeyToOrder(o_key);

    int8_t o_ol_cnt = o_ol_cnts[o_i];
    uint32_t o_entry_d = o_entry_ds[o_i];

    uint64_t start_ol_key = makeOrderLineKey(o_w_id, o_d_id, o_o_id, 1);
    uint64_t end_ol_key = makeOrderLineKey(o_w_id, o_d_id, o_o_id, o_ol_cnt);
    assert(o_ol_cnt > 0);

    vector<uint32_t> o_ol_delivery_ds(o_ol_cnt);
    vector<int32_t> o_ol_i_ids(o_ol_cnt);
    for (uint64_t ol_key = start_ol_key; ol_key <= end_ol_key; ++ol_key) {
      uint64_t ol_i = ctx.db.getOffset(ORLI, ol_key, ctx.ver);
      if (ol_i == -1) {
        end = true;
        break;
      }

      uint32_t *ol_delivery_d = 
        (uint32_t *) ol_tbl->getByOffset(ol_i, OL_DELIVERY_D, ctx.ver);

      if (ol_delivery_d == nullptr) {
        end = true;
        break;
      }

      o_ol_delivery_ds[ol_key - start_ol_key] = *ol_delivery_d;
      o_ol_i_ids[ol_key- start_ol_key] = ol_i_ids[ol_i];
    }

    for (int8_t i = 0; i < o_ol_cnt; ++i) {
      uint32_t ol1_delivery_d = o_ol_delivery_ds[i];
      int32_t ol_i_id = o_ol_i_ids[i];

      if (ol1_delivery_d < o_entry_d) continue;

      bool need_select = true;
      for (int j = 0; j < o_ol_cnt; ++j) {
        if (j == i) continue;
        if (o_ol_delivery_ds[j] > ol1_delivery_d) {
          need_select = false;
          break;
        }
      }

      if (!need_select) continue;

      uint64_t s_key = makeStockKey(o_w_id, ol_i_id);
      uint64_t s_i = ctx.db.getOffset(STOC, s_key);
      assert(s_i != -1);
 
      uint64_t su_suppkey = (o_w_id * ol_i_id) % 10000 + 1;
      uint64_t su_i = ctx.db.getOffset(SUPP, su_suppkey);
      assert(su_i != -1);
 
      int8_t su_nationkey = su_nationkeys[su_i];
      const char* su_name = su_names[su_i].data();
 
      if (su_nationkey == ctx.n_nationkey) {
        ++ctx.sub_results[su_name];
        ++cnt;
      }

    }
  }

  ctx.cnt = cnt;
}

} // namespace anonymous


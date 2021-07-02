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

#include <unordered_set>

#define Q11_STABLE_SORT 1

using namespace std;
using namespace nocc::oltp::ch;

namespace {

const char *N_NAME_STR = "GERMANY";

using ResultMap = unordered_map<int, int>;  // s_i_id -> sum(s_order_cnt)
using Set = unordered_set<uint64_t>;

struct ResultRow {
  int s_i_id;
  int ordercount;

  ResultRow(int s, int o) : s_i_id(s), ordercount(o) { }
  
  static inline bool Compare(const ResultRow &a, const ResultRow &b) {
    return a.ordercount >= b.ordercount;
  }
  
  static void print_header() {
    printf("%-16s %-16s\n", "s_i_id", "s_order_cnt");
  }

  void print() const {
    printf("%-16d %-16d\n", s_i_id, ordercount);
  }
};

struct Ctx : public BaseCtx {
  const Set &su_suppkeys;

  ResultMap sub_results;

  Ctx(BackupDB &d, uint64_t v, uint64_t b, uint64_t e, const Set &set) 
    : BaseCtx(d, v, b, e), su_suppkeys(set) { }

} __attribute__ ((aligned (CACHE_LINE_SZ)));

void query(void *arg);

} // namespace anonymous

namespace nocc {
namespace oltp {
namespace ch {

bool ChQueryWorker::query11(yield_func_t &yield) {
  uint64_t ver = get_read_ver_();
  uint64_t cnt = 0;

  // ResultRow data
  ResultMap res_map;
  vector<ResultRow> result_vec;

  Keys su_keys = su_tbl->getKeyCol();
  Set su_suppkeys; 
  
  for (uint64_t su_i = 0; su_i < su_keys.size(); ++su_i) {
    uint64_t nation_key = (uint64_t) su_nationkeys[su_i];
    uint64_t n_i = db_->getOffset(NATI, nation_key);

    if (strncmp(n_names[n_i].c_str(), N_NAME_STR, strlen(N_NAME_STR)) != 0)
      continue;

    su_suppkeys.insert(su_keys[su_i]);
  }

  // printf("su_suppkeys: %lu tuples\n", su_suppkeys.size());

  /**************** Parallel Part Begin ********************/
  db_cnt_ = s_tbl_sz;

  vector<Ctx> ctxs;
  ctxs.reserve(num_thread_);
  
  vector<vector<uint64_t>> workloads = part_workloads(s_tbl_sz);

  for (const auto &v : workloads) {
    uint64_t off_begin = v[0],
             off_end   = v[1];
    ctxs.emplace_back(*db_, ver, off_begin, off_end, su_suppkeys);
  }
 
  parallel_process(ctxs, query);

  /**************** Parallel Part End ********************/

  // Collect data from all slave threads
  for (const Ctx &ctx : ctxs) {
    auto t_iter = ctx.sub_results.begin();
    for ( ; t_iter != ctx.sub_results.end(); ++t_iter) {
      auto res_iter = res_map.find(t_iter->first);
      if (res_iter == res_map.end())
        res_map.emplace(t_iter->first, t_iter->second);
      else
        res_iter->second += t_iter->second;
    }

    cnt += ctx.cnt;
  }

  int total_s_order_cnt = 0;
  for (auto iter = res_map.begin(); iter != res_map.end(); ++iter) {
    // assert(iter->second != 0);
    result_vec.emplace_back(iter->first, iter->second);
    total_s_order_cnt += iter->second;
  }

#if Q11_STABLE_SORT
  stable_sort(result_vec.begin(), result_vec.end(), ResultRow::Compare);
#else
  sort(result_vec.begin(), result_vec.end(), ResultRow::Compare);
#endif

#if 1
  for (int i = 0; i < result_vec.size(); ++i) {
    if (result_vec[i].ordercount <= total_s_order_cnt * 0.005) {
      result_vec.erase(result_vec.begin() + i, result_vec.end());
      break;
    }
  }
#endif

  if (!q_verbose_) return true;
  
  printf("Result of query 11:\n");
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

  unique_ptr<BackupStore::ColCursor> 
    cnt_cur = s_tbl->getColCursor(S_ORDER_CNT, ctx.ver);
  Keys s_keys = s_tbl->getKeyCol();
  cnt_cur->seekOffset(ctx.off_begin, ctx.off_end);

  s_tbl->locateCol(S_ORDER_CNT, sizeof(int32_t));

  while (cnt_cur->nextRow()) {
    uint64_t s_i = cnt_cur->cur();
    uint64_t s_key = s_keys[s_i];
    int32_t s_i_id = stockKeyToItem(s_key);
    int32_t s_w_id = stockKeyToWare(s_key);
    uint64_t su_suppkey = (s_w_id * s_i_id) % 10000 + 1;
    if (ctx.su_suppkeys.find(su_suppkey) == ctx.su_suppkeys.end())
      continue;
    
    int32_t s_order_cnt = *(int32_t *) cnt_cur->value();
    if (ctx.sub_results.find(s_i_id) == ctx.sub_results.end()) {
      ctx.sub_results[s_i_id] = s_order_cnt;
    } else {
      ctx.sub_results[s_i_id] += s_order_cnt;
    }
    ++cnt;
  }

  ctx.cnt = cnt;
}

} // namespace anonymous


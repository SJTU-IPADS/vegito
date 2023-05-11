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

#define Q9_STAT_JOIN (STAT_JOIN == 9)
#define TIMER Q9_STAT_JOIN
#include "ch_query_timer.h"

namespace {

const char *I_DATA_STR = "BB";

struct ResultRow {
  const char *n_name;
  uint32_t l_year;
  AMOUNT sum_profit;

  ResultRow(const char *n, uint32_t l, AMOUNT a)
    : n_name(n), l_year(l), sum_profit(a) { }

  static bool Compare(ResultRow &a, ResultRow &b) {
    int res = strcmp(a.n_name, b.n_name);
    if (res == 0)
      return a.l_year >= b.l_year;
    else if (res < 0)
      return true;
    else
      return false;
  }
  
  static void print_header() {
    printf("%-15s %-8s %-16s\n", "n_name", "l_year", "sum_profit");
  }

  void print() const {
    printf("%-15s %-8u %-16.2f \n", n_name, l_year, sum_profit);
  }

};

using ResultMap = map<string, ResultRow>;

struct Ctx : public BaseCtx {
  // Query data
  ResultMap sub_results;

  Ctx(BackupDB &d, uint64_t v, uint64_t b, uint64_t e) 
    : BaseCtx(d, v, b, e) { }
} __attribute__ ((aligned (CACHE_LINE_SZ)));

void query(void *arg);

} // namespace anonymous

namespace nocc {
namespace oltp {
namespace ch {

bool ChQueryWorker::query09(yield_func_t &yield) {
  uint64_t ver = get_read_ver_();
  uint64_t cnt = 0;

  // ResultRow data
  ResultMap result_map;

  /**************** Parallel Part Begin ********************/
  // Calculate workload for each thread
  uint64_t o_tbl_sz = o_tbl->getOffsetHeader();
  db_cnt_ = o_tbl_sz;
  
  vector<Ctx> ctxs;
  ctxs.reserve(num_thread_);
  
  vector<vector<uint64_t>> workloads = part_workloads(o_tbl_sz);

  for (const auto &v : workloads) {
    uint64_t off_begin = v[0],
             off_end   = v[1];
    ctxs.emplace_back(*db_, ver, off_begin, off_end);
  }
 
  parallel_process(ctxs, query);

  /**************** Parallel Part End ********************/

  // Collect data from all slave threads
  for (const Ctx &ctx : ctxs) {
    auto t_iter = ctx.sub_results.begin();
    for (; t_iter != ctx.sub_results.end(); ++t_iter) {
      auto result_iter = result_map.find(t_iter->first);
      if (result_iter != result_map.end()) {
        result_iter->second.sum_profit += t_iter->second.sum_profit;
      } else {
        result_map.emplace(t_iter->first, t_iter->second);
      }
    }

    cnt += ctx.cnt;
  }
  
  // No need to sort. There are natural order in map

  print_timer(ctxs);
  if (!q_verbose_) return true;

  printf("Result of query 9:\n");
  ResultRow::print_header();
  for (const auto &p : result_map)
    p.second.print();
  printf("Result: %lu tuples, selected %lu tuples\n", result_map.size(), cnt);

  return true;
}
}  // namespace ch
}  // namespace oltp
}  // namespace nocc

namespace {

void query(void *arg) {
  declare_timer(timer);  // total join
  Ctx &ctx = *(Ctx *) arg;
  uint64_t cnt = 0;

  Keys o_keys = o_tbl->getKeyCol();

  // Nested-loop
  bool end = false;
  for (uint64_t o_i = ctx.off_begin; o_i < ctx.off_end && !end; ++o_i) {
    uint32_t l_year = o_entry_ds[o_i] % 10;

    int64_t o_key = o_keys[o_i];
    int32_t o_o_id = orderKeyToOrder(o_key);
    int32_t o_w_id = orderKeyToWare(o_key);
    int32_t o_d_id = orderKeyToDistrict(o_key);
    AMOUNT revenue = 0;

    timer_start(timer);
#if OL_GRAPH == 0
    int8_t o_ol_cnt = o_ol_cnts[o_i];
    uint64_t start_ol_key = makeOrderLineKey(o_w_id, o_d_id, o_o_id, 1);
    uint64_t end_ol_key = makeOrderLineKey(o_w_id, o_d_id, o_o_id, o_ol_cnt);
    assert(o_ol_cnt >= 1 && o_ol_cnt <= 15);

    // Nested-loop
    for (uint64_t ol_key = start_ol_key; ol_key <= end_ol_key; ++ol_key) 
#else  // graph
    uint64_t *o_edge = ctx.db.getEdge(ORDE, o_i);
    int8_t o_ol_cnt = o_edge[0];
    for (int i = 1; i <= o_ol_cnt; ++i)
#endif
    {
#if OL_GRAPH == 0
      uint64_t ol_i = ctx.db.getOffset(ORLI, ol_key, ctx.ver);
#else  // graph
      uint64_t ol_i = o_edge[i];  // graph
#endif
      if (ol_i == -1) {
        end = true;
        break;
      }

#if Q9_STAT_JOIN
      continue;
#endif
      int32_t ol_i_id = ol_i_ids[ol_i];
      int32_t ol_supply_w_id = ol_supply_w_ids[ol_i];
      AMOUNT ol_amount = ol_amounts[ol_i];

      auto i_data = 
        (inline_str_8<50> *) ctx.db.Get(ITEM, ol_i_id, I_DATA, ctx.ver);

      int suffix_len = strlen(I_DATA_STR);
      if (i_data->size() >= suffix_len &&
          strncmp(i_data->str().substr(i_data->size() - suffix_len, string::npos).data(), 
                  I_DATA_STR, suffix_len) ==0) {

        uint64_t s_key = makeStockKey(ol_supply_w_id, ol_i_id);
        uint64_t s_i = ctx.db.getOffset(STOC, s_key);
        assert(s_i != -1);

        uint64_t su_suppkey = (ol_supply_w_id, ol_i_id) % 10000 + 1;
        uint64_t su_nationkey = *(int8_t *) ctx.db.Get(SUPP, su_suppkey, SU_NATIONKEY, ctx.ver);
  
        auto n_name = 
          (inline_str_8<15> *) ctx.db.Get(NATI, su_nationkey, N_NAME, ctx.ver);
  
        string group_key = n_name->str(true); // zero padding
        group_key += to_string(9 - l_year);
  
        auto result_iter = ctx.sub_results.find(group_key);
        if (result_iter != ctx.sub_results.end()) {
          result_iter->second.sum_profit += ol_amount;
        } else {
          ResultRow res(n_name->data(), l_year, ol_amount);
          ctx.sub_results.emplace(group_key, res);
        }

        ++cnt;
      }
    }
    timer_end(timer, ctx, 0);

  }

  ctx.cnt = cnt;
}

} // namespace anonymous


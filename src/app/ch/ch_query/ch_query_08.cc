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

#include "ch_query_timer.h"

namespace {

const char* R_NAME_STR = "EUROPE";
const char* N_NAME_STR = "GERMANY";
const char CONST_I_DATA_CHAR_TEMPLATE = 'b';

using NatiSet = unordered_set<uint64_t>;
using ResultMap = map<uint32_t, AMOUNT>;  // l_year -> mkt_share

struct Ctx : public BaseCtx {
  // Query data
  const NatiSet &n1_nationkeys;
  const uint64_t n2_nationkey;

  ResultMap sub_results; // Result Table (l_year, mkt_share)

  Ctx(BackupDB &d, uint64_t v, uint64_t b, uint64_t e,
      const NatiSet &n1, uint64_t n2) 
    : BaseCtx(d, v, b, e),
      n1_nationkeys(n1), n2_nationkey(n2) { }
} __attribute__ ((aligned (CACHE_LINE_SZ)));

void query(void *arg);

} // namespace anonymous

namespace nocc {
namespace oltp {
namespace ch {

bool ChQueryWorker::query08(yield_func_t &yield) {
  uint64_t ver = get_read_ver_();
  uint64_t cnt = 0;
  declare_timer(timer);

  timer_start(timer);

  // Result data
  NatiSet n1_nationkeys;
  uint64_t n2_nationkey = -1;
  ResultMap result_table; // Result Table (l_year, mkt_share)

  // Region
  uint64_t n1_regionkey = -1;
  Keys r_keys = r_tbl->getKeyCol();
  for (uint64_t r_i = 0; r_i < r_keys.size(); ++r_i) {
    const char *r_name = r_names[r_i].data();
    if (strncmp(r_name, R_NAME_STR, strlen(R_NAME_STR)) == 0) {
      n1_regionkey = r_keys[r_i];
      break;
    }
  }
  assert(n1_regionkey != -1);

  // Nation
  Keys n_keys = n_tbl->getKeyCol();
  for (uint64_t n_i = 0; n_i < n_keys.size(); ++n_i) {
    if (n1_regionkey == n_regionkeys[n_i])
      n1_nationkeys.insert(n_keys[n_i]);

    const char* n_name = n_names[n_i].data();
    if (strncmp(n_name, N_NAME_STR, strlen(N_NAME_STR)) == 0)
      n2_nationkey = n_keys[n_i];
  }

  assert(!n1_nationkeys.empty() && n2_nationkey != -1);

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
    ctxs.emplace_back(*db_, ver, off_begin, off_end, n1_nationkeys, n2_nationkey);
  }
  
  timer_end_print(timer, "Pre");

  timer_start(timer);
 
  parallel_process(ctxs, query);
  timer_end_print(timer, "Parallel");

  /**************** Parallel Part End ********************/

  timer_start(timer);
  // Collect data from all slave threads
  for (const Ctx &ctx : ctxs) {
    auto t_iter = ctx.sub_results.begin();
    for (; t_iter != ctx.sub_results.end(); ++t_iter) {
      auto result_iter = result_table.find(t_iter->first);
      if (result_iter != result_table.end()) {
        result_iter->second += t_iter->second;  // XXX: a simple mistake
      } else {
        result_table.emplace(t_iter->first, t_iter->second);
      }
    }
    cnt += ctx.cnt;
  }
 
  // sort in the map
  timer_end_print(timer, "Post");
  print_timer(ctxs);

  if (!q_verbose_) return true;

  printf("Result of query 8:\n");

  printf("%-8s %-16s\n", "l_year", "mk_share");
  for (const auto &p : result_table) {
    printf("%-8u %-16.2f\n", p.first, p.second);
  }

  printf("Result: %lu tuples, selected %lu tuples\n", result_table.size(), cnt);
  
  return true;
}

}  // namespace ch
}  // namespace oltp
}  // namespace nocc

namespace {

void query(void *arg) {
  Ctx &ctx = *(Ctx *) arg;

  uint64_t cnt = 0;

  uint64_t o_begin = ctx.off_begin;
  uint64_t o_end   = ctx.off_end;

  Keys o_keys = o_tbl->getKeyCol();

  // Nested-loop
  bool end = false;
  for (uint64_t o_i = o_begin; o_i < o_end && !end; ++o_i) {
    uint32_t o_entry_d = o_entry_ds[o_i];
    if (o_entry_d == 0) continue;

    int64_t o_key = (int64_t) o_keys[o_i];
    int32_t o_o_id = orderKeyToOrder(o_key);
    int32_t o_w_id = orderKeyToWare(o_key);
    int32_t o_d_id = orderKeyToDistrict(o_key);
    int32_t o_c_id = o_c_ids[o_i];

    uint64_t c_key = makeCustomerKey(o_w_id, o_d_id, o_c_id);

    uint64_t c_i = ctx.db.getOffset(CUST, c_key);  // here, 3ms different
    assert(c_i != -1);

    const inline_str_fixed<2>* c_state = &c_states[c_i];
    uint64_t nation_key = (uint64_t) c_states[c_i].data()[0];
    if (ctx.n1_nationkeys.count(nation_key) == 0) continue;

    int8_t o_ol_cnt = o_ol_cnts[o_i];
    uint32_t l_year = o_entry_ds[o_i] % 10;
    uint64_t start_ol_key = makeOrderLineKey(o_w_id, o_d_id, o_o_id, 1);
    uint64_t end_ol_key = makeOrderLineKey(o_w_id, o_d_id, o_o_id, o_ol_cnt);
    assert(o_ol_cnt >= 1 && o_ol_cnt <= 15);

#if 1
    // Nested-loop
    for (uint64_t ol_key = start_ol_key; ol_key <= end_ol_key; ++ol_key) {
      uint64_t ol_i = ctx.db.getOffset(ORLI, ol_key, ctx.ver);
      if (ol_i == -1) {
        end = true;
        break;
      }
      int32_t ol_i_id = ol_i_ids[ol_i];
      int32_t ol_supply_w_id = ol_supply_w_ids[ol_i];
      AMOUNT ol_amount = ol_amounts[ol_i];

#if 1
      auto i_data = 
        (inline_str_8<50> *) ctx.db.Get(ITEM, ol_i_id, I_DATA, ctx.ver);

      if (i_data->size() >= 1 &&
          i_data->str()[i_data->size() - 1] == CONST_I_DATA_CHAR_TEMPLATE) 
#endif
      {
        uint64_t s_key = makeStockKey(ol_supply_w_id, ol_i_id);
        uint64_t s_i = ctx.db.getOffset(STOC, s_key);
        assert(s_i != -1);
        uint64_t su_suppkey = (ol_supply_w_id, ol_i_id) % 10000 + 1;
        int8_t su_nationkey = 
          *(int8_t *) ctx.db.Get(SUPP, su_suppkey, SU_NATIONKEY, ctx.ver);

        if (su_nationkey == ctx.n2_nationkey) {
          auto res_iter = ctx.sub_results.find(l_year);
          if (res_iter != ctx.sub_results.end())
            res_iter->second += ol_amount;
          else
            ctx.sub_results.emplace(l_year, ol_amount);
          ++cnt;
        }
      }
    }
#endif 

  }

  ctx.cnt = cnt;
}

} // namespace anonymous


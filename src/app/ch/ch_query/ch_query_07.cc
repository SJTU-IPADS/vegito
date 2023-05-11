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

const char *N1_NAME = "GERMANY";
const char *N2_NAME = "CAMBODIA";

struct ResultRow {
  int8_t supp_nation;
  char cust_nation;
  uint32_t l_year;
  AMOUNT revenue;

  ResultRow(int8_t su, char c, uint32_t l, AMOUNT a)
    : supp_nation(su), cust_nation(c), l_year(l), revenue(a) { }

  static bool Compare(const ResultRow &a, const ResultRow &b) {
    if (a.supp_nation == b.supp_nation) {
      return  a.cust_nation < b.cust_nation;
    }
    else {
      if (a.supp_nation == b.supp_nation) {
        return a.l_year < b.l_year;
      } else {
        return a.supp_nation < b.supp_nation;
      }
    }
  }

  static void print_header() {
    printf("%-12s %-12s %-8s %-16s\n", 
           "supp_nation", "cust_nation", "l_year", "revenue");
  }

  void print() const {
    printf("%-12u %-12c %-8u %-16.2f\n", 
           supp_nation, cust_nation, l_year, revenue);
  }
};

using CustMap = unordered_map<uint64_t, uint64_t>;  //  c_key -> n_key
using SuppMap = unordered_map<uint64_t, uint64_t>;  // su_key -> n_key

// Group key is encoded as supp_nationKey << 9 + cust_nationKey << 1 + l_year so that they are ordered by map
using ResultMap = map<uint64_t, ResultRow>; // group key word -> result row mapping

struct Ctx : public BaseCtx {
  // Query data
  const CustMap &c_buf; // Temporary table (customer key, nation key)
  const SuppMap &su_buf; // Temporary table (supplier key, nation key)

  ResultMap sub_results; // group key word -> result row mapping

  Ctx(BackupDB &d, uint64_t v, uint64_t b, uint64_t e, 
      const CustMap &c, const SuppMap &su) 
    : BaseCtx(d, v, b, e),
      c_buf(c), su_buf(su) { }
} __attribute__ ((aligned (CACHE_LINE_SZ)));

void query_0(void *arg);
void query_1(void *arg);

} // namespace anonymous


namespace nocc {
namespace oltp {
namespace ch {


bool ChQueryWorker::query07(yield_func_t &yield) {
  uint64_t ver = get_read_ver_();
  uint64_t cnt = 0;

  declare_timer(timer);

  timer_start(timer);
  // ResultRow data
  uint64_t n1_nationkey = 0; // GERMANY
  uint64_t n2_nationkey = 0; // CAMBODIA
  CustMap c_buf; // Temporary table (customer key, nation key)
  SuppMap su_buf; // Temporary table (supplier key, nation key)

  ResultMap result_table; // group key word -> result row mapping

  // Nation
  Keys n_keys = n_tbl->getKeyCol();
  for (uint64_t n_i = 0; n_i < n_keys.size(); ++n_i) {
    const char *n_name = n_names[n_i].data();
    if (strncmp(n_name, N1_NAME, strlen(N1_NAME)) == 0)
      n1_nationkey = n_keys[n_i];
    else if (strncmp(n_name, N2_NAME, strlen(N2_NAME)) == 0)
      n2_nationkey = n_keys[n_i];
  }
  assert(n1_nationkey != 0 && n2_nationkey != 0);

  // Customer
  Keys c_keys = c_tbl->getKeyCol();
  for (uint64_t c_i = 0; c_i < c_keys.size(); ++c_i) {
    uint64_t nation_key = (uint64_t) c_states[c_i].data()[0];
    if (nation_key == n1_nationkey || nation_key == n2_nationkey)
      c_buf.emplace(c_keys[c_i], nation_key);
  }
  assert(c_buf.size() != 0);

  // Supplier
  Keys su_keys = su_tbl->getKeyCol();
  for (uint64_t su_i = 0; su_i < su_keys.size(); ++su_i) {
    uint64_t nation_key = (uint64_t) su_nationkeys[su_i];
    if (nation_key == n1_nationkey || nation_key == n2_nationkey)
      su_buf.emplace(su_keys[su_i], nation_key);
  }
  assert(su_buf.size() != 0);
  
  uint64_t o_tbl_sz = o_tbl->getOffsetHeader();
  uint64_t ol_tbl_sz = ol_tbl->getOffsetHeader();
  db_cnt_ = ol_tbl_sz;

  /**************** Parallel Part Begin ********************/

  uint64_t max_offs[] = { o_tbl_sz, ol_tbl_sz };
  TaskFn*  queries[]  = {query_0, query_1 };

  const int M = 1;
  uint64_t max_off = max_offs[M];
  TaskFn *query = queries[M];

  vector<Ctx> ctxs;
  ctxs.reserve(num_thread_);
  
  vector<vector<uint64_t>> workloads = part_workloads(max_off);

  for (const auto &v : workloads) {
    uint64_t off_begin = v[0],
             off_end   = v[1];
    ctxs.emplace_back(*db_, ver, off_begin, off_end, c_buf, su_buf);
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
        result_iter->second.revenue += t_iter->second.revenue;
      } else {
        result_table.emplace(t_iter->first, t_iter->second);
      }
    }
    cnt += ctx.cnt;
  }
  timer_end_print(timer, "Post");
  print_timer(ctxs);

  // sort in the map

  if (!q_verbose_) return true;

  printf("Result of query 7:\n");

  ResultRow::print_header();
  for (const auto &p : result_table)
    p.second.print();

  printf("Result: %lu tuples, selected %lu tuples\n", result_table.size(), cnt);
  
  return true;
}

}  // namespace ch
}  // namespace oltp
}  // namespace nocc

namespace {

void query_0(void *arg) {

  Ctx &ctx = *(Ctx *) arg;
  uint64_t cnt = 0;

  // Order
  Keys o_keys = o_tbl->getKeyCol();

  // Nested-Loop
  bool end = false;
  for (uint64_t o_i = ctx.off_begin; o_i < ctx.off_end && !end; ++o_i) {
    uint32_t l_year = o_entry_ds[o_i] % 10;

    int64_t o_key = (int64_t) o_keys[o_i];
    int32_t o_w_id = orderKeyToWare(o_key);
    int32_t o_d_id = orderKeyToDistrict(o_key);
    int32_t o_o_id = orderKeyToOrder(o_key);
    int32_t o_c_id = o_c_ids[o_i];
    int8_t o_ol_cnt = o_ol_cnts[o_i];
    assert(o_ol_cnt >= 1 && o_ol_cnt <= 15);

    uint64_t c_key = (uint64_t) makeCustomerKey(o_w_id, o_d_id, o_c_id);
    auto c_it = ctx.c_buf.find(c_key);
    if (c_it == ctx.c_buf.end()) continue;

    uint64_t start_ol_key = 
      (uint64_t) makeOrderLineKey(o_w_id, o_d_id, o_o_id, 1);
    uint64_t end_ol_key = 
      (uint64_t) makeOrderLineKey(o_w_id, o_d_id, o_o_id, o_ol_cnt);

    // Nested-loop
    for (uint64_t ol_key = start_ol_key; ol_key <= end_ol_key; ++ol_key) {
      uint64_t ol_i = ctx.db.getOffset(ORLI, ol_key, ctx.ver);
      if (ol_i == -1) {
        end = true;
        break;
      }

      uint32_t *ol_delivery_d = 
        (uint32_t *) ol_tbl->getByOffset(ol_i, OL_DELIVERY_D, ctx.ver);
      assert(ol_delivery_d);
      if (*ol_delivery_d == 0) continue;

      int32_t ol_supply_w_id = ol_supply_w_ids[ol_i];
      int32_t ol_i_id = ol_i_ids[ol_i];
      uint64_t s_key = (uint64_t) makeStockKey(ol_supply_w_id, ol_i_id);
      uint64_t s_i = ctx.db.getOffset(STOC, s_key);
      assert(s_i != -1);

      uint64_t su_suppkey = (ol_supply_w_id * ol_i_id) % 10000 + 1;
      auto su_it = ctx.su_buf.find(su_suppkey);
      if (su_it == ctx.su_buf.end()) continue;
      
      // If customer and supplier are not from the same country
      if (c_it->second == su_it->second) continue;

      uint64_t group_key = l_year;
      group_key |= (c_it->second & 0xff) << 1;
      group_key |= (su_it->second & 0xff) << 9;

      auto res_iter = ctx.sub_results.find(group_key);
      if (res_iter == ctx.sub_results.end()) {
        ResultRow res(su_it->second, c_it->second, l_year, ol_amounts[ol_i]);
        ctx.sub_results.emplace(group_key, res);
        ++cnt;
      } else {
        res_iter->second.revenue += ol_amounts[ol_i];
        ++cnt;
      }
      
    }
    
  }
  ctx.cnt = cnt;
}

void query_1(void *arg) {

  Ctx &ctx = *(Ctx *) arg;
  uint64_t cnt = 0;

  // Orderline
  Keys ol_keys = ol_tbl->getKeyCol();

  // Nested-Loop
  for (uint64_t ol_i = ctx.off_begin; ol_i < ctx.off_end; ++ol_i) {
    int64_t ol_key = (int64_t) ol_keys[ol_i];
    int64_t o_key = orderLineKeyToOrderKey(ol_key);
    uint64_t o_i = ctx.db.getOffset(ORDE, o_key);
    
    uint32_t l_year = o_entry_ds[o_i] % 10;
    int32_t o_w_id = orderKeyToWare(o_key);
    int32_t o_d_id = orderKeyToDistrict(o_key);
    int32_t o_o_id = orderKeyToOrder(o_key);
    int32_t o_c_id = o_c_ids[o_i];
    int8_t o_ol_cnt = o_ol_cnts[o_i];
    assert(o_ol_cnt >= 1 && o_ol_cnt <= 15);
    assert(o_c_id >=1 && o_c_id <= 3000);

    uint64_t c_key = (uint64_t) makeCustomerKey(o_w_id, o_d_id, o_c_id);
    auto c_it = ctx.c_buf.find(c_key);
    if (c_it == ctx.c_buf.end()) continue;
      
    int32_t ol_supply_w_id = ol_supply_w_ids[ol_i];
    int32_t ol_i_id = ol_i_ids[ol_i];
    uint64_t s_key = (uint64_t) makeStockKey(ol_supply_w_id, ol_i_id);
    uint64_t s_i = ctx.db.getOffset(STOC, s_key);
    assert(s_i != -1);

    uint64_t su_suppkey = (ol_supply_w_id * ol_i_id) % 10000 + 1;
    auto su_it = ctx.su_buf.find(su_suppkey);
    if (su_it == ctx.su_buf.end()) continue;
    
    // If customer and supplier are not from the same country
    if (c_it->second == su_it->second) continue;

    uint32_t *ol_delivery_d = 
      (uint32_t *) ol_tbl->getByOffset(ol_i, OL_DELIVERY_D, ctx.ver);
    if (ol_delivery_d == nullptr) continue;

    assert(ol_delivery_d);
    if (*ol_delivery_d == 0) continue;

    uint64_t group_key = l_year;
    group_key |= (c_it->second & 0xff) << 1;
    group_key |= (su_it->second & 0xff) << 9;

    auto res_iter = ctx.sub_results.find(group_key);
    if (res_iter == ctx.sub_results.end()) {
      ResultRow res(su_it->second, c_it->second, l_year, ol_amounts[ol_i]);
      ctx.sub_results.emplace(group_key, res);
      ++cnt;
    } else {
      res_iter->second.revenue += ol_amounts[ol_i];
      ++cnt;
    }
      
  }
  ctx.cnt = cnt;
}
} // namespace anonymous


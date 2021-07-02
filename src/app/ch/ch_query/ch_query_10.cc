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

const char *I_DATA_STR = "BB";

struct ResultRow {
  int32_t c_id;
  const char* c_last;
  AMOUNT revenue;
  const char* c_city;
  const char* c_phone;
  const char* n_name;

  ResultRow(int32_t ci, const char *cl, AMOUNT r, const char *cc,
            const char *cp, const char *n)
    : c_id(ci), c_last(cl), revenue(r), c_city(cc), c_phone(cp), n_name(n) { }

  static bool Compare(const ResultRow &a, const ResultRow &b) {
    return a.revenue >= b.revenue;
  }
  
  static void print_header() {
    printf("%-12s %-16s %-12s %-20s %-16s %-15s\n",
           "c_id", "c_last", "revenue", "c_city", "c_phone", "n_name");
  }

  void print() const {
    printf("%-12d %-16s %-12.2f %-20s %-16.16s %-15s \n", 
           c_id, c_last, revenue, c_city, c_phone, n_name);
  }
};

using ResultMap = unordered_map<string, ResultRow>;

struct Ctx : public BaseCtx {
  // Query data
  ResultMap &sub_result;
  string &group_key;

  Ctx(BackupDB &d, uint64_t v, uint64_t b, uint64_t e, ResultMap &sr, 
      string &str) 
    : BaseCtx(d, v, b, e), sub_result(sr), group_key(str) { 
    sub_result.clear();  
  }
} __attribute__ ((aligned (CACHE_LINE_SZ)));

void query(void *arg);

// ResultRow data
thread_local vector<ResultRow> result_vec;
thread_local ResultMap result_table_map;
thread_local vector<ResultMap> g_sub_results;
thread_local vector<string> g_strs;

} // namespace anonymous

namespace nocc {
namespace oltp {
namespace ch {

void ChQueryWorker::query10_init() {
  result_vec.reserve(3000000);
  result_table_map.reserve(3000000);
  g_sub_results.resize(num_thread_);
  g_strs.resize(num_thread_);
  for (ResultMap &m : g_sub_results) {
    m.reserve(3000000);
  }
  for (string &s : g_strs) {
    s.reserve(100);
  }

}

bool ChQueryWorker::query10(yield_func_t &yield) {
  uint64_t ver = get_read_ver_();
  uint64_t cnt = 0;

  result_vec.clear();
  result_table_map.clear();

  /**************** Parallel Part Begin ********************/
  // Calculate workload for each thread
  uint64_t o_tbl_sz = o_tbl->getOffsetHeader();
  db_cnt_ = o_tbl_sz;
  
  vector<Ctx> ctxs;
  ctxs.reserve(num_thread_);
  
  vector<vector<uint64_t>> workloads = part_workloads(o_tbl_sz);

  for (int i = 0; i < num_thread_; ++i) {
    const auto &v = workloads[i];
    uint64_t off_begin = v[0],
             off_end   = v[1];
    ctxs.emplace_back(*db_, ver, off_begin, off_end, 
                      g_sub_results[i], g_strs[i]);
  }
 
  parallel_process(ctxs, query);

  /**************** Parallel Part End ********************/
  // Collect data from all slave threads
  for (const Ctx &ctx : ctxs) {
    auto t_iter = ctx.sub_result.begin();
    for (; t_iter != ctx.sub_result.end(); ++t_iter) {
      auto result_iter = result_table_map.find(t_iter->first);
      if (result_iter != result_table_map.end()) {
        result_iter->second.revenue += t_iter->second.revenue;
      } else {
        result_table_map.emplace(t_iter->first, t_iter->second);
      }
    }

    cnt += ctx.cnt;
  }
 
  for (const auto &pair : result_table_map) {
    result_vec.push_back(pair.second);
  }

  // stable_sort(result_vec.begin(), result_vec.end(), ResultRow::Compare);
  
  if (!q_verbose_) return true;

  printf("Result of query 10:\n");
  // ResultRow::print_header();
  // for (const auto &row : result_vec)
  //   row.print();

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
  string &group_key = ctx.group_key;
  // group_key.reserve(100);
  for (uint64_t o_i = ctx.off_begin; o_i < ctx.off_end && !end; ++o_i, ++cnt) {
    int64_t o_key = int64_t(o_keys[o_i]);
    int32_t o_o_id = orderKeyToOrder(o_key);
    int32_t o_w_id = orderKeyToWare(o_key);
    int32_t o_d_id = orderKeyToDistrict(o_key);
    
    uint32_t o_entry_d = o_entry_ds[o_i];
    if (o_entry_d == 0) continue;

    int32_t o_c_id = o_c_ids[o_i];
    int8_t o_ol_cnt = o_ol_cnts[o_i];
    uint64_t c_key = makeCustomerKey(o_w_id, o_d_id, o_c_id);
    
    uint64_t c_i = ctx.db.getOffset(CUST, c_key);
    assert(c_i != -1); 
    const inline_str_fixed<2>* c_state = &c_states[c_i];
    const inline_str_8<16>* c_last = &c_lasts[c_i];
    const inline_str_8<20>* c_city = &c_cities[c_i];
    const inline_str_fixed<16>* c_phone = &c_phones[c_i];

    uint64_t n_key = uint64_t(c_states[c_i].data()[0]);

    auto n_name = (inline_str_8<15> *) ctx.db.Get(NATI, n_key, N_NAME, ctx.ver);
    assert(n_name);

    uint64_t start_ol_key = makeOrderLineKey(o_w_id, o_d_id, o_o_id, 1);
    uint64_t end_ol_key = makeOrderLineKey(o_w_id, o_d_id, o_o_id, o_ol_cnt);
    assert(o_ol_cnt > 0);

    // Nested-loop
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

      if (o_entry_d > *ol_delivery_d) continue;
      AMOUNT ol_amount = ol_amounts[ol_i];

      group_key.clear();
      group_key += c_last->str(true);
      group_key += c_city->str(true);
      group_key += c_phone->str();
      group_key += n_name->str(true);
      group_key += to_string(o_c_id);

      auto res_iter = ctx.sub_result.find(group_key);
      if (res_iter != ctx.sub_result.end()) {
        res_iter->second.revenue += ol_amount;
      } else {
        ResultRow res(o_c_id, c_last->data(), ol_amount, c_city->data(),
                      c_phone->data(), n_name->data());
        ctx.sub_result.emplace(group_key, res);
      }
      cnt++; // access orderline record
    }
  }
  ctx.cnt = cnt;
}

} // namespace anonymous


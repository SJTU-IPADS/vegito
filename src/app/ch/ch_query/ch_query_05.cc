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

using namespace std;
using namespace nocc::oltp::ch;

#define TIMER 0

#define Q5_STABLE_SORT 0

#include "ch_query_timer.h"

namespace {

const char *R_NAME_STR = "EUROPE";

struct ResultRow {
  const char *n_name;
  AMOUNT revenue;

  ResultRow(const char *n_name, AMOUNT revenue)
    : n_name(n_name), revenue(revenue) {}

  static bool Compare(const ResultRow &a, const ResultRow &b) {
    return a.revenue >= b.revenue;
  }

  static void print_header() {
    printf("%-16s %-16s\n","n_name", "revenue");
  }

  void print() const {
    printf("%-16s %-16.2f\n", n_name, revenue);
  }
};

struct KeyComparator {
  bool operator() (const char *a, const char *b) const {
    return strcmp(a, b) < 0;
  }
};

using NatiMap = unordered_map<uint64_t, const char*>;  // nation_key -> n_name
using CustMap = unordered_map<uint64_t, uint64_t>;     // customer key -> nation key 
using SuppMap = unordered_map<uint64_t, uint64_t>;     // supplier key -> nation key
using ResultMap = map<const char*, AMOUNT, KeyComparator>; // n_name -> revenue
using Vec = vector<uint64_t>;

struct Ctx : public BaseCtx {
  // Query data
  const NatiMap &n_buf; 
  const CustMap &c_buf;
  const SuppMap &su_buf;

  Vec &ol_is;
  Vec &nation_keys;
  Vec &suppkeys;

  ResultMap sub_results; // n_name -> revenue mapping

  Ctx(BackupDB &d, uint64_t v, uint64_t b, uint64_t e,
      const NatiMap &n, const CustMap &c, const SuppMap &su,
      Vec &olis, Vec &nk, Vec &suk) 
    : BaseCtx(d, v, b, e),
      n_buf(n), c_buf(c), su_buf(su),
      ol_is(olis), nation_keys(nk), suppkeys(suk) { }
} __attribute__ ((aligned (CACHE_LINE_SZ)));

void query_0(void *arg);
void query_1(void *arg);
void query_2(void *arg);
  
thread_local vector<Vec> g_ol_is_;
thread_local vector<Vec> g_nation_keys_;
thread_local vector<Vec> g_suppkeys_;

} // namespace anonymous

namespace nocc {
namespace oltp {
namespace ch {

void ChQueryWorker::query05_init() {
  uint64_t  n_tbl_sz =  n_tbl->getKeyCol().size();
  uint64_t su_tbl_sz = su_tbl->getKeyCol().size();
  uint64_t ol_tbl_sz = ol_tbl->getOffsetHeader();
  g_ol_is_.resize(num_thread_);
  g_nation_keys_.resize(num_thread_);
  g_suppkeys_.resize(num_thread_);

  for (int i = 0; i < num_thread_; ++i) {
    g_ol_is_.reserve(ol_tbl_sz);
    g_nation_keys_.reserve(n_tbl_sz);
    g_suppkeys_.reserve(su_tbl_sz);
  }
}

bool ChQueryWorker::query05(yield_func_t &yield) {
  uint64_t ver = get_read_ver_();
  uint64_t cnt = 0;
  declare_timer(timer);
  timer_start(timer);

  // ResultRow data
  vector<ResultRow> results;
  NatiMap   n_buf; // nation_key -> n_name  mapping
  CustMap   c_buf; // customer key -> nation key mapping
  SuppMap   su_buf; // supplier key -> nation key mapping
  ResultMap result_map; // n_name -> revenue mapping

  Keys r_keys = r_tbl->getKeyCol();
  Keys n_keys = n_tbl->getKeyCol();
  
  // Nested-Loop
  for (uint64_t r_i = 0; r_i < r_keys.size(); ++r_i) {
    if (strncmp(r_names[r_i].data(), R_NAME_STR, strlen(R_NAME_STR)) != 0)
      continue; 
    
    // Nested-Loop
    for (uint64_t n_i = 0; n_i < n_keys.size(); ++n_i) {
      if (n_regionkeys[n_i] == r_keys[r_i]) {
        n_buf.emplace(n_keys[n_i], n_names[n_i].data());
      }
    }
  }

  Keys su_keys = su_tbl->getKeyCol();

  for (uint64_t su_i = 0; su_i < su_keys.size(); ++su_i) {
    uint64_t nation_key = (uint64_t) su_nationkeys[su_i];
    if (n_buf.count(nation_key)) {
      su_buf.emplace(su_keys[su_i], nation_key);
    }
  }

  // customer
  Keys c_keys = c_tbl->getKeyCol();

  for (uint64_t c_i = 0; c_i < c_keys.size(); ++c_i) {
    uint64_t nation_key = (uint64_t) c_states[c_i].data()[0];
    if (n_buf.count(nation_key)) {
      c_buf.emplace(c_keys[c_i], nation_key);
    }
  
  }

  /**************** Parallel Part Begin ********************/
  // Calculate workload for each thread
  uint64_t o_tbl_sz = o_tbl->getOffsetHeader();
  uint64_t ol_tbl_sz = ol_tbl->getOffsetHeader();
  db_cnt_ = ol_tbl_sz;

  const int M = 2;  // 0, 1, 2
  uint64_t max_offs[] = { o_tbl_sz, ol_tbl_sz, ol_tbl_sz };
  TaskFn*  queries[]  = { query_0, query_1, query_2 };

  uint64_t max_off = max_offs[M];
  TaskFn *query = queries[M];

  vector<Ctx> ctxs;
  ctxs.reserve(num_thread_);
  
  vector<vector<uint64_t>> workloads = part_workloads(max_off);

  for (int i = 0; i < num_thread_; ++i) {
    const auto &v = workloads[i];
    uint64_t off_begin = v[0],
             off_end   = v[1];
    ctxs.emplace_back(*db_, ver, off_begin, off_end, 
                      n_buf, c_buf, su_buf,
                      g_ol_is_[i], g_nation_keys_[i], g_suppkeys_[i]);
  }
 
  timer_end_print(timer, "Before parallel");
  
  timer_start(timer);
  parallel_process(ctxs, query);
  timer_end_print(timer, "Parallel");

  /**************** Parallel Part End ********************/

  timer_start(timer);
  // Collect data from all slave threads
  for (const Ctx &ctx : ctxs) {
    auto t_iter = ctx.sub_results.begin();
    for ( ; t_iter != ctx.sub_results.end(); ++t_iter) {
      auto res_iter = result_map.find(t_iter->first);  // XXX: compare pointer
      if (res_iter == result_map.end())
        result_map.emplace(t_iter->first, t_iter->second);
      else
        res_iter->second += t_iter->second;
    }
    cnt += ctx.cnt;
  }

  for (auto iter = result_map.begin(); iter != result_map.end(); ++iter) {
    assert(iter->second != 0);
    results.emplace_back(iter->first, iter->second);
  }

#if Q5_STABLE_SORT
  stable_sort(results.begin(), results.end(), ResultRow::Compare);
#else
  sort(results.begin(), results.end(), ResultRow::Compare);
#endif

  timer_end_print(timer, "After Parallel");
  print_timer(ctxs);

  if (!q_verbose_) return true;

  printf("Result of query 5:\n");

  ResultRow::print_header();
  for (const ResultRow &row : results)
    row.print();

  printf("Result: %lu tuples, selected %lu tuples\n", results.size(), cnt);
  
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
  Keys order_keys = o_tbl->getKeyCol();

  // Nested-loop
  for (uint64_t o_i = ctx.off_begin; o_i < ctx.off_end; ++o_i) {
    int64_t o_key = (int64_t) order_keys[o_i];
    int32_t o_w_id = orderKeyToWare(o_key);
    int32_t o_d_id = orderKeyToDistrict(o_key);
    int32_t o_c_id = o_c_ids[o_i];
    assert(o_c_id >=1 && o_c_id <= 3000);

    if (o_entry_ds[o_i] == 0) continue;

    uint64_t c_key = (uint64_t) makeCustomerKey(o_w_id, o_d_id, o_c_id);
    auto cbuf_iter = ctx.c_buf.find(c_key);
    if (cbuf_iter == ctx.c_buf.end()) continue;

    int32_t o_o_id = orderKeyToOrder(o_key);
    uint64_t start_ol_key = 
      (uint64_t) makeOrderLineKey(o_w_id, o_d_id, o_o_id, 1);
    uint64_t end_ol_key = 
      (uint64_t) makeOrderLineKey(o_w_id, o_d_id, o_o_id, o_ol_cnts[o_i]);
    assert(o_ol_cnts[o_i] > 0);

    // 0: hash tbale
    // 1: graph
    const int OL_M = 0;

    // Nested-loop
    switch (OL_M) {
    case 0:
      for (uint64_t ol_key = start_ol_key; ol_key <= end_ol_key; ++ol_key) {
        uint64_t ol_i = ctx.db.getOffset(ORLI, ol_key);
        int32_t ol_i_id = ol_i_ids[ol_i];
        uint64_t s_key = (uint64_t) makeStockKey(o_w_id, ol_i_id);
        uint64_t s_i = ctx.db.getOffset(STOC, s_key);
        assert(ctx.db.getOffset(STOC, s_key) != -1);
  
        uint64_t su_suppkey = (o_w_id * ol_i_id) % 10000 + 1;
        auto subuf_iter = ctx.su_buf.find(su_suppkey);
        if (subuf_iter != ctx.su_buf.end()) {
          uint64_t cust_nationkey = cbuf_iter->second;
          uint64_t supp_nationkey = subuf_iter->second;
          
          if (cust_nationkey != supp_nationkey) continue;
          auto nbuf_iter = ctx.n_buf.find(cust_nationkey);
          if (nbuf_iter == ctx.n_buf.end()) continue;
  
          auto res_iter = ctx.sub_results.find(nbuf_iter->second);
          if (res_iter == ctx.sub_results.end()) {
            AMOUNT revenue = ol_amounts[ol_i];
            ctx.sub_results.emplace(nbuf_iter->second, revenue);
            ++cnt;
          } else {
            res_iter->second += ol_amounts[ol_i];
            ++cnt;
          }
        }
      }
      break;
    case 1:
    {
      uint64_t *o_edge = ctx.db.getEdge(ORDE, o_i);
      int8_t o_ol_cnt = o_edge[0];
      assert(o_ol_cnts[o_i] == o_ol_cnt);
      for (int i = 1; i <= o_ol_cnt; ++i) {
        uint64_t ol_i = o_edge[i];
        int32_t ol_i_id = ol_i_ids[ol_i];
        uint64_t s_key = (uint64_t) makeStockKey(o_w_id, ol_i_id);
        uint64_t s_i = ctx.db.getOffset(STOC, s_key);
        assert(ctx.db.getOffset(STOC, s_key) != -1);
  
        uint64_t su_suppkey = (o_w_id * ol_i_id) % 10000 + 1;
        auto subuf_iter = ctx.su_buf.find(su_suppkey);
        if (subuf_iter != ctx.su_buf.end()) {
          uint64_t cust_nationkey = cbuf_iter->second;
          uint64_t supp_nationkey = subuf_iter->second;
          
          if (cust_nationkey != supp_nationkey) continue;
          auto nbuf_iter = ctx.n_buf.find(cust_nationkey);
          if (nbuf_iter == ctx.n_buf.end()) continue;
  
          auto res_iter = ctx.sub_results.find(nbuf_iter->second);
          if (res_iter == ctx.sub_results.end()) {
            AMOUNT revenue = ol_amounts[ol_i];
            ctx.sub_results.emplace(nbuf_iter->second, revenue);
            ++cnt;
          } else {
            res_iter->second += ol_amounts[ol_i];
            ++cnt;
          }
        }
      }
      break;
    }
    default:
      assert(false);
    }
        
    uint64_t *o_edge = ctx.db.getEdge(ORDE, o_i);
        int8_t o_ol_cnt = o_edge[0];
  }

  ctx.cnt = cnt;
}

void query_1(void *arg) {
  Ctx &ctx = *(Ctx *) arg;
  uint64_t cnt = 0;

  Keys order_keys = o_tbl->getKeyCol();
  Keys ol_keys = ol_tbl->getKeyCol();

  // Nested-loop
  for (uint64_t ol_i = ctx.off_begin; ol_i < ctx.off_end; ++ol_i) {
    int64_t ol_key = (int64_t) ol_keys[ol_i];
    int64_t o_key = orderLineKeyToOrderKey(ol_key);
    uint64_t o_i = ctx.db.getOffset(ORDE, o_key);
    
    int32_t o_w_id = orderKeyToWare(o_key);
    int32_t o_d_id = orderKeyToDistrict(o_key);
    int32_t o_c_id = o_c_ids[o_i];
    assert(o_c_id >=1 && o_c_id <= 3000);

    if (o_entry_ds[o_i] == 0) continue;

    uint64_t c_key = (uint64_t) makeCustomerKey(o_w_id, o_d_id, o_c_id);
    auto cbuf_iter = ctx.c_buf.find(c_key);
    if (cbuf_iter == ctx.c_buf.end()) continue;
      
    int32_t ol_i_id = ol_i_ids[ol_i];
    uint64_t s_key = (uint64_t) makeStockKey(o_w_id, ol_i_id);
    uint64_t s_i = ctx.db.getOffset(STOC, s_key);
    assert(ctx.db.getOffset(STOC, s_key) != -1);

    uint64_t su_suppkey = (o_w_id * ol_i_id) % 10000 + 1;
    auto subuf_iter = ctx.su_buf.find(su_suppkey);

    if (subuf_iter != ctx.su_buf.end()) {
      uint64_t cust_nationkey = cbuf_iter->second;
      uint64_t supp_nationkey = subuf_iter->second;
      
      if (cust_nationkey != supp_nationkey) continue;
      auto nbuf_iter = ctx.n_buf.find(cust_nationkey);
      if (nbuf_iter == ctx.n_buf.end()) continue;

      auto res_iter = ctx.sub_results.find(nbuf_iter->second);
      if (res_iter == ctx.sub_results.end()) {
        AMOUNT revenue = ol_amounts[ol_i];
        ctx.sub_results.emplace(nbuf_iter->second, revenue);
        ++cnt;
      } else {
        res_iter->second += ol_amounts[ol_i];
        ++cnt;
      }
    }
  }

  ctx.cnt = cnt;
}

void query_2(void *arg) {
  Ctx &ctx = *(Ctx *) arg;
  uint64_t cnt = 0;

  Keys order_keys = o_tbl->getKeyCol();
  Keys ol_keys = ol_tbl->getKeyCol();
  Keys c_keys = c_tbl->getKeyCol();

#if 0
  vector<uint64_t> ol_is, nation_keys, suppkeys;
#else
  Vec &ol_is = ctx.ol_is;
  Vec &nation_keys = ctx.nation_keys;
  Vec &suppkeys = ctx.suppkeys;

  ol_is.clear();
  nation_keys.clear();
  suppkeys.clear();
#endif

  // Nested-loop
  for (uint64_t ol_i = ctx.off_begin; ol_i < ctx.off_end; ++ol_i) {
    int64_t ol_key = (int64_t) ol_keys[ol_i];
    int64_t o_key = orderLineKeyToOrderKey(ol_key);
    uint64_t o_i = ctx.db.getOffset(ORDE, o_key);
    
    int32_t o_w_id = orderKeyToWare(o_key);
    int32_t o_d_id = orderKeyToDistrict(o_key);
    int32_t o_c_id = o_c_ids[o_i];
    assert(o_c_id >=1 && o_c_id <= 3000);

    if (o_entry_ds[o_i] == 0) continue;

    uint64_t c_key = (uint64_t) makeCustomerKey(o_w_id, o_d_id, o_c_id);
    uint64_t c_i = ctx.db.getOffset(CUST, c_key);
    uint64_t nation_key = (uint64_t) c_states[c_i].data()[0];
      
    int32_t ol_i_id = ol_i_ids[ol_i];
    uint64_t s_key = (uint64_t) makeStockKey(o_w_id, ol_i_id);
    uint64_t s_i = ctx.db.getOffset(STOC, s_key);
    assert(ctx.db.getOffset(STOC, s_key) != -1);
    uint64_t su_suppkey = (o_w_id * ol_i_id) % 10000 + 1;

    ol_is.push_back(ol_i);
    nation_keys.push_back(nation_key);
    suppkeys.push_back(su_suppkey);
  }

  for (int i = 0; i < ol_is.size(); ++i) {
    auto subuf_iter = ctx.su_buf.find(suppkeys[i]);
    if (subuf_iter == ctx.su_buf.end()) continue;

    uint64_t supp_nationkey = subuf_iter->second;
    if (supp_nationkey != nation_keys[i]) continue;

    auto nbuf_iter = ctx.n_buf.find(supp_nationkey);
    if (nbuf_iter == ctx.n_buf.end()) continue;

    uint64_t ol_i = ol_is[i];
    auto res_iter = ctx.sub_results.find(nbuf_iter->second);
    if (res_iter == ctx.sub_results.end()) {
      AMOUNT revenue = ol_amounts[ol_i];
      ctx.sub_results.emplace(nbuf_iter->second, revenue);
      ++cnt;
    } else {
      res_iter->second += ol_amounts[ol_i];
      ++cnt;
    }
  }

  ctx.cnt = cnt;
}

} // namespace anonymous


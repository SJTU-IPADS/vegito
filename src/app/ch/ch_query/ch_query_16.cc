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
#include <unordered_set>

using namespace std;
using namespace nocc::oltp::ch;

#define TIMER 0
#define Q16_STABLE_SORT 1

#include "ch_query_timer.h"

namespace {

const char* CONST_I_DATA_STRING_TEMPLATE = "zz";
const char* CONST_SU_COMMENT_STRING_TEMPLATE = "bad";

struct Group {
  const inline_str_8<24>* i_name;
  const char* brand;
  AMOUNT i_price;
  
  Group(const inline_str_8<24> *i_name, const char *brand, AMOUNT i_price)
    : i_name(i_name), brand(brand), i_price(i_price) { }
};

struct ResultRow {
  const inline_str_8<24>* i_name;
  const char* brand;
  AMOUNT i_price;
  uint32_t supplier_cnt;

  ResultRow(const Group &g, uint32_t su)
    : ResultRow(g.i_name, g.brand, g.i_price, su) { }

  ResultRow(const inline_str_8<24>*n, const char *b, AMOUNT p, uint32_t su)
    : i_name(n), brand(b), i_price(p), supplier_cnt(su) { }

  static bool Compare(const ResultRow &a, const ResultRow &b) {
    return a.supplier_cnt >= b.supplier_cnt;
  }
  
  static void print_header() {
    printf("%-24s %-6s %-12s %-12s\n", 
           "i_name", "brand", "i_price", "supplier_cnt");
  }

  void print() const {
    printf("%-24s %-6.3s %-12.2f %-12u\n", 
           i_name->c_str(), brand, i_price, supplier_cnt);
  }
};


struct MidResult {
  Group group;
  uint64_t su_suppkey;

  MidResult(const inline_str_8<24> *i_name, const char *brand,
            AMOUNT i_price, uint64_t su_suppkey)
    : group(i_name, brand, i_price), su_suppkey(su_suppkey) { }
};

struct KeyHasher {
  size_t operator()(const Group &a) const {
    hash<void *> hash1, hash2;
    hash<AMOUNT> hash3;

    size_t h1 = hash1((void *) a.i_name),
           h2 = hash2((void *) a.brand),
           h3 = hash3(a.i_price);
    return (h1 ^ h2 ^ h3 ^ h3);
  }
};

struct KeyComparator {
  // less_to
  bool operator()(const Group &a, const Group &b) const {
    return !(*a.i_name == *b.i_name && strncmp(a.brand, b.brand, 3) == 0 
            && a.i_price == b.i_price);
  }
};

struct KeyEqualer {
  // equal_to
  bool operator()(const Group &a, const Group &b) const {
    return (*a.i_name == *b.i_name && strncmp(a.brand, b.brand, 3) == 0 
            && a.i_price == b.i_price);
  }
};

using SuppSet = unordered_set<uint64_t>;
#if 1
using ResultMap = unordered_map<Group, SuppSet, KeyHasher, KeyEqualer>;
using MidVec = vector<MidResult>;
#else
// XXX: error implementation
using ResultMap = map<ResultRow, SuppSet, KeyComparator>;
#endif

struct Ctx : public BaseCtx {
  // Query data
  const SuppSet &subquery_su_suppkeys;
  MidVec &sub_results;  // ResultRow to supplier keys mapping

  Ctx(BackupDB &d, uint64_t v, uint64_t b, uint64_t e, const SuppSet &s,
      MidVec &mv) 
    : BaseCtx(d, v, b, e),  
      subquery_su_suppkeys(s), sub_results(mv) { 
      mv.clear();  
    }
} __attribute__ ((aligned (CACHE_LINE_SZ)));

void query(void *arg);

thread_local vector<MidVec> mid_vec_;
thread_local vector<ResultRow> result_vec;
thread_local ResultMap result_map; // ResultRow to supplier keys mapping

} // namespace anonymous


namespace nocc {
namespace oltp {
namespace ch {

void ChQueryWorker::query16_init() {
  mid_vec_.resize(num_thread_);
  for (int i = 0; i < num_thread_; ++i) {
    mid_vec_[i].reserve(800000);
  }
  result_vec.reserve(100000);
  result_map.reserve(200000);
}

bool ChQueryWorker::query16(yield_func_t &yield) {
#if 0
  // uint64_t ver = get_read_ver_();
  // uint64_t cnt = 0;
  uint64_t cnt = 0;
  // SuppSet subquery_su_suppkeys;
  // ResultMap result_map; // ResultRow to supplier keys mapping
  // vector<ResultRow> result_vec;

  // Supplier
  Keys su_suppkeys = su_tbl->getKeyCol();

  for (uint64_t su_i = 0; su_i < su_suppkeys.size(); ++su_i) {
      cnt += su_suppkeys[su_i];
  }
  return (cnt > 100);

#else
  uint64_t ver = get_read_ver_();
  uint64_t cnt = 0;
  declare_timer(timer);

  timer_start(timer);

  // ResultRow data
  SuppSet subquery_su_suppkeys;
  result_map.clear(); // ResultRow to supplier keys mapping
  result_vec.clear();

  // Supplier
  Keys su_suppkeys = su_tbl->getKeyCol();

  for (uint64_t su_i = 0; su_i < su_suppkeys.size(); ++su_i) {
    const char *str = su_comments[su_i].data();
    if (strncmp(str, CONST_SU_COMMENT_STRING_TEMPLATE, 3) == 0) 
    {
      subquery_su_suppkeys.insert(su_suppkeys[su_i]);
    }
  }
  // assert(!subquery_su_suppkeys.empty());

  /**************** Parallel Part Begin ********************/
  vector<Ctx> ctxs;
  ctxs.reserve(num_thread_);

  // Calculate workload for each thread
  db_cnt_ = s_tbl_sz;
  
  vector<vector<uint64_t>> workloads = part_workloads(s_tbl_sz);
  for (int i = 0; i < num_thread_; ++i) {
    const auto &v = workloads[i];
    uint64_t off_begin = v[0],
             off_end   = v[1];
    ctxs.emplace_back(*db_, ver, off_begin, off_end, subquery_su_suppkeys,
                      mid_vec_[i]);
  }
  timer_end_print(timer, "Before parallel");
  timer_start(timer);
 
  parallel_process(ctxs, query);
  timer_end_print(timer, "Parallel");
  /**************** Parallel Part End ********************/

  timer_start(timer);
  // Collect data from all slave threads
  for (const Ctx &ctx : ctxs) {
    for (const MidResult &mr : ctx.sub_results) {
      result_map[mr.group].insert(mr.su_suppkey);
    }
    
    cnt += ctx.cnt;
  }

  for (const auto &pair : result_map) {
    // ResultRow row(pa.first);
    // row.supplier_cnt = pa.second.size();
    // result_vec.push_back(row);
    result_vec.emplace_back(pair.first, pair.second.size());
  }
#if Q16_STABLE_SORT
  stable_sort(result_vec.begin(), result_vec.end(), ResultRow::Compare);
#else
  sort(result_vec.begin(), result_vec.end(), ResultRow::Compare);
#endif
  timer_end_print(timer, "After parallel");
  
  print_timer(ctxs); 

  if (!q_verbose_) return true;

  printf("Result of query 16:\n");
  ResultRow::print_header();
  for (const ResultRow &row : result_vec)
    row.print();

  printf("Result: %lu tuples, selected %lu tuples\n", result_vec.size(), cnt);
  
  return true;
#endif
}

}  // namespace ch
}  // namespace oltp
}  // namespace nocc

namespace {

void query(void *arg) {
  Ctx &ctx = *(Ctx *) arg;
  uint64_t cnt = 0;

  Keys s_keys = s_tbl->getKeyCol();

  for (uint64_t s_i = ctx.off_begin; s_i < ctx.off_end; ++s_i) {
    int64_t stoc_key = int64_t(s_keys[s_i]);
    int32_t s_w_id = stockKeyToWare(stoc_key);
    int32_t s_i_id = stockKeyToItem(stoc_key);
    uint64_t su_suppkey = (s_w_id * s_i_id) % 10000 + 1;

    if (ctx.subquery_su_suppkeys.count(su_suppkey) != 0) continue;

    uint64_t i_i = ctx.db.getOffset(ITEM, s_i_id);
    assert(i_i != -1);

    const char *str = i_datas[i_i].data();
    if (str[0] != 'z' || str[1] != 'z')
    {
      const inline_str_8<24> *i_name = &i_names[i_i];
      AMOUNT i_price = i_prices[i_i];

      ctx.sub_results.emplace_back(i_name, str, i_price, su_suppkey);

#if 0
      ResultRow res(i_name, str, i_price, 0);

      auto res_itr = ctx.sub_results.find(res);
      if (res_itr != ctx.sub_results.end()) {
        res_itr->second.insert(su_suppkey);
      } else {
        SuppSet su_suppkeys;
        su_suppkeys.insert(su_suppkey);
        ctx.sub_results.emplace(res, su_suppkeys);
      }
#endif
    }
    cnt++; // access item record
  }
  ctx.cnt = cnt;
}

} // namespace anonymous


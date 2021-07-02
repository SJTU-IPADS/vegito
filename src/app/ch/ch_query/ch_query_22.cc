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

namespace {

struct ResultRow {
  char country;
  uint64_t numcust;
  AMOUNT totacctbal;

  ResultRow(char c, AMOUNT t)
    : country(c), numcust(1), totacctbal(t) { }

  static bool Compare(const ResultRow &a, const ResultRow &b){
    return a.country < b.country;
  }
};

using ResultMap = map<char, ResultRow>;
using SubMap = unordered_map<char, vector<AMOUNT>>;
using KeySet = unordered_set<uint64_t>;

struct Ctx : public BaseCtx {
  // step 1
  vector<bool> cust_map;

  // step 2
  const uint64_t c_begin;
  const uint64_t c_end;
  AMOUNT sub_sum_balance;
  uint64_t sub_sum_count;

  const KeySet &no_order_custs;

  SubMap sub_result;

  uint64_t cnt_2;

  Ctx(BackupDB &d, uint64_t v, uint64_t b, uint64_t e, 
      uint64_t cb, uint64_t ce, const KeySet &ks) 
    : BaseCtx(d, v, b, e),
      cust_map(c_tbl_sz, false), 
      c_begin(cb), c_end(ce), sub_sum_balance(0), sub_sum_count(0),
      no_order_custs(ks), cnt_2(0) { }
} __attribute__ ((aligned (CACHE_LINE_SZ)));


void query_step_1(void *arg);
void query_step_2(void *arg);

}

namespace nocc {
namespace oltp {
namespace ch {

bool ChQueryWorker::query22(yield_func_t &yield) {
  uint64_t ver = get_read_ver_();
  uint64_t cnt_1 = 0, cnt_2 = 0;
  KeySet no_order_custs;
  AMOUNT avg_c_balance;
  ResultMap result_map;
  
  /**************** Parallel Part 1 Begin ********************/
  vector<Ctx> ctxs;
  ctxs.reserve(num_thread_);
  
  // Calculate workload for each thread
  uint64_t o_tbl_sz = o_tbl->getOffsetHeader();
  db_cnt_ = o_tbl_sz;
  
  vector<vector<uint64_t>> workloads0 = part_workloads(o_tbl_sz);
  vector<vector<uint64_t>> workloads1 = part_workloads(c_tbl_sz);
  for (int i = 0; i < workloads0.size(); ++i) {
    const auto &v0 = workloads0[i];
    const auto &v1 = workloads1[i];
    uint64_t off_begin = v0[0],
             off_end   = v0[1],
             c_begin   = v1[0],
             c_end     = v1[1];
    ctxs.emplace_back(*db_, ver, off_begin, off_end, 
                      c_begin, c_end, no_order_custs);
  }
 
  parallel_process(ctxs, query_step_1);
 
  /**************** Parallel Part 1 End ********************/

  Keys c_keys = c_tbl->getKeyCol();
  for (uint64_t c_i = 0; c_i < c_tbl_sz; ++c_i) {
    bool exist = false;
    for (const Ctx &ctx : ctxs) {
      if (ctx.cust_map[c_i]) {
        exist = true;
        break;
      }
    }
    if (exist) continue;
    
    no_order_custs.insert(c_keys[c_i]);
  }
    
  for (const Ctx &ctx : ctxs) {
    cnt_1 += ctx.cnt;
  }
  
  /**************** Parallel Part 2 Begin ********************/
  parallel_process(ctxs, query_step_2);
  /**************** Parallel Part 2 End ********************/
  AMOUNT sum_balance = 0;
  uint64_t sum_count = 0;
  for (const Ctx &ctx : ctxs) {
    sum_balance += ctx.sub_sum_balance;
    sum_count   += ctx.sub_sum_count;
  }

  avg_c_balance  = (sum_count == 0)? 0.0 : sum_balance / sum_count;

  for (const Ctx &ctx : ctxs) {
    for (const auto &p : ctx.sub_result) {
      const auto &vec = p.second;
      AMOUNT amount = 0;
      uint64_t count = 0;
      for (AMOUNT a : vec) {
        if (a < avg_c_balance) continue;
        amount += a;
        count  += 1;
      }

      char country = p.first;

      auto iter = result_map.find(country);
      if (iter != result_map.end()) {
        iter->second.numcust    += 1;
        iter->second.totacctbal += amount;
      } else {
        ResultRow res(country, amount);
        result_map.emplace(country, res);
      }
    }

    cnt_2 += ctx.cnt_2;
  }

#if 0
  printf("no_order_custs: %lu keys, sum_b = %f, sum_c = %lu, avg = %f\n", 
         no_order_custs.size(), sum_balance, sum_count, avg_c_balance);
#endif

  if (!q_verbose_) return true;

  printf("Result of query_step_1 22:\n");
#if 0
  ResultRow::print_header();
  for (const ResultRow &row : result_vec)
    row.print();
#endif

  printf("Result: %lu tuples, selected first %lu tuples, second %lu tuples\n", 
         result_map.size(), cnt_1, cnt_2);

  return true;

}
}  // namespace ch
}  // namespace oltp
}  // namespace nocc

namespace {

void query_step_1(void *arg) {
  Ctx &ctx = *(Ctx *) arg;
  uint64_t cnt_1 = 0;

  Keys o_keys = o_tbl->getKeyCol();
  for (int o_i = ctx.off_begin; o_i < ctx.off_end; ++o_i) {
    uint64_t o_key = o_keys[o_i];
    int32_t o_w_id = orderKeyToWare(o_key);
    int32_t o_d_id = orderKeyToDistrict(o_key);
    int32_t o_c_id = o_c_ids[o_i];
    uint64_t c_key = (uint64_t) makeCustomerKey(o_w_id, o_d_id, o_c_id);
    uint64_t c_i = ctx.db.getOffset(CUST, c_key, ctx.ver);
    ctx.cust_map[c_i] = true;
    ++cnt_1;
  }

#if 0
  for (int i = 0; i < ctx.cust_map.size(); ++i)
    ctx.cust_map[i] = true;
#endif

  ctx.cnt = cnt_1;
}

void query_step_2(void *arg) {
  Ctx &ctx = *(Ctx *) arg;
  uint64_t cnt_2 = 0;
  
  unique_ptr<BackupStore::ColCursor> 
    c_balance_cur = c_tbl->getColCursor(C_BALANCE, ctx.ver);
  assert(c_balance_cur.get());
  c_balance_cur->seekOffset(ctx.c_begin, ctx.c_end);
  
  c_tbl->locateCol(C_BALANCE, sizeof(AMOUNT));

  while (c_balance_cur->nextRow()) {
    uint64_t c_i = c_balance_cur->cur();
    char c_phone = c_phones[c_i].data()[0];
    if (!(c_phone >= '1' && c_phone <= '7')) continue;

    auto c_balance_ptr = (const AMOUNT *) c_balance_cur->value();
    assert(c_balance_ptr);
    AMOUNT c_balance = *c_balance_ptr;
    if (c_balance <= -20.00) continue;

    // assert(c_balance == -10);

    ctx.sub_sum_balance += c_balance;
    ++ctx.sub_sum_count;

    if (ctx.no_order_custs.count(c_i)) continue;

    char c_state = c_states[c_i].data()[0];
    ctx.sub_result[c_state].push_back(c_balance);
    ++cnt_2;
  }

  ctx.cnt_2 = cnt_2;
}

} // namespace anonymous

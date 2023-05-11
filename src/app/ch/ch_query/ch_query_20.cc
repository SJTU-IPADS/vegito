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

struct StocInfo {
  int32_t s_quantity;
  int32_t sum_ol_quantity;

  StocInfo(int16_t q1, int32_t q2)
    : s_quantity(q1), sum_ol_quantity(q2) { }
};

using KeySet = unordered_set<uint64_t>;
using StocMap = unordered_map<uint64_t, StocInfo>;

struct Ctx : public BaseCtx {
  // step 1
  const KeySet &i_key_set;

  KeySet supp_key_set;

  Ctx(BackupDB &d, uint64_t v, uint64_t b, uint64_t e, const KeySet &i) 
    : BaseCtx(d, v, b, e), 
      i_key_set(i) { }
} __attribute__ ((aligned (CACHE_LINE_SZ)));


void query(void *arg);

}

namespace nocc {
namespace oltp {
namespace ch {

bool ChQueryWorker::query20(yield_func_t &yield) {
  uint64_t ver = get_read_ver_();
  uint64_t cnt = 0;

  KeySet i_key_set;
  KeySet su_key_set;

  Keys i_keys = i_tbl->getKeyCol();
  for (int i_i = 0; i_i < i_tbl_sz; ++i_i) {
    const inline_str_8<50> *i_data = &i_datas[i_i];
    if (i_data->size() < 2) continue;

    char c0 = i_data->c_str()[0];
    char c1 = i_data->c_str()[1];
    if (!(c0 == 'c' && c1 == 'o')) continue;
    
    i_key_set.insert(i_keys[i_i]);
  }

  // printf("i_key_set: %lu keys\n", i_key_set.size());
  
  /**************** Parallel Part Begin ********************/
  vector<Ctx> ctxs;
  ctxs.reserve(num_thread_);
  
  // Calculate workload for each thread
  uint64_t ol_tbl_sz = ol_tbl->getOffsetHeader();
  db_cnt_ = ol_tbl_sz;
  
  vector<vector<uint64_t>> workloads0 = part_workloads(ol_tbl_sz);
  vector<vector<uint64_t>> workloads1 = part_workloads(s_tbl_sz);
  for (int i = 0; i < workloads0.size(); ++i) {
    uint64_t off_begin = workloads0[i][0],
             off_end   = workloads0[i][1];
    ctxs.emplace_back(*db_, ver, off_begin, off_end, i_key_set);
  }
 
  parallel_process(ctxs, query);
  
  /**************** Parallel Part End ********************/
  
  // Collect data from all slave threads
  for (const Ctx &ctx : ctxs) {
    for (uint64_t k : ctx.supp_key_set)
      su_key_set.insert(k); 
  }
  
  // printf("su_key_set: %lu keys\n", su_key_set.size());
 
#if 0 
  for (uint64_t su_i = 0; su_i < su_keys.size(); ++su_i) {
    uint64_t nation_key = (uint64_t) su_nationkeys[su_i];
    uint64_t n_i = db_->getOffset(NATI, nation_key);

    if (strncmp(n_names[n_i].c_str(), N_NAME_STR, strlen(N_NAME_STR)) != 0)
      continue;

    su_suppkeys.insert(su_keys[su_i]);
  }
#endif

  if (!q_verbose_) return true;

#if 0
  printf("Result of query 20:\n");

  ResultRow::print_header();
  for (const ResultRow &row : result_table)
    row.print();

  printf("Result: %lu tuples, selected %d tuples\n", result_table.size(), cnt);
#endif

  return true;

}
}  // namespace ch
}  // namespace oltp
}  // namespace nocc

namespace {

void query(void *arg) {
  Ctx &ctx = *(Ctx *) arg;
  uint64_t cnt = 0;

  StocMap stoc_map;

#if 0
  // column store
  unique_ptr<BackupStore::ColCursor> 
    del_cur = ol_tbl->getColCursor(OL_DELIVERY_D, ctx.ver);
  assert(del_cur.get());
  del_cur->seekOffset(ctx.off_begin, ctx.off_end);
  
  ol_tbl->locateCol(OL_DELIVERY_D, sizeof(uint32_t));

  while (del_cur->nextRow()) {
    uint32_t ol_delivery_d = *(uint32_t *) del_cur->value();
    if (ol_delivery_d == 0) continue;
        
    uint64_t ol_i = del_cur->cur();
    int32_t ol_i_id = ol_i_ids[ol_i];
    if (!ctx.i_key_set.count(ol_i_id)) continue;

  }
#else
  for (uint64_t ol_i = ctx.off_begin; ol_i < ctx.off_end; ++ol_i) {
    int32_t ol_i_id = ol_i_ids[ol_i];
    if (!ctx.i_key_set.count(ol_i_id)) continue;

    uint32_t *ol_delivery_d = 
      (uint32_t *) ol_tbl->getByOffset(ol_i, OL_DELIVERY_D, ctx.ver);
    if (ol_delivery_d == nullptr) continue;

    assert(ol_delivery_d);
    if (*ol_delivery_d == 0) continue;

    int32_t ol_supply_w_id = ol_supply_w_ids[ol_i];
    uint64_t s_key = (uint64_t) makeStockKey(ol_supply_w_id, ol_i_id);
    int8_t ol_quantity = ol_quantities[ol_i];

    auto iter = stoc_map.find(s_key);
    if (iter == stoc_map.end()) {
#if 0
      uint64_t s_i = ctx.db.getOffset(STOC, s_key);
      int16_t *s_quantity = 
        (int16_t *) s_tbl->getByOffset(s_i, S_QUANTITY, ctx.ver);
#else
      int16_t *s_quantity = (int16_t *) ctx.db.Get(STOC, s_key, S_QUANTITY, ctx.ver);
#endif
      assert(s_quantity);
      StocInfo info(*s_quantity, ol_quantity);
      stoc_map.emplace(s_key, info);
    } else {
      iter->second.sum_ol_quantity += ol_quantity;
    }
  }

  for (const auto &p : stoc_map) {
    if (p.second.s_quantity * 2 <= p.second.sum_ol_quantity) continue;
      
    uint64_t s_w_id = stockKeyToWare(p.first);
    uint64_t s_i_id = stockKeyToItem(p.first);
    
    uint64_t su_key = (s_w_id * s_i_id) % 10000 + 1; 

    ctx.supp_key_set.insert(su_key); 
  
  }

#endif

  ctx.cnt = cnt;
}

} // namespace anonymous

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

#define Q3_STAT_JOIN (STAT_JOIN == 3)
#define TIMER Q3_STAT_JOIN
#include "ch_query_timer.h"

#define Q3_STABLE_SORT 0

namespace {

const char CONST_C_STATE_TEMPLATE = 'A';

struct ResultRow {
  int32_t ol_o_id;
  int32_t ol_w_id;
  int32_t ol_d_id;
  AMOUNT revenue;
  uint32_t o_entry_d;

  ResultRow(int32_t ol_o_id, int32_t ol_w_id, int32_t ol_d_id,
            AMOUNT revenue, uint32_t o_entry_d)
    : ol_o_id(ol_o_id), ol_w_id(ol_w_id), ol_d_id(ol_d_id),
      revenue(revenue), o_entry_d(o_entry_d) { }

  static bool Compare(const ResultRow &a, const ResultRow &b) {
    if (a.revenue > b.revenue)
      return true;
    else if (a.revenue < b.revenue)
      return false;
    else
      return a.o_entry_d < b.o_entry_d;
  }
  
  static void print_header() {
    printf("%-12s %-8s %-8s %-20s %-16s\n", 
           "ol_o_id", "ol_w_id", "ol_d_id", "revenue", "o_entry_d");
  }

  void print() const {
    printf("%-12d %-8d %-8d %-20f %-16u\n", 
           ol_o_id, ol_w_id, ol_d_id, revenue, o_entry_d);
  }

};

using Vec = vector<uint64_t>;

struct Ctx : public BaseCtx {
  const Vec &m_c_keys;

  // Query data
  vector<ResultRow> sub_result;

  Ctx(BackupDB &d, uint64_t v, uint64_t b, uint64_t e, const Vec &vec) 
    : BaseCtx(d, v, b, e), m_c_keys(vec) { }
} __attribute__ ((aligned (CACHE_LINE_SZ)));


void query(void *arg);

} // namespace anonymous

namespace nocc {
namespace oltp {
namespace ch {

bool ChQueryWorker::query03(yield_func_t &yield) {
  uint64_t ver = get_read_ver_();

  // Result data
  vector<ResultRow> result_table;
  
  // declare_timer(gtimer);
  // timer_start(gtimer);

  Vec m_c_keys;
  Keys c_keys = c_tbl->getKeyCol();

  for (int c_i = 0; c_i < c_tbl_sz; ++c_i) {
    if (c_states[c_i].data()[0] == CONST_C_STATE_TEMPLATE) {
      m_c_keys.push_back(c_keys[c_i]);
    } 
  }
  sort(m_c_keys.begin(), m_c_keys.end());

  /**************** Parallel Part Begin ********************/

  // Calculate workload for each thread
  vector<Ctx> ctxs;
  ctxs.reserve(num_thread_);
  // db_cnt_ = o_tbl->getOffsetHeader();
  uint64_t no_tbl_sz = no_tbl->getOffsetHeader();
  const uint64_t max_sz = 215000;
  if (no_tbl_sz > max_sz) no_tbl_sz = max_sz;
  db_cnt_ = no_tbl_sz;

  vector<vector<uint64_t>> workloads = part_workloads(no_tbl_sz);

  for (const auto &v : workloads) {
    uint64_t off_begin = v[0],
             off_end   = v[1];
    ctxs.emplace_back(*db_, ver, off_begin, off_end, m_c_keys);
  }
  
  // timer_start(timer);
  parallel_process(ctxs, query);
  // timer_end_print(timer, "Parallel");
  
  /**************** Parallel Part End ********************/

  // Collect data from all slave threads
  // timer_start(timer);
  int cnt = 0;
  for (Ctx &ctx : ctxs) {
    result_table.insert(result_table.end(), 
                        ctx.sub_result.begin(), 
                        ctx.sub_result.end());
    cnt += ctx.cnt;
  }

#if Q3_STABLE_SORT
  stable_sort(result_table.begin(), result_table.end(), ResultRow::Compare);
#else
  sort(result_table.begin(), result_table.end(), ResultRow::Compare);
#endif
  
  // timer_end_print(timer, "After parallel");
  // timer_end_print(gtimer, "Total");
  
  print_timer(ctxs); 
  // print_each_timer(ctxs); 

  if (!q_verbose_) return true;

  printf("Result of query 3:\n");

  ResultRow::print_header();
  for (const ResultRow &row : result_table)
    row.print();

  printf("Result: %lu tuples, selected %d tuples\n", result_table.size(), cnt);

  return true;
}

}  // namespace ch
}  // namespace oltp
}  // namespace nocc

namespace {
void query(void *arg) {
  declare_timer(timer);
  uint64_t cnt = 0;

  // timer_start(gtimer);
  Ctx &ctx = *(Ctx *) arg;
  
  uint64_t no_begin = ctx.off_begin;
  uint64_t no_end   = ctx.off_end;

  BackupDB &db = ctx.db;
  Keys no_keys_vec = no_tbl->getKeyCol();
  const uint64_t *no_keys = &no_keys_vec[0];
  Keys ol_keys_vec = ol_tbl->getKeyCol();
  uint64_t ol_keys_sz = ol_keys_vec.size();
  const uint64_t *ol_keys = &ol_keys_vec[0];

  asm volatile("" ::: "memory");
  // timer_end(gtimer, ctx, 0);

  // timer_start(gtimer);
  for (uint64_t no_i = no_begin; no_i < no_end; ++no_i) {
    // timer_start(timer);
   
    uint64_t no_key = no_keys[no_i];
    // timer_end(timer, ctx, 1);

    // timer_start(timer);
    assert(no_key != uint64_t(-1));

    uint64_t o_i = db.getOffset(ORDE, no_key);

    // NEWO graph
    // uint64_t *no_edge = db.getEdge(NEWO, no_i);
    // uint64_t o_i = no_edge[1];  // for graph
    assert(o_i != -1);

    if (o_entry_ds[o_i] <= 0) continue;

    int32_t o_c_id = o_c_ids[o_i];
    int32_t no_w_id = orderKeyToWare(no_key);
    int32_t no_d_id = orderKeyToDistrict(no_key);
    int32_t no_o_id = orderKeyToOrder(no_key);
    uint64_t c_key = makeCustomerKey(no_w_id, no_d_id, o_c_id);

    int64_t off = key_binary_search(ctx.m_c_keys, c_key);
    bool find = (off != -1);
    if (!find) continue;

    {
      uint64_t start_ol_key = 
        (uint64_t) makeOrderLineKey(no_w_id, no_d_id, no_o_id, 1);
      uint64_t end_ol_key = (uint64_t) 
        makeOrderLineKey(no_w_id, no_d_id, no_o_id, o_ol_cnts[o_i]);
      assert(o_ol_cnts[o_i] > 0);

      AMOUNT revenue = 0;
#if OL_GRAPH == 0
      const int M = 1;
#else
      const int M = 2;
#endif
      timer_start(timer);
      if (M == 0) {
        // B+Tree index
        auto ol_sec_idx = db.getSecIndex(ORLI)->getIterator(ctx.ver);
        assert(ol_sec_idx.get());
        ol_sec_idx->Seek(start_ol_key);
        do {
          if (ol_sec_idx->Key() >  end_ol_key) break;
          uint64_t ol_i = ol_sec_idx->CurOffset();
          revenue += ol_amounts[ol_i];
          ++cnt; 
        } while (ol_sec_idx->Next());
      } else if (M == 1) {
        // fake hash index (know the range)
        for (uint64_t ol_key = start_ol_key; ol_key <= end_ol_key; ol_key++) {
          uint64_t ol_i = db.getOffset(ORLI, ol_key);
#if !Q3_STAT_JOIN
          revenue += ol_amounts[ol_i];
          ++cnt;
#endif
        }
      } else if (M == 2) {
        // graph
        uint64_t *o_edge = db.getEdge(ORDE, o_i);
        // for (int i = 1; i <= o_edge[0]; ++i) 
        for (int i = o_edge[0] - 1; i >= 0; --i)  // for cache locality 
        {
          uint64_t ol_i = o_edge[i];
#if !Q3_STAT_JOIN
          revenue += ol_amounts[ol_i];
          ++cnt;
#endif
        }
      
      } else {
        assert(false);
      }
      timer_end(timer, ctx, 0);

      ctx.sub_result.emplace_back(no_o_id, no_w_id, no_d_id, 
                                  revenue, o_entry_ds[o_i]);
    }
    // timer_end(timer, ctx, 2);
  }
  // timer_end(gtimer, ctx, 1);

  ctx.cnt = cnt;
}

} // namespace anonymous


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

#define Q12_STAT_JOIN (STAT_JOIN == 12)
#define TIMER Q12_STAT_JOIN
#include "ch_query_timer.h"

namespace {

const int MAX_ORLI = 15;

struct Ctx : public BaseCtx {
  // Query data
  vector<uint64_t> high_line_counts;
  vector<uint64_t> low_line_counts;

  Ctx(BackupDB &d, uint64_t v, uint64_t b, uint64_t e) 
    : BaseCtx(d, v, b, e), 
      high_line_counts(MAX_ORLI, 0), low_line_counts(MAX_ORLI, 0) { }
} __attribute__ ((aligned (CACHE_LINE_SZ)));

void query(void *arg);

} // namespace anonymous

namespace nocc {
namespace oltp {
namespace ch {

bool ChQueryWorker::query12(yield_func_t &yield) {
  uint64_t ver = get_read_ver_();
  uint64_t cnt = 0;

  // Result data
  vector<uint64_t> high_line_counts(MAX_ORLI, 0);
  vector<uint64_t> low_line_counts(MAX_ORLI, 0);

  /**************** Parallel Part Begin ********************/
  vector<Ctx> ctxs;
  ctxs.reserve(num_thread_);
  
  // Calculate workload for each thread
  uint64_t o_tbl_sz = o_tbl->getOffsetHeader();
  db_cnt_ = o_tbl_sz;
  
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
    for (int j = 0; j < MAX_ORLI; j ++) {
      high_line_counts[j] += ctx.high_line_counts[j];
      low_line_counts[j] += ctx.low_line_counts[j];
    }

    cnt += ctx.cnt;
    walk_cnt_ += ctx.walk_cnt;
  }

  print_timer(ctxs);
  if (!q_verbose_) return true;

  printf("Result of query 12:\n");
  printf("%-8s %-16s %-16s\n", "o_ol_cnt", "high_line_count", "low_line_count");
  
  for (int i = 4; i < MAX_ORLI; ++i) {
    printf("%-8d %-16lu %-16lu\n", i + 1, high_line_counts[i], low_line_counts[i]);
  }
  
  printf("Result: %lu tuples, selected %lu tuples\n", 11lu, cnt);

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

  // Order
  Keys o_keys = o_tbl->getKeyCol();

  unique_ptr<BackupStore::ColCursor>
    o_carrier_id_cur = o_tbl->getColCursor(O_CARRIER_ID, ctx.ver);
  assert(o_carrier_id_cur.get());
  o_carrier_id_cur->seekOffset(ctx.off_begin, ctx.off_end);
  o_tbl->locateCol(O_CARRIER_ID, sizeof(int32_t));

  bool end = false;
  for ( ; o_carrier_id_cur->nextRow(&ctx.walk_cnt) && !end; ++cnt) {
    uint64_t o_i = o_carrier_id_cur->cur();
    int64_t o_key = int64_t(o_keys[o_i]);
    int32_t o_w_id = orderKeyToWare(o_key);
    int32_t o_d_id = orderKeyToDistrict(o_key);
    int32_t o_o_id = orderKeyToOrder(o_key);

    uint32_t o_entry_d = o_entry_ds[o_i];
    int32_t *o_carrier_id = (int32_t *) o_carrier_id_cur->value();
    assert(o_carrier_id);

    timer_start(timer);
#if OL_GRAPH == 0
    int8_t o_ol_cnt = o_ol_cnts[o_i];
    uint64_t start_ol_key = makeOrderLineKey(o_w_id, o_d_id, o_o_id, 1);
    uint64_t end_ol_key = makeOrderLineKey(o_w_id, o_d_id, o_o_id, o_ol_cnt);
    assert(o_ol_cnt > 0);

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

#if Q12_STAT_JOIN
      continue;
#endif
      uint32_t *ol_delivery_d = 
        (uint32_t *) ol_tbl->getByOffset(ol_i, OL_DELIVERY_D, ctx.ver, &ctx.walk_cnt);
      if (ol_delivery_d == nullptr) {
        end = true;
        break;
      }

      if (*ol_delivery_d >= o_entry_d && *ol_delivery_d < (uint32_t) -1) {
        if (*o_carrier_id == 1 || *o_carrier_id == 2) {
          ctx.high_line_counts[o_ol_cnt - 1]++;
        } else {
          ctx.low_line_counts[o_ol_cnt - 1]++;
        }
        
        ++cnt;
      }
    }
    timer_end(timer, ctx, 0);
  }

  ctx.cnt = cnt;
}

} // namespace anonymous


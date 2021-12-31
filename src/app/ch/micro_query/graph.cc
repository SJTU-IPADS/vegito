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

namespace {

struct Ctx : public BaseCtx {
  // Query data
  uint64_t mid;

  Ctx(BackupDB &d, uint64_t v, uint64_t b, uint64_t e) 
    : BaseCtx(d, v, b, e), mid(0) { }
} __attribute__ ((aligned (CACHE_LINE_SZ)));


void query(void *arg);

} // namespace anonymous

namespace nocc {
namespace oltp {
namespace ch {

bool ChQueryWorker::micro_graph(yield_func_t &yield) {
  uint64_t ver = get_read_ver_();
  
  // declare_timer(timer);

  /**************** Parallel Part Begin ********************/

  // Calculate workload for each thread
  vector<Ctx> ctxs;
  ctxs.reserve(num_thread_);
  // db_cnt_ = o_tbl->getOffsetHeader();
  uint64_t o_tbl_sz = o_tbl->getOffsetHeader();
  // const uint64_t max_sz = 215000;
  // if (no_tbl_sz > max_sz) o_tbl_sz = max_sz;
  db_cnt_ = o_tbl_sz;

  vector<vector<uint64_t>> workloads = part_workloads(o_tbl_sz);

  for (const auto &v : workloads) {
    uint64_t off_begin = v[0],
             off_end   = v[1];
    ctxs.emplace_back(*db_, ver, off_begin, off_end);
  }
  
  // timer_start(timer);
  parallel_process(ctxs, query);
  // timer_end_print(timer, "Parallel");
  
  /**************** Parallel Part End ********************/

  // Collect data from all slave threads
  int cnt = 0;
  uint64_t res = 0;
  for (Ctx &ctx : ctxs) {
    res += ctx.mid;
    cnt += ctx.cnt;
  }

  // print_timer(ctxs); 
  printf("result: cnt %d, revenue %lu\n", cnt, res);

  return true;
}

}  // namespace ch
}  // namespace oltp
}  // namespace nocc

namespace {
void query(void *arg) {
  uint64_t cnt = 0;

  // timer_start(gtimer);
  Ctx &ctx = *(Ctx *) arg;
  
  uint64_t o_begin = ctx.off_begin;
  uint64_t o_end   = ctx.off_end;

  BackupDB &db = ctx.db;
  Keys ol_keys_vec = ol_tbl->getKeyCol();
  uint64_t ol_keys_sz = ol_keys_vec.size();
  const uint64_t *ol_keys = &ol_keys_vec[0];
  Keys o_keys_vec = o_tbl->getKeyCol();
  const uint64_t *o_keys = &o_keys_vec[0];

  asm volatile("" ::: "memory");

  AMOUNT revenue = 0;
  for (uint64_t o_i = o_begin; o_i < o_end; ++o_i) {
   
    uint64_t o_key = o_keys[o_i];
    assert(o_key != uint64_t(-1));

    int32_t o_w_id = orderKeyToWare(o_key);
    int32_t o_d_id = orderKeyToDistrict(o_key);
    int32_t o_o_id = orderKeyToOrder(o_key);

    uint64_t start_ol_key = 
      (uint64_t) makeOrderLineKey(o_w_id, o_d_id, o_o_id, 1);
    uint64_t end_ol_key = (uint64_t) 
      makeOrderLineKey(o_w_id, o_d_id, o_o_id, o_ol_cnts[o_i]);
    assert(o_ol_cnts[o_i] > 0);

    // 0: B+Tree
    // 1: Hash
    // 2: Graph
    const int M = 2;

    switch (M) {
    case 0:
    {
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
      break;
    } 
    case 1:
    {
      // fake hash index (know the range)
      for (uint64_t ol_key = start_ol_key; ol_key <= end_ol_key; ol_key++) {
        uint64_t ol_i = db.getOffset(ORLI, ol_key);
        revenue += ol_amounts[ol_i];
        ++cnt;
      }
      break; 
    }
    case 2:
    {
      // graph
      uint64_t *o_edge = db.getEdge(ORDE, o_i);
#if 1  // array
      for (int i = 1; i <= o_edge[0]; ++i) {
        uint64_t ol_i = o_edge[i];
        revenue += ol_amounts[ol_i];
        ++cnt;
      }
#else  // linked list
      while (o_edge[1] != 0) {
        uint64_t ol_i = o_edge[0];
        revenue += ol_amounts[ol_i];
        ++cnt;
        o_edge = (uint64_t *) o_edge[1];
      }
#endif
      break;
    }
    default:
      assert(false);
    
    
    }

  }

  ctx.mid = (uint64_t) revenue;
  ctx.cnt = cnt;

}

} // namespace anonymous


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

#include <thread>

#define TIMER 0

using namespace std;
using namespace nocc::oltp::ch;

namespace {

  struct Ctx : public BaseCtx {
  // Query data
  int64_t ol_quantity;

  Ctx(BackupDB &d, uint64_t v, uint64_t b, uint64_t e) 
    : BaseCtx(d, v, b, e), ol_quantity(0) { }
} __attribute__ ((aligned (CACHE_LINE_SZ))) ;

void query(void *arg) {
  Ctx &ctx = *(Ctx *) arg;
 
  register uint64_t cnt = 0;
  register uint64_t ol_quantity = 0;

  // uint64_t ver = ctx.ver;
  uint64_t ver = 0;

  BackupStore *store = ctx.db.getStore(ORLI);
  Keys ol_keys = store->getKeyCol(); 
  auto ol_sec_idx = ctx.db.getSecIndex(ORLI)->getIterator(ver);
  
#if 1
  for (int i = ctx.off_begin; i < ctx.off_end; ++i) {
    uint64_t ol_key = ol_keys[i];
    ol_sec_idx->Seek(ol_key);
    uint64_t ol_i = ol_sec_idx->CurOffset();
    ol_quantity += ol_i;
    ++cnt ;
  }
#endif
  ctx.ol_quantity = ol_quantity;
  ctx.cnt = cnt;
}

}

namespace nocc {
namespace oltp {
namespace ch {

// Tranverse stock table and sum the unchanged field 'ol_quantity'
bool ChQueryWorker::micro_index(yield_func_t &yield) {
  uint64_t ver = get_read_ver_();
  uint64_t cnt = 0;
  // Result data
  int64_t sum_ol_quantity = 0;

  /**************** Parallel Part Begin ********************/

  // Thread information
  vector<Ctx> ctxs;
  ctxs.reserve(num_thread_);

  // Calculate workload for each thread
  uint64_t ol_tbl_sz = db_->getStore(ORLI)->getOffsetHeader() * 0.8;

  
  vector<vector<uint64_t>> workloads = part_workloads(ol_tbl_sz);

  for (const auto &v : workloads) {
    uint64_t off_begin = v[0],
             off_end   = v[1];
    ctxs.emplace_back(*db_, ver, off_begin, off_end);
  }
 
  uint64_t begin = rdtsc();
  parallel_process(ctxs, query);
  
  // Collect data from all slave threads
  for (const Ctx &ctx : ctxs) {
    sum_ol_quantity += ctx.ol_quantity;
    cnt += ctx.cnt;
  }

  uint64_t end = rdtsc();
  float seconds = float(end - begin) / util::Breakdown_Timer::get_one_second_cycle();
  printf("sum_ol_quantity = %ld, cnt = %lu\n", sum_ol_quantity, cnt);
  printf("  %f secs\n", seconds);

  /**************** Parallel Part End ********************/

#if 0
#if SHOW_QUERY_RESULT
  printf("sum_ol_quantity\n");
  printf("%-lu\n", sum_ol_quantity);
#endif

  float seconds = float(end - begin) / SECOND_CYCLE_;
  printf("Query configuration: use %s and %s\n",
          stock_types[BACKUP_STORE_TYPE].c_str(),
          query_methods[QUERY_METHOD].c_str());
  printf("Total record cnt: %lu, result size: %d, %f secs\n",
          cnt, 1, seconds);
#endif
  return true;
}

}  // namespace ch
}  // namesapce oltp
}  // namespace nocc

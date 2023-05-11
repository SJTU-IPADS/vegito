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

#include "ldbc_graph_loader.h"
#include "ldbc_worker.h"

using namespace std;

namespace nocc {
namespace oltp {
namespace ldbc {

LDBCWorker::LDBCWorker(uint64_t worker_id, uint64_t seed, MemDB *store)
    : BenchWorker(worker_id, seed, store), elabel_(VTYPE_NUM) { 
  memset(edge_idx_, 0, sizeof(edge_idx_));

  for (int i = 0; i < ETYPE_NUM; ++i) {
    const vector<string> &edge_vec = TpEdgeLoader::inserted_edges[i];
    uint64_t sz = edge_vec.size();
    uint64_t total_worker = config.getNumTxnThreads();
    uint64_t sz_per_worker = sz / total_worker;
    edge_idx_begin[i] = worker_id * sz_per_worker;
    edge_idx_num[i] = sz_per_worker;  // TODO: still has some rest
  }
}

txn_result_t LDBCWorker::txn_insert_edge(yield_func_t &yield) {
  // int elabel = FORUM_HASMEMBER;
  int elabel = elabel_++;
  if (elabel_ == ETYPE_NUM) {
    elabel_ = VTYPE_NUM;  // from the first etype
  }
  int &idx = edge_idx_[elabel];

  const vector<string> &edge_vec = TpEdgeLoader::inserted_edges[elabel];
  // if (idx >= edge_vec.size())
  //   return txn_result_t(true, 0);
  if (idx >= edge_idx_num[elabel])
    idx %= edge_idx_num[elabel];

  const string &str = edge_vec[edge_idx_begin[elabel] + idx];
  idx++; 
  tx_->begin(elabel, db_logger_);
#if FRESHNESS == 1
  if (elabel == FORUM_HASMEMBER) {
    assert(str.size() == 2*sizeof(ID) + sizeof(uint64_t));
    uint64_t *ptr = (uint64_t *) &str[2*sizeof(ID)];
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    uint64_t now_us = (uint64_t(tv.tv_sec) * 1000000 + tv.tv_usec);
    *ptr = now_us;
    assert(*ptr);
  }
#endif
  tx_->stream_insert(elabel, 233, str.data(), str.size());
  bool res = tx_->end(yield);
  return txn_result_t(res, 0);
}

workload_desc_vec_t LDBCWorker::get_workload() const {
  workload_desc_vec_t w;
  w.emplace_back("InsertEdge", 1.0, TxnInsertEdge);

#if 0
  int pct = chConfig.getTxnWorkloadMix(ChConfig::NEW_ORDER);
  if (pct)
    w.emplace_back("NewOrder", double(pct) / 100.0, TxnNewOrder);
#endif

  return w;
}

}  // namespace ldbc
}  // namespace oltp
}  // namespace nocc

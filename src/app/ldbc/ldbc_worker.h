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

#pragma once

#include "ldbc_schema.h"

#include "framework/bench_worker.h"

namespace nocc {
namespace oltp {
namespace ldbc {

#if 0
struct ChReq {
  int32_t w_id;
};

struct ChResp {
  int32_t w_id;
};
#endif

/* Tx's implementation */
class LDBCWorker : public BenchWorker {

 public:
  LDBCWorker(uint64_t worker_id, uint64_t seed,
             MemDB *db);

 protected:
  virtual void process_param(const void *padding) override {
    // const ChReq *req = (const ChReq *) padding;
    // home_wid_ = req->w_id;
  }

  virtual void prepare_param_self(int cor_id) override {
    // home_wid_ = PickWarehouseId(rand_gen_[cor_id], start_w_, end_w_);
  }

  virtual void prepare_response(void *padding) override {
    // static_assert(sizeof(ChResp) <= TXN_PADDING_SZ);
    // ChResp *resp = (ChResp *) padding;
    // resp->w_id = home_wid_;
  }

  virtual workload_desc_vec_t get_workload() const override;
  
  // virtual void check_consistency() override;

 private:
  /* Wrapper for implementation of transaction */
  static txn_result_t TxnInsertEdge(BenchWorker *w,yield_func_t &yield) {
    return dynamic_cast<LDBCWorker *>(w)->txn_insert_edge(yield);
  }
 
  txn_result_t txn_insert_edge(yield_func_t &yield);

  int elabel_;

  int edge_idx_[ETYPE_NUM];
  int edge_idx_begin[ETYPE_NUM];
  int edge_idx_num[ETYPE_NUM];
};

#if 0
struct ChDeliveryParam {
  uint32_t w_id;
  uint32_t o_carrier_id;
  
  ChDeliveryParam(fast_random &r, uint32_t w_id) 
    : w_id(w_id), o_carrier_id(RandomNumber(r, 1, NumDistrictsPerWarehouse()))
  { }
};
#endif

}  // namespace ldbc
}  // namespace oltp
}  // namespace nocc

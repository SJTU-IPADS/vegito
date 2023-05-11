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

#ifndef NOCC_OLTP_LDBC_QUERY_H_
#define NOCC_OLTP_LDBC_QUERY_H_

#include <unordered_set>

#include "all.h"
#include "memstore/memdb.h"

#include "framework/bench_query.h"
#include "framework/framework.h"
#include "core/graph.hpp"
#include "ldbc_schema.h"
#include "ldbc_config.h"

#define STAT_JOIN 0

namespace nocc {
namespace oltp {

namespace ldbc {
#define DECLARE_QUERY(x) \
  protected: \
    static bool Q##x(QueryWorker *w, yield_func_t &yield) { \
      return static_cast<LDBCQueryWorker *>(w)->query##x(yield); \
    } \
  private: \
    bool query##x(yield_func_t &yield); \
    void query##x##_init(); \
  protected:

class LDBCQueryWorker : public QueryWorker {
 public:
  // response for [start_w, end_w)
  LDBCQueryWorker(uint32_t worker_id, uint32_t num_thread, uint32_t seed, 
                graph::GraphStore *graph, graph::RGMapping *rg_map);
  virtual QryDescVec get_workload() const;

 protected:
  // LDBC benchmark
  DECLARE_QUERY(IS5);
  DECLARE_QUERY(Freshness);

 private:
  graph::GraphStore *graph_;
  graph::RGMapping *rg_map_;
};


// extern const BackupStore *person_tbl;
// extern uint64_t person_tbl_sz;
// extern const String *person_firsts;
// extern const String *person_lasts;
// extern const ID *person_ids;

// extern const BackupStore *comment_tbl;
// extern const uint64_t *comment_tbl_sz;

// extern const BackupStore *post_tbl;
// extern const uint64_t *post_tbl_sz;


}  // namespace ldbc
}  // namespace oltp
}  // namespace nocc

#endif  // NOCC_OLTP_LDBC_QUERY_H_


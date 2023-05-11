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

#include "app/ldbc/ldbc_query.h"

using namespace std;
using namespace livegraph;

namespace nocc {
namespace oltp {
namespace ldbc {

// const BackupStore *person_tbl;
// uint64_t person_tbl_sz;
// const String *person_firsts;
// const String *person_lasts;
// const ID *person_ids;

// const BackupStore *comment_tbl;
// const uint64_t *comment_tbl_sz;

// const BackupStore *post_tbl;
// const uint64_t *post_tbl_sz;

LDBCQueryWorker::LDBCQueryWorker(uint32_t worker_id, uint32_t num_thread, 
                             uint32_t seed, graph::GraphStore *graph, graph::RGMapping *rg_map)
    : graph_(graph), rg_map_(rg_map),
      QueryWorker(worker_id, num_thread, seed) {
  if (!graph) assert(false);
}

QryDescVec LDBCQueryWorker::get_workload() const {
  QryDescVec w;
  const vector<std::string>& queries_workload = ldbcConfig.getQueryWorkload();
  std::map<std::string, QryDesc> queries = 
    {
      {"", {"", nullptr}},
      // LDBC queries 
      {"IS5", {"IS5", QIS5}},
      {"Freshness", {"Freshness", QFreshness}},
    };

  for (auto& query : queries_workload) {
    if (queries[query].fn != nullptr)
      w.push_back(queries[query]);
  }

  return w;
}

}  // namespace ldbc
}  // namespace oltp
}  // namespace nocc


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

#include <unordered_set>

#include "all.h"
#include "framework/bench_analytics.h"
#include "framework/framework.h"
#include "core/livegraph.hpp"
#include "ldbc_schema.h"
#include "ldbc_config.h"

namespace nocc {
namespace oltp {

namespace ldbc {

#define DECLARE_ANALYTICS(x) \
  protected: \
    static bool A_##x(AnalyticsWorker *w, const std::string& params) { \
      if(config.isUseGrapeEngine()) \
        return static_cast<LDBCAnalyticsWorker *>(w)->grape_##x(); \
      else \
        return static_cast<LDBCAnalyticsWorker *>(w)->process_##x(); \
    } \
  private: \
    bool process_##x(); \
    bool grape_##x(); \
    void process_##x##_init(); \
  protected:


class LDBCAnalyticsWorker : public AnalyticsWorker {
 public:
  // response for [start_w, end_w)
  LDBCAnalyticsWorker(uint32_t worker_id, 
                    uint32_t num_thread, 
                    uint32_t seed,
                    graph::GraphStore* graph_store,
                    graph::RGMapping* rg_map);

  virtual std::vector<AnaDesc> get_workload() const;

 protected:
  void thread_local_init() override;

  // graph computation benchmark
  DECLARE_ANALYTICS(pagerank);
  DECLARE_ANALYTICS(pagerank_breakdown);
  DECLARE_ANALYTICS(cc);
  DECLARE_ANALYTICS(bfs);
  DECLARE_ANALYTICS(sssp);

  // graph neural network benchmark
  DECLARE_ANALYTICS(gcn);
  DECLARE_ANALYTICS(graphsage);
  DECLARE_ANALYTICS(sgc);
  DECLARE_ANALYTICS(sgc_breakdown);
  DECLARE_ANALYTICS(sgc_flexprop);

  // graph query benchmark
#ifdef WITH_GAIA
  DECLARE_ANALYTICS(gaia);
#endif
  DECLARE_ANALYTICS(bi2);
  DECLARE_ANALYTICS(bi3);
  DECLARE_ANALYTICS(bi2_breakdown);
  DECLARE_ANALYTICS(bi3_breakdown);

  DECLARE_ANALYTICS(scan);
  DECLARE_ANALYTICS(flexcol);

  float scan(AnalyticsCtx* ctx) __attribute__((optimize("O0")));
};


}  // namespace ldbc
}  // namespace oltp
}  // namespace nocc


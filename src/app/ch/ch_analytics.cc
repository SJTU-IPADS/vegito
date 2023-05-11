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

#include "app/ch/ch_analytics.h"

using namespace std;

namespace nocc {
namespace oltp {
namespace ch {

ChAnalyticsWorker::ChAnalyticsWorker(uint32_t worker_id, 
                                     uint32_t num_thread, 
                                     uint32_t seed,
                                     graph::GraphStore* graph_store,
                                     graph::RGMapping* rg_map)
    : AnalyticsWorker(worker_id, num_thread, seed, graph_store, rg_map) {

}

void ChAnalyticsWorker::thread_local_init() {
  process_pagerank_init();
  process_gcn_init();
#ifdef WITH_GAIA
  process_gaia_init();
#endif
  process_bi3_init();
  process_bi2_init();
}

std::vector<AnaDesc> ChAnalyticsWorker::get_workload() const {
  std::vector<AnaDesc> w;
  const vector<std::string>& algo_names = chConfig.getAnalyticsWorkload();
  const vector<std::string>& algo_params = chConfig.getAnalyticsParams();
  std::map<std::string, AnaDesc> algos =
    { {"", {"", nullptr}},

      // graph AP
      {"pagerank", {"pagerank", A_pagerank}},
      {"cc", {"cc", A_cc}},
      {"bfs", {"bfs", A_bfs}},
      {"sssp", {"sssp", A_sssp}},
      {"gcn", {"gcn", A_gcn}},
      {"graphsage", {"graphsage", A_graphsage}},
      {"sgc", {"sgc", A_sgc}},
#ifdef WITH_GAIA
      {"gaia", {"gaia", A_gaia}},
#endif
      {"bi2", {"bi2", A_bi2}},
      {"bi3", {"bi3", A_bi3}},
      {"scan", {"scan", A_scan}}
    };

  assert(algo_names.size() == algo_params.size());
  for (int i = 0; i < algo_names.size(); i++) {
    auto& name = algo_names[i];
    if (algos[name].fn != nullptr)
      w.push_back(algos[name]);
      w.back().params = algo_params[i];
  }

  return w;
}

}  // namespace ch
}  // namespace oltp
}  // namespace nocc


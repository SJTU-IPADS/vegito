/** Copyright 2020 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#ifndef EXAMPLES_ANALYTICAL_APPS_RUN_APP_H_
#define EXAMPLES_ANALYTICAL_APPS_RUN_APP_H_

#ifdef WITH_GRAPE

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <grape/fragment/immutable_edgecut_fragment.h>
#include <grape/fragment/livegraph_wrapper.h>
#include <grape/fragment/seggraph_wrapper.h>
#include <grape/fragment/loader.h>
#include <grape/grape.h>
#include <grape/util.h>

#include <algorithm>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "pagerank/pagerank_livegraph.h"
#include "sssp/sssp_livegraph.h"
#include "gcn/gcn_livegraph.h"
#include "timer.h"

#ifndef __AFFINITY__
#define __AFFINITY__ false
#endif

namespace grape {

class GrapeEngine {
public:
  static void Init() {
    InitMPIComm();
    CommSpec comm_spec;
    comm_spec.Init(MPI_COMM_WORLD);
    if (comm_spec.worker_id() == kCoordinatorRank) {
      VLOG(1) << "Workers of libgrape-lite initialized.";
    }
  }

  static void Finalize() {
    FinalizeMPIComm();
    VLOG(1) << "Workers finalized.";
  }
};

template <typename FRAG_T, typename APP_T, typename... Args>
void RunApp(const CommSpec& comm_spec,
            const ParallelEngineSpec& mt_spec,
            nocc::graph::GraphStore* graph,
            nocc::graph::RGMapping* rg_map,
            const std::vector<int>& edge_labels,
            uint64_t read_epoch_id,
            const std::string& out_prefix,
            Args... args) {
  timer_next("load graph");
  nocc::util::Breakdown_Timer timer;
  timer.start();

  std::shared_ptr<FRAG_T> fragment = std::shared_ptr<FRAG_T>(new FRAG_T(graph, rg_map, edge_labels, read_epoch_id));
  auto app = std::make_shared<APP_T>();
  timer_next("load application");
  auto worker = APP_T::CreateWorker(app, fragment);
  worker->Init(comm_spec, mt_spec);
  timer_next("run algorithm");
  worker->Query(std::forward<Args>(args)...);
  timer_next("print output");

  std::cout << "Finish app, "
            << " time:" << timer.get_diff_ms()/1000 << " seconds" << std::endl;

  std::ofstream ostream;
  std::string output_path = grape::GetResultFilename(out_prefix, fragment->fid());
  ostream.open(output_path);
  worker->Output(ostream);
  ostream.close();
  worker->Finalize();
  std::cout << "Finish Finalize" << std::endl;
  timer_end();
}

// template <typename GRAPH_T, typename OID_T, typename VID_T, typename VDATA_T, typename EDATA_T>
// void Run(std::shared_ptr<GRAPH_T> graph,
//          nocc::oltp::ldbc::LDBCConfig& config) {
//   CommSpec comm_spec;
//   comm_spec.Init(MPI_COMM_WORLD);

//   bool is_coordinator = comm_spec.worker_id() == kCoordinatorRank;

//   auto mt_spec = MultiProcessSpec(comm_spec, __AFFINITY__);
//   mt_spec.thread_num = config.getNumAnalyticsThreads();
//   if (__AFFINITY__) {
//     if (mt_spec.cpu_list.size() >= mt_spec.thread_num) {
//       mt_spec.cpu_list.resize(mt_spec.thread_num);
//     } else {
//       uint32_t num_to_append = mt_spec.thread_num - mt_spec.cpu_list.size();
//       for (uint32_t i = 0; i < num_to_append; ++i) {
//         mt_spec.cpu_list.push_back(mt_spec.cpu_list[i]);
//       }
//     }
//   }

//   std::string name = config.getAnalyticsName();
//   if (name.find("livegraph") != std::string::npos) {
//     using GraphType = LiveGraphWrapper<OID_T, VID_T, VDATA_T, std::string_view>;
//     if (name == "livegraph_sssp") {
//       using AppType = SSSPLiveGraph<GraphType>;
//       RunApp<GraphType, AppType, OID_T>(comm_spec, mt_spec,
//                                         graph,
//                                         config.getAnalyticsOutputPath(),
//                                         config.getSSSPSource());
//     } else if (name == "livegraph_pagerank") {
//       using AppType = PageRankLiveGraph<GraphType>;
//       RunApp<GraphType, AppType, OID_T>(comm_spec, mt_spec,
//                                         graph,
//                                         config.getAnalyticsOutputPath(),
//                                         config.getPRDamplingFactor(),
//                                         config.getPRIterNum());
//     }
//   } else {
//     LOG(FATAL) << "No avaiable application named [" << name << "].";
//   }

// }

}  // namespace grape

#endif  // WITH_GRAPE

#endif  // EXAMPLES_ANALYTICAL_APPS_RUN_APP_H_

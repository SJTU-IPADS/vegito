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

#include "app/ldbc/ldbc_analytics.h"
#include "app/analytics/bfs.hpp"
#include "app/analytics/grape/grape_app.h"

using namespace std;
using namespace nocc::oltp::ldbc;
using namespace livegraph;

#define TIMER 0

namespace nocc {
namespace oltp {
namespace ldbc {

void LDBCAnalyticsWorker::process_bfs_init() {
  
}

bool LDBCAnalyticsWorker::process_bfs() {
  uint64_t read_ver = get_read_ver_();
  std::vector<int> edge_labels;
  std::cout<<"We enter BFS #######"<<std::endl;
  // TODO: hardcode
  edge_labels.push_back(KNOWS);

  struct timespec end_time;
  clock_gettime(CLOCK_REALTIME,&end_time);
  double elapsed_sec = util::DiffTimespec(end_time, LDBCAnalyticsWorker::start_time) / 1000.0;

  BFSCtx ctx(num_thread_, graph_store_, rg_map_, edge_labels, read_ver);
  ctx.params.active_in.resize(ctx.vertex_labels.size(), nullptr);
  ctx.params.active_out.resize(ctx.vertex_labels.size(), nullptr);
  ctx.params.visited.resize(ctx.vertex_labels.size(), nullptr);
  ctx.params.parent.resize(ctx.vertex_labels.size(), nullptr);

  // get data
  for(int idx = 0; idx < ctx.vertex_labels.size(); idx++) {
    auto vertices_num = ctx.vertex_nums[idx];
    ctx.params.active_in[idx] = new Bitmap(vertices_num);
    ctx.params.active_out[idx] = new Bitmap(vertices_num);
    ctx.params.visited[idx] = new Bitmap(vertices_num);

    ctx.params.visited[idx]->clear();
    ctx.params.active_in[idx]->clear();

    ctx.params.parent[idx] = (vertex_t*) malloc(vertices_num * sizeof(vertex_t));
    for(int i = 0; i < vertices_num; i++) {
      ctx.params.parent[idx][i] = ctx.total_vertices;
    }
  }

  ctx.params.visited[0]->set_bit(ctx.params.root);
  ctx.params.active_in[0]->set_bit(ctx.params.root);
  ctx.params.parent[0][ctx.params.root] = ctx.params.root;

  nocc::util::Breakdown_Timer timer;
  timer.start();

  // work
  parallel_process<BFSCtx>(ctx, run_bfs);

  for(int idx = 0; idx < ctx.propertys.size(); idx++) {
    free(ctx.params.active_in[idx]);
    free(ctx.params.active_out[idx]);
    free(ctx.params.visited[idx]);
    free(ctx.params.parent[idx]);
  }

  std::cout << "Finish BFS, "
            << " time:" << timer.get_diff_ms()/1000 << " seconds" << std::endl;

  return true;
}

bool LDBCAnalyticsWorker::grape_bfs() {
//   using LgFragType = grape::LiveGraphWrapper<int64_t, uint32_t, grape::EmptyType, std::string_view>;
//   using SegFragType = grape::SegGraphWrapper<int64_t, uint32_t, grape::EmptyType, std::string_view>;
//   using LgAppType = grape::BFSLiveGraph<LgFragType>;
//   using SegAppType = grape::BFSLiveGraph<SegFragType>;

//   std::vector<int> edge_labels;
//   std::cout<<"GRAPE cc ######"<<std::endl;
//   // TODO: hardcode
//   edge_labels.push_back(0);
//   edge_labels.push_back(1);

//   uint64_t read_ver = get_read_ver_();
//   std::string out_prefix = "/home/yzh/grape_output";
//   // TODO: YZH
//   if(config.isUseSegGraph()) {
//     // grape::RunApp<SegFragType, SegAppType, int64_t>(comm_spec_, mt_spec_,
//     //                                 graph_store_,
//     //                                 edge_labels,
//     //                                 read_ver,
//     //                                 out_prefix,
//     //                                 0.15,
//     //                                 5);
//   } else {
//     // grape::RunApp<LgFragType, LgAppType, int64_t>(comm_spec_, mt_spec_,
//     //                                 graph_store_,
//     //                                 rg_map_,
//     //                                 edge_labels,
//     //                                 read_ver,
//     //                                 out_prefix,
//     //                                 0.15,
//     //                                 5);
//   }
  return true;
}

}  // namespace ldbc
}  // namespace oltp
}  // namespace nocc


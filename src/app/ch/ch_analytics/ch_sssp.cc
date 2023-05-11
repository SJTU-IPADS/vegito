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
#include "app/analytics/sssp.hpp"
#include "app/analytics/grape/grape_app.h"
#include "app/ch/ch_schema.h"

using namespace std;
using namespace nocc::oltp::ch;
using namespace livegraph;

#define TIMER 0

namespace nocc {
namespace oltp {
namespace ch {

void ChAnalyticsWorker::process_sssp_init() {
  
}

bool ChAnalyticsWorker::process_sssp() {
  //uint64_t read_ver = get_read_ver_();
  uint64_t read_ver = 1;
  std::vector<int> edge_labels;
  std::cout<<"We enter SSSP #######"<<std::endl;
  // TODO: hardcode
  edge_labels.push_back(C_I);
  edge_labels.push_back(C_I_R);

  struct timespec end_time;
  clock_gettime(CLOCK_REALTIME,&end_time);
  double elapsed_sec = util::DiffTimespec(end_time, ChAnalyticsWorker::start_time) / 1000.0;

  SSSPCtx ctx(num_thread_, graph_store_, rg_map_, edge_labels, read_ver);
  ctx.params.active_in.resize(ctx.vertex_labels.size(), nullptr);
  ctx.params.active_out.resize(ctx.vertex_labels.size(), nullptr);
  ctx.params.distance.resize(ctx.vertex_labels.size(), nullptr);
  ctx.params.root_label = CUST;
  ctx.params.root = 0;

  // get data
  for(int idx = 0; idx < ctx.vertex_labels.size(); idx++) {
    auto vertices_num = ctx.vertex_nums[idx];
    ctx.params.active_in[idx] = new Bitmap(vertices_num);
    ctx.params.active_out[idx] = new Bitmap(vertices_num);

    ctx.params.active_in[idx]->clear();

    ctx.params.distance[idx] = (double*) malloc(vertices_num * sizeof(double));
  }

  nocc::util::Breakdown_Timer timer;
  timer.start();

  // load data
  for(int idx = 0; idx < ctx.vertex_labels.size(); idx++) {
    auto vertices_num = ctx.vertex_nums[idx];
    for (int i = 0; i < vertices_num; i++) {
      auto sp_col_id = 1;
      ctx.params.distance[idx][i] = *(double*)(ctx.propertys[idx]->getByOffset(i, sp_col_id, read_ver));
    }
  }

  // set root data
  auto root_label_idx = ctx.vlabel2idx[ctx.params.root_label];
  ctx.params.active_in[root_label_idx]->set_bit(ctx.params.root);
  ctx.params.distance[root_label_idx][ctx.params.root] = 0;

  // work
  parallel_process<SSSPCtx>(ctx, run_sssp);

  uint64_t edge_num = 0;
  float time_sec = timer.get_diff_ms()/1000;
  std::cout << "[Finish SSSP], "
            << " time:" << time_sec << " seconds" 
            << " total vertices:" << ctx.total_vertices
            << " vertex thpt:" << ctx.total_vertices/time_sec << " vertices/sec" 
            << " edge thpt:" << edge_num/time_sec << " edges/sec"
            << " edge write thpt:" << edge_num/elapsed_sec << " edges/sec" << std::endl;

  for(int idx = 0; idx < ctx.propertys.size(); idx++) {
    free(ctx.params.active_in[idx]);
    free(ctx.params.active_out[idx]);
    free(ctx.params.distance[idx]);
  }
  
  return true;
}

bool ChAnalyticsWorker::grape_sssp() {
  // UNINPLEMENTED
  return true;
}

}  // namespace ch
}  // namespace oltp
}  // namespace nocc


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
#include "app/analytics/pagerank.hpp"
#include "app/analytics/grape/grape_app.h"

using namespace std;
using namespace nocc::oltp::ch;
using namespace livegraph;

#define TIMER 0

namespace nocc {
namespace oltp {
namespace ch {

void ChAnalyticsWorker::process_pagerank_init() {

}

bool ChAnalyticsWorker::process_pagerank() {
  //uint64_t read_ver = get_read_ver_();
  uint64_t read_ver = 1;
  std::vector<int> edge_labels;
  std::cout<<"We enter pagerank #######"<<std::endl;
  // TODO: hardcode
  edge_labels.push_back(C_I);
  edge_labels.push_back(C_I_R);

  struct timespec end_time;
  clock_gettime(CLOCK_REALTIME,&end_time);
  double elapsed_sec = util::DiffTimespec(end_time, ChAnalyticsWorker::start_time) / 1000.0;

  PageRankCtx ctx(num_thread_, graph_store_, rg_map_, edge_labels, read_ver);
  ctx.params.iterations = 5;
  ctx.params.active.resize(ctx.vertex_labels.size(), nullptr);
  ctx.params.curr.resize(ctx.vertex_labels.size(), nullptr);
  ctx.params.next.resize(ctx.vertex_labels.size(), nullptr);
  ctx.params.out_degree.resize(ctx.vertex_labels.size(), nullptr);

  // get data
  for(int idx = 0; idx < ctx.vertex_labels.size(); idx++) {
    auto vertices_num = ctx.vertex_nums[idx];
    ctx.params.active[idx] = new Bitmap(vertices_num);
    ctx.params.active[idx]->fill();

    ctx.params.curr[idx] = (double*) malloc(vertices_num * sizeof(double));
    ctx.params.next[idx] = (double*) malloc(vertices_num * sizeof(double));
    ctx.params.out_degree[idx] = (size_t*) malloc(vertices_num * sizeof(size_t));

    memset(ctx.params.curr[idx], 0, vertices_num * sizeof(double));
    memset(ctx.params.next[idx], 0, vertices_num * sizeof(double));
    memset(ctx.params.out_degree[idx], 0, vertices_num * sizeof(size_t));
  }

  nocc::util::Breakdown_Timer timer;

  double mem_usage = BlockManager::allocated_mem_size;

  timer.start();

  // load data
  for(int idx = 0; idx < ctx.vertex_labels.size(); idx++) {
    auto vertices_num = ctx.vertex_nums[idx];
    for (int i = 0; i < vertices_num; i++) {
      auto pr_col_id = 0;
      ctx.params.curr[idx][i] = *(double*)(ctx.propertys[idx]->getByOffset(i, pr_col_id, read_ver));
    }
  }

  // work
  parallel_process<PageRankCtx>(ctx, run_pagerank);

  size_t edge_num = 0;

  /*
  // store data
  for(int idx = 0; idx < ctx.propertys.size(); idx++) {
    auto vertices_num = ctx.vertex_nums[idx];
    // update the properties produced by analytics
    for (int i = 0; i < vertices_num; i++) {
      edge_num += ctx.params.out_degree[idx][i];
      ctx.propertys[idx]->update(i, O_PR, (char*)&ctx.params.curr[idx][i], read_ver);
    }


    free(ctx.params.curr[idx]);
    free(ctx.params.next[idx]);
    free(ctx.params.out_degree[idx]);
    free(ctx.params.active[idx]);
  }
  */

  // master worker gather pr_sum
  double total_pr_sum = 0;
  for(int i = 0; i < ctx.worker_datas.size(); i++) {
      total_pr_sum += ctx.worker_datas[i].pr_sum;
  }

  size_t total_active_edges = 0;
  for(int i = 0; i < ctx.worker_datas.size(); i++) {
      total_active_edges += ctx.worker_datas[i].active_edges;
  }

  std::cout << "pr_sum = " << total_pr_sum << std::endl;

  float time_sec = timer.get_diff_ms()/1000;

  std::cout << "[Finish pagerank], "
            << " time:" << time_sec << " seconds"
            << " total vertices:" << ctx.total_vertices
            << " vertex thpt:" << ctx.total_vertices/time_sec << " vertices/sec"
            << " total edges:" << total_active_edges
            << " edge thpt:" << edge_num/time_sec << " edges/sec"
            << " edge write thpt:" << edge_num/elapsed_sec << " edges/sec" << std::endl;

  return true;
}

bool ChAnalyticsWorker::grape_pagerank() {
#ifdef WITH_GRAPE
  using LgFragType = grape::LiveGraphWrapper<int64_t, uint32_t, grape::EmptyType, std::string_view>;
  using SegFragType = grape::SegGraphWrapper<int64_t, uint32_t, grape::EmptyType, std::string_view>;
  using LgAppType = grape::PageRankLiveGraph<LgFragType>;
  using SegAppType = grape::PageRankLiveGraph<SegFragType>;

  std::vector<int> edge_labels;
  std::cout<<"GRAPE Pagerank ######"<<std::endl;
  // TODO: hardcode
  edge_labels.push_back(0);
  edge_labels.push_back(1);

  uint64_t read_ver = get_read_ver_();
  std::string out_prefix = "/home/yzh/grape_output";
  // TODO: YZH
  if(config.isUseSegGraph()) {
    // grape::RunApp<SegFragType, SegAppType, int64_t>(comm_spec_, mt_spec_,
    //                                 graph_store_,
    //                                 edge_labels,
    //                                 read_ver,
    //                                 out_prefix,
    //                                 0.15,
    //                                 5);
  } else {
    grape::RunApp<LgFragType, LgAppType, int64_t>(comm_spec_, mt_spec_,
                                    graph_store_,
                                    rg_map_,
                                    edge_labels,
                                    read_ver,
                                    out_prefix,
                                    0.15,
                                    5);
  }
#endif
  return true;
}

}  // namespace ch
}  // namespace oltp
}  // namespace nocc


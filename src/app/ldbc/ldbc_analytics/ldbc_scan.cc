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

using namespace std;
using namespace nocc::oltp::ldbc;
using namespace livegraph;

#define TIMER 0

namespace nocc {
namespace oltp {
namespace ldbc {

bool LDBCAnalyticsWorker::process_scan() {
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  uint64_t read_ver = get_read_ver_();
  // uint64_t read_ver = 1;
  std::vector<int> edge_labels;
  std::cout << "We enter edge scan on epoch " << read_ver <<std::endl;
  // TODO: hardcode
  edge_labels.push_back(KNOWS);

  AnalyticsCtx ctx(num_thread_, graph_store_, rg_map_, edge_labels, read_ver);

  // get data
  for(int idx = 0; idx < ctx.vertex_labels.size(); idx++) {
    auto vertices_num = ctx.vertex_nums[idx];
  }
  
  float max = 0;
  for (int i = 0; i < 10; i++) {
    float temp = scan(&ctx);
    max = temp > max ? temp : max;
  }
  std::cout << "  Maximum thpt: " << max << " edges/second" << std::endl;

  return true;
}

float LDBCAnalyticsWorker::scan(AnalyticsCtx* ctx) {
  bool use_seg_graph = ctx->use_seg_graph;
  auto& worker_states = ctx->worker_states;
  auto& local_worker_state = worker_states[0];
  nocc::util::Breakdown_Timer timer;
  uint64_t edge_num = 0;

  timer.start();
  for(int g_i = 0; g_i < local_worker_state.graph_idx.size(); g_i++) {
    int graph_idx = local_worker_state.graph_idx[g_i];
    int vlabel_idx = ctx->src_vlabel_idx[graph_idx];
    label_t edge_label = ctx->edge_labels[graph_idx];
    vertex_t num_vertices;
    if (use_seg_graph) {
      auto seg_graph = ctx->seg_graphs[vlabel_idx];
      auto seg_txn = ctx->seg_graph_readers[vlabel_idx];
      num_vertices = seg_graph->get_max_vertex_id();
      segid_t seg_id = 0;
      while (true) {
          if (seg_id >= seg_graph->get_max_seg_id()) break;
          auto segment = seg_txn->locate_segment(seg_id, edge_label);
          vertex_t v_start = seg_graph->get_seg_start_vid(seg_id), v_end = std::min(num_vertices, seg_graph->get_seg_end_vid(seg_id));
          // std::cout << v_start << " | " << v_end << std::endl;
          for(vertex_t v_i = v_start; v_i < v_end; v_i++) {
              auto edge_iter = seg_txn->get_edges_in_seg(segment, v_i, 0);
              while(edge_iter.valid()) {
                  vertex_t dst = edge_iter.dst_id();
                  edge_iter.next();
                  edge_num++;
              }
          }
          seg_id++;
      }
    } else {
      auto lg_graph = ctx->lg_graphs[vlabel_idx];
      auto lg_txn = ctx->lg_graph_readers[vlabel_idx];
      num_vertices = lg_graph->get_max_vertex_id();
      for(vertex_t v_i = 0; v_i < num_vertices; v_i++) {
          auto edge_iter = lg_txn->get_edges(v_i, edge_label, true);
          while(edge_iter.valid()) {
              vertex_t dst = edge_iter.dst_id();
              edge_iter.next();
              edge_num++;
          }
      }
    }
  }

  float used_time = timer.get_diff_ms() / 1000;
  std::cout << "  Finish scanning, "
            << " time:" << used_time << " seconds"
            << ", edge num:" << edge_num
            << ", thpt:" << edge_num / used_time << " edges/second"
            << std::endl;
  return edge_num / used_time;
}

bool LDBCAnalyticsWorker::grape_scan() {
  return false;
}

}  // namespace ldbc
}  // namespace oltp
}  // namespace nocc


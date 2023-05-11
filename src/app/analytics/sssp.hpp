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

#include "framework/bench_analytics.h"

namespace {

struct SSSPParam {
  label_t root_label = 0;
  vertex_t root = 0;
  std::vector<double*> distance;
  std::vector<Bitmap*> active_in;
  std::vector<Bitmap*> active_out;
};

struct SSSPData {
  size_t active_vertices;
  size_t active_edges;
};

struct SSSPCtx : public nocc::oltp::AnalyticsCtx {
  SSSPParam params;
  std::vector<SSSPData> worker_datas;

  SSSPCtx(int num_workers,
              nocc::graph::GraphStore* graph_store,
              nocc::graph::RGMapping* rg_map,
              std::vector<int> edge_labels,
              uint64_t read_ver)
    : AnalyticsCtx(num_workers, graph_store, rg_map, edge_labels, read_ver),
      worker_datas(num_workers) { }
} __attribute__ ((aligned (CACHE_LINE_SZ)));

void run_sssp(int worker_idx, void *arg) {
  bool is_master_worker = (worker_idx == 0);
  auto ctx = static_cast<SSSPCtx*>(arg);
  auto& params = ctx->params;
  auto& worker_datas = ctx->worker_datas;
  auto& barrier = ctx->barrier;

  auto& distance = params.distance;
  auto& active_in = params.active_in;
  auto& active_out = params.active_out;

  // 1. init active_vertices
  vertex_t total_active_vertices = 1;

  // 2. execute sssp
  //for (int i_i = 0; i_i < 5; i_i++) {
  for (int i_i = 0; total_active_vertices > 0; i_i++) {
      // master worker gather delta
      if (is_master_worker) {
          std::cout << "active(" << i_i << ")=" << total_active_vertices << std::endl;
          // for(auto& active : active_out) {
          //   active->clear();
          // }
      }

      // calculate active edges
      // worker_datas[worker_idx].active_edges = process_vertices<size_t>(
      //     [&](label_t vlabel, vertex_t vtx){
      //         return out_degree[vtx];
      //     },
      //     active
      // );

      barrier->wait();

      // master worker gather active_edges
      if (is_master_worker) {
          size_t total_active_edges = 0;
          for(int i = 0; i < worker_datas.size(); i++) {
              total_active_edges += worker_datas[i].active_edges;
          }
      }

      bool push_mode = false;

      // pagerank aggregate function
      if(push_mode) {
          worker_datas[worker_idx].active_vertices = nocc::oltp::process_edges_push<SSSPCtx, vertex_t>(worker_idx, ctx,
              [&](label_t src_vlabel, label_t dst_vlabel, vertex_t src, EdgeIteratorBase& outgoing_adj){
                  vertex_t activated = 0;
                  while(outgoing_adj.valid()) {
                      vertex_t dst = outgoing_adj.dst_id();
                      double edge_dist = *(double*)(outgoing_adj.edge_data().data());
                      double relax_dist = distance[src_vlabel][src] + edge_dist;
                      if(relax_dist < distance[dst_vlabel][dst]) {
                        write_min(&distance[dst_vlabel][dst], relax_dist);
                        active_out[dst_vlabel]->set_bit(dst);
                        activated += 1;
                      }
                      outgoing_adj.next();
                  }
                  return activated;
              },
              active_in
          );
      } else {
          worker_datas[worker_idx].active_vertices = nocc::oltp::process_edges_pull<SSSPCtx, vertex_t>(worker_idx, ctx,
              [&](label_t dst_vlabel, label_t src_vlabel, vertex_t dst, EdgeIteratorBase& incoming_adj) {
                  vertex_t activated = 0;
                  double dist_msg = distance[dst_vlabel][dst];
                  while(incoming_adj.valid()) {
                      vertex_t src = incoming_adj.dst_id();
                      double edge_dist = *(double*)(incoming_adj.edge_data().data());
                      double relax_dist = distance[src_vlabel][src] + edge_dist;
                      if(relax_dist < dist_msg) {
                        dist_msg = relax_dist;
                        active_out[dst_vlabel]->set_bit(dst);
                        activated = 1;
                      }
                      incoming_adj.next();
                  }
                  distance[dst_vlabel][dst] = dist_msg;
                  return activated;
              },
              active_in
          );
      }

      barrier->wait();

      total_active_vertices = 0;
      for(int i = 0; i < worker_datas.size(); i++) {
          total_active_vertices += worker_datas[i].active_vertices;
      }

      if (is_master_worker) {
        std::swap(active_in, active_out);
      }
  }
}

}
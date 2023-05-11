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

struct PageRankParam {
  int curr_iter;
  int iterations;
  std::vector<double*> curr;
  std::vector<double*> next;
  std::vector<Bitmap*> active;
  std::vector<size_t*> out_degree;
};

struct PageRankData {
  double delta;
  size_t active_edges;
  double pr_sum;
};

struct PageRankCtx : public nocc::oltp::AnalyticsCtx {
  PageRankParam params;
  std::vector<PageRankData> worker_datas;

  PageRankCtx(int num_workers,
              nocc::graph::GraphStore* graph_store,
              nocc::graph::RGMapping* rg_map,
              std::vector<int> edge_labels,
              uint64_t read_ver)
    : AnalyticsCtx(num_workers, graph_store, rg_map, edge_labels, read_ver),
      worker_datas(num_workers) { }
} __attribute__ ((aligned (CACHE_LINE_SZ)));

void run_pagerank(int worker_idx, void *arg) {
  bool is_master_worker = (worker_idx == 0);
  auto ctx = static_cast<PageRankCtx*>(arg);
  auto& params = ctx->params;
  auto& worker_datas = ctx->worker_datas;
  auto& barrier = ctx->barrier;

  int iterations = params.iterations;
  auto& curr = params.curr;
  auto& next = params.next;
  auto& out_degree = params.out_degree;
  auto& active = params.active;

  // 1. init rank value
  double delta = nocc::oltp::process_edges_push<PageRankCtx, double>(
      worker_idx,
      ctx,
      [&](label_t src_vlabel, label_t dst_vlabel, vertex_t src, EdgeIteratorBase& outgoing_adj){
          curr[src_vlabel][src] = 1;
          uint32_t deg = 0;
          while(outgoing_adj.valid()) {
              deg++;
              outgoing_adj.next();
          }
          if (deg > 0) {
              curr[src_vlabel][src] /= deg;
          }
          out_degree[src_vlabel][src] = deg;
          return 1;
      },
      active
  );
  worker_datas[worker_idx].delta = delta;
  barrier->wait();

  // 2. execute pagerank iteratively
  for (int iter = 0; iter < iterations; iter++) {
      // master worker gather delta
      if (is_master_worker) {
          double total_delta = 0;
          for(int i = 0; i < worker_datas.size(); i++) {
              total_delta += worker_datas[i].delta;
          }
          std::cout << "delta(" << iter << ")=" << total_delta << std::endl;
      }

      barrier->wait();

      bool push_mode = false;
      //active->fill();

      // pagerank aggregate function
      if(push_mode) {
          nocc::oltp::process_edges_push<PageRankCtx, double>(worker_idx, ctx,
              [&](label_t src_vlabel, label_t dst_vlabel, vertex_t src, EdgeIteratorBase& outgoing_adj){
                  while(outgoing_adj.valid()) {
                      vertex_t dst = outgoing_adj.dst_id();
                      //std::cout << "src: " << src << ", dst: " << dst << std::endl;
                      write_add(&next[dst_vlabel][dst], curr[src_vlabel][src]);
                      outgoing_adj.next();
                  }
                  return 0;
              },
              active
          );
      } else {
          nocc::oltp::process_edges_pull<PageRankCtx, double>(worker_idx, ctx,
              [&](label_t dst_vlabel, label_t src_vlabel, vertex_t dst, EdgeIteratorBase& incoming_adj) {
                  double sum = 0;
                  while(incoming_adj.valid()) {
                      vertex_t src = incoming_adj.dst_id();
                      sum += curr[src_vlabel][src];
                      incoming_adj.next();
                  }
                  next[dst_vlabel][dst] = sum;
                  return 0;
              },
              active
          );
      }

      barrier->wait();

      // pagerank apply function
      if (iter == iterations-1) {
          delta = nocc::oltp::process_vertices<PageRankCtx, double>(
              worker_idx, ctx,
              [&](label_t vlabel, vertex_t vtx) {
                  next[vlabel][vtx] = 0.15 + 0.85 * next[vlabel][vtx];
                  //std::cout << "iter:" << iter << ", pr:" << next[vtx] << std::endl;
                  return 0;
              },
              active
          );
      } else {
          delta = nocc::oltp::process_vertices<PageRankCtx, double>(
              worker_idx, ctx,
              [&](label_t vlabel, vertex_t vtx) {
                  next[vlabel][vtx] = 0.15 + 0.85 * next[vlabel][vtx];
                  //std::cout << "iter:" << iter << ", pr:" << next[vtx] << std::endl;
                  if (out_degree[vlabel][vtx] > 0) {
                      next[vlabel][vtx] /= out_degree[vlabel][vtx];
                      return std::fabs(next[vlabel][vtx] - curr[vlabel][vtx]) * out_degree[vlabel][vtx];
                  }
                  return std::fabs(next[vlabel][vtx] - curr[vlabel][vtx]);
              },
              active
          );
      }
      worker_datas[worker_idx].delta = delta;

      barrier->wait();
      if (is_master_worker) {
        std::swap(curr, next);
      }
      barrier->wait();
  }

  // calculate pagerank value sum
  barrier->wait();

  worker_datas[worker_idx].pr_sum = nocc::oltp::process_vertices<PageRankCtx, double>(
      worker_idx, ctx,
      [&](label_t vlabel, vertex_t vtx) {
          return curr[vlabel][vtx];
      },
      active
  );

  barrier->wait();

  // calculate active edges
  worker_datas[worker_idx].active_edges = nocc::oltp::process_vertices<PageRankCtx, size_t>(
      worker_idx, ctx,
      [&](label_t vlabel, vertex_t vtx){
          return out_degree[vlabel][vtx];
      },
      active
  );

  barrier->wait();
}

}
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

#include <Eigen/Dense>
#include "framework/bench_analytics.h"

using Eigen::MatrixXd;
using Eigen::VectorXd;

namespace {

struct GCNParam {
  std::vector<Bitmap*> active;
  size_t input_dim;
  size_t hidden_dim;
  size_t output_dim;

  MatrixXd weight_0;
  MatrixXd weight_1;

  std::vector<std::vector<VectorXd>> trans_0;
  std::vector<std::vector<VectorXd>> trans_1;

  std::vector<std::vector<VectorXd>> hidden_result;
  std::vector<std::vector<VectorXd>> result;
};


struct GCNCtx : public nocc::oltp::AnalyticsCtx {
  GCNParam params;

  GCNCtx(int num_workers,
              nocc::graph::GraphStore* graph_store,
              nocc::graph::RGMapping* rg_map,
              std::vector<int> edge_labels,
              uint64_t read_ver)
    : AnalyticsCtx(num_workers, graph_store, rg_map, edge_labels, read_ver) { }
} __attribute__ ((aligned (CACHE_LINE_SZ)));

void run_gcn(int worker_idx, void *arg) {
  bool is_master_worker = (worker_idx == 0);
  auto ctx = static_cast<GCNCtx*>(arg);
  auto& params = ctx->params;
  auto& barrier = ctx->barrier;

  auto& active = params.active;

  auto& trans_0 = params.trans_0;
  auto& trans_1 = params.trans_1;
  auto& hidden_result = params.hidden_result;
  auto& result = params.result;

  // apply layer 0
  nocc::oltp::process_vertices<GCNCtx, int>(
      worker_idx, ctx,
      [&](label_t vlabel, vertex_t vtx) {
          trans_0[vlabel][vtx] *= params.weight_0;
          return 0;
      },
      active
  );

  barrier->wait();

  // gather layer 0
  nocc::oltp::process_edges_pull<GCNCtx, int>(worker_idx, ctx,
      [&](label_t dst_vlabel, label_t src_vlabel, vertex_t dst, EdgeIteratorBase& incoming_adj) {
          while(incoming_adj.valid()) {
              vertex_t src = incoming_adj.dst_id();
              hidden_result[dst_vlabel][dst] += trans_0[src_vlabel][src];
              incoming_adj.next();
          }
          return 0;
      },
      active
  );

  barrier->wait();

  // active function: ReLU
  nocc::oltp::process_vertices<GCNCtx, int>(
      worker_idx, ctx,
      [&](label_t vlabel, vertex_t vtx) {
          hidden_result[vlabel][vtx] = hidden_result[vlabel][vtx].cwiseMax(0);
          return 0;
      },
      active
  );

  barrier->wait();

  // apply layer 1
  nocc::oltp::process_vertices<GCNCtx, int>(
      worker_idx, ctx,
      [&](label_t vlabel, vertex_t vtx) {
          trans_1[vlabel][vtx] = hidden_result[vlabel][vtx] * params.weight_1;
          return 0;
      },
      active
  );

  barrier->wait();

  // gather layer 1
  nocc::oltp::process_edges_pull<GCNCtx, int>(worker_idx, ctx,
      [&](label_t dst_vlabel, label_t src_vlabel, vertex_t dst, EdgeIteratorBase& incoming_adj) {
          while(incoming_adj.valid()) {
              vertex_t src = incoming_adj.dst_id();
              result[dst_vlabel][dst] += trans_1[src_vlabel][src];
              incoming_adj.next();
          }
          return 0;
      },
      active
  );

  barrier->wait();

  // active function: softmax
  nocc::oltp::process_vertices<GCNCtx, int>(
      worker_idx, ctx,
      [&](label_t vlabel, vertex_t vtx) {
          result[vlabel][vtx] = 1 / (1 + (-result[vlabel][vtx]).array().exp());
          return 0;
      },
      active
  );

  barrier->wait();
}

}
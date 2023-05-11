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
#include "app/analytics/gcn.hpp"
#include "app/analytics/grape/grape_app.h"

using namespace std;
using namespace nocc::oltp::ch;
using namespace livegraph;

#define TIMER 0

namespace nocc {
namespace oltp {
namespace ch {

void ChAnalyticsWorker::process_gcn_init() {
  
}

bool ChAnalyticsWorker::process_gcn() {
  // uint64_t read_ver = get_read_ver_();
  uint64_t read_ver = 1;
  std::vector<int> edge_labels;

  // TODO: hardcode
  edge_labels.push_back(C_I);
  edge_labels.push_back(C_I_R);

  GCNCtx ctx(num_thread_, graph_store_, rg_map_, edge_labels, read_ver);  

  ctx.params.input_dim = 3;
  ctx.params.hidden_dim = 64;
  ctx.params.output_dim = 16;
  ctx.params.weight_0 = MatrixXd::Random(ctx.params.input_dim, ctx.params.hidden_dim);
  ctx.params.weight_1 = MatrixXd::Random(ctx.params.hidden_dim, ctx.params.output_dim);

  ctx.params.active.resize(ctx.vertex_labels.size());

  ctx.params.trans_0.resize(ctx.vertex_labels.size());
  ctx.params.trans_1.resize(ctx.vertex_labels.size());
  ctx.params.hidden_result.resize(ctx.vertex_labels.size());
  ctx.params.result.resize(ctx.vertex_labels.size());

  for(int idx = 0; idx < ctx.vertex_labels.size(); idx++) {
    auto vertices_num = ctx.vertex_nums[idx];

    ctx.params.active[idx] = new Bitmap(vertices_num);
    ctx.params.active[idx]->fill();

    ctx.params.trans_0[idx].resize(vertices_num);
    ctx.params.trans_1[idx].resize(vertices_num);
    ctx.params.hidden_result[idx].resize(vertices_num);
    ctx.params.result[idx].resize(vertices_num);

    for (int i = 0; i < vertices_num; i++) {
      ctx.params.trans_0[idx][i] = VectorXd(ctx.params.input_dim);
      ctx.params.hidden_result[idx][i] = VectorXd::Zero(ctx.params.hidden_dim);
      ctx.params.result[idx][i] = VectorXd::Zero(ctx.params.output_dim);
    }
  }

  nocc::util::Breakdown_Timer timer;
  timer.start();

  // Load data and init trans_0
  auto load_start = rdtsc();
  for(int idx = 0; idx < ctx.vertex_labels.size(); idx++) {
    auto vertices_num = ctx.vertex_nums[idx];

    auto col_cursor = ctx.propertys[idx]->getColCursor(0, read_ver);
    auto row_cursor = ctx.propertys[idx]->getRowCursor(read_ver);
    if(col_cursor.get()) {
      std::cout << "[GCN] Use col cursor" << std::endl;
      col_cursor->seekOffset(0, vertices_num);
      col_cursor->nextRow();
      for (int i = 0; i < vertices_num; i++) {
        auto cursor_data = col_cursor->value();
        if(!cursor_data) {
          std::cout << "invalid cursor_data" << std::endl;
          break;
        }
        for(int d = 0; d < ctx.params.input_dim; d++) {
          ctx.params.trans_0[idx][i](d) = *(double*)(cursor_data + d*sizeof(double));
        }
        col_cursor->nextRow();
      }
    } else {
      std::cout << "[GCN] Use row cursor" << std::endl;
      assert(row_cursor);
      row_cursor->seekOffset(0, vertices_num);
      row_cursor->nextRow();
      for (int i = 0; i < vertices_num; i++) {
        auto data = row_cursor->value();
        for(int d = 0; d < ctx.params.input_dim; d++) {
          ctx.params.trans_0[idx][i](d) = *(double*)(data + d*sizeof(double));
        }
        row_cursor->nextRow();
      }
    }
  }
  auto load_end = rdtsc();
  std::cout << "Load property time:" 
            <<  ((float)(load_end - load_start) / Breakdown_Timer::get_one_second_cycle())
            << " seconds" << std::endl;

  // work
  parallel_process<GCNCtx>(ctx, run_gcn);

  std::cout << "Finish GCN, "
            << " time:" << timer.get_diff_ms()/1000 << " seconds" << std::endl;

  // TODO: store result back

  return true;
}

bool ChAnalyticsWorker::grape_gcn() {
  // UNINPLEMENTED
  return true;
}

}  // namespace ch
}  // namespace oltp
}  // namespace nocc


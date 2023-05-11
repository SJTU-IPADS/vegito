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
#include "app/analytics/gcn.hpp"
#include "app/analytics/grape/grape_app.h"

using namespace std;
using namespace nocc::oltp::ldbc;
using namespace livegraph;

#define TIMER 0

#define USE_CURSOR 1

namespace nocc {
namespace oltp {
namespace ldbc {

void LDBCAnalyticsWorker::process_gcn_init() {
  
}

bool LDBCAnalyticsWorker::process_gcn() {
  // uint64_t read_ver = get_read_ver_();
  uint64_t read_ver = 1;
  std::vector<int> edge_labels;

  // TODO: hardcode
  edge_labels.push_back(LIKES_POST);

  GCNCtx ctx(num_thread_, graph_store_, rg_map_, edge_labels, read_ver);  

  ctx.params.input_dim = 3;
  ctx.params.hidden_dim = 32;
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
  for(int idx = 0; idx < ctx.vertex_labels.size(); idx++) {
    auto vertices_num = ctx.vertex_nums[idx];

    int col_id;
    if(ctx.vertex_labels[idx] == PERSON) {
      std::cout << "Load PERSON!" << std::endl;
      col_id = Person::Cid::PR;
    } else if (ctx.vertex_labels[idx] == POST) {
      std::cout << "Load POST!" << std::endl;
      col_id = Post::Cid::PR;
    } else { assert(false); }

#if USE_CURSOR
    auto col_cursor = ctx.propertys[idx]->getColCursor(col_id, read_ver);
    auto row_cursor = ctx.propertys[idx]->getRowCursor(read_ver);
    if(col_cursor.get()) {
      std::cout << "[GCN] Use col cursor" << std::endl;
      col_cursor->seekOffset(0, vertices_num);
      col_cursor->nextRow();
      for (int i = 0; i < vertices_num; i++) {
        auto cursor_data = (double*)col_cursor->value();
        if(!cursor_data) {
          std::cout << "invalid cursor_data" << std::endl;
          break;
        }
        for(int d = 0; d < ctx.params.input_dim; d++) {
          ctx.params.trans_0[idx][i](d) = cursor_data[d];
        }
        col_cursor->nextRow();
      }
    } else {
      std::cout << "[GCN] Use row cursor" << std::endl;
      assert(row_cursor);
      uint64_t off = ctx.propertys[idx]->locateCol(col_id, 3*sizeof(double)); 
      row_cursor->seekOffset(0, vertices_num);
      row_cursor->nextRow();
      for (int i = 0; i < vertices_num; i++) {
        auto data = (double*)(row_cursor->value()+off);
        for(int d = 0; d < ctx.params.input_dim; d++) {
          ctx.params.trans_0[idx][i](d) = data[d];
        }
        row_cursor->nextRow();
      }
    }
#else
    for (int i = 0; i < vertices_num; i++) {
      auto data = (double*)(ctx.propertys[idx]->getByOffset(i, col_id, read_ver));
      for(int d = 0; d < ctx.params.input_dim; d++) {
        ctx.params.trans_0[idx][i](d) = data[d];
      }
    }
#endif
  }

  // work
  parallel_process<GCNCtx>(ctx, run_gcn);

  std::cout << "Finish GCN, "
            << " time:" << timer.get_diff_ms()/1000 << " seconds" << std::endl;

  // TODO: store result back

  for(int idx = 0; idx < ctx.propertys.size(); idx++) {
    free(ctx.params.active[idx]);
  }

  return true;
}

bool LDBCAnalyticsWorker::grape_gcn() {
  // UNIMPLEMENTED
  return true;
}

}  // namespace ldbc
}  // namespace oltp
}  // namespace nocc


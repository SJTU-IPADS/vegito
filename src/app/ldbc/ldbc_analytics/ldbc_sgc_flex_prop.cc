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
#include "app/analytics/sgc.hpp"
#include "app/analytics/grape/grape_app.h"
#include "app/ldbc/ldbc_schema.h"

using namespace std;
using namespace nocc::oltp::ldbc;
using namespace livegraph;

#define TIMER 0

#define USE_CURSOR 0

namespace nocc {
namespace oltp {
namespace ldbc {

void LDBCAnalyticsWorker::process_sgc_flexprop_init() {
  
}

bool LDBCAnalyticsWorker::process_sgc_flexprop() {
  // uint64_t read_ver = get_read_ver_();
  uint64_t read_ver = 1;
  std::vector<int> edge_labels;

  // TODO: hardcode
  edge_labels.push_back(LIKES_POST);

  SGCCtx ctx(num_thread_, graph_store_, rg_map_, edge_labels, read_ver);  

  ctx.params.input_dim = 4;
  // ctx.params.hidden_dim = 64;
  ctx.params.output_dim = 16;
  // ctx.params.weight_0 = MatrixXd::Random(ctx.params.input_dim, ctx.params.hidden_dim);
  // ctx.params.weight_1 = MatrixXd::Random(ctx.params.hidden_dim, ctx.params.output_dim);
  ctx.params.weight = MatrixXd::Random(ctx.params.input_dim, ctx.params.output_dim);

  ctx.params.active.resize(ctx.vertex_labels.size());

  // ctx.params.trans_0.resize(ctx.vertex_labels.size());
  // ctx.params.trans_1.resize(ctx.vertex_labels.size());
  ctx.params.raw_feature.resize(ctx.vertex_labels.size());
  ctx.params.hidden_result.resize(ctx.vertex_labels.size());
  ctx.params.result.resize(ctx.vertex_labels.size());
  ctx.params.final_result.resize(ctx.vertex_labels.size());

  for(int idx = 0; idx < ctx.vertex_labels.size(); idx++) {
    auto vertices_num = ctx.vertex_nums[idx];

    ctx.params.active[idx] = new Bitmap(vertices_num);
    ctx.params.active[idx]->fill();

    // ctx.params.trans_0[idx].resize(vertices_num);
    // ctx.params.trans_1[idx].resize(vertices_num);
    ctx.params.raw_feature[idx].resize(vertices_num);
    ctx.params.hidden_result[idx].resize(vertices_num);
    ctx.params.result[idx].resize(vertices_num);
    ctx.params.final_result[idx].resize(vertices_num);

    for (int i = 0; i < vertices_num; i++) {
      ctx.params.raw_feature[idx][i] = VectorXd(ctx.params.input_dim);
      ctx.params.hidden_result[idx][i] = VectorXd::Zero(ctx.params.input_dim);
      ctx.params.result[idx][i] = VectorXd::Zero(ctx.params.input_dim);
    }
  }

  struct timespec end_time;
  double elapsed_sec;
  clock_gettime(CLOCK_REALTIME,&end_time);
  elapsed_sec = util::DiffTimespec(end_time, LDBCAnalyticsWorker::start_time) / 1000.0;

  std::cout << "## Begin SGC AP ##, " << elapsed_sec << std::endl;

  nocc::util::Breakdown_Timer timer;
  timer.start();

  // get data and init trans_0
  auto load_start = rdtsc();
  for(int idx = 0; idx < ctx.vertex_labels.size(); idx++) {
    // check for flexible property
    auto snapshot_prop = 
      graph_store_->get_property_snapshot(ctx.vertex_labels[idx], read_ver);
    if(snapshot_prop) {
      // std::cout << "We find an available flexible storage for label " 
      //           << ctx.vertex_labels[idx] << std::endl;
      ctx.propertys[idx] = snapshot_prop;
    }

    auto vertices_num = ctx.vertex_nums[idx];

    int col_id;
    if(ctx.vertex_labels[idx] == PERSON) {
      col_id = Person::Cid::PR;
    } else if (ctx.vertex_labels[idx] == POST) {
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
          ctx.params.raw_feature[idx][i](d) = cursor_data[d];
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
          ctx.params.raw_feature[idx][i](d) = data[d];
        }
        row_cursor->nextRow();
      }
    }
#else
    std::vector<int> col_ids;
    if(ctx.vertex_labels[idx] == PERSON) {
      col_ids.push_back(Person::Cid::PR);
      col_ids.push_back(Person::Cid::CC);
      col_ids.push_back(Person::Cid::SP);
      col_ids.push_back(Person::Cid::BF);
    } else if (ctx.vertex_labels[idx] == POST) {
      col_ids.push_back(Post::Cid::PR);
      col_ids.push_back(Post::Cid::CC);
      col_ids.push_back(Post::Cid::SP);
      col_ids.push_back(Post::Cid::BF);
    } 
    for (int i = 0; i < vertices_num; i++) {
      for(int d = 0; d < ctx.params.input_dim; d++) {
        ctx.params.raw_feature[idx][i](d) = *(double*)(ctx.propertys[idx]->getByOffset(i, col_ids[d], read_ver));
      }
    }
#endif
  }
  auto load_end = rdtsc();
  std::cout << "Load property time:" 
            <<  ((float)(load_end - load_start) / Breakdown_Timer::get_one_second_cycle())
            << " seconds" << std::endl;


  // work
  parallel_process<SGCCtx>(ctx, run_sgc);

  std::cout << "Finish SGC, "
            << " time:" << timer.get_diff_ms()/1000 << " seconds" << std::endl;

  clock_gettime(CLOCK_REALTIME,&end_time);
  elapsed_sec = util::DiffTimespec(end_time, LDBCAnalyticsWorker::start_time) / 1000.0;

  std::cout << "## Finish SGC AP ##, " << elapsed_sec << std::endl;

  // TODO: store result back

  return true;
}

bool LDBCAnalyticsWorker::grape_sgc_flexprop() {
  // UNINPLEMENTED
  return true;
}

}  // namespace ldbc
}  // namespace oltp
}  // namespace nocc


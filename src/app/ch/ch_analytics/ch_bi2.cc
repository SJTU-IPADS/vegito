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
#include "app/ch/ch_schema.h"
#include <vector>

using namespace std;
using namespace nocc::oltp::ch;
using namespace livegraph;
// using namespace nocc::oltp::ldbc;

#define TIMER 0

namespace {

struct BI2Param {
  double start_pr;
  double end_pr;
  std::vector<int> items;
  int num_items;
};

struct BI2Data {

};

struct BI2Result {
  int count_small;
  int count_large;
};

struct BI2Ctx : public nocc::oltp::AnalyticsCtx {
  BI2Param params;
  std::vector<BI2Data> worker_datas;
  BI2Ctx(int num_workers,
              nocc::graph::GraphStore* graph_store,
              nocc::graph::RGMapping* rg_map,
              std::vector<int> edge_labels,
              uint64_t read_ver)
    : AnalyticsCtx(num_workers, graph_store, rg_map, edge_labels, read_ver),
      worker_datas(num_workers) { }
} __attribute__ ((aligned (CACHE_LINE_SZ)));


void process(int worker_idx, void *arg);

} // namespace anonymous

namespace nocc {
namespace oltp {
namespace ch {

void ChAnalyticsWorker::process_bi2_init() {

}

bool ChAnalyticsWorker::process_bi2() {
  // uint64_t read_ver = get_read_ver_();
  uint64_t read_ver = 1;
  std::vector<int> edge_labels;
  std::cout<<"We enter BI2 #######"<<std::endl;
  // TODO: hardcode
  edge_labels.push_back(C_I);
  edge_labels.push_back(O_OL);
  edge_labels.push_back(C_O);
  edge_labels.push_back(C_I_R);
  edge_labels.push_back(O_OL_R);
  edge_labels.push_back(C_O_R);

  BI2Ctx ctx(num_thread_, graph_store_, rg_map_, edge_labels, read_ver);

  ctx.params.start_pr = 0;
  ctx.params.end_pr = 1;


  auto item_vlabel_idx = ctx.vlabel2idx[ITEM];

  ctx.params.num_items = ctx.vertex_nums[item_vlabel_idx]/4;

  for (int i = 0; i < ctx.vertex_nums[item_vlabel_idx]/4; i++) {
    ctx.params.items.push_back(i);
  }


  struct timespec end_time;
  clock_gettime(CLOCK_REALTIME,&end_time);
  double elapsed_sec = util::DiffTimespec(end_time, ChAnalyticsWorker::start_time) / 1000.0;



//   int err = 0;
//   for (int i = 0; i < vertices_num; i++) {
//       uint64_t key = rg_map_.get_vid2key(ORDE, i);
//       if(key != 0) {
//         ctx.params.curr[i] = *(double*)(backup_store_.Get(ORDE, key, O_PR, read_ver));
//       }
//       else {
//         err++;
//       }
//   }

//   std::cout<<"PR ERROR COUNT:##########"<<err<<std::endl;

  nocc::util::Breakdown_Timer timer;

  double mem_usage = BlockManager::allocated_mem_size;

  timer.start();

  // work
  parallel_process<BI2Ctx>(ctx, process);


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


  float time_sec = timer.get_diff_ms()/1000;

  std::cout << "[Finish BI2], "
            << " time:" << time_sec << " seconds" << std::endl;

  return true;
}

bool ChAnalyticsWorker::grape_bi2() {

  return process_bi2();
}

}  // namespace ch
}  // namespace oltp
}  // namespace nocc

namespace {
void process(int worker_idx, void *arg) {
  auto ctx = static_cast<BI2Ctx*>(arg);
  auto& params = ctx->params;
  auto& read_ver = ctx->read_epoch_id;
  double start_pr = params.start_pr;
  double end_pr = params.end_pr;
  int num_items = params.num_items;
  std::vector<int> items = params.items;

  std::vector<BI2Result> res(num_items);
  for (int i = 0; i < num_items; i++) {
    res[i].count_small = 0;
    res[i].count_large = 0;
  }


  auto item_vlabel_idx = ctx->vlabel2idx[ITEM];
  auto cust_vlabel_idx = ctx->vlabel2idx[CUST];
  auto orde_vlabel_idx = ctx->vlabel2idx[ORDE];

  for (int i = 0; i < num_items; i++) {
    if(config.isUseSegGraph()) {
      auto edge_iter = ctx->seg_graph_readers[item_vlabel_idx]->get_edges(items[i], C_I_R, EOUT);
      while (edge_iter.valid()) {
          auto cust_id = edge_iter.dst_id();
          double cust_pr = *((double*)(ctx->propertys[cust_vlabel_idx]->getByOffset(cust_id, 0, read_ver)));
          auto orde_iter = ctx->seg_graph_readers[cust_vlabel_idx]->get_edges(cust_id, C_O, EOUT);
          while (orde_iter.valid()) {
            auto orde_id = orde_iter.dst_id();
            auto orde_pr = *((double*)(ctx->propertys[orde_vlabel_idx]->getByOffset(orde_id, 0, read_ver)));
            if (orde_pr >= start_pr && orde_pr <= end_pr) {
              if (cust_pr < 0.5) {
                res[i].count_small++;
              }
              else {
                res[i].count_large++;
              }
            }
            orde_iter.next();
          }
          edge_iter.next();
      }
    } else {
      auto edge_iter = ctx->lg_graph_readers[item_vlabel_idx]->get_edges(items[i], C_I_R, EOUT);
      while (edge_iter.valid()) {
          auto cust_id = edge_iter.dst_id();
          double cust_pr = *((double*)(ctx->propertys[cust_vlabel_idx]->getByOffset(cust_id, 0, read_ver)));
          auto orde_iter = ctx->lg_graph_readers[cust_vlabel_idx]->get_edges(cust_id, C_O, EOUT);
          while (orde_iter.valid()) {
            auto orde_id = orde_iter.dst_id();
            auto orde_pr = *((double*)(ctx->propertys[orde_vlabel_idx]->getByOffset(orde_id, 0, read_ver)));
            if (orde_pr >= start_pr && orde_pr <= end_pr) {
              if (cust_pr < 0.5) {
                res[i].count_small++;
              }
              else {
                res[i].count_large++;
              }
            }
            orde_iter.next();
          }
          edge_iter.next();
      }
    }
  }
  /*
  for (int i = 0; i < num_places; i++) {
      for (int j = 0; j < 12; j++) {
        std::cout<< "place = " << i << "gender = male month = " << j + 1
            << " msg_count = " << res[i].msg_count_male[j] << std::endl;
        std::cout<< "place = " << i << "gender = female month = " << j + 1
            << " msg_count = " << res[i].msg_count_female[j] << std::endl;
      }
  }
  */


}

} // namespace anonymous


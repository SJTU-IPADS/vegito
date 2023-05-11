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
#include "app/analytics/grape/grape_app.h"
#include "app/ldbc/ldbc_schema.h"
#include <vector>

using namespace std;
using namespace livegraph;
using namespace nocc::oltp::ldbc;

#define TIMER 0

namespace {

struct BI3Param {
  std::vector<int> tag_input;
  std::string year_month_before;
  std::string year_month_after;
};

struct BI3Data {
  
};

struct BI3Ctx : public nocc::oltp::AnalyticsCtx {
  BI3Param params;
  std::vector<BI3Data> worker_datas;
  BI3Ctx(int num_workers,
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
namespace ldbc {

void LDBCAnalyticsWorker::process_bi3_breakdown_init() {
  
}

bool LDBCAnalyticsWorker::process_bi3_breakdown() {
  // uint64_t read_ver = get_read_ver_();
  uint64_t read_ver = 1;
  std::vector<int> edge_labels;
  std::cout<<"We enter BI3 #######"<<std::endl;
  // TODO: hardcode
  edge_labels.push_back(ORG_ISLOCATIONIN);
  edge_labels.push_back(ISPARTOF);
  edge_labels.push_back(ISSUBCLASSOF);
  edge_labels.push_back(HASTYPE);

  edge_labels.push_back(COMMENT_HASCREATOR);
  edge_labels.push_back(COMMENT_HASTAG);
  edge_labels.push_back(COMMENT_ISLOCATIONIN);
  edge_labels.push_back(POST_HASCREATOR);
  edge_labels.push_back(REPLYOF_POST);

  edge_labels.push_back(POST_HASCREATOR);
  edge_labels.push_back(POST_HASTAG);
  edge_labels.push_back(POST_ISLOCATIONIN);

  edge_labels.push_back(FORUM_CONTAINEROF);
  edge_labels.push_back(FORUM_HASMODERATOR);
  edge_labels.push_back(FORUM_HASTAG);

  edge_labels.push_back(PERSON_HASINTEREST);
  edge_labels.push_back(PERSON_ISLOCATEDIN);

  edge_labels.push_back(FORUM_HASMEMBER);
  edge_labels.push_back(KNOWS);
  edge_labels.push_back(LIKES_COMMENT);
  edge_labels.push_back(LIKES_POST);
  edge_labels.push_back(STUDYAT);
  edge_labels.push_back(WORKAT);

  struct timespec end_time;
  clock_gettime(CLOCK_REALTIME,&end_time);
  double elapsed_sec = util::DiffTimespec(end_time, LDBCAnalyticsWorker::start_time) / 1000.0;

  BI3Ctx ctx(num_thread_, graph_store_, rg_map_, edge_labels, read_ver);

  auto vlabel_idx = ctx.vlabel2idx[TAG];

  std::cout << "Total number of tags = " << ctx.vertex_nums[vlabel_idx] << std::endl;

  for (int i = 0; i < ctx.vertex_nums[vlabel_idx]; i++) {
    ctx.params.tag_input.push_back(i);
  }

  ctx.params.year_month_before = "2011-11";
  ctx.params.year_month_after = "2011-12";

  nocc::util::Breakdown_Timer timer;

  double mem_usage = BlockManager::allocated_mem_size;
  
  timer.start();

  // work
  parallel_process<BI3Ctx>(ctx, process);

  float time_sec = timer.get_diff_ms()/1000;

  std::cout << "[Finish BI3], "
            << " time:" << time_sec << " seconds" << std::endl;

  return true;
}

bool LDBCAnalyticsWorker::grape_bi3_breakdown() {
  
  return process_bi3_breakdown();
}

}  // namespace ldbc
}  // namespace oltp
}  // namespace nocc

namespace {
void process(int worker_idx, void *arg) {
  auto ctx = static_cast<BI3Ctx*>(arg);
  auto& params = ctx->params;
  auto& read_ver = ctx->read_epoch_id;
  std::vector tag_input = params.tag_input;
  std::string year_month_before = params.year_month_before;
  std::string year_month_after = params.year_month_after;
  std::vector tag_count_before(tag_input.size(), 0);
  std::vector tag_count_after(tag_input.size(), 0);

  auto tag_vlabel_idx = ctx->vlabel2idx[TAG];
  auto post_vlabel_idx = ctx->vlabel2idx[POST];

  // only get topo
  nocc::util::Breakdown_Timer timer_topo;
  timer_topo.start();

  for (int i = 0; i < tag_input.size(); i++) {
    if(config.isUseSegGraph()) {
      auto edge_iter = ctx->seg_graph_readers[tag_vlabel_idx]->get_edges(i, POST_HASTAG, EIN);
      while (edge_iter.valid()) {
          auto post_id = edge_iter.dst_id();
          edge_iter.next();
      }
    } else {
      auto edge_iter = ctx->lg_graph_readers[tag_vlabel_idx]->get_edges(i, POST_HASTAG, EIN);
      while (edge_iter.valid()) {
          auto post_id = edge_iter.dst_id();
          edge_iter.next();
      }
    }
  }

  float topo_time_sec = timer_topo.get_diff_ms()/1000;

  std::cout << "Get topo "
            << " time: " << topo_time_sec << " seconds" << std::endl;

  nocc::util::Breakdown_Timer timer_topo_prop;
  timer_topo_prop.start();

  // get topo and property
  for (int i = 0; i < tag_input.size(); i++) {
    if(config.isUseSegGraph()) {
      auto edge_iter = ctx->seg_graph_readers[tag_vlabel_idx]->get_edges(i, POST_HASTAG, EIN);
      while (edge_iter.valid()) {
          auto post_id = edge_iter.dst_id();
          auto po_date = (DateTime*)(ctx->propertys[post_vlabel_idx]->getByOffset(post_id, 2, read_ver));
          auto po_data_str = po_date->str();
          edge_iter.next();
      }
    } else {
      auto edge_iter = ctx->lg_graph_readers[tag_vlabel_idx]->get_edges(i, POST_HASTAG, EIN);
      while (edge_iter.valid()) {
          auto post_id = edge_iter.dst_id();
          auto po_date = (DateTime*)(ctx->propertys[post_vlabel_idx]->getByOffset(post_id, 2, read_ver));
          auto po_data_str = po_date->str();
          edge_iter.next();
      }
    }
  }

  float topo_prop_time_sec = timer_topo_prop.get_diff_ms()/1000;

  std::cout << "Get property "
            << " time:" << topo_prop_time_sec - topo_time_sec << " seconds" << std::endl;

  // get all

  nocc::util::Breakdown_Timer timer_all;
  timer_all.start();

  for (int i = 0; i < tag_input.size(); i++) {
    if(config.isUseSegGraph()) {
      auto edge_iter = ctx->seg_graph_readers[tag_vlabel_idx]->get_edges(i, POST_HASTAG, EIN);
      while (edge_iter.valid()) {
          auto post_id = edge_iter.dst_id();
          auto po_date = (DateTime*)(ctx->propertys[post_vlabel_idx]->getByOffset(post_id, 2, read_ver));
          auto po_data_str = po_date->str();
          if (po_data_str.substr(0,7) == year_month_before) {
            tag_count_before[i]++;
          }
          else if (po_data_str.substr(0,7) == year_month_after) {
            tag_count_after[i]++;
          }
          edge_iter.next();
      }
    } else {
      auto edge_iter = ctx->lg_graph_readers[tag_vlabel_idx]->get_edges(i, POST_HASTAG, EIN);
      while (edge_iter.valid()) {
          auto post_id = edge_iter.dst_id();
          auto po_date = (DateTime*)(ctx->propertys[post_vlabel_idx]->getByOffset(post_id, 2, read_ver));
          auto po_data_str = po_date->str();
          if (po_data_str.substr(0,7) == year_month_before) {
            tag_count_before[i]++;
          }
          else if (po_data_str.substr(0,7) == year_month_after) {
            tag_count_after[i]++;
          }
          edge_iter.next();
      }
    }
  }

  float all_time_sec = timer_all.get_diff_ms()/1000;

  std::cout << " Get comp "
            << " time:" << all_time_sec - topo_prop_time_sec << " seconds" << std::endl;
/*
  for (int i = 0; i < tag_input.size(); i++) {
      std::cout<< "message diff for tag " << i << " is " << tag_count_before[i]-tag_count_after[i] << std::endl;
  }
*/

}

} // namespace anonymous


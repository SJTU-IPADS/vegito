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
// using namespace nocc::oltp::ch;
using namespace livegraph;
using namespace nocc::oltp::ldbc;

#define TIMER 0

namespace {

struct BI2Param {
  std::string start_date;
  std::string end_date;
  std::vector<int> places;
  int num_places;
};

struct BI2Data {
  
};

struct BI2Result {
  std::vector<int> msg_count_male;
  std::vector<int> msg_count_female;
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
namespace ldbc {

void LDBCAnalyticsWorker::process_bi2_breakdown_init() {
  
}

bool LDBCAnalyticsWorker::process_bi2_breakdown() {
  uint64_t read_ver = get_read_ver_();
  // uint64_t read_ver = 1;
  std::vector<int> edge_labels;
  std::cout<<"We enter BI2 #######"<<std::endl;
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

  BI2Ctx ctx(num_thread_, graph_store_, rg_map_, edge_labels, read_ver);

  ctx.params.start_date = "2011-01-01";
  ctx.params.end_date = "2011-12-31";

  ctx.params.num_places = 8000;

  auto place_vlabel_idx = ctx.vlabel2idx[PLACE];

  for (int i = 0; i < ctx.vertex_nums[place_vlabel_idx]; i++) {
    if(config.isUseSegGraph()) {
      auto edge_iter = ctx.seg_graph_readers[place_vlabel_idx]->get_edges(i, PERSON_ISLOCATEDIN, EIN);
      if (!edge_iter.empty()) {
        ctx.params.places.push_back(i);
      }
    } else {
      auto edge_iter = ctx.lg_graph_readers[place_vlabel_idx]->get_edges(i, PERSON_ISLOCATEDIN, EIN);
      if (!edge_iter.empty()) {
        ctx.params.places.push_back(i);
      }
    }
    if (ctx.params.places.size() >= ctx.params.num_places) {
      break;
    }

  }


  struct timespec end_time;
  clock_gettime(CLOCK_REALTIME,&end_time);
  double elapsed_sec = util::DiffTimespec(end_time, LDBCAnalyticsWorker::start_time) / 1000.0;



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

bool LDBCAnalyticsWorker::grape_bi2_breakdown() {
  
  return process_bi2_breakdown();
}

}  // namespace ch
}  // namespace oltp
}  // namespace nocc

namespace {
void process(int worker_idx, void *arg) {
  auto ctx = static_cast<BI2Ctx*>(arg);
  auto& params = ctx->params;
  auto& read_ver = ctx->read_epoch_id;
  std::string start_date = params.start_date;
  std::string end_date = params.end_date;
  int num_places = params.num_places;
  std::vector<int> places = params.places;

  std::vector<BI2Result> res(num_places);
  for (int i = 0; i < num_places; i++) {
    res[i].msg_count_male.resize(12, 0);
    res[i].msg_count_female.resize(12, 0);
  }


  auto place_vlabel_idx = ctx->vlabel2idx[PLACE];
  auto person_vlabel_idx = ctx->vlabel2idx[PERSON];
  auto post_vlabel_idx = ctx->vlabel2idx[POST];

  // only topo

  nocc::util::Breakdown_Timer timer_topo;
  timer_topo.start();

  for (int i = 0; i < num_places; i++) {
    if(config.isUseSegGraph()) {
      auto edge_iter = ctx->seg_graph_readers[place_vlabel_idx]->get_edges(places[i], PERSON_ISLOCATEDIN, EIN);
      while (edge_iter.valid()) {
          auto person_id = edge_iter.dst_id();
          auto post_iter = ctx->seg_graph_readers[person_vlabel_idx]->get_edges(person_id, POST_HASCREATOR, EIN);
          while (post_iter.valid()) {
            auto post_id = post_iter.dst_id();
            post_iter.next();
          }
          edge_iter.next();
      }
    } else {
      auto edge_iter = ctx->lg_graph_readers[place_vlabel_idx]->get_edges(places[i], PERSON_ISLOCATEDIN, EIN);
      while (edge_iter.valid()) {
          auto person_id = edge_iter.dst_id();
          auto post_iter = ctx->lg_graph_readers[person_vlabel_idx]->get_edges(person_id, POST_HASCREATOR, EIN);
          while (post_iter.valid()) {
            auto post_id = post_iter.dst_id();
            post_iter.next();
          }
          edge_iter.next();
      }
    }
  }

  float topo_time_sec = timer_topo.get_diff_ms()/1000;

  std::cout << "Get topo "
            << " time:" << topo_time_sec << " seconds" << std::endl;

  //topo and property

  nocc::util::Breakdown_Timer timer_topo_prop;
  timer_topo_prop.start();

  for (int i = 0; i < num_places; i++) {
    if(config.isUseSegGraph()) {
      auto edge_iter = ctx->seg_graph_readers[place_vlabel_idx]->get_edges(places[i], PERSON_ISLOCATEDIN, EIN);
      while (edge_iter.valid()) {
          auto person_id = edge_iter.dst_id();
          auto gender = ((String*)(ctx->propertys[person_vlabel_idx]->getByOffset(person_id, 3, read_ver)))->str();
          auto post_iter = ctx->seg_graph_readers[person_vlabel_idx]->get_edges(person_id, POST_HASCREATOR, EIN);
          while (post_iter.valid()) {
            auto post_id = post_iter.dst_id();
            auto po_date = (DateTime*)(ctx->propertys[post_vlabel_idx]->getByOffset(post_id, 2, read_ver));
            auto po_date_str = po_date->str().substr(0,10);
            post_iter.next();
          }
          edge_iter.next();
      }
    } else {
      auto edge_iter = ctx->lg_graph_readers[place_vlabel_idx]->get_edges(places[i], PERSON_ISLOCATEDIN, EIN);
      while (edge_iter.valid()) {
          auto person_id = edge_iter.dst_id();
          auto gender = ((String*)(ctx->propertys[person_vlabel_idx]->getByOffset(person_id, 3, read_ver)))->str();
          auto post_iter = ctx->lg_graph_readers[person_vlabel_idx]->get_edges(person_id, POST_HASCREATOR, EIN);
          while (post_iter.valid()) {
            auto post_id = post_iter.dst_id();
            auto po_date = (DateTime*)(ctx->propertys[post_vlabel_idx]->getByOffset(post_id, 2, read_ver));
            auto po_date_str = po_date->str().substr(0,10);
            post_iter.next();
          }
          edge_iter.next();
      }
    }
  }

  float topo_prop_time_sec = timer_topo_prop.get_diff_ms()/1000;

  std::cout << "Get property "
            << " time:" << topo_prop_time_sec - topo_time_sec << " seconds" << std::endl;

  // all
  nocc::util::Breakdown_Timer timer_all;
  timer_all.start();

  for (int i = 0; i < num_places; i++) {
    if(config.isUseSegGraph()) {
      auto edge_iter = ctx->seg_graph_readers[place_vlabel_idx]->get_edges(places[i], PERSON_ISLOCATEDIN, EIN);
      while (edge_iter.valid()) {
          auto person_id = edge_iter.dst_id();
          auto gender = ((String*)(ctx->propertys[person_vlabel_idx]->getByOffset(person_id, 3, read_ver)))->str();
          auto post_iter = ctx->seg_graph_readers[person_vlabel_idx]->get_edges(person_id, POST_HASCREATOR, EIN);
          while (post_iter.valid()) {
            auto post_id = post_iter.dst_id();
            auto po_date = (DateTime*)(ctx->propertys[post_vlabel_idx]->getByOffset(post_id, 2, read_ver));
            auto po_date_str = po_date->str().substr(0,10);
            if (po_date_str >= start_date && po_date_str <= end_date) {
              int month = std::stoi(po_date_str.substr(5,2)) -1 ;
              if (gender == "male") {
                res[i].msg_count_male[month]++;
              }
              else {
                res[i].msg_count_female[month]++;
              }
            }
            post_iter.next();
          }
          edge_iter.next();
      }
    } else {
      auto edge_iter = ctx->lg_graph_readers[place_vlabel_idx]->get_edges(places[i], PERSON_ISLOCATEDIN, EIN);
      while (edge_iter.valid()) {
          auto person_id = edge_iter.dst_id();
          auto gender = ((String*)(ctx->propertys[person_vlabel_idx]->getByOffset(person_id, 3, read_ver)))->str();
          auto post_iter = ctx->lg_graph_readers[person_vlabel_idx]->get_edges(person_id, POST_HASCREATOR, EIN);
          while (post_iter.valid()) {
            auto post_id = post_iter.dst_id();
            auto po_date = (DateTime*)(ctx->propertys[post_vlabel_idx]->getByOffset(post_id, 2, read_ver));
            auto po_date_str = po_date->str().substr(0,10);
            if (po_date_str >= start_date && po_date_str <= end_date) {
              int month = std::stoi(po_date_str.substr(5,2)) -1 ;
              if (gender == "male") {
                res[i].msg_count_male[month]++;
              }
              else {
                res[i].msg_count_female[month]++;
              }
            }
            post_iter.next();
          }
          edge_iter.next();
      }
    }
  }

  float all_time_sec = timer_all.get_diff_ms()/1000;

  std::cout << "Get comp "
            << " time:" << all_time_sec - topo_prop_time_sec << " seconds" << std::endl;
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


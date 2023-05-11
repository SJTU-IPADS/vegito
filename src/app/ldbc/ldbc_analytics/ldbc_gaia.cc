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

#ifdef WITH_GAIA

#include "app/ldbc/ldbc_analytics.h"
#include "app/ldbc/ldbc_schema.h"
#include "app/analytics/grape/grape_app.h"

using namespace std;
using namespace nocc::oltp::ldbc;
using namespace livegraph;

#define TIMER 0

extern "C" {
  void initialize_pegasus(int32_t worker_num);
  void initialize_graph(int64_t ctx, int32_t worker_num);
  void evaluate_query_plan(const char *query_name /* NULL-terminated */, int32_t worker_num);
  void evaluate_query_plan_ith(int32_t query_index, int32_t worker_num);
}

namespace nocc {
namespace oltp {
namespace ldbc {

void LDBCAnalyticsWorker::process_gaia_init() {
   std::cout<< "Init gaia ######"<<std::endl;
   initialize_pegasus(1);
}

bool LDBCAnalyticsWorker::process_gaia() {
  uint64_t read_ver = get_read_ver_();
  std::vector<int> edge_labels;
  std::cout<<"We enter gaia query ..."<<std::endl;
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




  AnalyticsCtx ctx(num_thread_, graph_store_, rg_map_, edge_labels, read_ver); 
  auto vlabel_idx = ctx.vlabel2idx[PERSON];
  for (int i = 0; i < ctx.vertex_nums[vlabel_idx]; i++) {
    auto edge_iter = ctx.seg_graph_readers[vlabel_idx]->get_edges(i, KNOWS, EOUT);
    if(edge_iter.size()>0) {
      int64_t label_id = PERSON;
      int64_t global_id = i;
      global_id = global_id | (label_id << 56);
      std::cout<< "global id = " << global_id << " local id = "<< i <<std::endl;
      break;
    }
    /*
    int64_t p_id = *(int64_t*)(ctx.propertys[vlabel_idx]->getByOffset(i, 0, read_ver));
    if (p_id == 6597069780295) {
       auto first_name = (String*)(ctx.propertys[vlabel_idx]->getByOffset(i, 1, read_ver));
       auto last_name = (String*)(ctx.propertys[vlabel_idx]->getByOffset(i, 2, read_ver));
       std::cout<< "first name = " << *first_name << " last name = " << *last_name << std::endl;
       break;
    }
    */
    /*
    std::vector<int> in_neighbors, out_neighbors, friends;
    auto edge_iter = ctx.seg_graph_readers[vlabel_idx]->get_edges(i, KNOWS, EOUT);
    while (edge_iter.valid()) {
      out_neighbors.push_back(edge_iter.dst_id());
      edge_iter.next();
    }
    auto in_edge_iter = ctx.seg_graph_readers[vlabel_idx]->get_edges(i, KNOWS, EIN);

    while (in_edge_iter.valid()) {
      in_neighbors.push_back(in_edge_iter.dst_id());
      in_edge_iter.next();
    }
    std::sort(in_neighbors.begin(), in_neighbors.end());
    std::sort(out_neighbors.begin(), out_neighbors.end());
     std::set_intersection(in_neighbors.begin(), in_neighbors.end(),
        out_neighbors.begin(), out_neighbors.end(),
        std::back_inserter(friends)); 
    
    if (friends.size() > 0) {
      std::cout << "Vertex id = " << i << " neighbor size = " << friends.size() <<std::endl;
      int64_t p_id = *(int64_t*)(ctx.propertys[vlabel_idx]->getByOffset(i, 0, read_ver));
      std::cout << " PID = " << p_id << std::endl;
      int64_t label_id = PERSON;
      int64_t global_id = i;
      global_id = global_id | (label_id << 56);
      std::cout<< "global id = " << global_id <<std::endl;
      break;
    }
    */
    if ( i == ctx.vertex_nums[vlabel_idx] - 1) {
      std::cout << "cannot find from vertices "<< ctx.vertex_nums[vlabel_idx] <<std::endl;
    }
    
  }
  
  /*
  int label = 2;
  int64_t label_id = ctx.vertex_labels[label];
  LOG(INFO) << "Label id = " << label_id;
  for (int i = 0; i < ctx.vertex_nums[2]; i++) {
    auto edge_iter = ctx.seg_graph_readers[2]->get_edges(i, 12);
    auto edge_size = edge_iter.size();
    if (edge_size > 0) {
      int64_t global_id = i;
      global_id = global_id | (label_id << 56);
      LOG(INFO) << "global id = " << global_id << " vertex id = " << i << " neighbor size = " << edge_size;
      break;
    }
    if (i == ctx.vertex_nums[2] - 1) {
      LOG(INFO)<< "cannot find from vertices "<< ctx.vertex_nums[2];
    }
  }
  
  */  
  // AnalyticsCtx ctx(num_thread_, graph_store_, rg_map_, edge_labels, in_edge_labels, read_ver);
  intptr_t pctx = reinterpret_cast<intptr_t>(&ctx);

  initialize_graph(pctx, 1);
  // evaluate_query_plan("ldbc_query_1" /* which ldbc query to evaluate */, 1);
  //if (false) {
  nocc::util::Breakdown_Timer is3_timer;
  is3_timer.start();
  evaluate_query_plan("/disk1/wanglei/htap/src/gremlin-queries/ldbc/query_plans/IS_3.plan" /* which ldbc query to evaluate */, 1);
  std::cout << "IS3 latency:" << is3_timer.get_diff_ms() << "ms" << std::endl;
  if (false) {
  nocc::util::Breakdown_Timer is5_timer;
  is5_timer.start();
  evaluate_query_plan("/disk1/wanglei/htap/src/gremlin-queries/ldbc/query_plans/IS_5.plan" /* which ldbc query to evaluate */, 1);
  std::cout << "IS5 latency:" << is5_timer.get_diff_ms() << "ms" << std::endl;

  nocc::util::Breakdown_Timer is7_timer;
  is7_timer.start();
  evaluate_query_plan("/disk1/wanglei/htap/src/gremlin-queries/ldbc/query_plans/IS_7.plan" /* which ldbc query to evaluate */, 1);
  std::cout << "IS7 latency:" << is7_timer.get_diff_ms() << "ms" << std::endl;
  
  
  nocc::util::Breakdown_Timer ic9_timer;
  ic9_timer.start();
  evaluate_query_plan("/disk1/wanglei/htap/src/gremlin-queries/ldbc/query_plans/test_5.plan" /* which ldbc query to evaluate */, 1);
  std::cout << "IC9 latency:" << ic9_timer.get_diff_ms() << "ms" << std::endl;
  }
  return true;
}

bool LDBCAnalyticsWorker::grape_gaia() {
  return process_gaia();
}

}  // namespace ldbc
}  // namespace oltp
}  // namespace nocc

#endif  // WITH_GAIA
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

#include "app/ldbc/ldbc_query.h"

using namespace std;
using namespace nocc::oltp::ldbc;
using namespace livegraph;

#define TIMER 0

namespace nocc {
namespace oltp {
namespace ldbc {

bool LDBCQueryWorker::queryFreshness(yield_func_t &yield) {
  Breakdown_Timer timer;
  volatile uint64_t ver = get_read_ver_();
  
  printf("start @epoch = %lu\n", ver);

  uint64_t f_id = 893353198399l;  // SF = 0.1
  // uint64_t f_id = 2755l;    // SF = 3

  timer.start();
  uint64_t f_vid = rg_map_->get_key2vid(FORUM, f_id);

  // SegGraph *f_graph = graph_->get_graph<SegGraph>(FORUM);
  Graph *f_graph = graph_->get_graph<Graph>(FORUM);
  auto f_reader = f_graph->create_graph_reader(ver);

  while (f_vid == UINT64_MAX) {
    f_vid = rg_map_->get_key2vid(FORUM, f_id);
    ver = get_read_ver_();
  }

  printf("find vertex @epoch = %lu\n", ver);
    
  auto iter = f_reader.get_edges(f_vid, FORUM_HASMEMBER, livegraph::dir_t::EOUT);
  while (!iter.valid()) {
    ver = LogWorker::get_read_epoch(); 
    auto f_reader = f_graph->create_graph_reader(ver);
    iter = f_reader.get_edges(f_vid, FORUM_HASMEMBER, livegraph::dir_t::EOUT);
  }
  printf("find edge iterator @epoch = %lu\n", ver);

  auto person_vid = iter.dst_id();
  uint64_t insert_us = *((uint64_t*)iter.edge_data().data());
  assert(insert_us != 0);
  struct timeval tv;
  gettimeofday(&tv, nullptr);
  uint64_t now_us = (uint64_t(tv.tv_sec) * 1000000 + tv.tv_usec);
  float visibility = (now_us - insert_us) / 1000.0;

  printf("Visibility: %.02f ms\n", visibility);
  assert(visibility < 100);
  
  RpcReply *reply = (RpcReply *) payload_ptr_;
  reply->size = sizeof(uint64_t);
  uint64_t *buf = (uint64_t*) (payload_ptr_ + sizeof(RpcReply));
  *buf = insert_us;


  float used_time = timer.get_diff_ms();

#if 0
  printf("Result of query IS5:\n");
  printf("person_id       first_name      last_name       \n");
  printf("%-16lu %-16s %-16s \n", personId, firstName->c_str(), lastName->c_str());
#endif
  printf("Used time: %f ms\n", used_time);
  return true;
}

}  // namespace ch
}  // namesapce oltp
}  // namespace nocc


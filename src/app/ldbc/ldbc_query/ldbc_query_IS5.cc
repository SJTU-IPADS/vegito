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

bool LDBCQueryWorker::queryIS5(yield_func_t &yield) {
  Breakdown_Timer timer;
  uint64_t ver = get_read_ver_();
  uint64_t msg_id = 1030792151049l;    // SF = 0.1
  // uint64_t msg_id = 4123168604168l;    // SF = 3

  timer.start();
  uint64_t c_vid = rg_map_->get_key2vid(COMMENT, msg_id);
  uint64_t po_vid = rg_map_->get_key2vid(POST, msg_id);

  SegGraph *c_graph = graph_->get_graph<SegGraph>(COMMENT);
  SegGraph *po_graph = graph_->get_graph<SegGraph>(POST);
  SegGraph *p_graph = graph_->get_graph<SegGraph>(PERSON);

  auto c_reader = c_graph->create_graph_reader(ver);
  auto po_reader = po_graph->create_graph_reader(ver);
  auto p_reader = p_graph->create_graph_reader(ver);

  uint64_t person_vid;
  if (c_vid != UINT64_MAX) {
    auto iter = c_reader.get_edges(c_vid, COMMENT_HASCREATOR, livegraph::dir_t::EOUT);
    assert(iter.valid());
    person_vid = iter.dst_id();
  } else if (po_vid != UINT64_MAX) {
    auto iter = po_reader.get_edges(po_vid, POST_HASCREATOR, livegraph::dir_t::EOUT);
    assert(iter.valid());
    person_vid = iter.dst_id();
  } else {
    std::cout << "Nonexistent message id!" << std::endl;
    return false;
  }

  uint64_t personId = rg_map_->get_vid2key(PERSON, person_vid);
  BackupStore *p_property = graph_->get_property(PERSON);
  String* firstName = (String*)p_property->getByOffset(person_vid, Person::FistName, ver);
  String* lastName = (String*)p_property->getByOffset(person_vid, Person::LastName, ver);

  float used_time = timer.get_diff_ms();

  printf("Result of query IS5:\n");
  printf("person_id       first_name      last_name       \n");
  printf("%-16lu %-16s %-16s \n", personId, firstName->c_str(), lastName->c_str());
  printf("Used time: %f ms\n", used_time);
  return true;
}

}  // namespace ch
}  // namesapce oltp
}  // namespace nocc


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
#include "app/ldbc/ldbc_schema.h"

using namespace std;
using namespace nocc::oltp::ldbc;
using namespace livegraph;

#define TIMER 0

namespace nocc {
namespace oltp {
namespace ldbc {

void LDBCAnalyticsWorker::process_flexcol_init() {
  
}

bool LDBCAnalyticsWorker::process_flexcol() {
  uint64_t snapshot_ver = 1;
  std::vector<int> vertices_labels;
  std::cout<<"Creating flexible column store #######"<<std::endl;

  // stop subworkers
  this->subs_.clear();

  // TODO: hardcode
  vertices_labels.push_back(PERSON);
  vertices_labels.push_back(POST);

  std::this_thread::sleep_for(std::chrono::seconds(20));

  struct timespec end_time;
  double elapsed_sec;
  clock_gettime(CLOCK_REALTIME,&end_time);
  elapsed_sec = util::DiffTimespec(end_time, LDBCAnalyticsWorker::start_time) / 1000.0;

  std::cout << "## Begin Merge ##, " << elapsed_sec << std::endl;

  nocc::util::Breakdown_Timer timer;
  timer.start();

  std::vector<std::vector<uint32_t>> person_merges = {{
                                                Person::Cid::PR,
                                                Person::Cid::CC,
                                                Person::Cid::SP,
                                                Person::Cid::BF
                                                }};

  std::vector<std::vector<uint32_t>> post_merges = {{
                                                Post::Cid::PR,
                                                Post::Cid::CC,
                                                Post::Cid::SP,
                                                Post::Cid::BF
                                                }};
  graph_store_->merge_columns(PERSON, snapshot_ver, person_merges);
  graph_store_->merge_columns(POST, snapshot_ver, post_merges);

  float time_sec = timer.get_diff_ms()/1000;

  std::cout << "[Finish flexible column merging], "
            << " time:" << time_sec << " seconds" 
            << std::endl;

  clock_gettime(CLOCK_REALTIME,&end_time);
  elapsed_sec = util::DiffTimespec(end_time, LDBCAnalyticsWorker::start_time) / 1000.0;

  std::cout << "## Finish Merge ##, " << elapsed_sec << std::endl;

  // sleep from now
  while(true) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  return true;
}

bool LDBCAnalyticsWorker::grape_flexcol() {
  // NOT USED
  return true;
}

}  // namespace ldbc
}  // namespace oltp
}  // namespace nocc


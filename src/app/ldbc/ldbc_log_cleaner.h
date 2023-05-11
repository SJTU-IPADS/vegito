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

#pragma once

#include "ldbc_worker.h"
#include "ldbc_schema.h"
#include "framework/log_cleaner.h"
#include "core/livegraph.hpp"

#include <vector>

namespace nocc {
namespace oltp {
namespace ldbc {

class LDBCLogCleaner : public LogCleaner {
 public:
  LDBCLogCleaner();
  virtual int clean_log(int table_id, uint64_t key, 
                        uint64_t seq, char *val,int length) { assert(false); }
  virtual int clean_log(int log_id, int partition_id, int tx_id,
                        int table_id, uint64_t key, uint64_t seq, 
                        char *val, int length, uint32_t op_bit, 
                        uint64_t write_epoch);

 private:
  template <class GraphType>
  int clean_graph_log(int partition_id, int table_id,
                      char *val, int length,
                      uint64_t write_epoch);

  // std::vector<int> ware_tp_col_;
};

}  // namesapce ldbc
}  // namespace oltp
}  // namespace nocc

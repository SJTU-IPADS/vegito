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

#include "ddl.h"
#include "backup_store/backup_db.h"
#include "livegraph/core/transaction.hpp"

#include <vector>

namespace nocc {
namespace graph {

class GraphReplay {
 public:
  GraphReplay(livegraph::Graph &graph, const BackupDB &db,
              RGMapping* mapping);

  void Insert(int table_id, uint64_t offset, char *value,
              const std::vector<int> &cols, uint64_t ver,
              uint64_t fk = uint64_t(-1));

 private:
  livegraph::Graph &graph_;
  const BackupDB &db_;
  RGMapping *mapping_;
};

}  // namespace graph
}  // namespace nocc


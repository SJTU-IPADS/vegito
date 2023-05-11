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

#include "bench_loader.h"
#include "util/util.h"

using namespace nocc::util;
using namespace nocc::graph;

namespace nocc {
namespace oltp {

/* Abstract bench loader */
BenchLoader::BenchLoader(uint64_t seed, MemDB *db)
  : rand_(seed), store_(db), backup_store_(nullptr), isBackup_(false) { }

BenchLoader::BenchLoader(uint64_t seed, BackupDB *db)
  : rand_(seed), store_(nullptr), backup_store_(db), isBackup_(true) { }

void BenchLoader::run() {
  if(isBackup_) {
    BindToCore(TotalCores() - 1);
    loadBackup();
  }
  else {
    BindToCore(0);
    load();
  }
}

GraphLoader::GraphLoader(uint64_t seed, 
                         graph::GraphStore *graph_store,
                         RGMapping *mapping, bool is_vertex) 
  : rand_(seed), graph_store_(graph_store), mapping_(mapping),
    is_vertex_(is_vertex) { }

void GraphLoader::run() {
  BindToCore(TotalCores() - 1);
  load();
}

}  // namespace nocc
}  // namespace nocc

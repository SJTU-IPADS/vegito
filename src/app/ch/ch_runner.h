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

#include "framework/bench_runner.h"
#include "framework/framework_cfg.h"

namespace nocc {
namespace oltp {
namespace ch {

class ChRunner : public Runner {
 public:
  ChRunner() { };
  virtual std::vector<BenchLoader *> make_loaders(int partition, MemDB* store = nullptr);
  virtual std::vector<BenchLoader *>
      make_backup_loaders(int partition, BackupDB *store);
  virtual std::vector<BenchWorker *> make_workers();
  virtual std::vector<BenchClient *> make_clients();
  virtual std::vector<LogWorker *> make_backup_workers();
  virtual std::vector<QueryWorker *> make_qry_workers();
  virtual std::vector<AnalyticsWorker *> make_ana_workers();
  virtual void init_store(MemDB* &store);
  virtual void init_backup_store(BackupDB &store);

  virtual void warmup_buffer(char *);

  // for graph 
  virtual std::vector<GraphLoader *>
    make_graph_loaders(int partition,
                       graph::GraphStore *graph_store,
                       graph::RGMapping *mapping) override;
  virtual void init_graph_store(graph::GraphStore *graph_store,
                                graph::RGMapping *rg_map) override;
};

}  // namespace ch
}  // namespace oltp
}  // namespace nocc

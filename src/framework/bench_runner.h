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

#ifndef BENCH_RUNNER_H_
#define BENCH_RUNNER_H_

#include "bench_loader.h"
#include "bench_worker.h"
#include "bench_query.h"
#include "bench_analytics.h"
#include "backup_worker.h"

#include "memstore/memdb.h"
#include "backup_store/backup_db.h"

#include "graph/ddl.h"
#include "graph/graph_store.h"

#include <vector>

namespace nocc {
namespace oltp {

/* Bench runner is used to bootstrap system */
class Runner {
 public:
  Runner() { };

  /* warm up the rdma buffer, can be replaced by the application */
  void run();

 protected:
  /*   below 2 functions are used to init data structure.
       The first is called before any RDMA connections are made, which is used 
       to init data structures resides on RDMA registered area.
       The second is called after RDMA connections are made, which is used to 
       init global data structure that needs RDMA for communication
  */
  virtual void warmup_buffer(char *buffer) { }

  virtual std::vector<BenchLoader *> 
    make_loaders(int partition, MemDB *store) = 0;

  virtual std::vector<BenchWorker *> make_workers() = 0;
  virtual std::vector<BenchClient *> make_clients() {
    assert(false);
    std::vector<BenchClient *> ret;
    return ret;
  }
  
  virtual std::vector<BenchLoader *> 
  make_backup_loaders(int partition, BackupDB *backup_store) { 
    return std::vector<BenchLoader *>(); 
  }
  
  virtual std::vector<GraphLoader *> 
  make_graph_loaders(int partition, 
                     graph::GraphStore *graph_store,
                     graph::RGMapping *mapping) { 
    return std::vector<GraphLoader *>(); 
  }

  virtual std::vector<LogWorker *> make_backup_workers() {
    return std::vector<LogWorker *>();
  }

  virtual std::vector<QueryWorker *> make_qry_workers() {
    return std::vector<QueryWorker *>();
  }

  virtual std::vector<AnalyticsWorker *> make_ana_workers() {
    return std::vector<AnalyticsWorker *>();
  }

  virtual void init_store(MemDB* &store) = 0;  // inital schema
  virtual void init_backup_store(MemDB* &store) { }
  virtual void init_backup_store(BackupDB &store) { }

  virtual void init_graph_store(graph::GraphStore *graph_store,
                                graph::RGMapping *rg_map) { }

  std::vector<MemDB *> stores_;
  std::vector<BackupDB *> backup_stores_;

  // for graph
  std::vector<graph::GraphStore *> graph_stores_;
  std::vector<graph::RGMapping *> rg_maps_;

 private:
  void load_primary_partitions_(int mac_id);
  void load_backup_partitions_(int mac_id);
  void load_graph_partitions_(int mac_id);
};

}  // namesapce oltp
}  // namespace nocc

#endif  // BENCH_RUNNER_H_

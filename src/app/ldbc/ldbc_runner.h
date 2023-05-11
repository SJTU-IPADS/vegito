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
namespace ldbc {

class LDBCRunner : public Runner {
 public:
  LDBCRunner();
  
  // RDMA buffer preprocess
  virtual void warmup_buffer(char *) override;

  // worker threads
  virtual std::vector<BenchWorker *> make_workers() override;
  virtual std::vector<BenchClient *> make_clients() override;
  virtual std::vector<LogWorker *> make_backup_workers() override;
  virtual std::vector<QueryWorker *> make_qry_workers() override;
  virtual std::vector<AnalyticsWorker *> make_ana_workers() override;

  // relational partitions: not used for LDBC 
  virtual std::vector<BenchLoader *> 
  make_loaders(int partition, MemDB* store = nullptr) override;

  virtual std::vector<BenchLoader *>
      make_backup_loaders(int partition, BackupDB *store) override {
    return std::vector<BenchLoader *> ();
  }

  virtual void init_store(MemDB* &store) override { };
  virtual void init_backup_store(BackupDB &store) override { };

  // for graph 
  virtual std::vector<GraphLoader *>
    make_graph_loaders(int partition,
                       graph::GraphStore *graph_store,
                       graph::RGMapping *mapping) override;
  virtual void init_graph_store(graph::GraphStore *graph_store,
                                graph::RGMapping *rg_map) override;

 private:
  struct EdgeDef {
    std::string file_name;
    int etype;
    int src_vtype;
    int dst_vtype;
    int num_prop = 0;
    bool is_dir = true;  // true: directed, false: undirected
    size_t prop_size = 0;
  
    EdgeDef() { };
    EdgeDef(std::string f, int e, int s, int d, int p = 0, bool isd = true, size_t p_sz = 0)
      : file_name(f), etype(e), src_vtype(s), dst_vtype(d), num_prop(p),
        is_dir(isd), prop_size(p_sz) { }
  };
  std::vector<EdgeDef> edge_meta_;
};

}  // namespace ldbc
}  // namespace oltp
}  // namespace nocc

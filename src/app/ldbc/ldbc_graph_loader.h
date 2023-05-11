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

#include "ldbc_schema.h"

#include <string>
#include <vector>

#include "core/transaction.hpp"
#include "framework/bench_loader.h"
#include "framework/framework_cfg.h"

namespace nocc {
namespace oltp {
namespace ldbc {

/* Loaders */
#define VLoader(vlabel) \
  class vlabel##Loader : public GraphLoader { \
   public: \
    vlabel##Loader(std::string csv_file, int partition, \
          graph::GraphStore* graph_store, graph::RGMapping* mapping) \
      : GraphLoader(0, graph_store, mapping), csv_file_(csv_file) { \
    partition_ = partition; \
  } \
  \
   protected: \
    virtual void load() override { \
      if(framework::config.isUseSegGraph()) { \
        load_graph<livegraph::SegGraph>(graph_store_); \
      } else { \
        load_graph<livegraph::Graph>(graph_store_); \
      } \
    } \
  \
    template <class GraphType> \
    void load_graph(graph::GraphStore* graph_store); \
  \
   private: \
    void parse_cid_(const std::vector<std::string> &headers, std::vector<int> &cids); \
    const std::string csv_file_; \
};

/**************** Vertex ******************/
// Static
VLoader(Organisation);
VLoader(Place);
VLoader(Tag);
VLoader(TagClass);

// Dynamic
VLoader(Person);
VLoader(Comment);
VLoader(Post);
VLoader(Forum);

/**************** Edge ******************/
class EdgeLoader : public GraphLoader {
 public:
  EdgeLoader(std::string csv_file,
                   int elabel, 
                   int src_vtype, int dst_vtype,
                   int num_prop, bool is_dir,
                   int partition, int load_pct,
                   graph::GraphStore* graph_store, 
                   graph::RGMapping* mapping)
      : GraphLoader(0, graph_store, mapping, false), csv_file_(csv_file),
        elabel_(elabel), src_vtype_(src_vtype), dst_vtype_(dst_vtype), 
        num_prop_(num_prop), is_dir_(is_dir), load_pct_(load_pct) {
    assert(num_prop_ == 0 || num_prop_ == 1); 
    assert(load_pct >= 0 && load_pct <= 100);
    partition_ = partition;    
  }

  static std::vector<std::string> old_inserted_edges[ETYPE_NUM];  // XXX: cache align?

 protected:
  virtual void load() override {
    if(framework::config.isUseSegGraph()) {
      load_graph<livegraph::SegGraph>(graph_store_);
    } else {
      load_graph<livegraph::Graph>(graph_store_);
    }
  }

  template <class GraphType> 
  void load_graph(graph::GraphStore* graph_store);

 private:
  void deal_prop(const std::string &word, std::string &buf);

  const std::string csv_file_;
  const int elabel_;
  const int src_vtype_;
  const int dst_vtype_;
  const int num_prop_;
  const bool is_dir_;
  const int load_pct_;
};

// loader for inserted edges used by TP
class TpEdgeLoader : public BenchLoader {
 public:
  TpEdgeLoader(std::string csv_file,
                   int elabel, 
                   int src_vtype, int dst_vtype,
                   int num_prop, bool is_dir,
                   int partition, int load_pct)
      : BenchLoader(0, (MemDB *) nullptr), csv_file_(csv_file),
        elabel_(elabel), src_vtype_(src_vtype), dst_vtype_(dst_vtype), 
        num_prop_(num_prop), is_dir_(is_dir), load_pct_(load_pct) {
    assert(num_prop_ == 0 || num_prop_ == 1); 
    assert(load_pct >= 0 && load_pct <= 100);
    partition_ = partition;    
  }

  static std::vector<std::string> inserted_edges[ETYPE_NUM];  // XXX: cache align?

 protected:
  virtual void load() override;

 private:
  void deal_prop(const std::string &word, std::string &buf);

  const std::string csv_file_;
  const int elabel_;
  const int src_vtype_;
  const int dst_vtype_;
  const int num_prop_;
  const bool is_dir_;
  const int load_pct_;
};

// not used
#if 0
class KnowsLoader : public GraphLoader {
 public:
  KnowsLoader(std::string csv_file, int partition, 
              graph::GraphStore* graph_store, graph::RGMapping* mapping)
      : GraphLoader(0, graph_store, mapping, false), csv_file_(csv_file) { 
    partition_ = partition;    
  }

 protected:
  virtual void load() override {
    if(framework::config.isUseSegGraph()) {
      load_graph<livegraph::SegGraph>(graph_store_);
    } else {
      load_graph<livegraph::Graph>(graph_store_);
    }
  }

  template <class GraphType> 
  void load_graph(graph::GraphStore* graph_store);

 private:
  void parse_cid_(const std::vector<std::string> &headers, std::vector<int> &cids); 
  const std::string csv_file_;
};
#endif

}  // namespace ldbc
}  // namespace oltp
}  // namespace nocc

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

#include <cassert>
#include <vector>
#include <unordered_map>

#include "framework/framework_cfg.h"
#include "core/livegraph.hpp"
#include "backup_store/backup_store.h"
#include "backup_store/backup_store_kv.h"
#include "backup_store/backup_store_row.h"
#include "backup_store/backup_store_col.h"
#include "backup_store/backup_store_flex.h"
#include "backup_store/backup_store_col2.h"
#include "util/util.h"

#ifdef WITH_V6D
#include "fragment/shared_storage.h"
#endif

using namespace nocc::util;

namespace nocc {
namespace graph {

struct SchemaImpl {
  // property name -> property idx
  std::unordered_map<std::string, int> property_id_map;
  // vertex label -> property offset
  std::unordered_map<int, int> vlabel2prop_offset;
  // label name -> label id
  std::unordered_map<std::string, int> label_id_map;
  // <label id, property idx> -> dtype
  std::map<std::pair<int, int>, BackupStoreDataType> dtype_map;
};

class GraphStore {

public:
  GraphStore() {}

  template<class GraphType>
  GraphType* get_graph(uint64_t vlabel);

  inline BackupStore* get_property(uint64_t vlabel) {
    return property_stores_[vlabel];
  }

  inline BackupStore* get_property_snapshot(uint64_t vlabel, uint64_t version) {
    if(property_stores_snapshots_.count({vlabel, version}))
      return property_stores_snapshots_[{vlabel, version}];
    else
      return nullptr;
  }

  inline BackupDB::Schema get_property_schema(uint64_t vlabel) {
    return property_schemas_[vlabel];
  }

  inline void set_schema(SchemaImpl schema) {
    this->schema_ = schema;
  }

  inline SchemaImpl get_schema() {
    return this->schema_;
  }

  void update_offset() {
    for (auto [vlabel, property] : property_stores_) {
      if (property) property->updateHeader();
    }
  }

  inline void add_vgraph(uint64_t vlabel, RGMapping* rg_map) {
    if(framework::config.isUseSegGraph()) {
      seg_graphs_[vlabel] = new livegraph::SegGraph(rg_map);
#ifdef WITH_V6D
      auto &blob_schema = seg_graphs_[vlabel]->get_blob_schema();
      blob_schema.set_vlabel(vlabel);
      blob_schemas_[vlabel] = blob_schema;
#endif
    } else {
      live_graphs_[vlabel] = new livegraph::Graph();
    }
  }

  inline void get_blob_json() const {
#ifdef WITH_V6D
    using namespace rapidjson;
    Document dom;
    dom.SetArray();
    for (const auto &pair : blob_schemas_) {
      uint64_t vlabel = pair.first;
      Value val = pair.second.json(dom.GetAllocator());
      dom.PushBack(val, dom.GetAllocator());
    }
    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    dom.Accept(writer);
    std::cout << buffer.GetString() << std::endl;
#endif
  }

  inline void add_vprop(uint64_t vlabel, BackupDB::Schema schema) {
    property_schemas_[vlabel] = schema;
    switch(schema.store_type) {
      case BSTORE_KV: {
        property_stores_[vlabel] = new BackupStoreKV(schema);
        break;
      }
      case BSTORE_COLUMN: {
        property_stores_[vlabel] = new BackupStoreCol(schema);
        break;
      }
      case BSTORE_COLUMN2: {
        property_stores_[vlabel] = new BackupStoreCol2(schema);
        break;
      }
      default:
        printf("Not implement this backup store type: %d\n", schema.store_type);
        assert(false);
    }
  }

  inline void merge_columns(uint64_t vlabel, uint64_t version, const std::vector<std::vector<uint32_t>> &merges) {
    if (property_stores_snapshots_.count({vlabel, version})) {
      free(property_stores_snapshots_[{vlabel, version}]);
    }
    BackupStore* store = new BackupStoreFlex(property_schemas_[vlabel], property_stores_[vlabel], version, merges);
    property_stores_snapshots_[{vlabel, version}] = store;
  }

  // TODO:
  livegraph::Transaction create_graph_writer(uint64_t vlabel, uint64_t write_epoch_id);
  //livegraph::EpochGraphWriter create_seggraph_writer(uint64_t vlabel, uint64_t write_epoch_id);

#ifdef WITH_V6D
  std::unordered_map<uint64_t, gart::BlobSchema> &get_blob_schemas() {
    return blob_schemas_;
  }
#endif

 private:
  static const int MAX_TABLES = 30;
  static const int MAX_COLS = 10;
  static const int MAX_VPROPS = 10;
  static const int MAX_VLABELS = 30;
  static const int MAX_ELABELS = 30;

  // graph store schema
  SchemaImpl schema_;

  // vlabel -> graph storage
  std::unordered_map<uint64_t, livegraph::SegGraph*> seg_graphs_;
  std::unordered_map<uint64_t, livegraph::Graph*> live_graphs_;

  // vlabel -> vertex property storage
  std::unordered_map<uint64_t, BackupStore*> property_stores_;
  std::unordered_map<uint64_t, BackupDB::Schema> property_schemas_;

#ifdef WITH_V6D
  // vlabel -> vertex blob schemas
  std::unordered_map<uint64_t, gart::BlobSchema> blob_schemas_;
#endif

  // (vlabel, version) -> vertex property storage snapshot
  std::map<std::pair<uint64_t, uint64_t>, BackupStore*> property_stores_snapshots_;
};

}  // namespace graph
}  // namespace nocc

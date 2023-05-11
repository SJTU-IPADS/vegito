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

#include "ldbc_log_cleaner.h"

#include "app/ldbc/ldbc_schema.h"
#include "framework/framework_cfg.h"
#include "framework/view_manager.h"

#include "core/transaction.hpp"

#include <stdexcept>
#include <vector>

using namespace std;
using namespace nocc::framework;

using namespace livegraph;
using namespace nocc::graph;

namespace nocc {
namespace oltp {
namespace ldbc {

LDBCLogCleaner::LDBCLogCleaner()
    : LogCleaner(true) {
  // ware_tp_col_ = { W_YTD };
  // const int M = config.getColSplitType();  // always M == 2 
}

int LDBCLogCleaner::clean_log(int log_id, int partition_id, int tx_id,
                            int table_id, uint64_t key, uint64_t seq, char *val,
                            int length, uint32_t op_bit, uint64_t write_epoch) {
  assert (op_bit == LOG_OP_I && seq == 2);

  if (view.is_gp(log_id)) {
    assert(key == 233);   // XXX: magic number
    if(config.isUseSegGraph()) {
      return clean_graph_log<SegGraph>(partition_id, table_id,
                                                  val, length, write_epoch);
    } else {
      return clean_graph_log<Graph>(partition_id, table_id,
                                               val, length, write_epoch);
    }
  }
}

// TODO: now only support edge insertion (w/o vertex)
template <class GraphType>
int LDBCLogCleaner::clean_graph_log(int partition_id, int table_id,
                                    char *val, int length,
                                    uint64_t write_epoch) {

  GraphStore *graph_store = graph_stores_[partition_id];
  const RGMapping* mapping = mappings_[partition_id];

  // TODO: default table_id == elabel
  int elabel = table_id;
  const EdgeMeta &meta = mapping->get_edge_meta(elabel);
  int src_type = meta.src_vlabel;
  int dst_type = meta.dst_vlabel; 
  
  GraphType *p_src_graph = graph_store->get_graph<GraphType>(src_type);
  GraphType *p_dst_graph = graph_store->get_graph<GraphType>(dst_type);
  // this `auto` is necessary, the p_write may Transaction or EpochGraphWriter
  auto p_src_writer = p_src_graph->create_graph_writer(write_epoch);  // write epoch
  auto p_dst_writer = p_dst_graph->create_graph_writer(write_epoch);  // write epoch

  const EdgeProp *edgeProp = reinterpret_cast<const EdgeProp *>(val);
  uint64_t prop_sz = EdgeProp::prop_size(length);
  uint64_t src_key = edgeProp->src_key;
  uint64_t dst_key = edgeProp->dst_key;
    
  // insert edge
  uint64_t p1_vid = mapping->get_key2vid(src_type, src_key),
           p2_vid = mapping->get_key2vid(dst_type, dst_key);

  // p_property->insert(p_v, p_key.id, (char *) &p_value, write_seq, write_epoch);
#if FRESHNESS == 1
  if (elabel == FORUM_HASMEMBER) {
    uint64_t f_id = 893353198399l;  // SF = 0.1
    // if (src_key == f_id) {
    //   printf("insert traced fourm @epoch %lu\n", write_epoch);
    // }
    assert(prop_sz == sizeof(uint64_t));
    string_view prop(edgeProp->prop, prop_sz);
    uint64_t *ts_ptr = (uint64_t *) &edgeProp->prop[0];
    assert(*ts_ptr != 0);
    // TODO: add property
    p_src_writer.put_edge(p1_vid, elabel, EOUT, p2_vid, prop);
    p_dst_writer.put_edge(p2_vid, elabel, EIN, p1_vid, prop);
  } else {
    p_src_writer.put_edge(p1_vid, elabel, EOUT, p2_vid);
    p_dst_writer.put_edge(p2_vid, elabel, EIN, p1_vid);
  }
#else
  // TODO: Add property
  string_view prop(edgeProp->prop, prop_sz);
  // TODO: this two lines are performance bottleneck

#ifdef WITH_EDGE_PROP
  if(elabel == LIKES_POST) {
    double distance = 1.0;
    std::string_view edge_data((char*)&distance, sizeof(double));
    p_src_writer.put_edge(p1_vid, elabel, EOUT, p2_vid, edge_data);
    p_dst_writer.put_edge(p2_vid, elabel, EIN, p1_vid, edge_data);

    // add another direction for undirected edges
    p_dst_writer.put_edge(p2_vid, elabel, EOUT, p1_vid, edge_data);  // src of another dir
    p_src_writer.put_edge(p1_vid, elabel, EIN, p2_vid, edge_data);   // dst of another dir
  } else {
    p_src_writer.put_edge(p1_vid, elabel, EOUT, p2_vid);
    p_dst_writer.put_edge(p2_vid, elabel, EIN, p1_vid);

    if (meta.undirected) {
      // add another direction for undirected edges
      p_dst_writer.put_edge(p2_vid, elabel, EOUT, p1_vid);  // src of another dir
      p_src_writer.put_edge(p1_vid, elabel, EIN, p2_vid);   // dst of another dir
    }
  }
#else
  p_src_writer.put_edge(p1_vid, elabel, EOUT, p2_vid);
  p_dst_writer.put_edge(p2_vid, elabel, EIN, p1_vid);

  if (meta.undirected) {
    // add another direction for undirected edges
    p_dst_writer.put_edge(p2_vid, elabel, EOUT, p1_vid);  // src of another dir
    p_src_writer.put_edge(p1_vid, elabel, EIN, p2_vid);   // dst of another dir
  }
#endif
#endif

  return 0;
}

template
int LDBCLogCleaner::clean_graph_log<SegGraph>(int partition, int table_id, 
                                              char *val, int length,
                                              uint64_t wepoch);
template
int LDBCLogCleaner::clean_graph_log<Graph>(int partition, int table_id, 
                                              char *val, int length,
                                              uint64_t wepoch);

} // namespace ldbc
} // namespace oltp
} // namesapce nocc

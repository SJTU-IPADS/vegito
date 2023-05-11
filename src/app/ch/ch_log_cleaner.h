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

#ifndef NOCC_OLTP_CH_LOG_CLEANER_H_
#define NOCC_OLTP_CH_LOG_CLEANER_H_

#include "ch_worker.h"
#include "ch_mixin.h"
#include "ch_schema.h"
#include "framework/log_cleaner.h"
#include "core/livegraph.hpp"

#include <vector>

#define GEN_O_OL_EDGE 0
#define GEN_C_O_EDGE 0
#define GEN_C_I_EDGE 1

extern size_t current_partition;

namespace nocc {
namespace oltp {
namespace ch {

class ChLogCleaner : public LogCleaner {
 public:
  ChLogCleaner();
  virtual int clean_log(int table_id, uint64_t key, 
                        uint64_t seq, char *val,int length);
  virtual int clean_log(int log_id, int partition_id, int tx_id,
                        int table_id, uint64_t key, uint64_t seq, 
                        char *val, int length, uint32_t op_bit, 
                        uint64_t write_epoch);

  template <class GraphType>
  int clean_graph_log(nocc::graph::GraphStore* graph,
                      int log_id, int partition_id, int tx_id,
                      int table_id, uint64_t key, uint64_t seq, 
                      char *val, int length, uint32_t op_bit, 
                      uint64_t write_epoch);

  virtual void balance_index();
  virtual void prepare_balance(int num_workers);
  virtual void parallel_balance(int worker_id);
  virtual void end_balance();

  // virtual void balance_index(const std::vector<int> &threads);
  // TODO: 240 is so small
  // uint64_t last_no_o_ids_[240][10];

 private:
  std::vector<int> ware_tp_col_;
  std::vector<int> dist_tn_col_;
  std::vector<int> dist_tp_col_;
  std::vector<int> cust_tp_col_;
  std::vector<int> cust_td_col_;
  std::vector<int> orde_td_col_;
  std::vector<int> orli_td_col_;
  std::vector<int> stoc_tn_col_;

};

template <class GraphType>
int ChLogCleaner::clean_graph_log(nocc::graph::GraphStore* graph_store,
                                  int log_id, int partition_id, int tx_id,
                                  int table_id, uint64_t key, uint64_t seq, char *val,
                                  int length, uint32_t op_bit, uint64_t write_epoch) {
  if (op_bit != LOG_OP_I) return 0;
  nocc::graph::RGMapping *mapping = mappings_[partition_id];

  int vlabel = mapping->get_vlabel(table_id);
  if (vlabel == nocc::graph::RGMapping::NO_EXIST) return 0;

  assert(vlabel == ORDE || vlabel == ORLI);

  auto graph = graph_store->get_graph<GraphType>(vlabel);
  auto property_store = graph_store->get_property(vlabel);
  auto writer = graph->create_graph_writer(write_epoch);

  auto order_graph = graph_store->get_graph<GraphType>(ORDE);
  auto orderline_graph = graph_store->get_graph<GraphType>(ORLI);
  auto customer_graph = graph_store->get_graph<GraphType>(CUST);
  auto item_graph = graph_store->get_graph<GraphType>(ITEM);
  auto order_property = graph_store->get_property(ORDE);
  auto orderline_property = graph_store->get_property(ORLI);
  auto order_writer = order_graph->create_graph_writer(write_epoch);
  auto orderline_writer = orderline_graph->create_graph_writer(write_epoch);
  auto customer_writer = customer_graph->create_graph_writer(write_epoch);
  auto item_writer = item_graph->create_graph_writer(write_epoch);

  /* put vertex */
  livegraph::vertex_t vid;
  {
    vid = writer.new_vertex();

    // build vertex property
    double pr_init_val = 1.0, sp_init_val = 1000000000, cc_init_val = vid;
    string vprops;
    vprops.append((char*)&pr_init_val, sizeof(pr_init_val));
    vprops.append((char*)&sp_init_val, sizeof(sp_init_val));
    vprops.append((char*)&cc_init_val, sizeof(cc_init_val));
    vprops.append(val, length);
    
    property_store->insert(vid, key, vprops.data(), 0, 0);
    mapping->set_key2vid(vlabel, key, (uint64_t) vid);
  }

  /* put 1->N edge */
  nocc::graph::EdgeMeta meta;
  int elabel = mapping->get_edge_meta(vlabel, meta);
  if (vlabel == ORLI) {
#if GEN_O_OL_EDGE
    // int src_tableid = mapping->get_table(meta.src_vlabel);
    // assert(src_tableid == ORDE);
    // assert(elabel == O_OL);

    int src_tableid = ORDE;
    // TODO: get order id
    uint64_t order_key = orderLineKeyToOrderKey(key);  // XXX: hard code
    livegraph::vertex_t order_vid = (livegraph::vertex_t) mapping->get_key2vid(src_tableid, order_key); 
    //TODO [YZH]: edge property
    order_writer.put_edge(order_vid, O_OL, vid);
    orderline_writer.put_edge(vid, O_OL_R, order_vid);
#endif
  } else if (vlabel == ORDE) {
#if GEN_C_O_EDGE
    // int src_tableid = mapping->get_table(meta.src_vlabel);
    // assert(src_tableid == CUST);
    // assert(elabel == C_O);

    int src_tableid = CUST;
    uint64_t customer_key = makeCustomerKey(orderKeyToWare(key), orderKeyToDistrict(key), ((oorder::value *)val)->o_c_id);  // XXX: hard code
    livegraph::vertex_t customer_vid = (livegraph::vertex_t) mapping->get_key2vid(CUST, customer_key); 
    customer_writer.put_edge(customer_vid, C_O, vid);
    order_writer.put_edge(vid, C_O_R, customer_vid);
#endif
  }

  /* put N->N edge */
  if(vlabel == ORLI) {
#if GEN_C_I_EDGE
    int src_tableid = CUST;
    int dst_tableid = ITEM;

    livegraph::vertex_t c_vid = (livegraph::vertex_t) mapping->get_key2vid(src_tableid, ((order_line::value *)val)->ol_c_key); 
    livegraph::vertex_t i_vid = (livegraph::vertex_t) mapping->get_key2vid(dst_tableid, ((order_line::value *)val)->ol_i_id); 

#ifdef WITH_EDGE_PROP
    double distance = 1.0;
    std::string_view edge_data((char*)&distance, sizeof(double));
    customer_writer.put_edge(c_vid, C_I, i_vid, edge_data);
    item_writer.put_edge(i_vid, C_I_R, c_vid, edge_data);
#else
    customer_writer.put_edge(c_vid, C_I, i_vid);
    item_writer.put_edge(i_vid, C_I_R, c_vid);
#endif
#endif
  }
  return 0;
}

}  // namesapce ch
}  // namespace oltp
}  // namespace nocc


#endif  // NOCC_OLTP_CH_LOG_CLEANER_H_

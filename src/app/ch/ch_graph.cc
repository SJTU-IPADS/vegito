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

#include "app/ch/ch_schema.h"
#include "backup_store/backup_store.h"
#include "ch_runner.h"
#include "ch_mixin.h"

#include "core/transaction.hpp"

using namespace livegraph;
using namespace std;
using namespace nocc::graph;

namespace nocc {
namespace oltp {
namespace ch {

class ChGraphLoader : public GraphLoader, public ChMixin {
 public:
  ChGraphLoader(uint64_t seed, int partition, GraphStore* graph_store, RGMapping* mapping)
      : GraphLoader(seed, graph_store, mapping) { 
    partition_ = partition;
  }

 protected:
  virtual void load() override;

  template <class GraphType> 
  void load_graph(graph::GraphStore* graph_store);
};

void ChGraphLoader::load() {
  if(config.isUseSegGraph()) {
    load_graph<SegGraph>(graph_store_);
  } else {
    load_graph<Graph>(graph_store_);
  }
}

template <class GraphType> 
void ChGraphLoader::load_graph(graph::GraphStore* graph_store) {
  // TODO: a simple case
  uint64_t order_line_total_sz = 0, n_order_lines = 0;
  uint64_t oorder_total_sz = 0, n_oorders = 0;
  uint64_t new_order_total_sz = 0, n_new_orders = 0;

  const uint w_start = GetStartWarehouse(partition_);
  const uint w_end   = GetEndWarehouse(partition_);

  uint64_t o_i = 0;
  auto order_graph = graph_store->get_graph<GraphType>(ORDE);
  auto orderline_graph = graph_store->get_graph<GraphType>(ORLI);
  auto customer_graph = graph_store->get_graph<GraphType>(CUST);
  auto item_graph = graph_store->get_graph<GraphType>(ITEM);
  auto order_property = graph_store->get_property(ORDE);
  auto orderline_property = graph_store->get_property(ORLI);
  auto customer_property = graph_store->get_property(CUST);
  auto item_property = graph_store->get_property(ITEM);
  auto order_writer = order_graph->create_graph_writer(0);
  auto orderline_writer = orderline_graph->create_graph_writer(0);
  auto customer_writer = customer_graph->create_graph_writer(0);
  auto item_writer = item_graph->create_graph_writer(0);

  // create item vertices
  uint64_t total_sz = 0;
  for (uint i = 1; i <= NumItems(); i++) {
    item::value val;
    item::value *v = &val;
    const string i_name =
      RandomStr(rand_, RandomNumber(rand_, 14, 24));
    v->i_name.assign(i_name);
    v->i_price = (float) RandomNumber(rand_, 100, 10000) / 100.0;
    const int len = RandomNumber(rand_, 26, 50);
    if (RandomNumber(rand_, 1, 100) > 10) {
      const string i_data = RandomStr(rand_, len);
      v->i_data.assign(i_data);
    } else {
      const int startOriginal = RandomNumber(rand_, 2, (len - 8));
      const string i_data = RandomStr(rand_, startOriginal + 1) +
        "ORIGINAL" + RandomStr(rand_, len - startOriginal - 7);
      v->i_data.assign(i_data);
    }
    v->i_im_id = RandomNumber(rand_, 1, 10000);
    const size_t sz = sizeof(*v);
    total_sz += sz;

    vertex_t i_v = item_writer.new_vertex();
    auto i_off = item_property->getNewOffset();
    assert(i_off == i_v);
    mapping_->set_key2vid(ITEM, i, (uint64_t) i_v);
    double pr_init_val = 1.0, sp_init_val = 1000000000, cc_init_val = i_v;
    std::string i_vprops;
    i_vprops.append((char*)&pr_init_val, sizeof(pr_init_val));
    i_vprops.append((char*)&sp_init_val, sizeof(sp_init_val));
    i_vprops.append((char*)&cc_init_val, sizeof(cc_init_val));
    i_vprops.append((char*)&val, sizeof(val));
    item_property->insert(i_v, i, i_vprops.data(), 0, 0);
  }

  for (uint w = w_start; w <= w_end; w++) {
    for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {
      set<uint> c_ids_s;
      vector<uint> c_ids;
      while (c_ids.size() != NumCustomersPerDistrict()) {
        const uint x =
            (rand_.next() % NumCustomersPerDistrict()) + 1;
        if (c_ids_s.count(x))
          continue;
        c_ids_s.insert(x);
        c_ids.emplace_back(x);
      }

      for (uint c = 1; c <= NumCustomersPerDistrict(); c++) {
        // new customer
        uint64_t ckey = makeCustomerKey(w, d, c_ids[c - 1]);
        // new order
        uint64_t okey = makeOrderKey(w, d, c);
        const oorder::key k_oo { okey };

        oorder::value val_oo;
        oorder::value *v_oo = &val_oo;
        v_oo->o_c_id = c_ids[c - 1];
        if (c < 2101)
          v_oo->o_carrier_id = RandomNumber(rand_, 1, 10);
        else
          v_oo->o_carrier_id = 0;
        v_oo->o_ol_cnt = RandomNumber(rand_, 5, 15);

        v_oo->o_all_local = 1;
        v_oo->o_entry_d = GetCurrentTimeMillis();
        const size_t sz = sizeof(*v_oo);
        oorder_total_sz += sz;
        n_oorders++;

        // TODO: YZH: o_i (index), add it to column store
        auto o_schema = graph_store->get_property_schema(ORDE);
        auto c_schema = graph_store->get_property_schema(CUST);
        auto& o_val_lens = order_property->get_val_lens();
        auto& o_val_offs = order_property->get_val_offs();
        auto& c_val_lens = customer_property->get_val_lens();
        auto& c_val_offs = customer_property->get_val_offs();

        vertex_t c_v = customer_writer.new_vertex();
        vertex_t o_v = order_writer.new_vertex();
        // TODO: [YZH] build CUSTOMER property according prop mapping
        customer::value val_c;
        double pr_init_val = 1.0, sp_init_val = 1000000000, cc_init_val = c_v;
        std::string c_vprops;
        c_vprops.append((char*)&pr_init_val, sizeof(pr_init_val));
        c_vprops.append((char*)&sp_init_val, sizeof(sp_init_val));
        c_vprops.append((char*)&cc_init_val, sizeof(cc_init_val));
        c_vprops.append((char*)&val_c, sizeof(val_c));

        cc_init_val = o_v;
        std::string o_vprops;
        o_vprops.append((char*)&pr_init_val, sizeof(pr_init_val));
        o_vprops.append((char*)&sp_init_val, sizeof(sp_init_val));
        o_vprops.append((char*)&cc_init_val, sizeof(cc_init_val));
        o_vprops.append((char*)&val_oo, sizeof(val_oo));

        auto c_off = customer_property->getNewOffset();
        auto o_off = order_property->getNewOffset();
        assert(c_off == c_v);
        assert(o_off == o_v);
        customer_property->insert(c_v, ckey, c_vprops.data(), 0, 0);
        order_property->insert(o_v, okey, o_vprops.data(), 0, 0);
        mapping_->set_key2vid(CUST, ckey, (uint64_t) c_v);
        mapping_->set_key2vid(ORDE, okey, (uint64_t) o_v);

        // add customer-order edge
        customer_writer.put_edge(c_v, C_O, o_v);
        order_writer.put_edge(o_v, C_O_R, c_v);

#if 0
        // new order
        if (c >= 2101) {
          uint64_t nokey = makeNewOrderKey(w, d, c);
          new_order::value v_no;
          const size_t sz = sizeof(v_no);
          new_order_total_sz += sz;
          n_new_orders++;
          uint64_t no_i = backup_store_->Insert(NEWO, nokey, (char *) &v_no);
        }
#endif

        // order line
        for (uint l = 1; l <= uint(v_oo->o_ol_cnt); l++) {
          uint64_t olkey = makeOrderLineKey(w, d, c, l);

          order_line::value val_ol;
          order_line::value *v_ol = &val_ol;
          v_ol->ol_i_id = RandomNumber(rand_, 1, 100000);
          if (c < 2101) {
            v_ol->ol_delivery_d = v_oo->o_entry_d;
            assert(v_ol->ol_delivery_d != 0);
            v_ol->ol_amount = 0;
          } else {
            v_ol->ol_delivery_d = 0;
            /* random within [0.01 .. 9,999.99] */
            v_ol->ol_amount =
                (float) (RandomNumber(rand_, 1, 999999) / 100.0);
          }

          v_ol->ol_supply_w_id = w;
          v_ol->ol_quantity = 5;
          v_ol->ol_c_key = ckey;
          // v_ol.ol_dist_info comes from stock_data(ol_supply_w_id, ol_o_id)
          const size_t sz = sizeof(*v_ol);
          order_line_total_sz += sz;
          n_order_lines++;
          // uint64_t ol_i = backup_store_->Insert(ORLI, olkey, (char *) v_ol);
          assert(l <= 15);
          
          vertex_t ol_v = orderline_writer.new_vertex();
          double pr_init_val = 1.0, sp_init_val = 1000000000, cc_init_val = ol_v;
          std::string ol_vprops;
          ol_vprops.append((char*)&pr_init_val, sizeof(pr_init_val));
          ol_vprops.append((char*)&sp_init_val, sizeof(sp_init_val));
          ol_vprops.append((char*)&cc_init_val, sizeof(cc_init_val));
          ol_vprops.append((char*)&val_ol, sizeof(val_ol));

          auto ol_off = orderline_property->getNewOffset();
          assert(ol_off == ol_v);
          orderline_property->insert(ol_v, olkey, ol_vprops.data(), 0, 0);
          mapping_->set_key2vid(ORLI, olkey, (uint64_t) ol_v);

          // add order-orderline edge
          string o_ol_eprops;
          order_writer.put_edge(o_v, O_OL, ol_v);
          orderline_writer.put_edge(ol_v, O_OL_R, o_v);

          // add customer-item edge according to ol_c_key and ol_i_id
          vertex_t i_v = mapping_->get_key2vid(ITEM, v_ol->ol_i_id);
#ifdef WITH_EDGE_PROP
          double distance = 1.0;
          std::string_view edge_data((char*)&distance, sizeof(double));
          customer_writer.put_edge(c_v, C_I, i_v, edge_data);
          item_writer.put_edge(i_v, C_I_R, c_v, edge_data);
#else
          customer_writer.put_edge(c_v, C_I, i_v);
          item_writer.put_edge(i_v, C_I_R, c_v);
#endif
        }
      }
    }
  }
  printf("n_order_lines = %lu\n", n_order_lines);
  /* order loader */
}

// initial schema of graph
void ChRunner::init_graph_store(graph::GraphStore *graph_store, RGMapping* rg_map) {
  rg_map->define_vertex(CUST, CUST);
  rg_map->define_vertex(ITEM, ITEM);
  rg_map->define_vertex(ORDE, ORDE);  // (vlabel, table_id)
  rg_map->define_vertex(ORLI, ORLI);

  rg_map->define_1n_edge(C_O, CUST, ORDE, 0);   // FK 
  rg_map->define_1n_edge(C_O_R, ORDE, CUST, 0);   // FK 
  rg_map->define_1n_edge(O_OL, ORDE, ORLI, 0);   // FK 
  rg_map->define_1n_edge(O_OL_R, ORLI, ORDE, 0);   // FK 
  rg_map->define_nn_edge(C_I, CUST, ITEM, 0, 0);   // FK 
  rg_map->define_nn_edge(C_I_R, ITEM, CUST, 0, 0);   // FK 

  // add schema
  SchemaImpl tpcc_schema;

  // vertex type
  tpcc_schema.label_id_map["item"] = ITEM;
  tpcc_schema.label_id_map["customer"] = CUST;
  tpcc_schema.label_id_map["order"] = ORDE;
  tpcc_schema.label_id_map["orderline"] = ORLI;

  // edge type
  tpcc_schema.label_id_map["customer_item"] = C_I;
  tpcc_schema.label_id_map["order_orderline"] = O_OL;
  tpcc_schema.label_id_map["customer_order"] = C_O;

  // property
  tpcc_schema.property_id_map["c_pr"] = 0;
  tpcc_schema.property_id_map["c_sp"] = 1;
  tpcc_schema.property_id_map["c_cc"] = 2;
  tpcc_schema.property_id_map["c_balance"] = 3;
  tpcc_schema.property_id_map["c_ytd_payment"] = 4;
  tpcc_schema.property_id_map["c_payment_cnt"] = 5;
  tpcc_schema.property_id_map["c_delivery_cnt"] = 6;
  tpcc_schema.property_id_map["c_data"] = 7;
  tpcc_schema.property_id_map["c_discount"] = 8;
  tpcc_schema.property_id_map["c_credit"] = 9;
  tpcc_schema.property_id_map["c_last"] = 10;
  tpcc_schema.property_id_map["c_credit_lim"] = 11;
  tpcc_schema.property_id_map["c_street_1"] = 12;
  tpcc_schema.property_id_map["c_street_2"] = 13;
  tpcc_schema.property_id_map["c_city"] = 14;
  tpcc_schema.property_id_map["c_state"] = 15;
  tpcc_schema.property_id_map["c_zip"] = 16;
  tpcc_schema.property_id_map["c_phone"] = 17;
  tpcc_schema.property_id_map["c_since"] = 18;
  tpcc_schema.property_id_map["c_middle"] = 19;

  tpcc_schema.property_id_map["i_pr"] = 20;
  tpcc_schema.property_id_map["i_sp"] = 21;
  tpcc_schema.property_id_map["i_cc"] = 22;
  tpcc_schema.property_id_map["i_name"] = 23;
  tpcc_schema.property_id_map["i_price"] = 24;
  tpcc_schema.property_id_map["i_data"] = 25;
  tpcc_schema.property_id_map["i_im_id"] = 26;

  tpcc_schema.property_id_map["o_pr"] = 27;
  tpcc_schema.property_id_map["o_sp"] = 28;
  tpcc_schema.property_id_map["o_cc"] = 29;
  tpcc_schema.property_id_map["o_carrier_id"] = 30;
  tpcc_schema.property_id_map["o_c_id"] = 31;
  tpcc_schema.property_id_map["o_ol_cnt"] = 32;
  tpcc_schema.property_id_map["o_all_local"] = 33;
  tpcc_schema.property_id_map["o_entry_d"] = 34;

  tpcc_schema.property_id_map["ol_pr"] = 35;
  tpcc_schema.property_id_map["ol_sp"] = 36;
  tpcc_schema.property_id_map["ol_cc"] = 37;
  tpcc_schema.property_id_map["ol_delivery_d"] = 38;
  tpcc_schema.property_id_map["ol_i_id"] = 39;
  tpcc_schema.property_id_map["ol_amount"] = 40;
  tpcc_schema.property_id_map["ol_supply_w_id"] = 41;
  tpcc_schema.property_id_map["ol_quantity"] = 42;
  tpcc_schema.property_id_map["ol_c_key"] = 43;

  tpcc_schema.dtype_map[{CUST, 0}] = DOUBLE;
  tpcc_schema.dtype_map[{CUST, 1}] = DOUBLE;
  tpcc_schema.dtype_map[{CUST, 2}] = DOUBLE;
  tpcc_schema.dtype_map[{CUST, 3}] = FLOAT;
  tpcc_schema.dtype_map[{CUST, 4}] = FLOAT;
  tpcc_schema.dtype_map[{CUST, 5}] = INT;
  tpcc_schema.dtype_map[{CUST, 6}] = INT;
  tpcc_schema.dtype_map[{CUST, 7}] = STRING;
  tpcc_schema.dtype_map[{CUST, 8}] = FLOAT;
  tpcc_schema.dtype_map[{CUST, 9}] = STRING;
  tpcc_schema.dtype_map[{CUST, 10}] = STRING;
  tpcc_schema.dtype_map[{CUST, 11}] = STRING;
  tpcc_schema.dtype_map[{CUST, 12}] = FLOAT;
  tpcc_schema.dtype_map[{CUST, 13}] = STRING;
  tpcc_schema.dtype_map[{CUST, 14}] = STRING;
  tpcc_schema.dtype_map[{CUST, 15}] = STRING;
  tpcc_schema.dtype_map[{CUST, 16}] = STRING;
  tpcc_schema.dtype_map[{CUST, 17}] = STRING;
  tpcc_schema.dtype_map[{CUST, 18}] = STRING;
  tpcc_schema.dtype_map[{CUST, 19}] = INT;
  tpcc_schema.dtype_map[{CUST, 20}] = STRING;

  tpcc_schema.dtype_map[{ITEM, 0}] = DOUBLE;
  tpcc_schema.dtype_map[{ITEM, 1}] = DOUBLE;
  tpcc_schema.dtype_map[{ITEM, 2}] = DOUBLE;
  tpcc_schema.dtype_map[{ITEM, 3}] = STRING;
  tpcc_schema.dtype_map[{ITEM, 4}] = FLOAT;
  tpcc_schema.dtype_map[{ITEM, 5}] = STRING;
  tpcc_schema.dtype_map[{ITEM, 6}] = INT;

  tpcc_schema.dtype_map[{ORDE, 0}] = DOUBLE;
  tpcc_schema.dtype_map[{ORDE, 1}] = DOUBLE;
  tpcc_schema.dtype_map[{ORDE, 2}] = DOUBLE;
  tpcc_schema.dtype_map[{ORDE, 3}] = INT;
  tpcc_schema.dtype_map[{ORDE, 4}] = INT;
  tpcc_schema.dtype_map[{ORDE, 5}] = CHAR;
  tpcc_schema.dtype_map[{ORDE, 6}] = BOOL;
  tpcc_schema.dtype_map[{ORDE, 7}] = INT;

  tpcc_schema.dtype_map[{ORLI, 0}] = DOUBLE;
  tpcc_schema.dtype_map[{ORLI, 1}] = DOUBLE;
  tpcc_schema.dtype_map[{ORLI, 2}] = DOUBLE;
  tpcc_schema.dtype_map[{ORLI, 3}] = INT;
  tpcc_schema.dtype_map[{ORLI, 4}] = INT;
  tpcc_schema.dtype_map[{ORLI, 5}] = FLOAT;
  tpcc_schema.dtype_map[{ORLI, 6}] = INT;
  tpcc_schema.dtype_map[{ORLI, 7}] = CHAR;
  tpcc_schema.dtype_map[{ORLI, 8}] = LONG;

  graph_store->set_schema(tpcc_schema);

  // add v-topology
  graph_store->add_vgraph(ITEM, rg_map);
  graph_store->add_vgraph(CUST, rg_map);
  graph_store->add_vgraph(ORDE, rg_map);
  graph_store->add_vgraph(ORLI, rg_map);

  // add v-prop
  BackupDB::Schema schema;
  schema.klen = sizeof(uint64_t);
  schema.sec_index_type = NO_BINDEX;
  switch(config.getBackupStoreType()) {
    case 0:
      schema.store_type = BSTORE_KV;
      break;
    case 1:
      schema.store_type = BSTORE_ROW;
      break;
    case 2:
      schema.store_type = BSTORE_COLUMN;
      break;
    case 3:
      schema.store_type = BSTORE_COLUMN2;
      break;
    default:
      printf("No backup store type: %d\n", config.getBackupStoreType());
      assert(false);
      break;
  }
  size_t PAGE_SIZE = 4 * 1024;  // 1K items

  // item
  {
    schema.table_id = ITEM;
    item::value i;
    schema.cols = 
        { 
#if 1
          { sizeof(double), true, PAGE_SIZE / sizeof(double), NO_BINDEX },
          { sizeof(double), true, PAGE_SIZE / sizeof(double), NO_BINDEX },
          { sizeof(double), true, PAGE_SIZE / sizeof(double), NO_BINDEX },
#else
          { 3*sizeof(double), true, PAGE_SIZE / (3*sizeof(double)), NO_BINDEX},
#endif
          { sizeof(i.i_name), false },
          { sizeof(i.i_price), false },
          { sizeof(i.i_data), false },
          { sizeof(i.i_im_id), false }
        };
    schema.max_items = NumItems();

    graph_store->add_vprop(ITEM, schema);
  }

  // customer
  {
    customer::value c;
    schema.table_id = CUST;
    schema.cols = 
        {
#if 1
          { sizeof(double), true, PAGE_SIZE / sizeof(double), NO_BINDEX},
          { sizeof(double), true, PAGE_SIZE / sizeof(double), NO_BINDEX},
          { sizeof(double), true, PAGE_SIZE / sizeof(double), NO_BINDEX},
#else
          { 3*sizeof(double), true, PAGE_SIZE / (3*sizeof(double)), NO_BINDEX},
#endif
          { sizeof(c.c_balance), true, PAGE_SIZE / sizeof(c.c_balance) },
          { sizeof(c.c_ytd_payment), true, PAGE_SIZE / sizeof(c.c_ytd_payment) },
          { sizeof(c.c_payment_cnt), true, PAGE_SIZE / sizeof(c.c_payment_cnt) },
          { sizeof(c.c_delivery_cnt), true, PAGE_SIZE / sizeof(c.c_delivery_cnt) },
          { sizeof(c.c_data), true, 1 },
          { sizeof(c.c_discount), false },
          { sizeof(c.c_credit), false },
          { sizeof(c.c_last), false },
          { sizeof(c.c_first), false },
          { sizeof(c.c_credit_lim), false },
          { sizeof(c.c_street_1), false },
          { sizeof(c.c_street_2), false },
          { sizeof(c.c_city), false },
          { sizeof(c.c_state), false },
          { sizeof(c.c_zip), false },
          { sizeof(c.c_phone), false },
          { sizeof(c.c_since), false },
          { sizeof(c.c_middle), false }
        };
    schema.max_items = scale_factor * NumDistrictsPerWarehouse()
                       * NumCustomersPerDistrict();

    graph_store->add_vprop(CUST, schema);
  }

  // order
  {
    schema.table_id = ORDE;
    oorder::value oo;
    schema.cols = 
        { 
#if 1
          { sizeof(double), true, PAGE_SIZE / sizeof(double), NO_BINDEX },
          { sizeof(double), true, PAGE_SIZE / sizeof(double), NO_BINDEX },
          { sizeof(double), true, PAGE_SIZE / sizeof(double), NO_BINDEX },
#else
          { 3*sizeof(double), true, PAGE_SIZE / (3*sizeof(double)), NO_BINDEX},
#endif
          { .vlen = sizeof(oo.o_carrier_id), .updatable = true, .page_size = PAGE_SIZE / sizeof(oo.o_carrier_id) },
          { .vlen = sizeof(oo.o_c_id), .updatable = false },
          { .vlen = sizeof(oo.o_ol_cnt), .updatable = false },
          { .vlen = sizeof(oo.o_all_local), .updatable = false },
          { .vlen = sizeof(oo.o_entry_d), .updatable = false, .page_size = 0 }
        };
    schema.max_items = TimeScale;

    graph_store->add_vprop(ORDE, schema);
  }

  // order-line
  {
    schema.table_id = ORLI;
    order_line::value ol;
    schema.cols = 
        { 
#if 1
          { sizeof(double), true, PAGE_SIZE / sizeof(double), NO_BINDEX },
          { sizeof(double), true, PAGE_SIZE / sizeof(double), NO_BINDEX },
          { sizeof(double), true, PAGE_SIZE / sizeof(double), NO_BINDEX },
#else
          { 3*sizeof(double), true, PAGE_SIZE / (3*sizeof(double)), NO_BINDEX},
#endif
          { .vlen = sizeof(ol.ol_delivery_d), .updatable = true, .page_size = PAGE_SIZE / sizeof(ol.ol_delivery_d) },
          { .vlen = sizeof(ol.ol_i_id), .updatable = false },
          { .vlen = sizeof(ol.ol_amount), .updatable = false },
          { .vlen = sizeof(ol.ol_supply_w_id), .updatable = false },
          { .vlen = sizeof(ol.ol_quantity), .updatable = false },
          { .vlen = sizeof(ol.ol_c_key), .updatable = false }
        };
    schema.max_items = 15 * TimeScale;

    graph_store->add_vprop(ORLI, schema);
  }
}

vector<GraphLoader *>
ChRunner::make_graph_loaders(int partition, GraphStore* graph_store, RGMapping *mapping) { 
  vector<GraphLoader *> ret;

  // the seed is same as the ChOrderLoader 
  ret.push_back(new ChGraphLoader(2343352, partition, graph_store, mapping));

  return ret;
} 

}  // namespace ch
}  // namespace oltp
}  // namespace nocc

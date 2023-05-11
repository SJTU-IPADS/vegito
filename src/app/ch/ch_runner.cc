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

#include "ch_runner.h"

#include "ch_mixin.h"
#include "ch_loader.h"
#include "ch_worker.h"
#include "ch_query.h"
#include "ch_analytics.h"
#include "ch_log_cleaner.h"

namespace nocc {
namespace oltp {
namespace ch {

int start_w_id;
vector<WorkerBitMap> wid2tid;  // (w_id - start_w_id) -> ChWorker id

namespace {

uint64_t ware_hash_fn(uint64_t wid) {
  return (wid - 1) % config.getScaleFactorPerPartition();
}

uint64_t dist_hash_fn(uint64_t did) {
  uint64_t w = (did / 10 - 1) % config.getScaleFactorPerPartition();
  uint64_t d = (did - 1) % NumDistrictsPerWarehouse();
  return (w * NumDistrictsPerWarehouse() + d);
}

uint64_t cust_hash_fn(uint64_t cid) {
  uint64_t did = cid >> 32;
  uint64_t w = (did / 10 - 1) % config.getScaleFactorPerPartition();
  uint64_t d = (did - 1) % NumDistrictsPerWarehouse();
  uint64_t c = (cid & 0xffffffff) - 1;
  if (c >= NumCustomersPerDistrict()) {
    printf("cid: %lu, c: %lu\n", cid, c);
    assert(false);
  }
  return (w * NumDistrictsPerWarehouse() + d) * NumCustomersPerDistrict() + c;
}

uint64_t item_hash_fn(uint64_t iid) {
  return (iid - 1);
}

uint64_t stoc_hash_fn(uint64_t sid) {
  uint64_t w = (sid / NumItems() - 1) % config.getScaleFactorPerPartition();
  uint64_t d = (sid - 1) % NumItems();
  return (w * NumItems() + d);
}

uint64_t orde_hash_fn(uint64_t oid) {
  uint64_t did = oid >> 32;
  uint64_t w = (did / 10 - 1) % config.getScaleFactorPerPartition();
  uint64_t d = (did - 1) % NumDistrictsPerWarehouse();
  uint64_t o = (oid << 32 >> 32) - 1;
  if (o >= TimeScale2) {
    printf("No enough order memory!\n");
    assert(false);
  }
  return (w * 10 + d) * TimeScale2 + o;
}

uint64_t orli_hash_fn(uint64_t olid) {
  uint64_t num = (olid - 1) % 15;
  uint64_t oid = (olid - 1) / 15;
  uint64_t o = (oid - 1) % 10000000;
  assert(o < TimeScale2);

  uint64_t did = (oid - 1) / 10000000;
  uint64_t w = (did / 10 - 1) % config.getScaleFactorPerPartition();
  uint64_t d = (did - 1) % NumDistrictsPerWarehouse();
  return ((w * 10 + d) * TimeScale2 + o) * 15 + num;

}

}  /* End of anonymous namespace*/

int GetStartWarehouse(int partition) {
  return partition * config.getScaleFactorPerPartition() + 1;
}

int GetEndWarehouse(int partition) {
  return (partition + 1) * config.getScaleFactorPerPartition();
}

int NumWarehouses() {
  return (int)(config.getScaleFactorPerPartition() * config.getNumPrimaries());
}

int WarehouseToPartition(int wid) {
  return (wid - 1) / config.getScaleFactorPerPartition();
}

void ChRunner::init_store(MemDB* &store){
  assert(store == nullptr);
  store = new MemDB();
  // int meta_size = radGetMetalen(); // for SI
  int meta_size = 0;

  store->AddSchema(WARE,TAB_BTREE,sizeof(uint64_t),sizeof(warehouse::value),meta_size);
  store->AddSchema(DIST,TAB_BTREE,sizeof(uint64_t),sizeof(district::value),meta_size);
  store->AddSchema(STOC,TAB_BTREE,sizeof(uint64_t),sizeof(stock::value),meta_size);

  if (ONE_SIDED) {
    store->EnableRemoteAccess(WARE,cm);
    store->EnableRemoteAccess(DIST,cm);
    store->EnableRemoteAccess(STOC,cm);
  }

  store->AddSchema(CUST,TAB_BTREE,sizeof(uint64_t),sizeof(customer::value),meta_size);
  store->AddSchema(HIST,TAB_BTREE,sizeof(uint64_t),sizeof(history::value),meta_size);
  store->AddSchema(NEWO,TAB_BTREE,sizeof(uint64_t),sizeof(new_order::value),meta_size);
  store->AddSchema(ORDE,TAB_BTREE,sizeof(uint64_t),sizeof(oorder::value),meta_size);
  store->AddSchema(ORLI,TAB_BTREE,sizeof(uint64_t),sizeof(order_line::value),meta_size);
  store->AddSchema(ITEM,TAB_BTREE,sizeof(uint64_t),sizeof(item::value),meta_size);

  // secondary index
  store->AddSchema(CUST_INDEX,TAB_BTREE1, 5,16,meta_size);
  store->AddSchema(ORDER_INDEX,TAB_BTREE, sizeof(uint64_t),16,meta_size);
}

void ChRunner::init_backup_store(BackupDB &store) {
  bool ap = store.isAP();
  BackupDB::Schema schema;
  schema.klen = sizeof(uint64_t);
  schema.sec_index_type = NO_BINDEX;
  const int M = config.getColSplitType();  // always M == 2 
  size_t PAGE_SIZE = 4 * 1024;  // 1K items 
  if (M == 0) PAGE_SIZE = uint64_t(-1);
  if (M == 3) PAGE_SIZE = uint64_t(1);
  int scale_factor = config.getScaleFactorPerPartition();

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
  if (!ap)
    schema.store_type = BSTORE_KV;

  // 0 warehouse
  {
    warehouse::value w;
    schema.cols = 
        { { sizeof(w.w_ytd), true, PAGE_SIZE },
          { sizeof(w.w_tax), false },
          { sizeof(w.w_name), false },
          { sizeof(w.w_street_1), false },
          { sizeof(w.w_street_2), false },
          { sizeof(w.w_city), false },
          { sizeof(w.w_state), false },
          { sizeof(w.w_zip), false } };
    schema.max_items = scale_factor;
    schema.hash_fn = ware_hash_fn;

    // schema.index_type = BINDEX_BTREE;
    schema.index_type = BINDEX_HASH;
    store.AddSchema(WARE, schema);
  }

  // 1 district
  {
    district::value d;
    schema.cols = 
        { { sizeof(d.d_ytd), true, PAGE_SIZE / sizeof(d.d_ytd) },
          { sizeof(d.d_next_o_id), true, PAGE_SIZE / sizeof(d.d_next_o_id) },
          { sizeof(d.d_tax), false },
          { sizeof(d.d_name), false },
          { sizeof(d.d_street_1), false },
          { sizeof(d.d_street_2), false },
          { sizeof(d.d_city), false },
          { sizeof(d.d_state), false },
          { sizeof(d.d_zip), false} };
    schema.max_items = scale_factor * NumDistrictsPerWarehouse();
    schema.hash_fn = dist_hash_fn;

    // schema.index_type = BINDEX_BTREE;
    schema.index_type = BINDEX_HASH;
    store.AddSchema(DIST, schema);
  }

  // 2 customer
  {
    customer::value c;
    schema.cols = 
        { 
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
    schema.hash_fn = cust_hash_fn;
    // schema.index_type = BINDEX_BTREE;
    schema.index_type = BINDEX_HASH;
    store.AddSchema(CUST, schema);
  }

  // 3 history
  {
    history::value h;
    schema.cols = 
        { { sizeof(h.h_date), false },
          { sizeof(h.h_amount), false },
          { sizeof(h.h_data), false} };
    schema.max_items = scale_factor * NumDistrictsPerWarehouse()
                       * NumCustomersPerDistrict() * 1.1;
    schema.hash_fn = nullptr;

    schema.index_type = BINDEX_BTREE;
    store.AddSchema(HIST, schema);
  }

  // 4 new-order
  {
    schema.cols = vector<BackupDB::Column>();
    schema.max_items = TimeScale;

    schema.hash_fn = orde_hash_fn;
    // schema.index_type = BINDEX_BTREE;
    schema.index_type = BINDEX_HASH;

    store.AddSchema(NEWO, schema);
    // store.AddEdge(NEWO);  // NEWO graph
  }

  // 5 order
  {
    oorder::value oo;
    schema.cols = 
        { 
          { .vlen = sizeof(double), .updatable = true, .page_size = PAGE_SIZE / sizeof(double) },
#if 1
          { .vlen = sizeof(oo.o_carrier_id), .updatable = true, .page_size = PAGE_SIZE / sizeof(oo.o_carrier_id) },
#else
          { .vlen = sizeof(oo.o_carrier_id), .updatable = true, .page_size = 1 },
#endif
          { .vlen = sizeof(oo.o_c_id), .updatable = false },
          { .vlen = sizeof(oo.o_ol_cnt), .updatable = false },
          { .vlen = sizeof(oo.o_all_local), .updatable = false },
#if 1
          { .vlen = sizeof(oo.o_entry_d), .updatable = false, .page_size = 0 }
#else
          { .vlen = sizeof(oo.o_entry_d), .updatable = false, .page_size = 0, .sec_index = BINDEX_BTREE }
#endif
        };
    schema.max_items = TimeScale;
    schema.hash_fn = orde_hash_fn;

    // schema.index_type = BINDEX_BTREE;
    schema.index_type = BINDEX_HASH;

    store.AddSchema(ORDE, schema);
    store.AddEdge(ORDE);
  }

  // 6 order-line
  {
    order_line::value ol;
    schema.cols = 
        { 
          { .vlen = sizeof(double), .updatable = true, .page_size = PAGE_SIZE / sizeof(double) },
#if 1
          { .vlen = sizeof(ol.ol_delivery_d), .updatable = true, .page_size = PAGE_SIZE / sizeof(ol.ol_delivery_d) },
#else
          { .vlen = sizeof(ol.ol_delivery_d), .updatable = true, .page_size = uint64_t(-1) },
#endif
          { .vlen = sizeof(ol.ol_i_id), .updatable = false },
          { .vlen = sizeof(ol.ol_amount), .updatable = false },
          { .vlen = sizeof(ol.ol_supply_w_id), .updatable = false },
          { .vlen = sizeof(ol.ol_quantity), .updatable = false },
          { .vlen = sizeof(ol.ol_c_key), .updatable = false } };
    schema.max_items = 15 * TimeScale;
    schema.hash_fn = orli_hash_fn;

    // schema.index_type = BINDEX_BTREE;
    schema.index_type = BINDEX_HASH;
    if (config.isUseIndex()) {
      schema.sec_index_type = BINDEX_BTREE;
    }

    store.AddSchema(ORLI, schema);
  }

  schema.sec_index_type = NO_BINDEX;

  // 7 item
  {
    item::value i;
    schema.cols = 
        { { sizeof(i.i_name), false },
          { sizeof(i.i_price), false },
          { sizeof(i.i_data), false },
          { sizeof(i.i_im_id), false } };
    schema.max_items = NumItems();
    schema.hash_fn = item_hash_fn;

    // schema.index_type = BINDEX_BTREE;
    schema.index_type = BINDEX_HASH;
    store.AddSchema(ITEM, schema);
  }

  // 8 stock
  {
    stock::value s;
    if (M == 0) {
      schema.cols = 
          { { sizeof(s.s_quantity), true, uint64_t(-1) },
            { sizeof(s.s_ytd), true, uint64_t(-1) },
            { sizeof(s.s_order_cnt), true, PAGE_SIZE },
            { sizeof(s.s_remote_cnt), true, PAGE_SIZE } };
    } else if (M == 1) {
      schema.cols = 
          { { sizeof(s.s_quantity), true, PAGE_SIZE },
            { sizeof(s.s_ytd), true, PAGE_SIZE },
            { sizeof(s.s_order_cnt), true, PAGE_SIZE },
            { sizeof(s.s_remote_cnt), true, PAGE_SIZE } };
    } else if (M == 2) {
#if 1
      schema.cols = 
          { { sizeof(s.s_quantity) + sizeof(s.s_ytd), true, PAGE_SIZE / 2 },
            { 0, false },
            { sizeof(s.s_order_cnt), true, PAGE_SIZE },
            { sizeof(s.s_remote_cnt), true, PAGE_SIZE } };
#else
      schema.cols =
       { { sizeof(stock), true, 256 } }; 
#endif
    } else if (M == 3) {
      schema.cols = 
          { { sizeof(s.s_quantity), true, 1 },
            { sizeof(s.s_ytd), true, 1 },
            { sizeof(s.s_order_cnt), true, 1 },
            { sizeof(s.s_remote_cnt), true, 1 } };
    }

    schema.max_items = scale_factor * NumItems();
    schema.hash_fn = stoc_hash_fn;

    // schema.index_type = BINDEX_BTREE;
    schema.index_type = BINDEX_HASH;
    store.AddSchema(STOC, schema);
  }

  // 9 supplier
  {
    supplier::value su;
    schema.cols = 
        { { sizeof(su.su_nationkey), false },
          { sizeof(su.su_acctbal), false },
          { sizeof(su.su_phone), false },
          { sizeof(su.su_name), false },
          { sizeof(su.su_address), false },
          { sizeof(su.su_comment), false } };
    schema.max_items = NumSupplier();
    schema.hash_fn = item_hash_fn;

    // schema.index_type = BINDEX_BTREE;
    schema.index_type = BINDEX_HASH;
    store.AddSchema(SUPP, schema);
  }

  // 10 nation
  {
    nation::value n;
    schema.cols =
        { { sizeof(n.n_regionkey), false },
          { sizeof(n.n_name), false },
          { sizeof(n.n_comment), false } };
    schema.max_items = NumNation();

    schema.index_type = BINDEX_BTREE;
    store.AddSchema(NATI, schema);
  }

  // 11 region
  {
    region::value r;
    schema.cols =
        { { sizeof(r.r_name), false },
          { sizeof(r.r_comment), false } };
    schema.max_items = NumRegion();

    schema.index_type = BINDEX_BTREE;
    store.AddSchema(REGI, schema);
  }

}

vector<BenchLoader *>
ChRunner::make_loaders(int partition, MemDB* store) {
  /* Current we do not consider parallal loading */
  vector<BenchLoader *> ret;
  ret.push_back(new ChWarehouseLoader(9324,partition, store));
  ret.push_back(new ChItemLoader(235443,partition,store));
  ret.push_back(new ChStockLoader(89785943,partition,store));
  ret.push_back(new ChDistrictLoader(129856349,partition,store));
  ret.push_back(new ChCustomerLoader(923587856425,partition,store));
  ret.push_back(new ChOrderLoader(2343352,partition,store));
  return ret;
}

vector<BenchLoader *>
ChRunner::make_backup_loaders(int partition, BackupDB* store) {
  assert(store != nullptr);
  vector<BenchLoader *> ret;
  ret.push_back(new ChWarehouseLoader(9324,partition, store));
  ret.push_back(new ChItemLoader(235443,partition,store));
  ret.push_back(new ChStockLoader(89785943,partition,store));
  ret.push_back(new ChDistrictLoader(129856349,partition,store));
  ret.push_back(new ChCustomerLoader(923587856425,partition,store));
  ret.push_back(new ChOrderLoader(2343352,partition,store));
  ret.push_back(new ChSupplierLoader(138250201,partition,store));
  ret.push_back(new ChNationLoader(138250201,partition,store));
  ret.push_back(new ChRegionLoader(138250201,partition,store));

  return ret;
}

vector<BenchWorker *> ChRunner::make_workers() {
  int rest_worker = config.getNumTxnThreads();

  int mac_id = config.getServerID();
  vector<int> primaries = view.get_primaries(mac_id);
  int num_primaries = primaries.size();
  int scale_factor = config.getScaleFactorPerPartition();  // per partition

  assert(num_primaries == 1);  // XXX: Todo

  int start_pid = primaries[0];
  fast_random r(23984543 + start_pid);

  vector<BenchWorker *> ret;

  start_w_id = start_pid * scale_factor + 1;
    
  int rest_ware = scale_factor * num_primaries;
 
  wid2tid.reserve(scale_factor); 
  for (int i = 0; i < scale_factor; ++i) {
    wid2tid.emplace_back(rest_worker); 
  }

  if (rest_ware >= rest_worker) {
    // 1 worker -> 1 or multi warehouse
    int start_ware = start_w_id, end_ware;
    for (int i = 0; rest_worker > 0; ++i, --rest_worker) {
      // reponse for [start_ware, end_ware)
      int num_ware = (rest_ware + rest_worker - 1) / rest_worker;
      end_ware = start_ware + num_ware; 
      for (int w = start_ware; w < end_ware; ++w) {
        wid2tid[w - start_w_id].add_worker(i);
      }
  
      printf("[ChWorker] worker %d w_id [%d, %d)\n", i, start_ware, end_ware);
      // uint32_t start_ware = start_pid * scale_factor + 1;
      // uint32_t end_ware = (start_pid + 1) * scale_factor + 1;
      ret.push_back(new ChWorker(i, r.next(), start_ware, end_ware,
                                 stores_[0] // here notice!
                                 ));
      start_ware = end_ware;
      rest_ware -= num_ware;
    }
  } else {
    // multi worker -> 1 warehouse
    int start_worker = 0, end_worker;
    for (int i = 0; rest_ware > 0; ++i, --rest_ware) {
      int num_worker = (rest_worker + rest_ware - 1) / rest_ware;
      end_worker = start_worker + num_worker;
      for (int w = start_worker; w < end_worker; ++w) {
        wid2tid[i].add_worker(w); 
        printf("[ChWorker] worker %d w_id %d\n", w, i + start_w_id);
        ret.push_back(new ChWorker(w, r.next(), i + start_w_id, i + start_w_id + 1,
                                 stores_[0] // here notice!
                                 ));
      }
      start_worker = end_worker;
      rest_worker -= num_worker;
    }
  }

  for (int i = 0; i < wid2tid.size(); ++i) {
    wid2tid[i].print(i);
  }

  return ret;
}

vector<BenchClient *> ChRunner::make_clients() {
  vector<BenchClient *> ret;
  
  int num_txn_workers = config.getNumTxnThreads();
  // ret.push_back(new ChClient(num_txn_workers));
  ret.push_back(new BenchClient(num_txn_workers));

  return ret;
}

vector<QueryWorker *> ChRunner::make_qry_workers() {
  vector<QueryWorker *> ret;

  vector<int> backups = view.get_ap_backups(config.getServerID());
  vector<int> graphs = view.get_gp_backups(config.getServerID());
  int num_session = config.getQuerySession();
  int num_qry = config.getNumQueryThreads();
  if (backups.size() == 0 && graphs.size() == 0) return ret;
  if (num_qry == 0 || num_session == 0) return ret;

  assert(backups.size() <= 1);
  assert(graphs.size() <= 1);
  
  // reponse for [start_ware, end_ware)
  // TODO: now only support only one backup store
  int scale_factor = config.getScaleFactorPerPartition();  // per partition
  int start_pid = 0;
  // XXX: graphs[0] same as backups[0] if both exist
  if (backups.size() != 0)
    start_pid = backups[0];
  else
    start_pid = graphs[0]; 
  fast_random r(23984543 + start_pid);

  // for each worker
  BackupDB *db = nullptr;
  for (BackupDB *d : backup_stores_) {
    if (d && d->isAP()) {
      db = d;  // XXX: for the first AP partition
      break;
    }
  }
  // assert(db);
  
  assert(graph_stores_.size() <= 1);

  uint32_t start_ware = start_pid * scale_factor + 1;
  uint32_t end_ware = (start_pid + 1) * scale_factor + 1;
  int num_qry_per_session = num_qry / num_session;
  for (int i = 0; i < num_session; ++i) {
    int qry_id = num_qry_per_session * i;
    ret.push_back(new ChQueryWorker(qry_id, num_qry_per_session, r.next(), 
                                    start_ware, end_ware, db, graph_stores_[0]));
  }
#if 0
  int start_pid = primaries[0];
  int num_query_workers = config.getNumQueryThreads();
  int n_ware_per_qw = num_query_workers? scale_factor / query_nthreads : 0;

  if (num_query_workers > 0) {
    // only build one QueryWorker
    int qworker_id = i - num_txn_workers;
    printf("[CH Query Worker] query for partition %d, sf %d, %d wares\n",
           backups[0], scale_factor, n_ware_per_qw);
    uint32_t start_ware = backups[0] * scale_factor
                          + qworker_id * n_ware_per_qw + 1;
    uint32_t end_ware = start_ware + n_ware_per_qw;
    ret.push_back(new ChQueryWorker(i, r.next(), backup_stores_[0],
                                    start_ware, end_ware));
  }
#endif

  return ret;
}

vector<AnalyticsWorker*> ChRunner::make_ana_workers() {
#ifdef WITH_GRAPE
  // init grape engine environment
  google::InitGoogleLogging("analytical_apps");
  if(config.isUseGrapeEngine()) {
    grape::GrapeEngine::Init();
  }
#endif

  std::vector<AnalyticsWorker*> ret;

  printf("[CHRunner] Make analytics workers\n");

  int num_ana_threads = config.getNumAnalyticsThreads();
  int num_ana_session = config.getAnalyticsSession();
  
  if (num_ana_threads == 0 || num_ana_session == 0) return ret;

  int num_ana_threads_per_session = num_ana_threads / num_ana_session;

  if(num_ana_threads_per_session == 0) return ret;

  bool use_seg_graph = config.isUseSegGraph();

  for (int i = 0; i < num_ana_session; i++) {
    int worker_idx = i * num_ana_threads_per_session;
    ret.push_back(new ChAnalyticsWorker(worker_idx,
                                        num_ana_threads_per_session,
                                        use_seg_graph,
                                        this->graph_stores_[0],
                                        this->rg_maps_[0]));
  }

  return ret;
}

vector<LogWorker *> ChRunner::make_backup_workers() {
  int backup_nthreads = config.getNumBackupThreads();
  vector<LogWorker *> ret;

  printf("[Runner] Make backup workers\n");
  int mac_id = config.getServerID();
  LogCleaner* log_cleaner = new ChLogCleaner;

  for (BackupDB *db : backup_stores_) {
    assert(db);
    log_cleaner->add_backup_store(db);
  }

  assert(graph_stores_.size() == rg_maps_.size());
  for (int i = 0; i < rg_maps_.size(); ++i) {
    log_cleaner->add_graph_store(graph_stores_[i], rg_maps_[i]);
  }

  DBLogger::set_log_cleaner(log_cleaner);
  for (uint i = 0; i < backup_nthreads; i++) {
    ret.push_back(new LogWorker(i));
  }

  return ret;
}

void ChRunner::warmup_buffer(char *buffer) {
  int num_mac = config.getNumServers();
  LogTSManager::initialize_meta_data(buffer, num_mac);
#ifdef SI_TX
  TSManager::initilize_meta_data(buffer, num_mac);
#endif
}


}  // namespace ch
}  // namespace oltp
}  // namespace nocc


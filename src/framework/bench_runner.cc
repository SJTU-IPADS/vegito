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

#include "bench_runner.h"
#include "bench_reporter.h"
#include "bench_listener.h"
#include "log_listener.h"

#include "framework_cfg.h"
#include "view_manager.h"
#include "rdma_config.h"
#include "framework.h"

#include "custom_config.h"
#include "util/util.h"

using namespace nocc::util;
using namespace nocc::framework;
using namespace std;


namespace nocc {
namespace oltp {

namespace {
vector<QueryWorker *> qry_workers;
vector<AnalyticsWorker *> ana_workers;
}

uint64_t get_min_using_ver() {
  uint64_t min = -1;
  for (const QueryWorker *w : qry_workers) {
    uint64_t ver = w? w->get_read_ver_() : -1;
    if (min > ver) min = ver;
  }
  for (const AnalyticsWorker *w : ana_workers) {
    uint64_t ver = w? w->get_read_ver_() : -1;
    if (min > ver) min = ver;
  }
  return (min == -1)? LogWorker::get_read_epoch() : min;
}

void Runner::load_primary_partitions_(int mac_id) {
  vector<int> primaries = view.get_primaries(mac_id);
  int num_primaries = primaries.size();
  assert(num_primaries <= 1);

  stores_.assign(num_primaries, nullptr);
  for (int i = 0; i < num_primaries; ++i) {
    int p_id = primaries[i];
    printf("***** Load Primary Partition %d *****\n", p_id);
    init_store(stores_[i]);  // schema
    const vector<BenchLoader *> loaders = make_loaders(p_id, stores_[i]);

    uint64_t mem_before = get_system_memory_info().first;
    for (BenchLoader *loader : loaders) loader->start();
    for (BenchLoader *loader : loaders) loader->join();
    uint64_t mem_after = get_system_memory_info().first;

    const int64_t delta = int64_t(mem_before) - int64_t(mem_after); // free mem
    const double delta_mb = double(delta)/1048576.0;
    printf("***** Size %lf MB ******\n", delta_mb);
  }
}

void Runner::load_backup_partitions_(int mac_id) {
  Breakdown_Timer timer;
  vector<int> tp_backups = view.get_tp_backups(mac_id);
  int num_tp_backups = tp_backups.size();
  vector<int> ap_backups = view.get_ap_backups(mac_id);
  int num_ap_backups = ap_backups.size();

  int num_backups = num_tp_backups + num_ap_backups;
  backup_stores_.assign(num_backups, nullptr);
  for(int i = 0; i < num_backups; ++i) {
    bool ap = (i >= num_tp_backups);
    int p_id = ap? ap_backups[i - num_tp_backups] : tp_backups[i];

    printf("\n***** Load Backup %s Partition %d ****\n", ap? "AP" : "TP", p_id);
    uint64_t mem_before = get_system_memory_info().first;
    backup_stores_[i] = new BackupDB(p_id, ap);
    init_backup_store(*backup_stores_[i]);

    const vector<BenchLoader *> loaders =
      make_backup_loaders(p_id, backup_stores_[i]);

    timer.start();
    for (BenchLoader *loader : loaders) loader->start();
    for (BenchLoader *loader : loaders) loader->join();
    backup_stores_[i]->UpdateOffset();
    float time_ms = timer.get_diff_ms();
    uint64_t mem_after = get_system_memory_info().first;

    const int64_t delta = int64_t(mem_before) - int64_t(mem_after); // free mem
    const double delta_mb = double(delta)/1048576.0;
    printf("***** Size %lf MB, time %lf ms *****\n", delta_mb, time_ms);
    // getchar();
  }

#if 0  // for format conversion
  {
    printf("\n***** Copy Row -> Row ****\n");
    bool ap = false;
    int p_id = tp_backups[0];
    BackupDB *db = new BackupDB(p_id, ap);
    init_backup_store(*db);

    uint64_t mem_before = get_system_memory_info().first;
    timer.start();

    db->CopyFrom(*backup_stores_[0]);

    float time_ms = timer.get_diff_ms();
    uint64_t mem_after = get_system_memory_info().first;

    const int64_t delta = int64_t(mem_before) - int64_t(mem_after); // free mem
    const double delta_mb = double(delta)/1048576.0;
    printf("***** Size %lf MB, time %lf ms *****\n", delta_mb, time_ms);
  }

  {
    printf("\n***** Copy Row -> Col ****\n");
    bool ap = true;
    int p_id = tp_backups[0];
    BackupDB *db = new BackupDB(p_id, ap);
    init_backup_store(*db);

    uint64_t mem_before = get_system_memory_info().first;
    timer.start();

    db->CopyFrom(*backup_stores_[0]);

    float time_ms = timer.get_diff_ms();
    uint64_t mem_after = get_system_memory_info().first;

    const int64_t delta = int64_t(mem_before) - int64_t(mem_after); // free mem
    const double delta_mb = double(delta)/1048576.0;
    printf("***** Size %lf MB, time %lf ms *****\n", delta_mb, time_ms);
  }

  while(1) ;

#endif
}

void Runner::load_graph_partitions_(int mac_id) {
  Breakdown_Timer timer;

  vector<int> gp_backups = view.get_gp_backups(mac_id);
  int num_gp_backups = gp_backups.size();
  graph_stores_.assign(num_gp_backups, nullptr);
  rg_maps_.assign(num_gp_backups, nullptr);
  for(int i = 0; i < num_gp_backups; ++i) {
    int p_id = gp_backups[i];
    printf("\n***** Load Graph Partition %d ****\n", p_id);
    uint64_t mem_before = get_system_memory_info().first;

    // load graph
    graph_stores_[i] = new graph::GraphStore();
    rg_maps_[i] = new graph::RGMapping(p_id);
    init_graph_store(graph_stores_[i], rg_maps_[i]);
    const vector<GraphLoader *> loaders =
      make_graph_loaders(p_id, graph_stores_[i], rg_maps_[i]);

    timer.start();

    // Load vertex, then load edges
    for (GraphLoader *loader : loaders) {
      if (loader->is_vertex()) loader->start();
    }

    for (GraphLoader *loader : loaders) {
      if (loader->is_vertex()) loader->join();
    }

    for (GraphLoader *loader : loaders) {
      if (!loader->is_vertex()) loader->start();
    }

    for (GraphLoader *loader : loaders) {
      if (!loader->is_vertex()) loader->join();
    }

    graph_stores_[i]->update_offset();
    float time_ms = timer.get_diff_ms();
    uint64_t mem_after = get_system_memory_info().first;

    const int64_t delta = int64_t(mem_before) - int64_t(mem_after); // free mem
    const double delta_mb = double(delta)/1048576.0;
    printf("***** Size %lf MB, time %lf ms *****\n", delta_mb, time_ms);
    // getchar();
  }
}

void Runner::run() {
  Breakdown_Timer::get_one_second_cycle();
  // for sub-class rdma initialize
  warmup_buffer(rdmaConfig.getFreeBuf());

#ifdef WITH_RDMA
  cm->start_server(); // listening server for receive QP connection requests
#endif

  /*************** Load Data ****************/
  int mac_id = config.getServerID();

  BindToCore(0);

  load_primary_partitions_(mac_id);

  BindToCore(TotalCores() - 1);

  load_backup_partitions_(mac_id);
  load_graph_partitions_(mac_id);

#ifndef WITH_RDMA
  printf("[Runner] Complete Loading!\n");
  while(1) ;    // FIXME: for the Test on vineyard
#endif

  /*************** Backup Workers ****************/
  vector<LogWorker *> backup_workers;
  if (config.getNumBackupThreads() != 0
      && (backup_stores_.size() + graph_stores_.size() != 0)) {
    backup_workers = make_backup_workers();
    for (LogWorker *w : backup_workers)
      w->start();
  } else {
    for (int i = 0; i < config.getNumBackupThreads(); ++i) {
      LogWorker *w = new LogWorker(i, true);
      w->start();
      // w->thread_local_init();  // for create qp
    }
  }

  /*************** TP Workers ****************/
  vector<BenchWorker *> workers;
  vector<BenchClient *> clients;

  if (config.getNumTxnThreads() != 0 && stores_.size() != 0) {
    workers = make_workers();

    if (workers.size() != 0) {
      if (config.isUseClient()) {
        clients = make_clients();
        for (BenchClient *c : clients)
          c->start();
        for (BenchClient *c : clients)
          while (!c->init_) ;
      }
    }

    for (BenchWorker *w : workers)
      w->start();
  } else {
    for (int i = 0; i < config.getNumTxnThreads(); ++i) {
      BenchWorker *w = new BenchWorker(i, 0, nullptr);
      w->fake_run();
    }
  }

  /*************** Query Workers ****************/
  vector<QueryClient *> qry_clients;

  if ((backup_stores_.size() + graph_stores_.size() != 0)
      && (config.getQuerySession() * config.getNumQueryThreads() != 0)) {
    qry_workers = make_qry_workers();
    if (qry_workers.size() != 0) {
      if (config.isUseQryClient()) {
        qry_clients.push_back(new QueryClient(qry_workers.size()));
        for (QueryClient *c : qry_clients)
          c->start();
        for (QueryClient *c : qry_clients)
          while (!c->init_) ;
      }
    }
    for (QueryWorker *w : qry_workers)
      w->start();
  } else {
    for (int i = 0; i < config.getNumQueryThreads(); ++i) {
      QueryWorker *w = new QueryWorker(i, 0, 0);
      w->fake_run();
    }
  }

  /*************** Analytics Workers ****************/
  if ((config.getNumAnalyticsThreads() * config.getAnalyticsSession() != 0)
      && graph_stores_.size() != 0) {
    ana_workers = make_ana_workers();
    for (auto worker : ana_workers)
      worker->start();
  }

  /*************** Other Workers ****************/
  if (config.getEpochType() > 0) {
    LogListener *logListener = new LogListener();
    logListener->start();

    if (backup_stores_.size() != 0) {
      GCollector *gcollector = new GCollector(backup_stores_);
      gcollector->start();
    }
  }

  Reporter reporter(workers, backup_workers, qry_workers, ana_workers,
                    clients, qry_clients, backup_stores_);

  // use this thread as listener directly
  Listener(workers, backup_workers, qry_workers, ana_workers, reporter).run();
}


}  // namespace nocc
}  // namespace nocc

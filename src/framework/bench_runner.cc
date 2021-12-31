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
}
 
uint64_t get_min_using_ver() {
  uint64_t min = -1;
  for (const QueryWorker *w : qry_workers) {
    uint64_t ver = w? w->get_read_ver_() : -1;
    if (min > ver) min = ver;
  }
  return (min == -1)? LogWorker::get_read_epoch() : min; 
}

void Runner::load_primary_partitions_(int mac_id) {
  vector<int> primaries = view.get_primaries(mac_id);
  int num_primaries = primaries.size();
  assert(num_primaries == 1);

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
#if INDEX_BUILD_EVAL == 0
    // parallel
    for (BenchLoader *loader : loaders) loader->start();
    for (BenchLoader *loader : loaders) loader->join();
#else
    for (BenchLoader *loader : loaders) {
      loader->start();
      loader->join();
    }
#endif
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

void Runner::run() {
  Breakdown_Timer::get_one_second_cycle();
  // for sub-class rdma initialize
  warmup_buffer(rdmaConfig.getFreeBuf());
  cm->start_server(); // listening server for receive QP connection requests

  // load database
  int mac_id = config.getServerID();
  
  BindToCore(0);

  load_primary_partitions_(mac_id);
  // getchar();

  BindToCore(TotalCores() - 1);

  load_backup_partitions_(mac_id);

  // while(1) ;

  const vector<LogWorker *> backup_workers = make_backup_workers();
  for (LogWorker *w : backup_workers)
    w->start();

  const vector<BenchWorker *> workers = make_workers();
  vector<BenchClient *> clients;

  if (config.isUseClient()) {
    clients = make_clients();
    for (BenchClient *c : clients)
      c->start();
    for (BenchClient *c : clients)
      while (!c->init_) ;
  }

  for (BenchWorker *w : workers)
    w->start();

  qry_workers = make_qry_workers();
  vector<QueryClient *> qry_clients;

  if (config.isUseQryClient()) {
    qry_clients.push_back(new QueryClient(qry_workers.size()));
    for (QueryClient *c : qry_clients)
      c->start();
    for (QueryClient *c : qry_clients)
      while (!c->init_) ;
  }

  for (QueryWorker *w : qry_workers)
    w->start();

  if (config.getEpochType() > 0) {
    LogListener *logListener = new LogListener();
    logListener->start();

    GCollector *gcollector = new GCollector(backup_stores_);
    gcollector->start();
  }

  Reporter reporter(workers, backup_workers, qry_workers,
                    clients, qry_clients, backup_stores_);

  // use this thread as listener directly
  Listener(workers, backup_workers, qry_workers, reporter).run();
}


}  // namespace nocc
}  // namespace nocc

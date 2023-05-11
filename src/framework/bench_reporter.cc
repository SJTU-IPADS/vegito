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

#include "bench_reporter.h"
#include "config.h"
#include "custom_config.h"

#include <string>
#include <cstring>
#include <vector>
#include <unordered_map>

#define SHOW_WORKER_THR 0
#define SHOW_BACKUP_THR 0

#define PRINT_TXN_LOG 1

using namespace std;

extern size_t txn_nthreads;

static string normalize_throughput(uint64_t thpt) {

  const uint64_t K = 1000;
  const uint64_t M = K * 1000;

  char buf[64];
#if 0
  if(thpt > M) {
    sprintf(buf,"%f M",thpt / (double)M);
  } else if(thpt > K) {
    sprintf(buf,"%f K",thpt / (double)K);
  } else {
    sprintf(buf,"%lu",thpt);
  }
#endif
  sprintf(buf,"%f M",thpt / (double)M);
  return string(buf);
}

namespace nocc {
namespace oltp {

Reporter::Reporter(const vector<BenchWorker *> &workers,
                   const vector<LogWorker *> &replayers,
                   const vector<QueryWorker *> &queryers,
                   const vector<AnalyticsWorker *> &analyzers,
                   const vector<BenchClient *> &clients,
                   const vector<QueryClient *> &qry_clients,
                   const vector<BackupDB *> &backup_dbs)
  : workers_(workers), replayers_(replayers), queryers_(queryers),
    clients_(clients), qry_clients_(qry_clients), backup_dbs_(backup_dbs), 
    sec_cyc_(util::Breakdown_Timer::get_one_second_cycle())
{
  init_();
}

void Reporter::init_() {

  // statistic for workers
  size_t wsz = workers_.size();
  prev_commits_.assign(wsz, 0);
  prev_log_send_.assign(wsz, 0);
  prev_aborts_.assign(wsz, 0);
  prev_gossip_.assign(wsz, 0);

  // statistic for replayers
  size_t rsz = replayers_.size(); 
  prev_logs_.assign(rsz, 0);
  prev_log_cycles_.assign(rsz, 0);
  prev_index_cycles_.assign(rsz, 0);
  prev_balancing_cycles_.assign(rsz, 0);

  // statistic for queryers
  size_t qsz = queryers_.size();
  prev_queries_.assign(qsz, 0);
  prev_freshness_.assign(qsz, 0);
  prev_readop_.assign(qsz, 0);

  num_update = 0;
  num_copy = 0;
  prev_num_update = 0;
  prev_num_copy = 0;
}

// collect (master/slave) -> merge (master) -> report (master)
void Reporter::merge_data(const char *data) {
  const ReportData *p = (const ReportData *) data;
  rd_.txn_thr += p->txn_thr;
  rd_.log_thr += p->log_thr;
  rd_.qry_thr += p->qry_thr;
  rd_.send_log_thr += p->send_log_thr;

  rd_.copy_thr += p->copy_thr;
  rd_.read_thr += p->read_thr;

  rd_.gossip_thr += p->gossip_thr;

  rd_.aborts += p->aborts;
}

// report the cluster data
void Reporter::report_data(uint64_t epoch, std::ofstream &log_file) {

  // calculate latency
  double latency = 0.0;
  double c_latency = 0.0;
  double q_c_latency = 0.0;

  if (workers_.size() != 0) {
    workers_[0]->workload_report();
    latency = workers_[0]->latency_timer_.report() / sec_cyc_ * 1000 * 1000;
  }
  if (clients_.size() != 0)
    c_latency = clients_[0]->timer_.report() / sec_cyc_ * 1000 * 1000;
  if (qry_clients_.size() != 0)
    q_c_latency = qry_clients_[0]->timer_.report() / sec_cyc_ * 1000;
  
  total_thr_.emplace_back(rd_.txn_thr);
  total_lat_.emplace_back(latency);
  total_c_lat_.emplace_back(c_latency);
  total_q_c_lat_.emplace_back(q_c_latency);
  total_qry_thr_.emplace_back(rd_.qry_thr);
  total_gossip_.emplace_back(rd_.gossip_thr);

  if (LISTENER_PRINT_PERF) {
    printf("@%02lu System thr %s",
           epoch,normalize_throughput(rd_.txn_thr).c_str());
    // log clean
    // printf(", log: send %s clean %s", 
    //        normalize_throughput(rd_.send_log_thr).c_str(),
    //        normalize_throughput(rd_.log_thr).c_str());
    printf(", clean %s", normalize_throughput(rd_.log_thr).c_str());

    // clients latency
    // printf(", log %s, lat %lf us, qry_thr %lf",
    //        normalize_throughput(rd_.log_thr).c_str(),
    //        latency, rd_.qry_thr);
    // printf(", c_lat %lf us, q_c_lat %lf ms", c_latency, q_c_latency);
#if 0
    printf(", c_lat %lf us, queries %s",
           c_latency,
           normalize_throughput(rd_.qry_thr).c_str());
#endif
    printf(", queries %lf",
           rd_.qry_thr);
#if UPDATE_STAT == 1
    printf(", copy thpt %s", normalize_throughput(rd_.copy_thr).c_str());
#endif

#if 0
    // for micro-benchmark, thpt of read operations on column store
    printf(", read thpt %s", normalize_throughput(rd_.read_thr).c_str());
#endif

    printf(", gossip thpt %lf", rd_.gossip_thr);
    printf("\n");

  }

  if(LOG_RESULTS && epoch > 5) {
    /* warm up for 5 seconds, also the calcuation script will skip some seconds*/
    /* record the result */
    log_file << (rd_.txn_thr) << " "
             << latency << std::endl;
  }

  // clear the report data
  rd_.clear();
}

// calcuate the local data
void Reporter::collect_data(char *data,struct  timespec &start_t) {

  // txn workers
  uint64_t res = calc_delta_(&TxnProf::commit, prev_commits_, TXN_THR);
  uint64_t send_log = calc_delta_(&TxnProf::send_log, prev_log_send_);
  uint64_t abort_num = calc_delta_(&TxnProf::abort, prev_aborts_);
  uint64_t gossip_num = calc_delta_gossip(prev_gossip_);

  // query workers
  uint64_t queries = calc_delta_(&QryProf::commit, prev_queries_);
  uint64_t freshness_ms = calc_delta_(&QryProf::freshness_ms, prev_freshness_);
  uint64_t reads = calc_delta_(&QryProf::read_op, prev_readop_);

  // log replayers
  uint64_t logs = calc_delta_(&LogProf::log, prev_logs_, LOG_THR);
  uint64_t log_cycles = calc_delta_(&LogProf::log_c, prev_log_cycles_);
  uint64_t index_cycles = calc_delta_(&LogProf::index_c, prev_index_cycles_);
  uint64_t balancing_cycles = calc_delta_(&LogProf::balance_c, prev_balancing_cycles_);

  // backup stores
  calc_store();
  uint64_t updates = num_update - prev_num_update;
  uint64_t copies = num_copy - prev_num_copy;
  prev_num_update = num_update;
  prev_num_copy = num_copy;

  // re-set timer
  struct timespec end_t;
  clock_gettime(CLOCK_REALTIME, &end_t);
  double elapsed_sec = util::DiffTimespec(end_t,start_t) / 1000.0;
  clock_gettime(CLOCK_REALTIME, &start_t);

  double my_thr = (double)res / elapsed_sec;
  double my_send_log = (double) send_log / elapsed_sec;
  double my_queries = (double)queries / elapsed_sec;
  double my_readop = (double) reads / elapsed_sec;
  double my_log = (double) logs / elapsed_sec;
  double my_clean_log = (double) log_cycles / (elapsed_sec * sec_cyc_)
                        / prev_log_cycles_.size() * 100;
  double my_index = (double) index_cycles / (elapsed_sec * sec_cyc_)
                        / prev_index_cycles_.size() * 100;
  double my_balancing = (double) balancing_cycles / (elapsed_sec * sec_cyc_)
                        / prev_index_cycles_.size() * 100;

  double my_updates = (double) updates / elapsed_sec;
  double my_copies = (double) copies / elapsed_sec;
  double my_gossip = (double) gossip_num / elapsed_sec;

  double my_freshness_ms = (double) freshness_ms / queries;


  // printf("  my throughput %s", normalize_throughput(my_thr).c_str());
  // printf(", ");
  // printf("log %s", normalize_throughput(my_log).c_str());
  // printf(", queries %lf", my_queries);
  // printf("\n");
#if 0
  printf("  log time %f%%, index %f%%, balance %f%%\n",
         my_clean_log, my_index, my_balancing);
#endif
#if PRINT_TXN_LOG
  printf("  txn epoch %lu, read epoch %lu, freshness %lf ms", 
         BenchWorker::get_txn_epoch(), LogWorker::get_read_epoch(), 
         my_freshness_ms);
  // printf(", update %lf, copies sz %lf, copy %lf\%", 
  //         my_updates, my_copies, my_copies / my_updates * 100);
  printf("\n");
#endif

  // log cleaner tailer
#if 0
  for (int i = 0; i < replayers_.size(); ++i) {
    printf("Log Cleaner %d: ", i);
    const vector<uint64_t> log_tailer = replayers_[i]->get_log_tailer();
    for (int j = 0; j < log_tailer.size(); ++j) {
      if (log_tailer[j] == 0) continue;

      uint64_t n_id, t_id, l_id;
      replayers_[i]->decode(j, n_id, t_id, l_id);
      printf("[%lu, %lu, %lu] %lu | ", n_id, t_id, l_id, log_tailer[j]);
    }
    printf("\n");
  } 
#endif

  ReportData *p = (ReportData *)data;
  p->txn_thr = my_thr;
  p->send_log_thr = my_send_log;
  p->aborts = abort_num;
  p->log_thr = my_log;
  p->copy_thr = my_copies;
  p->read_thr = my_readop;
  p->gossip_thr = my_gossip;

  if (config.getServerID() == 0) {
    p->qry_thr = my_queries;
  } else {
    p->qry_thr = 0;
  }
  return;
}

// medain of T[start, end] (include)
template<typename T>
inline T median(vector<T> array, int start, int end) {
  std::sort(array.begin() + start, array.begin() + end + 1);
  int num = end - start + 1;
  if (num % 2) {
    return array[start + num / 2];
  } else {
    return (array[start + num / 2] + array[start + num / 2 - 1]) / 2;
  }

}

void Reporter::print_end_stat(std::ofstream &log_file) const {
  double sum_thr = 0.0;
  double sum_lat = 0.0;
  double sum_c_lat = 0.0;
  double sum_q_thr = 0.0;
  double sum_q_c_lat = 0.0;
  double sum_gossip_thr = 0.0;
  int    cnt     = 0;
  int    qcnt = 0;

  // int begin = 13, end = 19;  // For HTAP-OLTP
  // int begin = 5, end = 9;  // For HTAP-OLAP
  int begin = 3, end = config.getRunSec() - 2;  // For HTAP
  for (int i = begin; i <= end; ++i, ++cnt) {
    sum_thr += total_thr_[i - 1];
    sum_lat += total_lat_[i - 1];
    sum_c_lat += total_c_lat_[i - 1];

    sum_q_thr += total_qry_thr_[i - 1];
    sum_q_c_lat += total_q_c_lat_[i - 1];

    sum_gossip_thr += total_gossip_[i - 1];
  }
  qcnt = cnt;
 
#if 0 
  int qbegin = 0, qend = 0;
  for (int i = 0; i < total_qry_thr_.size(); ++i) {
    if (total_qry_thr_[i] == 0) continue;
    qbegin = i;
    break;
  }
  
  for (int i = total_qry_thr_.size()  - 1; i >= 0; --i) {
    if (total_qry_thr_[i] == 0) continue;
    qend = i;
    break;
  }

  for (int i = qbegin; i <= qend; ++i) {
    if (total_qry_thr_[i] == 0) continue;
    sum_q_thr += total_qry_thr_[i];
    sum_q_c_lat += total_q_c_lat_[i];
    ++qcnt;
  }
#endif

  double med_thr = median(total_thr_, begin - 1, end - 1);
  double avg_thr_m = sum_thr / cnt / 1000.0 / 1000.0;
  double med_thr_m = med_thr  / 1000.0 / 1000.0;
  double avg_lat_us = sum_lat / cnt; 
  double avg_c_lat_us = sum_c_lat / cnt;
  double avg_gossip = sum_gossip_thr / cnt;
  double avg_q_thr = sum_q_thr / qcnt;
  double avg_q_c_lat_ms = sum_q_c_lat / qcnt;

  printf("--- benchmark statistics ---\n");
  printf("  (1) data cnt: %d\n", cnt);
  printf("  (2) OLTP fly: %d, OLTP thr: %lf M txn/s (med %lf) lat: %lf us, %lf gossips/s\n", 
                config.getOnFly(), avg_thr_m, med_thr_m, avg_c_lat_us, avg_gossip);
  printf("  (3) OLAP fly: %d, OLAP thr: %lf qry/s lat: %lf ms\n", 
                config.getQryOnFly(), avg_q_thr, avg_q_c_lat_ms);

#if 0
  // caluclate local latency
  auto &timer = workers_[0]->latency_timer_;
  timer.calculate_detailed();
  double m_l = timer.report_medium() / sec_cyc_ * 1000;
  double m_9 = timer.report_90() / sec_cyc_ * 1000;
  double m_99 = timer.report_99() / sec_cyc_ * 1000;
  printf("Local latency: ");
  std::cout << m_l << " " << m_9<<" " <<m_99<<std::endl;
  if (LOG_RESULTS)
    log_file << m_l << " " << m_9<<" " <<m_99<<std::endl;

  uint64_t total_executed(0), total_aborted(0);
  /* Maybe we need better calculations */
  for(auto it = workers.begin();
      it != workers.end();++it) {
    volatile uint64_t committed = (*it)->ntxn_commits_;
    (*it)->check_consistency();
    total_executed += (*it)->ntxn_commits_;
    total_aborted  += (*it)->ntxn_aborts_;
  }

  const unsigned long elapsed = t.lap(); // lap() must come after do_txn_finish(),
  const double elapsed_sec = double(elapsed) / 1000000.0;

  const pair<uint64_t, uint64_t> mem_info_after = get_system_memory_info();
  const int64_t delta = int64_t(mem_info_before.first) - int64_t(mem_info_after.first); // free mem
  const double delta_mb = double(delta)/1048576.0;

  cerr << "--- benchmark statistics ---" << endl;
  cerr << "runtime: " << elapsed_sec << " sec" << endl;
  cerr << "memory delta: " << delta_mb  << " MB" << endl;
#endif
}

uint64_t Reporter::calc_delta_(const uint64_t TxnProf::*p,
                               vector<uint64_t> &prevs, int type) const {

  uint64_t sum = 0;
  assert(prevs.size() == workers_.size());

  for(uint i = 0; i < prevs.size(); ++i) {
    uint64_t snap = workers_[i]->get_profile().*p;

    // printf detailed information
    switch(type) {
      case TXN_THR:
        if (SHOW_WORKER_THR)
          printf("worker %d, %lu\n", i, snap - prevs[i]);
        break;
      default:
        break;
    }

    // add sum
    assert(snap >= prevs[i]);
    sum += (snap - prevs[i]);

    // reset prevs
    prevs[i] = snap;
  }

  return sum;
}
  
uint64_t Reporter::calc_delta_gossip(std::vector<uint64_t> &prevs) const {
  uint64_t sum = 0;
  assert(prevs.size() == workers_.size());

  for(uint i = 0; i < prevs.size(); ++i) {
    uint64_t snap = workers_[i]->get_worker_gossip();

    // add sum
    assert(snap >= prevs[i]);
    sum += (snap - prevs[i]);

    // reset prevs
    prevs[i] = snap;
  }

  return sum;

}

uint64_t Reporter::calc_delta_(const uint64_t LogProf::*p,
                               vector<uint64_t> &prevs, int type) const {

  uint64_t sum = 0;
  assert(prevs.size() == replayers_.size());

  for(uint i = 0; i < prevs.size(); ++i) {
    uint64_t snap = replayers_[i]->get_profile().*p;

    // printf detailed information
    switch(type) {
      case LOG_THR:
        if (SHOW_BACKUP_THR)
          printf("replayer %d, %lu\n", i, snap - prevs[i]);
        break;
      default:
        break;
    }

    // add sum
    assert(snap >= prevs[i]);
    sum += (snap - prevs[i]);

    // reset prevs
    prevs[i] = snap;
  }

  return sum;
}

uint64_t Reporter::calc_delta_(const uint64_t QryProf::*p,
                               vector<uint64_t> &prevs, int type) const {

  uint64_t sum = 0;
  assert(prevs.size() == queryers_.size());

  for(uint i = 0; i < prevs.size(); ++i) {
    uint64_t snap = queryers_[i]->get_profile().*p;

    // printf detailed information
    switch(type) {
      default:
        break;
    }

    // add sum
    assert(snap >= prevs[i]);
    sum += (snap - prevs[i]);

    // reset prevs
    prevs[i] = snap;
  }
  return sum;
}

void Reporter::calc_store() {
  uint64_t update = 0, copy = 0;

  for (const BackupDB *db : backup_dbs_) {
    const vector<BackupStore *> stores = db->get_stores();
    for (BackupStore *b : stores) {
      if (!b) continue;
      const StoreProf &prof = b->get_profile();
      update += prof.num_update;
      copy += prof.num_copy; 
    }
  }

  num_update = update;
  num_copy = copy;

}

}  // namespace oltp
}  // namespace nocc

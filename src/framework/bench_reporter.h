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

#ifndef BENCH_REPORT_H_
#define BENCH_REPORT_H_

#include <vector>
#include "bench_worker.h"
#include "backup_worker.h"
#include "bench_query.h"

#include "backup_store/backup_db.h"

namespace nocc {
namespace oltp {

class Reporter { // not thread safe
 public:
   
   // the data exchanged between servers
  struct ReportData {

    // throughput
    double txn_thr = 0.0;  // txn
    double send_log_thr = 0.0;  // logs sent by txn
    double log_thr = 0.0;  // log replay
    double qry_thr = 0.0;  // query

    double copy_thr = 0.0;
    double read_thr = 0.0;

    double gossip_thr = 0.0;

    // abort
    int32_t aborts = 0;  // abort number

    void clear() {
      txn_thr = 0.0;
      send_log_thr = 0.0;
      log_thr = 0.0;
      qry_thr = 0.0;

      copy_thr = 0.0;
      read_thr = 0.0;

      gossip_thr = 0.0;

      aborts = 0;
    }
  };

  Reporter(const std::vector<BenchWorker *> &workers,
           const std::vector<LogWorker *> &replayers,
           const std::vector<QueryWorker *> &queryers,
           const std::vector<BenchClient *> &clients,
           const std::vector<QueryClient *> &qry_clients,
           const std::vector<BackupDB *> &backup_dbs);

  inline size_t data_len() const { return sizeof(ReportData); }

  // merge local report data (rd_) with data from other reporters
  void merge_data(const char *data_buf);

  // print result on stdout and files
  void report_data(uint64_t epoch,std::ofstream &log_file);

  // calculate all of data
  void collect_data(char *data,struct timespec &start_t);

  // benchmark statistic
  void print_end_stat(std::ofstream &log_file) const;

 private:

  // for print information type
  enum PRINT_TYPE {
    NONE = 0,
    TXN_THR,
    LOG_THR
  };

  void init_();

  // threads
  const std::vector<BenchWorker *> &workers_;
  const std::vector<LogWorker *> &replayers_;
  const std::vector<QueryWorker *> &queryers_;
  const std::vector<BenchClient *> &clients_;
  const std::vector<QueryClient *> &qry_clients_;

  // stores
  const std::vector<BackupDB *> &backup_dbs_;

  uint64_t sec_cyc_;  // #cycles for a second

  ReportData rd_;

  std::vector<uint64_t> prev_commits_;
  std::vector<uint64_t> prev_aborts_;
  std::vector<uint64_t> prev_log_send_;
  std::vector<uint64_t> prev_gossip_;
  std::vector<double> total_thr_;
  std::vector<double> total_log_send_;
  std::vector<double> total_qry_thr_;
  std::vector<double> total_lat_;
  std::vector<double> total_c_lat_;
  std::vector<double> total_q_c_lat_;
  std::vector<double> total_freshness_;
  std::vector<double> total_gossip_;

  // for backups
  std::vector<uint64_t> prev_logs_;
  std::vector<uint64_t> prev_log_cycles_;
  std::vector<uint64_t> prev_index_cycles_;
  std::vector<uint64_t> prev_balancing_cycles_;

  // for query workers
  std::vector<uint64_t> prev_queries_;
  std::vector<uint64_t> prev_freshness_;
  std::vector<uint64_t> prev_readop_;

  // for backup stores
  uint64_t num_update;
  uint64_t num_copy;
  uint64_t prev_num_update;
  uint64_t prev_num_copy;



  /**
   *  calculaiton functions
   */
  // for txn workers
  uint64_t calc_delta_(const uint64_t TxnProf::*p,
                       std::vector<uint64_t> &prevs, int type = NONE) const;
  uint64_t calc_delta_gossip(std::vector<uint64_t> &prevs) const;

  // for backups
  uint64_t calc_delta_(const uint64_t LogProf::*p,
                       std::vector<uint64_t> &prevs, int type = NONE) const;

  // for query workers
  uint64_t calc_delta_(const uint64_t QryProf::*p,
                       std::vector<uint64_t> &prevs, int type = NONE) const;

  // for backup stores
  void calc_store();


};

} // namespace oltp
} // namespace nocc

#endif

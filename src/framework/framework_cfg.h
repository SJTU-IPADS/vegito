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

#ifndef FRAMEWORK_CFG_H_
#define FRAMEWORK_CFG_H_

#include "custom_config.h"
#include <string>
#include <vector>

#define FRESHNESS 0

namespace nocc {
namespace framework {

// config for whole system
class Config {
 public:
  // 1. Environments
  inline const std::string &getExeName() const { return exe_name_; }
  inline const std::string &getConfigFile() const { return config_file_; }
  inline bool getVerbose() const { return verbose_; }
  inline int isShowQueryResult() const { return q_verbose_; } 

  // 2. Benchmark
  inline const std::string &getBenchType() const { return bench_type_; }
  inline const std::string &getBenchOptions() const { return bench_opts_; }
  inline int getScaleFactorPerPartition() const { return scale_factor_; }
  inline int getQuerySession() const { return qry_session_; } 
  inline int getAnalyticsSession() const { return ana_session_; } 
  inline int getDistRatio() const { return distributed_ratio_; }

  // 3. data partition
  inline int getTPFactor() const { return rep_tp_factor_; }
  inline int getAPFactor() const { return rep_ap_factor_; }
  inline int getGPFactor() const { return rep_gp_factor_; }
  inline int getNumPrimaries() const { return num_primaries_; }
  
  // 4. server config
  inline int getNumServers() const { return num_servers_; }  // #machines
  inline const std::vector<std::string> 
               &getServerHosts() const { return server_hosts_; }
  inline int getServerID() const { return server_id_; }  // machine id
  
  // 5. threads
  inline int getNumTxnThreads() const { return txn_nthreads_; }
  inline int getNumQueryThreads() const { return query_nthreads_; }
  inline int getNumAnalyticsThreads() const { return analytics_nthreads_; }
  inline int getNumWorkerThreads() const { return txn_nthreads_ + query_nthreads_; }
  inline int getNumBackupThreads() const { return backup_nthreads_; }
  inline int getNumRoutines() const { return num_routines_; }

  // 6. Log
  inline bool isUseLogger() const { return use_logger_; }
  inline bool isCleanLog() const { return clean_log_; }

  // 7. Epoch
  inline bool isUseEpoch() const { return use_epoch_; }
  inline int getEpochType() const { return epoch_type_; }
  inline double getEpochSeconds() const { return sync_seconds_; }

  // 8. Backup Store
  inline int getBackupStoreType() const { return backup_store_type_; }
  inline int getColSplitType() const { return col_split_type_; }
  inline bool isUseIndex() const { return use_index_; }
  inline bool isLazyIndex() const { return lazy_index_; }

  // 9. Client
  inline bool isUseClient() const { return use_client_; }
  inline int getOnFly() const { return on_fly_; }
  inline int getSendRate() const { return send_rate_; }
  inline bool isUseQryClient() const { return q_use_client_; }
  inline int getQryOnFly() const { return q_on_fly_; }
  inline int getQrySendRate() const { return q_send_rate_; }

  // 10. Time
  inline int getRunSec() const { return run_sec_; }
  inline int getTxnEndSec() const { return txn_end_sec_; }
  inline int getQryStartSec() const { return q_start_sec_; }
  inline int getAnaStartSec() const { return a_start_sec_; }
  inline int getQryRound() const { return q_round_; }
  inline int getAnaRound() const { return a_round_; }
  inline int getCleanerStartSec() const { return cleaner_start_sec_; }

  // 11. Graph
  inline bool isUseSegGraph() const { return use_seg_graph_; }
  inline bool isUseGrapeEngine() const { return use_grape_engine_; }

  void parse_sys_args(int argc, char **argv);
  void parse_sys_xml();
  void printConfig() const;

 private:
  std::string exe_name_;
  std::string bench_type_;   // ch/wiki/ldbc
  std::string config_file_;  // xml file
  std::string bench_opts_;
  int num_servers_ = 1;
  int server_id_ = 0;
  int distributed_ratio_ = 1;
  int txn_nthreads_ = 8;
  int query_nthreads_ = 0;
  int analytics_nthreads_ = 0;
  int backup_nthreads_ = 1;
  int rep_tp_factor_ = 1;  // replication factor for TP/Backup
  int rep_ap_factor_ = 0;  // replication factor for AP/Backup
  int rep_gp_factor_ = 0;  // replication factor for Graph
  int num_routines_ = 1;
  int scale_factor_ = 1;  // scale factor per partition
  int qry_session_ = 1;
  int ana_session_ = 1;
  int num_primaries_ = -1;  // #primaries partitions (default: 1 for 1 mac)
  double sync_seconds_ = 0.05;  // epoch time
  bool verbose_ = false;
  bool q_verbose_ = false;
  bool use_logger_ = 0;
  bool clean_log_ = 1;

  bool use_client_ = 0;
  int on_fly_ = -1;
  int send_rate_ = 1;
  
  bool q_use_client_ = 0;
  int q_on_fly_ = -1;
  int q_send_rate_ = 1;

  // 0 w/o ts, 1 global ts, 2 global vec, 3 local ts, 4 real epoch
  bool use_epoch_ = 0;
  int epoch_type_ = 0;  
  
  // 0 for kv-store, 1 for row-store, 2 for column-store, 3 for naive col
  int backup_store_type_ = 0;
  int col_split_type_ = 0;

  // index
  bool use_index_ = false;
  bool lazy_index_ = false;

  // time
  int run_sec_ = 40;
  int txn_end_sec_ = 100;
  int q_start_sec_ = 0;
  int a_start_sec_ = 0;
  int q_round_ = 100;
  int a_round_ = 10;
  int cleaner_start_sec_ = 0;

  // macro
  bool use_backup_store_ = USE_BACKUP_STORE;

  // graph
  bool use_seg_graph_ = false;
  bool use_grape_engine_ = false;

  std::vector<std::string> server_hosts_;
};  // class Config

extern Config config;

} // namespace framework
} // namespace nocc

#endif  // FRAMEWORK_CFG_H_

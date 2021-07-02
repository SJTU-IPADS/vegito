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

#ifndef NOCC_OLTP_BENCH_BACKUP_H
#define NOCC_OLTP_BENCH_BACKUP_H

#include "framework.h"

#include "all.h"
#include "db/db_logger.h"
#include "./utils/thread.h"

#include "view_manager.h"
#include "log_cleaner.h"

extern size_t backup_nthreads;

namespace nocc {
namespace oltp {

uint64_t get_min_using_ver();

struct LogProf {
  uint64_t log;       // # of logs
  uint64_t log_c;     // clean log cycles
  uint64_t index_c;   // lazy update index cycles
  uint64_t balance_c; // balance index cycles

  LogProf()
    : log(0), log_c(0), index_c(0), balance_c(0) { }
};

class LogWorker : public ndb_thread {
 public:
  LogWorker(unsigned worker_id);
  void run();
  const LogProf &get_profile() const { return prof_; }

  inline static uint64_t get_read_epoch() {
    return epoch / (backup_nthreads + other_step_);
  }

  const std::vector<uint64_t> &get_log_tailer() const {
    return log_tailer_;
  }

  volatile bool started;

  inline void 
  decode(uint64_t idx, uint64_t &n_id, uint64_t &t_id, uint64_t &l_id) const {
    l_id = idx % num_logs_;
    t_id = idx / num_logs_ % num_threads_;
    n_id = idx / num_logs_ / num_threads_;
  }

 private:
  
  inline static uint64_t get_log_epoch_() {
    return (epoch / (backup_nthreads + other_step_) + 1);
  }

  inline uint64_t idx_(int n_id, int t_id, int l_id) const {
    return (n_id * num_threads_ + t_id) * num_logs_ + l_id;
  }

  void thread_local_init();
  void create_qps();
  void update_offsets_(int n_id, int t_id, int l_id, uint64_t total_size);
  void update_epoch_(uint32_t start_tid, uint32_t final_tid,
                    uint32_t thread_gap);
  void barrier_(uint64_t old_epoch);

  bool backup_clean_log(int n_id, int t_id, int l_id, char *poll_ptr,
                        char *tailer_ptr, uint64_t msg_siz);

  static volatile uint64_t epoch;

  static const uint64_t other_step_ = 1;

  const int mac_id_; 
  const int worker_id_;
  const int thread_id_;

  const int num_nodes_;
  const int num_threads_;
  const int num_logs_;

  const uint32_t buf_sz_;
  const uint32_t log_threshold_;

  bool need_barrier_;
  uint64_t barrier_e_;

  std::vector<Qp *> qp_vec_;
  std::vector<uint64_t> tailer_raddr_;  // address of tailer in remote machines
  std::vector<uint64_t> last_tailer_;
  std::vector<uint64_t> log_tailer_;
  std::vector<uint64_t> min_log_epoch_;

  LogProf prof_;
};

class GCollector : public ndb_thread {
 public:
  GCollector(const std::vector<BackupDB *> &stores); 
  void run();

 private:
  const std::vector<BackupDB *> &stores_;
  const int thread_id_;
};

}  // namespace oltp
}  // namespace nocc
#endif

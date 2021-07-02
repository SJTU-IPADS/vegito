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

#include "framework_cfg.h"
#include "log_area.h"
#include "backup_worker.h"
#include "bench_worker.h"
#include "log_cleaner.h"

#include "ring_msg.h"
#include "ud_msg.h"

/* global config constants */
extern size_t current_partition;
extern size_t txn_nthreads;

using namespace nocc::framework;
using namespace rdmaio::ringmsg;
using namespace std;

namespace nocc {
using namespace util;
__thread oltp::LogWorker *backup_worker = NULL;
namespace oltp {

volatile uint64_t LogWorker::epoch = 0;

extern RdmaCtrl *cm;

LogWorker::LogWorker(unsigned worker_id)
    : mac_id_(current_partition),
      worker_id_(worker_id), 
      // critical code for cpu binding
      // thread_id_(txn_nthreads > 8? (21 - worker_id) : (8 + worker_id_)),
      thread_id_(getCpuTxn() + txn_nthreads + worker_id),
      num_nodes_(cm->get_num_nodes()),
      num_threads_(txn_nthreads),
      num_logs_(logArea.num_log()),
      started(false),
      buf_sz_(logArea.getLogBufferSize()),
      log_threshold_(buf_sz_ / 8),
      need_barrier_(false),
      barrier_e_(0),
      tailer_raddr_(num_nodes_ * num_threads_ * num_logs_, 0),
      last_tailer_(num_nodes_ * num_threads_ * num_logs_, 0),
      log_tailer_(num_nodes_ * num_threads_ * num_logs_, 0),
      min_log_epoch_(num_nodes_ * num_threads_ * num_logs_, 0)
{

  assert(cm != NULL);
  assert(worker_id_ < backup_nthreads);
  assert(other_step_ >= 1);

  for(int n_id = 0; n_id < num_nodes_; ++n_id) {
    for(int t_id = 0; t_id < num_threads_; ++t_id) {
      for (int l_id = 0; l_id < num_logs_; ++l_id) {
        uint64_t addr = logArea.getMetaBase(n_id, t_id, l_id) +  
                        mac_id_ * sizeof(uint64_t);
        tailer_raddr_[idx_(n_id, t_id, l_id)] = addr;
      }
    }
  }

}

void LogWorker::run() {
  thread_local_init();

  int cpu_num = BindToCore(thread_id_);
  printf("[Backup Worker %d] thread_id %d\n", worker_id_, thread_id_);

  assert(backup_worker == NULL);
  backup_worker = this;

  uint32_t start_tid = worker_id_;
  uint32_t final_tid = num_threads_;
  uint32_t thread_gap = backup_nthreads;

  //main loop for backup worker
  while(true) {
    if (!started) continue;

    for (int l_id = 0; l_id < num_logs_; ++l_id) {
      bool is_ap = view.is_ap(l_id);
      if (is_ap && need_barrier_) {
        barrier_(barrier_e_);
        continue;
      }

      for(int t_id = start_tid; t_id < final_tid; t_id += thread_gap) {
  
        for(int n_id = 0; n_id < num_nodes_; ++n_id) {
          if (is_ap && need_barrier_) break;

          char *ptr = (char *)cm->conn_buf_+ logArea.getBufferBase(n_id, t_id, l_id);
          assert(log_tailer_[idx_(n_id, t_id, l_id)] < buf_sz_);
          volatile char *poll_ptr = (ptr + log_tailer_[idx_(n_id, t_id, l_id)]);
          uint64_t msg_size = 0;
          char *tailer_ptr = DBLogger::check_log_completion(poll_ptr, &msg_size);
          bool need_update = false;
          if (tailer_ptr == NULL) {
            // printf("[WARNING] log is empty! n_id %d t_id %d l_id %d\n",
            //        n_id, t_id, l_id);
            if (is_ap && config.isUseEpoch() && config.getEpochType() != 1) {
              uint64_t log_epoch = get_log_epoch_();
              uint64_t txn_e = BenchWorker::get_txn_epoch();
              if (txn_e > log_epoch) {
                uint64_t idx = idx_(n_id, t_id, l_id);
                if (txn_e > min_log_epoch_[idx])
                  min_log_epoch_[idx] = txn_e;
                need_update = true;
              }
            }
          } else {
            // msg_size = |header| + |content| + |tailer|
            need_update = backup_clean_log(n_id, t_id, l_id, 
                                           (char *) poll_ptr, tailer_ptr, 
                                           msg_size);
          }
          if (need_update)
            update_epoch_(start_tid, final_tid, thread_gap);
        }  // end loop threads
      }  // end loop machines
    }  // end loop logs

  }  // end main loop
}

inline bool LogWorker::
backup_clean_log(int n_id, int t_id, int l_id, char *poll_ptr,
                 char *tailer_ptr, uint64_t msg_size) {
  // vector<uint64_t> *tsp = NULL;
  TXTailer *tailer = (TXTailer *) tailer_ptr;

  uint64_t begin = rdtsc(), end;

  int p_id = view.get_backup_pid(mac_id_, l_id);
  bool is_ap = view.is_ap(l_id);

  if (!config.isUseEpoch() || !is_ap) {
    if (config.isCleanLog())
      DBLogger::clean_log(l_id, p_id, poll_ptr, tailer_ptr, 0);
  } else {
    uint64_t ts = tailer->ts_vec;
    uint64_t log_epoch = get_log_epoch_();

    if (ts < log_epoch) {
      // printf("log epoch %lu, bound %lu\n", ts, log_epoch);
      ts = log_epoch;      // XXX: tricky problem because of async-epoch
    }
  
    if (ts > log_epoch) {
      uint64_t idx = idx_(n_id, t_id, l_id);
      if (ts > min_log_epoch_[idx])
        min_log_epoch_[idx] = ts;
      return true; 
    }

#if 0  // always 0, 1 for no versions
    ts = 0;
#endif
    if (config.isCleanLog())
      DBLogger::clean_log(l_id, p_id, poll_ptr, tailer_ptr, ts);
  }

  // make the log invalid
  // total_size = |cksum| + msg_size + |cksum|
  uint64_t total_size = sizeof(uint64_t) * 2 + msg_size;
  memset((void*)poll_ptr, 0, total_size);

  // update log_tailer_ and head_offsets if necessary
  update_offsets_(n_id, t_id, l_id, total_size);

  end = rdtsc();
  prof_.log_c += end - begin;
  // prof_.index_c += end2 - begin2;
  ++prof_.log;

  return false;
}

inline void LogWorker::
update_offsets_(int n_id, int t_id, int l_id, uint64_t total_size) {
  // update log_tailer_
  uint64_t idx = idx_(n_id, t_id, l_id); 
  uint64_t tailer = (log_tailer_[idx] + total_size) % buf_sz_;
  log_tailer_[idx] = tailer;

  // update last_tailer_ if necessary
  uint64_t cleaned_sz = (tailer + buf_sz_ - last_tailer_[idx]) % buf_sz_;

  if(unlikely(cleaned_sz >= log_threshold_)) {
    Qp* qp = qp_vec_[n_id];
    int flags = IBV_SEND_INLINE;
    if(qp->first_send()){
      flags |= IBV_SEND_SIGNALED;
    }
    if(qp->need_poll()){
      qp->poll_completion();
    }
    qp->rc_post_send(IBV_WR_RDMA_WRITE, (char*) &tailer, sizeof(uint64_t),
                        tailer_raddr_[idx] ,flags);

    last_tailer_[idx] = tailer;
  }
}

inline void LogWorker::update_epoch_(uint32_t start_tid, uint32_t final_tid,
                                    uint32_t thread_gap) {
  volatile uint64_t log_epoch = get_log_epoch_();
  for (int l_id = 0; l_id < num_logs_; ++l_id) {
    bool is_ap = view.is_ap(l_id);
    if (!is_ap) continue;

    for (int n_id = 0; n_id < num_nodes_; ++n_id) {
      for (int t_id = start_tid; t_id < final_tid; t_id += thread_gap) {
        uint64_t idx = idx_(n_id, t_id, l_id);
        if (min_log_epoch_[idx] <= log_epoch) return; 
      }
    }
  }
  FAA(&epoch, 1);
  
  need_barrier_ = true;
  barrier_e_ = log_epoch;
  // barrier_(log_epoch);
}

void LogWorker::barrier_(uint64_t old_epoch) {
  // NOTE:  worker 0 should be left at last
  if (config.isLazyIndex() && config.isUseIndex())
    assert(old_epoch == get_log_epoch_());

  if (old_epoch == get_log_epoch_() 
      && epoch % (backup_nthreads + other_step_) != backup_nthreads) {
    return;
  }

  // start step 0
  if (!config.isLazyIndex()) {
    if (worker_id_ == 0) {
      // epoch += other_step_;
      assert (epoch % (backup_nthreads + other_step_) == backup_nthreads) ;
      FAA(&epoch, other_step_); // NOTE: this work should be left at last
    } 
  } else {
    LogCleaner *cleaner = DBLogger::log_cleaner_;

#if 1  // 1 for parallel, 0 for single
    if (backup_nthreads > 1) {
      if (worker_id_ == 0) {
        cleaner->prepare_balance(backup_nthreads);
        cleaner->parallel_balance(worker_id_);
        cleaner->end_balance();
        // epoch += other_step_;
        FAA(&epoch, other_step_);
      } else {
        cleaner->parallel_balance(worker_id_);
      }
    } else {
      cleaner->balance_index();
      // epoch += other_step_;
      FAA(&epoch, other_step_);
    }
#else
    if (worker_id_ == 0) {
      cleaner->balance_index();
      epoch += other_step_;
    }
#endif
  }
  while (old_epoch == get_log_epoch_()) ;
  assert(old_epoch + 1 == get_log_epoch_());

  need_barrier_ = false;
}

void LogWorker::create_qps() {
  // FIXME: current use hard coded dev id and port id
  // int use_port = 0;  // link to NUMA1
  int use_port = util::choose_nic(thread_id_);

  int dev_id = cm->get_active_dev(use_port);
  int port_idx = cm->get_active_port(use_port);

  cm->thread_local_init();
  cm->open_device(dev_id);
  cm->register_connect_mr(dev_id); // register memory on the specific device

  cm->link_connect_qps(thread_id_, dev_id, port_idx, 0, IBV_QPT_RC);
}

void LogWorker::thread_local_init() {
#if SINGLE_MR == 0
      /* create set of qps */
  create_qps();
#endif
  for(int i = 0;i < num_nodes_; i++) {
    Qp *qp = cm->get_rc_qp(thread_id_, i);
    assert(qp != NULL);
    qp_vec_.push_back(qp);
  }
}
  
GCollector::GCollector(const vector<BackupDB *> &stores) 
    // : stores_(stores), thread_id_(22) { }
    : stores_(stores), thread_id_(getCpuGC()) { }

void GCollector::run() {
  int cpu_num = BindToCore(thread_id_);
  printf("[GC] thread_id %d bind CPU %d\n", thread_id_, cpu_num);
  Breakdown_Timer timer;

  while (true) {
    sleep(1);

    timer.start();
    for (BackupDB *db : stores_) {
      // db->GC(LogWorker::get_read_epoch());
      db->GC(get_min_using_ver());
    }
    float time_ms = timer.get_diff_ms();
    // printf("GC time %lf ms\n", time_ms);
  }

}

}  // namespace oltp
}  // namespace nocc

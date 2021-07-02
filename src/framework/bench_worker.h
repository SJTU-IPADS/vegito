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

#ifndef BENCH_WORKER_H_
#define BENCH_WORKER_H_

#include "rdmaio.h"
#include "rworker.h"

#include "all.h"
#include "db/db_logger.h"
#include "db/txs/log_ts_manager.h"
#include "db/txs/tx_handler.h"

#include "framework.h"
#include "framework/utils/thread.h"
#include "framework/rpc.h"
#include "bench_query.h"

#include "util/fast_random.h"

using namespace rdmaio;
using namespace nocc::db;

#define TXN_PADDING_SZ 1024

namespace nocc {
namespace oltp {

struct TxnProf {
  uint64_t commit;
  uint64_t send_log;  // # of logs sending
  uint64_t abort;
  uint64_t execute;

  std::vector<uint64_t> txn_commits;
  std::vector<uint64_t> txn_aborts;

  TxnProf(size_t max_tx)
    : commit(0), abort(0), execute(0),
      txn_commits(max_tx, 0),
      txn_aborts(max_tx, 0) { }
};

/* Txn result return type */
typedef std::pair<bool, double> txn_result_t;

/* Registerered Txn execution function */
class BenchWorker;
typedef txn_result_t (*txn_fn_t)(BenchWorker *,yield_func_t &yield);

struct workload_desc {
  workload_desc() {}
  workload_desc(const std::string &name, double frequency, txn_fn_t fn)
    : name(name), frequency(frequency), fn(fn)
  {
    ALWAYS_ASSERT(frequency > 0.0);
    ALWAYS_ASSERT(frequency <= 1.0);
  }
  std::string name;
  double frequency;
  txn_fn_t fn;
  util::Breakdown_Timer latency_timer; // calculate the latency for each TX
  Profile p; // per tx profile
};
typedef std::vector<workload_desc> workload_desc_vec_t;

/**
 * Txn Worker
 */
class BenchWorker : public RWorker {
 public:
  /* methods */
  BenchWorker(int worker_id, uint32_t seed, MemDB *store);
  const TxnProf &get_profile() const { return prof_; }

  inline static uint64_t get_txn_epoch() {
    const uint64_t M1 = 1024ll * 1024ll;
    uint64_t txn_e = *(volatile uint64_t *) (cm->conn_buf_ + M1);
    return txn_e;
  }

  inline uint64_t get_worker_gossip() const {
    uint64_t res = 0;
    for (const TXHandler *tx : txs_) {
      res += tx->get_gossip_cnt();
    }
    return res;
  } 

  // simple wrapper to the underlying routine layer
  virtual void change_ctx_() override {
    tx_ = txs_[cor_id_];
    assert(tx_);
  }

  virtual workload_desc_vec_t get_workload() const = 0;
  virtual void check_consistency() {};
  virtual void workload_report() {
#if POLL_CYCLES == 1
    rdma_sched_.report();
    if(msg_handler_)
      msg_handler_->report();
#endif
  };

  void set_qeury_worker(QueryWorker *qw) { qw_ = qw; }

  volatile bool   running;
  volatile bool   inited;
  volatile bool   stop;
  util::Breakdown_Timer latency_timer_;

 protected:
  virtual void process_param(const void *padding) { }
  virtual void prepare_param_self(int cor_id) { }
  virtual void prepare_response(void *padding) { }

  TXHandler *tx_;       /* current coroutine's tx handler */
  DBLogger *db_logger_;
  std::vector<util::fast_random> rand_gen_;

 private:
  virtual void user_init_() override;
  virtual void register_rpc_() override final { }
  virtual void run_body_(yield_func_t &yield) override;

  void wThreadLocalInit_();
  void init_logger_();
  void init_txs_(); 

  void worker_routine(yield_func_t &yield);
  void master_exit();
  
  const int worker_id_;
  MemDB *const store_;
  std::vector<TXHandler *> txs_;
  QueryWorker *qw_;

  TxnProf prof_;
  RoutineMeta *routine_meta_;
  LogTSManager tsm_;  // XXX: update!
  
  std::vector<workload_desc_vec_t> workloads_;
  
  LAT_VARS(yield);
};

class BenchClient : public ndb_thread {
 public:
  BenchClient(int num_workers);

  virtual void run() override;

  util::Breakdown_Timer timer_;
  volatile bool init_;

#if 0
 protected:
  int prepare_worker_id() { 
    return (send_worker_id_++) % num_workers_;
  }

  virtual void prepare_param(void *padding) { }
  
  virtual void process_response(const void *padding) { }
#endif
 
 private:
  const int num_workers_;
  const int thread_id_;
  
  int on_fly_;
  int send_rate_;
  int change_rate_;
  int send_worker_id_;
};

} // namesapce oltp
} // namesapce nocc

#endif

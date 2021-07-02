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

#ifndef QUERY_WORKER_H_
#define QUERY_WORKER_H_

#include "rdmaio.h"

#include "all.h"

#include "framework.h"
#include "framework/utils/thread.h"
#include "framework/rpc.h"
#include "rworker.h"

#include "util/util.h"
#include "util/fast_random.h"

#include "backup_worker.h"

using namespace rdmaio;
using namespace nocc::db;

namespace nocc {
namespace oltp {

struct QryProf {
  uint64_t commit;
  uint64_t freshness_ms;
  uint64_t read_op;
  std::vector<uint64_t> qry_commits;
  std::vector<uint64_t> db_cnts;
  std::vector<uint64_t> walk_cnts;

  QryProf(size_t max_qry)
    : commit(0), read_op(0),
       qry_commits(max_qry, 0), db_cnts(max_qry, 0),
      walk_cnts(max_qry, 0) { }
};

/* Registerered Txn execution function */
class QueryWorker;
typedef bool (*qry_fn_t) (QueryWorker *, yield_func_t &yield);

struct QryDesc {
  std::string name;
  qry_fn_t fn;
  util::Breakdown_Timer latency_timer; // calculate the latency for each Qry

  QryDesc(const std::string &name, qry_fn_t fn)
    : name(name), fn(fn) { }
};
typedef std::vector<QryDesc> QryDescVec;

typedef void TaskFn (void *);

class SubQueryWorker : public ndb_thread {
 public:
  SubQueryWorker(int worker_id);
  virtual void run() override;

  void set_task(TaskFn *task, void *args);
  void clear();  // join

 private:
  const int worker_id_;
  const int thread_id_;
 
  TaskFn *volatile task_;
  void *  volatile args_;
  volatile bool complete_;
};

/**
 * Txn Worker
 */
class QueryWorker : public RWorker {
 public:
  /* methods */
  QueryWorker(int worker_id, int num_thread, uint32_t seed);
  const QryProf &get_profile() const { return prof_; }

  inline uint64_t get_read_ver_() const {
    // return LogWorker::get_read_epoch(); 
    return using_ver_;
  }

  virtual QryDescVec get_workload() const = 0;
  virtual void register_rpc_() override final;
  virtual void thread_local_init() { };

  int get_worker_id() const { return worker_id_; }

  volatile unsigned int cor_id_; /* identify which co-routine is executing */

  volatile bool   running;
  volatile bool   inited;
  volatile bool   started;
  util::Breakdown_Timer latency_timer_;

 protected:
  struct RpcReply {
    int qid;
    int size;
  };

  const int worker_id_;
  const int num_thread_;
  const int thread_id_;
  const int q_verbose_;

  uint64_t db_cnt_;
  uint64_t walk_cnt_;
  uint64_t read_op_;
  char *payload_ptr_;
  char *user_payload_;
  
  QryProf prof_;

  // used to generate benchmark's random numbers
  std::vector<util::fast_random> rand_gen_;

  std::vector<SubQueryWorker *> subs_;
  std::vector<rdmaio::Qp *> qps_;

  LAT_VARS(yield);
  

 private:

  virtual void user_init_() override;
  virtual void run_body_(yield_func_t &yield) override;
  void wThreadLocalInit_();
 
  static void master_routine(yield_func_t &yield, int cor_id, 
                             QueryWorker *ctx);
  static void worker_routine(yield_func_t &yield, int cor_id, 
                             QueryWorker *ctx);
  static void master_routine2(yield_func_t &yield, int cor_id, 
                             QueryWorker *ctx);
  static void master_exit(QueryWorker *ctx, int cor_id);

  static const int MAX_QRY = 30;

  void timer_begin_(int cor_id, int qry_id);
  void timer_end_(int cor_id, int qry_id);

  void rpc_recv_req(int id, int cid, char *msg, void *arg);
  void rpc_recv_reply(int id, int cid, char *msg, void *arg);

  util::fast_random rand_generator_;
  RDMA_sched rdma_sched_;
  RoutineMeta *routine_meta_;
  
  std::vector<QryDescVec> workloads_;

  uint64_t using_ver_;
  int stat_;
  int recv_reply_;
  int qid_;
  int round_;
  util::Breakdown_Timer round_timer_; // calculate the latency for each Qry
};

class QueryClient : public ndb_thread {
 public:
  QueryClient(int num_session)
    : num_session_(num_session), init_(false), 
      thread_id_(nocc::util::getCpuClientQry()) { }

  virtual void run() override;

  util::Breakdown_Timer timer_;
  volatile bool init_;

 private:
  const int num_session_;
  const int thread_id_;
 
};


} // namesapce oltp
} // namesapce nocc

#endif

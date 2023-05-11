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

#include "bench_worker.h"
#include <boost/bind.hpp>

#include "db/txs/dbrad.h"
#include "db/txs/dbtx.h"

#include "framework.h"
#include "framework_cfg.h"
#include "view_manager.h"

#include "ud_msg.h"

#include "util/util.h"
#include "db/req_buf_allocator.h"

#include "structure/queue.h"

#include <atomic>

using namespace std;
using namespace nocc::util;
using namespace nocc::db;
using namespace nocc::framework;

#define PROCESS_RESP 0

namespace {

const int MAX_TXN = 24;

struct TxnReq {
  uint64_t start;
  // uint64_t padding[TXN_PADDING_SZ];
};

struct TxnResp {
  uint64_t start;
  uint64_t end;
  // uint64_t padding[TXN_PADDING_SZ];
};

const int MAX_WORKER = 24;
Queue<TxnReq> req_qs[MAX_WORKER];  // XXX: hard code
Queue<TxnResp> resp_qs[MAX_WORKER];

struct QryReq {
  uint64_t start;
  uint32_t qid;
};

struct QryResp {
  uint64_t start;
  uint64_t end;
};
extern const int MAX_SESSION;
extern Queue<QryReq> q_req_qs[];  // XXX: hard code
extern Queue<QryResp> q_resp_qs[];


atomic_int resp_num[MAX_WORKER];
atomic_int req_num[MAX_WORKER];

// critical code for cpu binding
int calc_tid(int worker_id) {
  // if (worker_id == 15) return 23;
  return getCpuTxn() + worker_id;
  // return (worker_id % 2 * 12 + worker_id / 2);
}

}

namespace nocc {

__thread oltp::BenchWorker *worker = nullptr;
__thread RPCMemAllocator *msg_buf_alloctors = nullptr;

namespace oltp {

BenchWorker::BenchWorker (int worker_id, uint32_t seed, MemDB *store) 
 // : RWorker(cm, worker_id, 1, worker_id, 1),  // TODO: hard code
 : RWorker(cm, calc_tid(worker_id), 0,  // NIC 0 for 20 workers 
           calc_tid(worker_id), 1),  // TODO: hard code
   worker_id_(worker_id),
   prof_(MAX_TXN),
   running(false), // true flag of whether to start the worker
   inited(false),
   stop(false),
   db_logger_(nullptr),
   tsm_(cm, calc_tid(worker_id)),
   store_(store),
   txs_(1 + coroutine_num, nullptr),
   workloads_(1 + coroutine_num),
   rand_gen_(1 + coroutine_num)
{
  assert(cm != nullptr);
  
  util::fast_random rand(seed);
  for (auto r : rand_gen_)
    r.set_seed0(rand.next());

  INIT_LAT_VARS(yield);
}

void BenchWorker::user_init_simple_() { 
#if USE_UD_MSG == 0
  // cm-> // normal usage QP for RC based RDMA messaging
  //   link_connect_qps(worker_id_, dev_id, port_idx, 0, IBV_QPT_RC);
#endif // USE_UD_MSG == 0

  // create qp for one_sided RDMA which logging and Farm use
  // used by `get_rc_qp` in `DBLogger::thread_local_init()`
  link_rc_qps_(1);

  // cm-> // create qp for one_sided RDMA which logging and Farm use
  //   link_connect_qps(worker_id_, dev_id, port_idx, 2, IBV_QPT_RC);
 
  // create qp for log timestamp
  // used by `get_rc_qp` in `LogTSManager::initialize_connect`
  link_rc_qps_(3);

  // tsm_.initialize_connect();  // get qps in ts_manager 
  
  // if (config.isUseLogger())
  //   init_logger_();

  printf("[Bench Worker %d] bind CPU %d\n", worker_id_, get_cpu_id());
}

void BenchWorker::user_init_() {
#if USE_UD_MSG == 0
  // cm-> // normal usage QP for RC based RDMA messaging
  //   link_connect_qps(worker_id_, dev_id, port_idx, 0, IBV_QPT_RC);
#endif // USE_UD_MSG == 0

  // create qp for one_sided RDMA which logging and Farm use
  // used by `get_rc_qp` in `DBLogger::thread_local_init()`
  link_rc_qps_(1);

  // cm-> // create qp for one_sided RDMA which logging and Farm use
  //   link_connect_qps(worker_id_, dev_id, port_idx, 2, IBV_QPT_RC);
 
  // create qp for log timestamp
  // used by `get_rc_qp` in `LogTSManager::initialize_connect`
  link_rc_qps_(3);

  tsm_.initialize_connect();  // get qps in ts_manager 
  
  if (config.isUseLogger())
    init_logger_();

  init_txs_();

  /* init routines */
  wThreadLocalInit_();

  printf("[Bench Worker %d] bind CPU %d\n", worker_id_, get_cpu_id());

  this->inited = true;
  while(!this->running) {
    asm volatile("" ::: "memory");
  }
}

void BenchWorker::init_logger_() {
  assert(db_logger_ == nullptr);
  int thread_id = calc_tid(worker_id_);
#if LOGGER_USE_RPC == 0
  db_logger_ = new DBLogger(worker_id_, thread_id, cm, &rdma_sched_);
#elif LOGGER_USE_RPC == 1
  db_logger_ = new DBLogger(worker_id_, thread_id, cm, rpc_handler_);
#elif LOGGER_USE_RPC == 2
  db_logger_ = new DBLogger(worker_id_, thread_id, cm, &rdma_sched_,rpc_handler_);
#endif
  db_logger_->set_send_log_cnt(&prof_.send_log);
  db_logger_->thread_local_init();
}

void BenchWorker::init_txs_() {
  for (int i = 0; i < coroutine_num + 1; ++i) {

#ifdef RAD_TX
    txs_[i] = new DBRad(store_,worker_id_,rpc_handler_,i);
#elif defined(OCC_TX)
    txs_[i] = new DBTX(store_,worker_id_,rpc_handler_,i, &this->tsm_);
    // txs_[i] = new DBTX(store_,worker_id_,rpc_handler_,i);
#elif defined(SI_TX)
    assert(ts_manager != NULL);
    txs_[i] = new DBSI(store_,worker_id_,rpc_handler_,ts_manager,i);
#else
    fprintf(stderr,"No transaction layer used!\n");
    assert(false);
#endif
  }
  /* init local tx so that it is not a null value */
  tx_ = txs_[cor_id_];
}

void BenchWorker::wThreadLocalInit_() {
  // these two thread_local variables cannot be initialized in constructor
  worker = this;
  msg_buf_alloctors = new RPCMemAllocator[1 + coroutine_num];

  for(int i = 1;i <= coroutine_num; ++i) {
    set_routine_fn(i, coroutine_func_t(bind(&BenchWorker::worker_routine, 
                                            this, _1)));
  }
}

// A new event loop channel
void BenchWorker::
run_body_(yield_func_t &yield) {
  while (true) {
    if(unlikely(!running)) {
      if(current_partition == 0 && worker_id_ == 0) 
        master_exit();

      return;
    }

    // first we need to poll rpcs
#if USE_UD_MSG == 1
    msg_handler_->poll_comps();
#else
    rpc_handler_->poll_comps();
#endif
    rdma_sched_.poll_comps();

    yield_next(yield);
  } // end main worker forever loop
}

// this flag is very tricky, it should be set this way
void 
// __attribute__((optimize("O1"))) 
BenchWorker::worker_routine(yield_func_t &yield) {
  yield_next(yield);

  using namespace db;
  /* worker routine that is used to run transactions */
  workloads_[cor_id_] = get_workload();
  auto &workload = workloads_[cor_id_];

  // Used for OCC retry
  uint64_t abort_seed = 73;
  while(abort_seed == rand_gen_[cor_id_].get_seed()) {
    // avoids seed collision
    ++abort_seed;
  }

  while(true) {
    /* select the workload */
    if (stop) {
      yield_next(yield);
      continue;
    }

    // get request
    const TxnReq *req;
    uint64_t start_ts;
    if (config.isUseClient()) {
      req = req_qs[worker_id_].pre_dequeue();
      bool exist_flag = bool(req);
      // bool exist_flag = (req_num[worker_id_].load() != 0);
      if (!exist_flag) {
        // printf("[WARNING] worker %d req_size %d, resp_size %d!\n", 
        //        worker_id_, req_num[worker_id_].load(),
        //        resp_num[worker_id_].load());
        // printf("[WARNING] worker %d no requests, size %lu!\n", 
        //        worker_id_, req_qs[worker_id_].size());
        // assert(req_qs[worker_id_].size() == 0);
        // assert(false);
        yield_next(yield);
        continue;
      } else {
        start_ts = req->start;
        // process_param(req->padding);
        req_qs[worker_id_].post_dequeue();
        // resp.end = rdtsc();
        // --req_num[worker_id_];
      }
    }
    prepare_param_self(cor_id_);

    // get transaction 
    double d = rand_gen_[cor_id_].next_uniform();
    uint tx_idx = 0;
    for(size_t i = 0;i < workload.size();++i) {
      if((i + 1) == workload.size() || d < workload[i].frequency) {
        tx_idx = i;
        break;
      }
      d -= workload[i].frequency;
    }

    const unsigned long old_seed = rand_gen_[cor_id_].get_seed();

    uint64_t start_clk = rdtsc();
    // assert(resp.start < start_clk);
    uint64_t end_clk = 0;
#if CALCULATE_LAT == 1
    if(cor_id_ == 1) {
      // only profile the latency for cor 1
#if LATENCY == 1
      latency_timer_.start();
#else
      (workload[tx_idx].latency_timer).start();
#endif
    }
#endif

  abort_retry:
    ++prof_.execute;

    txn_result_t ret = workload[tx_idx].fn(this, yield);
    
#if NO_ABORT == 1
    ret.first = true;
#endif
    if(likely(ret.first)) {
      // commit case
      end_clk = rdtsc();
#if CALCULATE_LAT == 1
      if(cor_id_ == 1) {
#if LATENCY == 1
        // latency_timer_.end();  // XXX: counter bug!
        latency_timer_.emplace(end_clk - start_clk);
#else
        workload[tx_idx].latency_timer.end();
#endif
      }
#endif
      ++prof_.commit;
      ++prof_.txn_commits[tx_idx];
#if PROFILE_RW_SET == 1 || PROFILE_SERVER_NUM == 1
      if(ret.second > 0)
        workload[tx_idx].p.process_rw(ret.second);
#endif
    } else {
      // abort case
      if(old_seed != abort_seed) {
        /* avoid too much calculation */
        // ctx->ntxn_abort_ratio_ += 1;
        abort_seed = old_seed;
        ++prof_.txn_aborts[tx_idx];
      }
      ++prof_.abort;

      yield_next(yield);

      // reset the old seed
      rand_gen_[cor_id_].set_seed(old_seed);
      goto abort_retry;
    }

    if (config.isUseClient()) {
      assert(ret.first);
#if 1
      TxnResp *resp = resp_qs[worker_id_].pre_enqueue();
      assert(resp);
      resp->start = start_ts;
      // prepare_response(resp->padding);
      resp->end = rdtsc();
      resp_qs[worker_id_].post_enqueue();
      // assert(resp.start < start_clk);
      // assert(resp.end > end_clk && end_clk != 0);
#else
      // TxnResp resp;
      // resp.start = start_ts;
      // bool flag = resp_qs[worker_id_].enqueue(resp);
      // assert(flag);
      resp_num[worker_id_]++;
#endif
    }

    yield_next(yield);
  }// end worker main loop
}

void BenchWorker::master_exit() {
  // only sample a few worker information
  auto &workload = workloads_[1];
  auto second_cycle = Breakdown_Timer::get_one_second_cycle();
  const TxnProf &prof = get_profile();

  printf("aborts: ");

  for(uint i = 0;i < workload.size();++i) {
    printf("        %s ratio: %f, executed %lu, rw_size %f",
            workload[i].name.c_str(),
            (double)(prof.txn_aborts[i]) / (prof.txn_commits[i] + (prof.txn_commits[i] == 0)),
            prof.txn_commits[i],
            workload[i].p.report());
    if (LATENCY != 1) {
      Breakdown_Timer &timer = workload[i].latency_timer;
      timer.calculate_detailed();
      printf(", latency: %f ms, m %f, 90 %f, 99 %f",
             timer.report() / second_cycle * 1000,
             timer.report_medium() / second_cycle * 1000,
             timer.report_90() / second_cycle * 1000,
             timer.report_99() / second_cycle * 1000);
    }
    printf("\n");
  }
  printf("\n");

  printf("total: ");
  for(uint i = 0;i < workload.size();++i) {
    printf(" %d %lu; ",i, prof.txn_commits[i]);
  }
  printf("succs ratio %f\n", double(prof.execute) / prof.commit);

  check_consistency();

  printf("[Bench Worker] master routine exit...\n");
}

BenchClient::BenchClient(int num_workers)
  : num_workers_(num_workers), init_(false), thread_id_(getCpuClientTxn()),
    on_fly_(config.getOnFly()), send_rate_(config.getSendRate())
    , change_rate_(0), send_worker_id_(0) 
  { 
    for (int i = 0; i < MAX_WORKER; ++i) {
      req_num[i] = 0;
      resp_num[i] = 0;
    }
  }

#if 0
void BenchClient::run() {
  int on_fly = on_fly_;
  int rate = send_rate_;

  BindToCore(thread_id_);
  printf("[Txn Client] bind CPU %d\n", thread_id_);

  for (int i = 0; i < on_fly; ++i) {
    // int worker_id = (s++) % num_workers_;
    int worker_id = i % num_workers_;
#if 0
    TxnReq *req = req_qs[worker_id].pre_enqueue();
    assert(req);
    // req->start = rdtsc();
    req_qs[worker_id].post_enqueue(); 
#endif
    req_num[worker_id]++;
  }

  init_ = true;

  uint64_t received = 0;
  uint64_t send_serv = 0;
  uint64_t s = 0;
  while (1) {
#if 0
    const TxnResp *resp = resp_qs[s % num_workers_].pre_dequeue();

    if (resp) {
      // deal with resp
      // uint64_t diff = resp->end - resp->start;
      uint64_t diff = rdtsc() - resp->start;
      resp_qs[s % num_workers_].post_dequeue();
      timer_.emplace(diff);

      ++received;

      if (received == rate) {
        for (int i = 0; i < rate; ++i) {
          int worker_id = (send_serv++) % num_workers_;
          TxnReq *req = req_qs[worker_id].pre_enqueue();
          assert(req);
          req->start = rdtsc();
          req_qs[worker_id].post_enqueue(); 
        }
        received = 0;
      } 
    } else {
      // printf("[WARNING] worker %d no response!\n", s % num_workers_);
    } 
    ++s;
#endif
    int worker_id = s % num_workers_;
    int rn = resp_num[worker_id].load();
    ++s;
    if (rn > 0) {
#if 0
      TxnReq *req = req_qs[worker_id].pre_enqueue();
      assert(req);
      req->start = rdtsc();
      req_qs[worker_id].post_enqueue();
      resp_flag[worker_id] = false;
#endif

      // req_num[worker_id] += rn;
      int before = atomic_fetch_add(&req_num[worker_id], rn);

#if 0
      if (before + rn != 50) 
        printf("before %d, rn %d\n", before, rn);
      // ++req_num[worker_id];
      for (int i = 0; i < num_workers_; ++i) {
        // if (i != (worker_id + 1) % num_workers_) continue;
        if (i != worker_id) continue;
        req_num[i] += rn;
        assert(req_num[i] = 50);
      }
#endif
      int before_resp = atomic_fetch_sub(&resp_num[worker_id], rn);
    }
  }
}
#else
void BenchClient::run() {
  int on_fly = on_fly_;
  int rate = send_rate_;

  BindToCore(thread_id_);
  printf("[Txn Client] bind CPU %d\n", thread_id_);

  for (int i = 0; i < on_fly; ++i) {
    // int worker_id = prepare_worker_id();
    int worker_id = i % num_workers_;
    TxnReq *req = req_qs[worker_id].pre_enqueue();
    assert(req);
    req->start = rdtsc();
    // prepare_param(req->padding);
    req_qs[worker_id].post_enqueue(); 
  }

  init_ = true;

  uint64_t received = 0;
  uint64_t s = 0;
  uint64_t sec_cyc = util::Breakdown_Timer::get_one_second_cycle();
  uint64_t start_ts = rdtsc();
  while (1) {
    uint64_t ts = rdtsc();
    float elapsed_sec = float(ts - start_ts) / sec_cyc;

    if (elapsed_sec > 2.0 && on_fly_ > 0 && change_rate_ == 0) {
#if 0
      change_rate_ = 5;  // TODO: bug!
      on_fly_ += change_rate_;
      printf("[Client] start change on fly to %d\n", on_fly_);
#endif
      start_ts = ts;
    }

    const TxnResp *resp = resp_qs[s % num_workers_].pre_dequeue();

    if (resp) {
      // deal with resp
      uint64_t diff = resp->end - resp->start;
      // uint64_t diff = rdtsc() - resp->start;
#if PROCESS_RESP
      process_response(resp->padding);
#endif
      resp_qs[s % num_workers_].post_dequeue();
      timer_.emplace(diff);

      ++received;

      if (received == rate) {
        int addition = 0;
        if (change_rate_ < 0) {
          change_rate_ += rate;
          if (change_rate_ <= 0) {
            ++s;
            if (change_rate_ == 0)
              printf("[Client] derease on fly to %d\n", on_fly_);
            received = 0;
            continue;
          } else {
            addition = -change_rate_;
            change_rate_ = 0;
          }
        }
        if (change_rate_ > 0) {
          addition = (change_rate_ < rate)? change_rate_ : rate;
          change_rate_ -= addition;
          if (change_rate_ == 0) {
            printf("[Client] increase on fly to %d\n", on_fly_);
          }
        } 
        for (int i = 0; i < rate + addition; ++i) {
          int worker_id = s % num_workers_;
          // int worker_id = prepare_worker_id();

          // printf("send_id: %d, receive_id %d\n", worker_id, s % num_workers_);
          TxnReq *req = req_qs[worker_id].pre_enqueue();
          assert(req);
          req->start = rdtsc();
          // prepare_param(req->padding);
          req_qs[worker_id].post_enqueue(); 
        }
        received = 0;
      } 
    }  
    ++s;

  }

}
#endif

} // namesapce oltp
} // namesapce nocc

#undef PROCESS_RESP

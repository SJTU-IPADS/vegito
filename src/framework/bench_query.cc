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

#include "bench_query.h"
#include <boost/bind.hpp>

#include "framework.h"
#include "framework_cfg.h"

#include "ud_msg.h"

#include "util/util.h"
#include "db/req_buf_allocator.h"

#include "structure/queue.h"

#define CLIENT_QID 0

using namespace std;
using namespace nocc::util;
using namespace nocc::db;
using namespace nocc::framework;

namespace {

// critical code for cpu binding
// const int query_start_thread = 12;
// const int query_start_thread = 0;  // only for 24 olap threads

inline int assign_thread(int worker_id) {
#if 0
  const int num_numa_core = CorePerSocket();
  if (config.getNumQueryThreads() > num_numa_core - 2)
    return getCpuTxn() + worker_id;
  else
    return num_numa_core + worker_id; 
#endif
  return getCpuQry() - worker_id;
}

enum {
  RPC_REQ = 0,
  RPC_REPLY
};

struct RpcReq {
  int qid;
};


struct QryReq {
  uint64_t start;
  uint32_t qid;
};

struct QryResp {
  uint64_t start;
  uint64_t end;
};

const int MAX_SESSION = 24;
Queue<QryReq> q_req_qs[MAX_SESSION];  // XXX: hard code
Queue<QryResp> q_resp_qs[MAX_SESSION];

}

namespace nocc {

__thread oltp::QueryWorker *qry_worker = nullptr;
__thread RPCMemAllocator *qry_msg_buf_alloctors = nullptr;

namespace oltp {
SubQueryWorker::SubQueryWorker(int worker_id)
  : worker_id_(worker_id), 
    thread_id_(assign_thread(worker_id)),
    task_(nullptr), args_(nullptr), complete_(false) { 
  }

void SubQueryWorker::run() {
  int cpu_num = BindToCore(thread_id_); // really specified to platforms
  printf("[Query Worker %d] bind CPU %d\n", worker_id_, cpu_num);

  while(1) {
    if (task_ == nullptr) {
      asm volatile("" ::: "memory");
      continue;
    }

    assert(!complete_);
    assert(args_ != nullptr);

    (*task_)(args_);
    complete_ = true;

    task_ = nullptr;
    args_ = nullptr;
  }
}

void SubQueryWorker::set_task(TaskFn *task, void *args) {
  assert(task_ == nullptr);
  args_ = args;
  task_ = task;
}

void SubQueryWorker::clear() {
  while (!complete_) {
    asm volatile("" ::: "memory");
  }

  complete_ = false;

  // task_ = nullptr;
  // args_ = nullptr;
}

QueryWorker::QueryWorker (int worker_id, int num_thread, uint32_t seed) 
 : RWorker(cm, assign_thread(worker_id),
           -1, 
           assign_thread(worker_id), 1),
   worker_id_(worker_id),
   thread_id_(assign_thread(worker_id_)), 
   num_thread_(num_thread),
   rand_generator_(seed),
   prof_(MAX_QRY),
   running(false), // true flag of whether to start the worker
   inited(false),
   started(false),
   db_cnt_(0),
   walk_cnt_(0),
   read_op_(0),
   q_verbose_(config.isShowQueryResult()),
   workloads_(1 + coroutine_num),
   subs_(num_thread_, nullptr),
   using_ver_(-1),
   stat_(0), recv_reply_(0), qid_(0),
   // r-set some local members
   qps_(config.getNumServers()),
   round_(0)
{
  assert(cm != nullptr);

  for (int i = 1; i < num_thread_; ++i) {
    subs_[i] = new SubQueryWorker(i);
  }
    
  char *msg_buf = (char *)Rmalloc(1024);
  payload_ptr_ = msg_buf + sizeof(uint64_t) + sizeof(rpc_header);

  INIT_LAT_VARS(yield);
}

void QueryWorker::user_init_() {
  // normal usage QP for RC based RDMA messaging
  link_rc_qps_(0);
  
  // create set of qps
  for (int i = 0; i < qps_.size(); ++i) {
    qps_[i] = cm->get_rc_qp(thread_id_, i, 0);
    assert(qps_[i] != nullptr);
  }
  wThreadLocalInit_();
  thread_local_init();
  
  // printf("[Query Worker %d] bind CPU %d\n", worker_id_, cpu_num);

  for (int i = 1; i < num_thread_; i++)
    subs_[i]->start();

  this->inited = true;
  while(!this->running) {
    asm volatile("" ::: "memory");
  }
}

void QueryWorker::wThreadLocalInit_() {
  qry_worker = this;
  /* worker routines related stuff */
  qry_msg_buf_alloctors = new RPCMemAllocator[1 + coroutine_num];

  rand_gen_.resize(1 + coroutine_num);
  for (auto r : rand_gen_)
    r.set_seed0(rand_generator_.next());

  for(uint i = 1;i <= coroutine_num;++i) {
    set_routine_fn(i, coroutine_func_t(bind(QueryWorker::worker_routine, 
                                            _1, i, this)));
  }
}

void QueryWorker::
master_routine2(yield_func_t &yield, int cor_id, QueryWorker *ctx) {
  ctx->workloads_[cor_id] = ctx->get_workload();
  auto &workload = ctx->workloads_[cor_id];
  
  // warmup
  if (current_partition == 0) {
    int num_mac = ctx->msg_handler_->get_num_nodes();
    // assert(num_mac > 1);
    int *node_ids = new int[num_mac];
    for(int i = 1; i < num_mac; ++i) {
      node_ids[i-1] = i;
    }

    QryProf &prof = ctx->prof_;
    // int &round = ctx->round_;
    int round = 0;
    int session_id = ctx->worker_id_ / ctx->num_thread_;
    bool use_client = config.isUseQryClient();
    QryReq req;
    QryResp resp;

    while (true) {
      volatile bool running = ctx->running;
      if(unlikely(!running)) {
        if (ctx->worker_id_ == 0) master_exit(ctx, cor_id);
        return;
      }
      if (!ctx->started || round >= config.getQryRound() || workload.size() == 0) 
      // if (!ctx->started || workload.size() == 0) 
      {
        continue;
      }
      
      switch (ctx->stat_) {
      case 0:
      {
        if (use_client) {
          bool recv = q_req_qs[session_id].dequeue(req);
          if (!recv) {
            break;
          } else {
            resp.start = req.start;
          }
        }

        // send request
#if CLIENT_QID == 1
        ctx->qid_ = req.qid % workload.size();
#endif
        ctx->timer_begin_(cor_id, ctx->qid_);
        if (ctx->qid_ == 0) {
          ctx->round_timer_.start();
        }
        assert(ctx->recv_reply_ == 0);
        RpcReq *req = (RpcReq *) ctx->payload_ptr_;
        req->qid = ctx->qid_;
        ctx->rpc_handler_->set_msg(ctx->payload_ptr_);
        ctx->rpc_handler_->send_reqs_ud(RPC_REQ, sizeof(RpcReq),
                                        node_ids, num_mac - 1, 0);
        
        ctx->stat_ = 1;
        // if (ctx->qid_ == 0) ctx->round_timer_.start();
        break;
      }
      case 1:
        // execute req
        // if (ctx->qid_ == 0) ctx->round_timer_.start();
        ctx->db_cnt_ = 0;
        ctx->walk_cnt_ = 0;
        ctx->read_op_ = 0;
        ctx->using_ver_ = LogWorker::get_read_epoch(); 
        workload[ctx->qid_].fn(ctx,yield);
        ctx->using_ver_ = -1;
        ctx->stat_ = 2;
        break;
      case 2:
        // receive reply
        ctx->msg_handler_->poll_comps();
        if (ctx->recv_reply_ == num_mac - 1) {
          // printf("[Master Node] receive all replies\n");
          ctx->recv_reply_ = 0;
          ctx->stat_ = 3;
        }
        break;

      case 3:
        // Aggregator
        // XXX: todo!
        // end

        if (use_client) {
          resp.end = rdtsc();
          bool flag = q_resp_qs[session_id].enqueue(resp);
          assert(flag);
        }
        ++prof.commit;
        ++prof.qry_commits[ctx->qid_];
        ++prof.read_op += ctx->read_op_;
        prof.db_cnts[ctx->qid_] += ctx->db_cnt_;
        prof.walk_cnts[ctx->qid_] += ctx->walk_cnt_;
        ctx->timer_end_(cor_id, ctx->qid_);

        if (ctx->qid_ == workload.size() - 1) {
#if CLIENT_QID == 0
          ctx->qid_ = 0;
          printf("[Bench Query] session %d round %d complete!\n", 
                  session_id, round);
          ctx->round_timer_.end();
#endif
          ++round;
        } else {
          ++ctx->qid_;
        }

        ctx->stat_ = 0;

        break;
      default:
        assert(false);
      }
    }
  
  } else {
    int master_id = 0;
    while (true) {
      switch (ctx->stat_) {
      case 0:
        // receive request
        ctx->msg_handler_->poll_comps();
        break;
      case 1:
        // execute req
        ctx->db_cnt_ = 0;
        ctx->walk_cnt_ = 0;
        ctx->read_op_ = 0;
        ctx->using_ver_ = LogWorker::get_read_epoch(); 
        workload[ctx->qid_].fn(ctx,yield);
        ctx->using_ver_ = -1;
        ctx->stat_ = 2;
        break;
      case 2:
      {
        // send reply
        RpcReply *reply = (RpcReply *) ctx->payload_ptr_;
        reply->qid = ctx->qid_;
        ctx->rpc_handler_->set_msg(ctx->payload_ptr_);
        ctx->rpc_handler_->send_reqs_ud(RPC_REPLY, sizeof(RpcReply) + reply->size,
                                        &master_id, 1, 0);
        
        ctx->stat_ = 0;
        break;
      }
      default:
        assert(false);
      }
    }
  }
}

void QueryWorker::register_rpc_() {
  using std::placeholders::_1;
  using std::placeholders::_2;
  using std::placeholders::_3;
  using std::placeholders::_4;

  register_callbacks(std::bind(&QueryWorker::rpc_recv_req, this,
                               _1, _2, _3, _4), RPC_REQ);
  register_callbacks(std::bind(&QueryWorker::rpc_recv_reply, this,
                               _1, _2, _3, _4), RPC_REPLY);
}

void QueryWorker::rpc_recv_req(int id, int cid, char *msg, void *arg) {
  RpcReq *req = (RpcReq *) msg;
  qid_ = req->qid;
  // printf("[Mac %d] receive qid = %d\n", current_partition, qid_); 

  stat_ = 1;
}

void QueryWorker::rpc_recv_reply(int id, int cid, char *msg, void *arg) {
  // printf("[Master Node] receive reply from %d\n", id);
  ++recv_reply_;

#if FRESHNESS   // 1 for freshness
  if (id != 2) return;

  RpcReply *reply = (RpcReply *) msg;
  assert(reply->size == sizeof(uint64_t));
  uint64_t *time = (uint64_t*) (msg + sizeof(RpcReply));
  struct timeval tv;
  gettimeofday(&tv, nullptr);
  uint64_t now_us = (uint64_t(tv.tv_sec) * 1000000 + tv.tv_usec);
  prof_.freshness_ms += (now_us - *time) / 1000.0;
  printf("epoch: %d ms, freshness: %lf ms\n",
         int(config.getEpochSeconds() * 1000), (now_us - *time) / 1000.0);
#endif
}

// A new event loop channel
void QueryWorker::
master_routine(yield_func_t &yield, int cor_id, QueryWorker *ctx) {
#if 0
  auto routine_meta = get_routine_meta(MASTER_ROUTINE_ID);

  while (true) {
    if(unlikely(!ctx->running)) {
      if(current_partition == 0 && ctx->worker_id_ == 0) 
        master_exit(ctx, cor_id);

      return;
    }

    // first we need to poll rpcs
#if USE_UD_MSG == 1
    ctx->msg_handler_->poll_comps();
#else
    ctx->rpc_handler_->poll_comps();
#endif
    ctx->rdma_sched_.poll_comps();

    auto next = routine_header->next_;
    //if(current_partition != 0) continue;
    if(next != routine_meta) {
      ctx->cor_id_ = next->id_;
      ctx->routine_meta_ = next;
      next->yield_to(yield);
    } else {
      
    }
  } // end main worker forever loop
#endif
}

void QueryWorker::timer_begin_(int cor_id, int qry_id) {
#if CALCULATE_LAT == 1
    // if(cor_id == 1) 
    {
      // only profile the latency for cor 1
// #if LATENCY == 1
#if 0
      latency_timer_.start();
#else
      (workloads_[cor_id][qry_id].latency_timer).start();
#endif
    }
#endif
}

void QueryWorker::timer_end_(int cor_id, int qry_id) {
#if CALCULATE_LAT == 1
    // if(cor_id == 1) 
    {
      // only profile the latency for cor 1
// #if LATENCY == 1
#if 0
      latency_timer_.end();
#else
      (workloads_[cor_id][qry_id].latency_timer).end();
#endif
    }
#endif
}

void QueryWorker::run_body_(yield_func_t &yield) {
  master_routine2(yield, 0, this);
}

// this is an aborted function!!!
// this flag is very tricky, it should be set this way
void __attribute__((optimize("O1"))) 
QueryWorker::worker_routine(yield_func_t &yield, int cor_id, QueryWorker *ctx) {
  ctx->yield_next(yield);

  assert(false);

#if 0
  /* worker routine that is used to run transactions */
  ctx->workloads_[cor_id] = ctx->get_workload();
  auto &workload = ctx->workloads_[cor_id];
  QryProf &prof = ctx->prof_;

  int round = 0;
  int session_id = ctx->worker_id_ / ctx->num_thread_;
  int qid = 0;
  while(true) {
    /* select the workload */
    if (!ctx->started || round >= config.getQryRound() || workload.size() == 0) {
      ctx->yield_next(yield);
      continue;
    }

    bool recv = false;
    {
      QryReq req;
      QryResp resp;
      if (config.isUseQryClient()) {
        recv = q_req_qs[session_id].dequeue(req);
        if (!recv) {
          // assert(false);
          ctx->yield_next(yield);
          continue;
        } else {
          resp.start = req.start;
          // resp.end = rdtsc();
        }

      }

      ctx->db_cnt_ = 0;
      ctx->timer_begin_(cor_id, qid);
      ctx->using_ver_ = LogWorker::get_read_epoch(); 
      bool ret = workload[qid].fn(ctx,yield);
      if (current_partition == 0) {
      
      } else {
      
      }

      ctx->using_ver_ = -1;
      assert(ret);
      ctx->timer_end_(cor_id, qid);

      ++prof.commit;
      ++prof.qry_commits[qid];
      prof.db_cnts[qid] += ctx->db_cnt_;
    
      if (config.isUseQryClient()) {
        resp.end = rdtsc();
        bool flag = q_resp_qs[session_id].enqueue(resp);
        assert(flag);
        // assert(resp.start < start_clk);
        // assert(resp.end > end_clk && end_clk != 0);
      }
      ctx->yield_next(yield);
    }
    ++qid;
    if (qid == workload.size()) {
      // printf("[Query Worker %d] round %d complete!\n", ctx->worker_id_, round);
      qid = 0;
      ++round;
    }

  } // end worker main loop
#endif
}

void QueryWorker::master_exit(QueryWorker *ctx, int cor_id) {
  // only sample a few worker information
  auto &workload = ctx->workloads_[cor_id];
  auto second_cycle = Breakdown_Timer::get_one_second_cycle();
  const QryProf &prof = ctx->get_profile();

  printf("[Query Worker] exit:\n");
  double lat_prod = 1.0;
  for (int i = 0; i < workload.size(); ++i) {
    Breakdown_Timer &timer = workload[i].latency_timer;
    if (timer.size() == 0) return;

    assert(timer.size() != 0);
    timer.calculate_detailed();

    uint64_t commit = prof.qry_commits[i];

    printf("%s: executed %lu",
           workload[i].name.c_str(),
           commit);

    if (commit == 0) {
      printf("\n");
      continue;
    }
    
    // printf(", avg db sz %lu, latency %f ms, m %f, 90 %f, 99 %f\n",
    //        prof.db_cnts[i] / prof.qry_commits[i],
    //        timer.report() / second_cycle * 1000,
    //        timer.report_medium() / second_cycle * 1000,
    //        timer.report_90() / second_cycle * 1000,
    //        timer.report_99() / second_cycle * 1000);
    float med_latency = timer.report_medium() / second_cycle * 1000;
    printf(", avg db sz %lu,\t walk %lu,\t latency %f ms",
           prof.db_cnts[i] / prof.qry_commits[i],
           prof.walk_cnts[i] / prof.qry_commits[i],
           med_latency);
    lat_prod *= med_latency;
    // printf(",\t m %f", timer.report_medium() / second_cycle * 1000);
    printf("\n");
  }

  if (workload.size() > 0)
    printf("GM: %f ms", (float) pow(lat_prod, 1.0 / workload.size()));
  printf("\n");

  // printf("total: ");
  // for(uint i = 0;i < workload.size();++i) {
  //   printf(" %d %lu; ",i, prof.qry_commits[i]);
  // }
  // printf("\n");
  printf("Round: latency %f sec\n",
         ctx->round_timer_.report_medium() / second_cycle);
  printf("[Query Worker] master routine exit...\n");
}

void QueryClient::run() {
  const int on_fly = config.getQryOnFly();
  const int rate = config.getQrySendRate();

  // XXX: bind
  int cpu_num = BindToCore(thread_id_);
  printf("[Query Client] bind CPU %d\n", thread_id_);

  uint64_t qid = 0;

  for (int i = 0; i < on_fly; ++i) {
    QryReq req;
    req.start = rdtsc(); 
    req.qid = qid++;
    bool flag = q_req_qs[i % num_session_].enqueue(req);
    assert(flag);
  }

  init_ = true;

  uint64_t received = 0;
  uint64_t s = 0;
  uint64_t send_serv = 0;

  while (1) {
    QryResp resp;
    bool rec = q_resp_qs[s % num_session_].dequeue(resp);

    if (rec) {
      // deal with resp
      timer_.emplace(resp.end - resp.start);

      ++received;
      if (received == rate) {
        for (int i = 0; i < rate; ++i) {
          QryReq req;
          req.start = rdtsc();
          req.qid = qid++;

          // Queue<QryReq> &q = q_req_qs[send_serv % num_session_];
          Queue<QryReq> &q = q_req_qs[s % num_session_];
          bool flag = q.enqueue(req);
          assert(flag);
          // printf("send to %lu, size %lu\n", send_serv % num_session_, q.size());
          ++send_serv;
        }
        received = 0;
      } 
    }  // end if (rec)
    ++s;
  }

}

} // namesapce oltp
} // namesapce nocc

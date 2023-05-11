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

#include "bench_listener.h"
#include "framework.h"
#include "config.h"

#include <signal.h>
#include <vector>
#include <boost/bind.hpp>
#include <boost/algorithm/string.hpp>

#include "framework_cfg.h"

// RDMA related
#include "ralloc.h"
#include "ring_msg.h"
#include "ud_msg.h"

#include "rpc.h"
#include "routine.h"

#include "util/util.h"

/* bench listener is used to monitor the various performance field of the current system */

/* global config constants */
extern size_t nthreads;
extern size_t current_partition;
extern size_t coroutine_num;
extern size_t distributed_ratio;

#define RPC_REPORT 1
#define RPC_EXIT   2
#define RPC_START  3

// for  LOG_RESULTS
#include <fstream>
std::ofstream log_file;

using namespace rdmaio;
using namespace nocc::util;
using namespace nocc::framework;

namespace nocc {

extern volatile bool cluster_running;

namespace oltp {

extern RdmaCtrl *cm;

Listener::Listener(const std::vector<BenchWorker *> &workers,
                   const std::vector<LogWorker *> &replayers,
                   const std::vector<QueryWorker *> &queryers,
                   const std::vector<AnalyticsWorker *> &analyzers,
                   Reporter &reporter)
  :epoch_(0),
   inited_(false),
   workers_(workers),
   replayers_(replayers),
   queryers_(queryers),
   analyzers_(analyzers),
   reporter_(reporter),
   num_mac_(config.getNumServers()),
   n_returned_(0),
   ret_bitmap_(1),  // 1 << MASTER_ID
   txn_stopped_(false), qry_started_(false), log_started_(false), ana_started_(false)
{
  assert(cm != NULL);
  assert(num_mac_ < 64);   // for ret_bitmap_

  if(LOG_RESULTS && current_partition == 0) {
    using namespace nocc::framework;
    char log_file_name[64];
    snprintf(log_file_name, 64, "./results/%s_%s_%lu_%lu_%lu_%lu.log",
             config.getExeName().c_str(), config.getBenchType().c_str(),
             uint64_t(num_mac_),nthreads,coroutine_num,distributed_ratio);
    log_file.open(log_file_name,std::ofstream::out);
    if(!log_file.is_open()) {
      // create the directory if necessary
    } else {
      fprintf(stdout,"log to %s\n",log_file_name);
    }
  }
}

void Listener::thread_local_init() {

  // add a bind?
  util::BindToCore(getCpuListener());

#ifdef WITH_RDMA
  // create qps and init msg handlers
  // int use_port = 0;  // NIC0 near to NUMA1
  int use_port = util::choose_nic(getCpuListener());  // NIC0 near to NUMA1
  int dev_id = cm->get_active_dev(use_port);
  int port_idx = cm->get_active_port(use_port);
  cm->thread_local_init();
  cm->open_device(dev_id);
  cm->register_connect_mr(dev_id); // register memory on the specific device

  using namespace rdmaio::udmsg;

  // int tid = nthreads;
  int tid = 40;
  int send_qp_num = 1;
  int total_threads = 1;

  rpc_handler_ = new Rpc(NULL,nthreads);
  msg_handler_ = new UDMsg(cm, tid, total_threads, MAX_SERVER_TO_SENT,
                           std::bind(&Rpc::poll_comp_callback,rpc_handler_,
                                     std::placeholders::_1,
                                     std::placeholders::_2),
                           dev_id, port_idx, send_qp_num);

  rpc_handler_->message_handler_ = msg_handler_; // reset the msg handler
  assert(msg_handler_->get_num_nodes() == num_mac_);


  if(rpc_handler_ != NULL) {
    rpc_handler_->register_callback(std::bind(&Listener::get_result_rpc_handler,this,
                                              std::placeholders::_1,
                                              std::placeholders::_2,
                                              std::placeholders::_3,
                                              std::placeholders::_4),RPC_REPORT);
    rpc_handler_->register_callback(std::bind(&Listener::exit_rpc_handler,this,
                                              std::placeholders::_1,
                                              std::placeholders::_2,
                                              std::placeholders::_3,
                                              std::placeholders::_4),RPC_EXIT);
    rpc_handler_->register_callback(std::bind(&Listener::start_rpc_handler,this,
                                              std::placeholders::_1,
                                              std::placeholders::_2,
                                              std::placeholders::_3,
                                              std::placeholders::_4),RPC_START);
  } else
    assert(false);

  // init rpc
  if(rpc_handler_ != NULL) {
    rpc_handler_->init();
    rpc_handler_->thread_local_init();
  }
  else
    assert(false);

  // init routines
  // RoutineMeta::thread_local_init();
#endif

  // for Rmalloc
  RThreadLocalInit();
}

void Listener::run() {
  // Some prepartion stuff
  thread_local_init();

  fprintf(stdout,"[Listener]: Monitor running!\n");

  struct timespec start_t,end_t;
  clock_gettime(CLOCK_REALTIME,&start_t);
  clock_gettime(CLOCK_REALTIME,&AnalyticsWorker::start_time);

  // wait till all workers is running before start the worker
  while(1) {
    int done = 0;
    for (const BenchWorker *w : workers_) {
      if (w->inited) ++done;
    }
    if (done == workers_.size()) break;
  }

  while(1) {
    int done = 0;
    for (const QueryWorker *w : queryers_) {
      if (w->inited) ++done;
    }
    if (done == queryers_.size()) break;
  }

  while(1) {
    int done = 0;
    for (const AnalyticsWorker *w : analyzers_) {
      if (w->inited) ++done;
    }
    if (done == analyzers_.size()) break;
  }

  if(current_partition == 0) {
    /* the first server is used to report results */
    char *msg_buf = (char *)Rmalloc(1024);
    char *payload_ptr = msg_buf + sizeof(uint64_t) + sizeof(rpc_header);

    fprintf(stdout,"[Listener]: Enter main loop\n");
    uint64_t start_ts = rdtsc();

    while(true) {

      /*********** Ending ***************/
      if(unlikely(cluster_running == false)) {

        fprintf(stdout,"[Listener] receive ending..\n");
        rpc_handler_->clear_reqs();
        //	  rpc_handler_->get_req_buf();
        int *node_ids = new int[msg_handler_->get_num_nodes()];
        for(uint i = 1;i < msg_handler_->get_num_nodes();++i) {
          node_ids[i-1] = i;
        }
        /* so that the send requests shall poll completion */
        //msg_handler_->force_sync(node_ids,msg_handler_->get_num_nodes() - 1);
        if(msg_handler_->get_num_nodes() > 1) {
          rpc_handler_->set_msg(payload_ptr);
          rpc_handler_->send_reqs_ud(RPC_EXIT,sizeof(uint64_t),
                                  node_ids,msg_handler_->get_num_nodes() - 1,0);
        }

        this->exit_rpc_handler(0,0,NULL,NULL);
        return;
        /* shall never return... */
      }

      if(num_mac_ == 1) {
        /* Single server special case */
        sleep(1);
      }
      msg_handler_->poll_comps();

      /*********** Collect Data ***********/
      if(n_returned_ == (num_mac_ - 1)) {

        if(!inited_) {
          // first time: send start rpc to others
          fprintf(stdout,"[Listener]: Send start rpc to others\n");
          int *node_ids = new int[msg_handler_->get_num_nodes()];
          for(uint i = 1;i < msg_handler_->get_num_nodes();++i) {
            node_ids[i-1] = i;
          }
          if(msg_handler_->get_num_nodes() > 1) {
            rpc_handler_->set_msg(payload_ptr);
            rpc_handler_->send_reqs_ud(RPC_START,sizeof(uint64_t),
                                    node_ids,msg_handler_->get_num_nodes() - 1,0);
            // printf("send RPC_START!\n");
          }

          // for me to start
          start_rpc_handler(0,0,NULL,NULL);
          inited_ = true;
        }

        epoch_ += 1;

        /* Calculate the first server's performance */
        char *buffer = new char[reporter_.data_len()];
        reporter_.collect_data(buffer,start_t);
        reporter_.merge_data((const char *) buffer);
        delete[] buffer;

        reporter_.report_data(epoch_,log_file);

        n_returned_ = 0;
        ret_bitmap_ = 1;
        start_ts = rdtsc();
        /* end monitoring */
      } // got all results
#if 1
      else if (rdtsc() - start_ts >
               util::Breakdown_Timer::get_one_second_cycle() * 2 && inited_) {
        // some server must die!
        epoch_ += 1;

        /* Calculate the first server's performance */
        char *buffer = new char[reporter_.data_len()];
        reporter_.collect_data(buffer,start_t);
        reporter_.merge_data((const char *) buffer);
        delete[] buffer;

        reporter_.report_data(epoch_,log_file);

        for (int i = 0; i < num_mac_; ++i) {
          bool exist = ret_bitmap_ & (1 << i);
          if (!exist) {
            printf("Server %d is dead!\n", i);
          }
        }

        n_returned_ = 0;
        ret_bitmap_ = 1;
        start_ts = rdtsc();
      }
#endif
      if (!txn_stopped_ && epoch_ >= config.getTxnEndSec()) {
        for (BenchWorker *w : workers_) w->stop = true;
        txn_stopped_ = true;
      }
      if (!qry_started_ && epoch_ >= config.getQryStartSec()) {
        for (QueryWorker *w : queryers_) w->started = true;
        qry_started_ = true;
      }
      if (!ana_started_ && epoch_ >= config.getAnaStartSec()) {
        for (AnalyticsWorker *w : analyzers_) w->started = true;
        ana_started_ = true;
      }
      if (!log_started_ && epoch_ >= config.getCleanerStartSec()) {
        for (LogWorker *w : replayers_) w->started = true;
        log_started_ = true;
      }

      if(epoch_ >= config.getRunSec()) {
        /* exit */
        cluster_running = false;
        fprintf(stdout,"[Listener] Master exit\n");
      }
    }

  }  // end master

  else {

    // other server's case
    char *msg_buf = (char *)Rmalloc(1024);
    char *payload_ptr = msg_buf + sizeof(uint64_t) + sizeof(rpc_header);

    while(true) {
      /* report results one time one second */
      msg_handler_->poll_comps();
      /* count the throughput of current server*/

      reporter_.collect_data(payload_ptr,start_t);
      rpc_handler_->clear_reqs();

      /* master server's id is 0 */
      int master_id = 0;
      rpc_handler_->set_msg(payload_ptr);
      // fprintf(stdout,"send to master %p\n",rpc_handler_);
      rpc_handler_->send_reqs_ud(RPC_REPORT, reporter_.data_len(),
                              &master_id,1,0);
      epoch_ += 1;
      if (!txn_stopped_ && epoch_ >= config.getTxnEndSec()) {
        for (BenchWorker *w : workers_) w->stop = true;
        txn_stopped_ = true;
      }
      if (!qry_started_ && epoch_ >= config.getQryStartSec()) {
        for (QueryWorker *w : queryers_) w->started = true;
        qry_started_ = true;
      }
      if (!ana_started_ && epoch_ >= config.getAnaStartSec()) {
        for (AnalyticsWorker *w : analyzers_) w->started = true;
        ana_started_ = true;
      }
      if (!log_started_ && epoch_ >= config.getCleanerStartSec()) {
        for (LogWorker *w : replayers_) w->started = true;
        log_started_ = true;
      }

      if(epoch_ >= (config.getRunSec() + 15)) {
        /* slave exit slightly if not receive RPC_EXIT from the master */
        fprintf(stderr, "[NOCC] Master has dead, so I kill myself\n");
        this->exit_rpc_handler(0,0,NULL,NULL);
        return;
      }
      sleep(1);
    } // end forever loop
  }   // end slave

}

void Listener::start_rpc_handler(int id,int cid,char *msg,void *arg) {
  printf("[Listener] start RPC handler\n");
  for (BenchWorker *w : workers_) w->running = true;
  for (QueryWorker *w : queryers_) w->running = true;
  for (AnalyticsWorker *w : analyzers_) w->running = true;
}

void Listener::exit_rpc_handler(int id,int cid, char *msg, void *arg) {
  for (BenchWorker *w : workers_) w->running = false;
  for (BenchWorker *w : workers_) w->join();
  for (QueryWorker *w : queryers_) w->running = false;
  for (QueryWorker *w : queryers_) w->join();
  for (AnalyticsWorker *w : analyzers_) w->running = false;
  for (AnalyticsWorker *w : analyzers_) w->join();

  reporter_.print_end_stat(log_file);
  ending_();
}

void Listener::get_result_rpc_handler(int id, int cid,char *msg, void *arg) {
  reporter_.merge_data((const char *) msg);
  n_returned_ += 1;
  ret_bitmap_ |= 1 << id;  // id is the mac id
}

void Listener::ending_() {
  if(current_partition == 0) {
    uint64_t *test_ptr = (uint64_t *) cm->conn_buf_;
    fprintf(stdout,"sanity checks...%lu\n",*test_ptr);
  }
  fprintf(stdout,"Benchmark ends... \n");
  if(LOG_RESULTS && current_partition == 0) {
    fprintf(stdout,"Flush results\n");
    log_file.close();
  }
  // delete cm;
  /* wait for ending message to pass */
  sleep(1);
}

} // end namespace oltp
} // end namespace nocc

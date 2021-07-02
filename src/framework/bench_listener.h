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

#ifndef BENCH_LISTENER_H_
#define BENCH_LISTENER_H_

#include "rpc.h"
#include "rdmaio.h"

#include "framework.h"
#include "bench_worker.h"
#include "backup_worker.h"
#include "bench_reporter.h"

namespace nocc {
namespace oltp {

/* Bench listener is used to monitor system wide performance */
class Listener {
 public:

  Listener(const std::vector<BenchWorker *> &workers, 
           const std::vector<LogWorker *> &replayers,
           const std::vector<QueryWorker *> &queryers,
           Reporter &report);

  /* used to handler system exit */
  void run();

 private:
  void thread_local_init();
  void ending_();

  void get_result_rpc_handler(int id,int cid,char *msg,void *arg);
  void exit_rpc_handler(int id,int cid,char *msg,void *arg);
  void start_rpc_handler(int id,int cid,char *msg,void *arg);

  const std::vector<BenchWorker *> &workers_;
  const std::vector<LogWorker *> &replayers_;
  const std::vector<QueryWorker *> &queryers_;

  Rpc *rpc_handler_;
  MsgHandler *msg_handler_;
  Reporter &reporter_;

  int n_returned_;
  uint64_t ret_bitmap_; 
  bool inited_; //whether all workers has inited
  uint64_t epoch_;

  bool txn_stopped_;
  bool qry_started_;
  bool log_started_;

};

} // namesapce oltp
} // namespace nocc

#endif

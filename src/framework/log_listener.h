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

#ifndef LOG_LISTENER_H_
#define LOG_LISTENER_H_

#include <vector>
#include "rworker.h"
#include "backup_worker.h"

namespace nocc {
namespace oltp {

class LogListener : public nocc::framework::RWorker {

 public:
  LogListener();
  virtual void register_rpc_() override { };
  virtual void run_body_(yield_func_t &yield) override;
  virtual void user_init_() override;

  void slave_func(yield_func_t &yield, int cor_id);

 protected:
  virtual int get_qp_num() override { return 1; }
 
 private:
  // void thread_local_init();
  static const uint64_t M1 = 1024 * 1024ll;
  const uint64_t SECOND_CYCLE_;
  const int master_node_; 
  std::vector<rdmaio::Qp *> qps_;
};

}  // namespace oltp
}  // namespace nocc 
 
#endif  // LOG_LISTENER_H_

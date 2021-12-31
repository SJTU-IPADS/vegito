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

#ifndef NOCC_OLTP_CH_LOG_CLEANER_H_
#define NOCC_OLTP_CH_LOG_CLEANER_H_

#include "ch_worker.h"
#include "ch_mixin.h"
#include "ch_schema.h"
#include "framework/log_cleaner.h"

#include <vector>

extern size_t current_partition;

namespace nocc {
namespace oltp {
namespace ch {

class ChLogCleaner : public LogCleaner {
 public:
  ChLogCleaner();
  virtual int clean_log(int table_id, uint64_t key, 
                        uint64_t seq, char *val,int length);
  virtual int clean_log(int log_id, int partition_id, int tx_id,
                        int table_id, uint64_t key, uint64_t seq, 
                        char *val, int length, uint32_t op_bit, 
                        uint64_t write_epoch);

  virtual void balance_index();
  virtual void prepare_balance(int num_workers);
  virtual void parallel_balance(int worker_id);
  virtual void end_balance();

  // virtual void balance_index(const std::vector<int> &threads);
  // TODO: 240 is so small
  // uint64_t last_no_o_ids_[240][10];

 private:
  std::vector<int> ware_tp_col_;
  std::vector<int> dist_tn_col_;
  std::vector<int> dist_tp_col_;
  std::vector<int> cust_tp_col_;
  std::vector<int> cust_td_col_;
  std::vector<int> orde_td_col_;
  std::vector<int> orli_td_col_;
  std::vector<int> stoc_tn_col_;

};

}  // namesapce ch
}  // namespace oltp
}  // namespace nocc


#endif  // NOCC_OLTP_CH_LOG_CLEANER_H_

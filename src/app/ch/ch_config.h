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

#ifndef NOCC_OLTP_CH_CONFIG_H_
#define NOCC_OLTP_CH_CONFIG_H_

#include <cassert>
#include <vector>
#include <string>

#include "framework/framework_cfg.h"

namespace nocc {
namespace oltp {
namespace ch {

class ChConfig {
 public:
  enum { NEW_ORDER = 0, PAYMENT, DELIVERY, ORDER_STATUS, STOCK_LEVEL };

  inline bool getUniformItemDist() const { return g_uniform_item_dist_; }
  inline int getNewOrderRemoteItemPct() const { 
    return nocc::framework::config.getDistRatio();
  }

  inline int getTxnWorkloadMix(int txnId) const { 
    assert(txnId >= 0 && txnId < ChConfig::TXN_NUM);
    return g_txn_workload_mix_[txnId]; 
  }

  inline const std::vector<int> &getQueryWorkload() const { 
    return g_query_workload_; 
  };

  void parse_ch_args(int argc, char **argv);
  void parse_ch_xml(const std::string &xml);
  void printConfig() const;

 private:
  static const int TXN_NUM = 5;
  
  bool g_uniform_item_dist_ = false;
  int g_txn_workload_mix_[TXN_NUM] = { 45, 43, 4, 4, 4 };
  std::vector<int> g_query_workload_;  // id of queries
};

extern ChConfig chConfig;


}  // namespace ch
}  // namespace oltp
}  // namespace nocc
#endif // NOCC_OLTP_CH_CONFIG_H_

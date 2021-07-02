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

#pragma once

#include <cassert>
#include <vector>
#include <string>

#include "framework/framework_cfg.h"

namespace nocc::oltp::mindex {

class MIndexConfig {
 public:
  enum { PUT = 0, GET };
  
  inline int getTxnWorkloadMix(int txn_id) const {
    assert(txn_id >= 0 && txn_id < TXN_NUM);
    return txn_mix_[txn_id];
  }

  void parse_args(int argc, char **argv);
  void parse_xml(const std::string &xml);
  void print_config() const;

 private:
  static const int TXN_NUM = 2;

  int txn_mix_[TXN_NUM] = { 0, 100 }; 
};

extern MIndexConfig miConfig;

}

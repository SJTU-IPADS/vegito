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

#include "wiki_config.h"
#include "wiki_update.h"
#include "wiki_query.h"
#include "wiki_analytics.h"
#include "wiki_scan.h"

namespace nocc {
namespace oltp {
namespace wiki {

/* Bench listener is used to monitor system wide performance */
class Listener {
public:
    Listener(const wiki::WikiConfig& config,
             const std::vector<std::shared_ptr<wiki::WikiUpdateWorker>> &update_worker, 
             const std::vector<std::shared_ptr<wiki::WikiQueryWorker>> &query_worker,
             const std::vector<std::shared_ptr<wiki::WikiAnalyticsWorker>> &analytics_worker,
             const std::vector<std::shared_ptr<wiki::WikiScanWorker>> &scan_worker);

    /* used to handler system exit */
    void run();

private:
  const std::vector<std::shared_ptr<wiki::WikiUpdateWorker>> &update_worker_;
  const std::vector<std::shared_ptr<wiki::WikiQueryWorker>> &query_worker_;
  const std::vector<std::shared_ptr<wiki::WikiAnalyticsWorker>> &analytics_worker_;
  const std::vector<std::shared_ptr<wiki::WikiScanWorker>> &scan_worker_;

  const wiki::WikiConfig& config_;
  bool inited_;  // whether all workers has inited
  uint64_t epoch_;

  bool update_stopped_;
  bool query_stopped_;
  bool analytics_stopped_;
  bool scan_stopped_;

};

}  // namespace wiki
}  // namespace oltp
}  // namespace nocc

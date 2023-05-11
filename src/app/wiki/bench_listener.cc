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

#include <vector>
#include <memory>
#include "bench_listener.h"
#include "utils/util.h"

/* bench listener is used to monitor the various performance field of the current system */

namespace nocc {
namespace oltp {
namespace wiki {

extern volatile bool cluster_running;

Listener::Listener(const wiki::WikiConfig& config,
                   const std::vector<std::shared_ptr<wiki::WikiUpdateWorker>> &update_worker, 
                   const std::vector<std::shared_ptr<wiki::WikiQueryWorker>> &query_worker,
                   const std::vector<std::shared_ptr<wiki::WikiAnalyticsWorker>> &analytics_worker,
                   const std::vector<std::shared_ptr<wiki::WikiScanWorker>> &scan_worker)
  :epoch_(0),
   config_(config),
   update_worker_(update_worker),
   query_worker_(query_worker),
   analytics_worker_(analytics_worker),
   scan_worker_(scan_worker) {}

void Listener::run() {
    std::cout << "[Listener]: Monitor running!" << std::endl;
    cluster_running = true;
    uint64_t start_ts = rdtsc();

    while(true) {

        /*********** Ending ***************/
        if(unlikely(cluster_running == false)) {
            std::cout << "[Listener] receive ending.." << std::endl;
            return;
        }

        std::this_thread::sleep_for(std::chrono::seconds(1));
        epoch_++;

        if(epoch_ >= config_.getRunSec()) {
            /* exit */
            cluster_running = false;
            std::cout << "[Listener] Master exit" << std::endl;
        }
    }
}

}  // namespace wiki
}  // namespace oltp
}  // namespace nocc

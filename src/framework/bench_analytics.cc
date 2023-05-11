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

#include "bench_analytics.h"

#include "framework.h"
#include "framework_cfg.h"
#include "backup_worker.h"

namespace nocc {

namespace oltp {

SubAnalyticsWorker::SubAnalyticsWorker(int worker_id, int num_thread)
  : worker_id_(worker_id),
    num_thread_(num_thread),
    thread_id_(assign_thread(worker_id)),
    task_(nullptr), args_(nullptr), complete_(false) { 
}

void SubAnalyticsWorker::run() {
  int cpu_num = BindToCore(thread_id_); // really specified to platforms
  printf("[Analytics Worker %d] bind CPU %d\n", worker_id_, cpu_num);

  // main loop for analytics worker
  while(true) {
      if (task_ == nullptr) {
          asm volatile("" ::: "memory");
          continue;
      }

      assert(!complete_);
      assert(args_ != nullptr);

      (task_)(worker_id_ % num_thread_, args_);
      complete_ = true;

      task_ = nullptr;
      args_ = nullptr;
  }  // end main loop
}

void SubAnalyticsWorker::set_task(AnaTaskFn *task, void *args) {
  assert(task_ == nullptr);
  args_ = args;
  task_ = task;
}

void SubAnalyticsWorker::clear() {
  while (!complete_) {
    asm volatile("" ::: "memory");
  }

  complete_ = false;
}

struct timespec AnalyticsWorker::start_time;

AnalyticsWorker::AnalyticsWorker(int worker_id, 
                                 int num_thread, 
                                 uint32_t seed,
                                 graph::GraphStore* graph_store,
                                 graph::RGMapping* rg_map)
  : worker_id_(worker_id),
    num_thread_(num_thread),
    thread_id_(assign_thread(worker_id)),
    use_seg_graph_(config.isUseSegGraph()),
    rand_generator_(seed),
    graph_store_(graph_store),
    rg_map_(rg_map),
    running(false), // true flag of whether to start the worker
    inited(false),
    started(false)
{
  if(config.isUseGrapeEngine()) {
#ifdef WITH_GRAPE
    comm_spec_.Init(MPI_COMM_WORLD);
    bool is_coordinator = comm_spec_.worker_id() == grape::kCoordinatorRank;
    mt_spec_ = grape::MultiProcessSpec(comm_spec_, true);
    mt_spec_.thread_num = this->num_thread_;
    if (mt_spec_.cpu_list.size() >= mt_spec_.thread_num) {
      mt_spec_.cpu_list.resize(mt_spec_.thread_num);
    } else {
      uint32_t num_to_append = mt_spec_.thread_num - mt_spec_.cpu_list.size();
      for (uint32_t i = 0; i < num_to_append; ++i) {
        mt_spec_.cpu_list.push_back(mt_spec_.cpu_list[i]);
      }
    }
#else
    printf("Not compile with Grape\n");
    assert(false);
#endif
  } else {
    subs_.resize(num_thread_);
    for (int i = 1; i < num_thread_; ++i) {
      subs_[i] = new SubAnalyticsWorker(worker_id + i, num_thread_);
    }
  }
}

void AnalyticsWorker::master_init_() {
  for (int i = 1; i < num_thread_; i++)
    subs_[i]->start();

  this->inited = true;
  while(!this->running) {
    asm volatile("" ::: "memory");
  }
}

void AnalyticsWorker::master_exit() {

}

void AnalyticsWorker::run() {
  int cpu_num = BindToCore(thread_id_); // really specified to platforms
  printf("[Analytics Worker %d] bind CPU %d\n", worker_id_, cpu_num);

  thread_local_init();
  master_init_();
  master_process_();
}

void AnalyticsWorker::master_process_() {
  auto workload = get_workload();

  int round = 0;
  int session_id = worker_id_ / num_thread_;

  while (true) {
    if(unlikely(!this->running)) {
      master_exit();
      return;
    }
    if (!this->started || round >= config.getAnaRound() || workload.size() == 0) {
      if(workload.size() == 0) {
        std::cout << "No analytics workload..." << std::endl;
      }
      std::this_thread::sleep_for(std::chrono::seconds(1));
      continue;
    }
    this->using_ver_ = LogWorker::get_read_epoch();
    workload[session_id].fn(this, workload[session_id].params);
    this->using_ver_ = -1;

    round++;

    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
}

} // namespace oltp
} // namespace nocc

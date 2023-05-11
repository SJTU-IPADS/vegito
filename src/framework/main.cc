/*
 *  The code is part of our project called DrTM, which leverages HTM and RDMA for speedy distributed
 *  in-memory transactions.
 *
 *
 * Copyright (C) 2015 Institute of Parallel and Distributed Systems (IPADS), Shanghai Jiao Tong University
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  For more about this software, visit:  http://ipads.se.sjtu.edu.cn/drtm.html
 *
 */

#include <vector>
#include <string>

#include <stdio.h>
#include <signal.h>
#include <getopt.h>

#include "framework_cfg.h"
#include "view_manager.h"
#include "rdma_config.h"
#include "log_area.h"
#include "util/spinlock.h"
#include "app/ch/ch_main.h"
#include "app/micro_index/mindex_main.h"
#include "app/wiki/wiki_main.h"
#include "app/ldbc/ldbc_main.h"

using namespace std;
using namespace nocc::framework;

// benchmark parameters
extern size_t coroutine_num;
extern size_t txn_nthreads;
extern size_t distributed_ratio;
extern size_t scale_factor;

// For distributed use
extern size_t total_partition;
extern size_t current_partition;
extern size_t nthreads;                      // total of worker in used
extern size_t backup_nthreads;               // number of log cleaner
extern size_t query_nthreads;
extern size_t scale_factor;                  // the scale of the database
extern double sync_seconds;    // sync interval on backup

namespace nocc {

volatile bool cluster_running = true;

namespace oltp {

extern RdmaCtrl *cm;
extern char *store_buffer;
extern uint64_t ring_padding;
extern uint64_t ringsz;

} // end namespace oltp
} // namespace nocc
using namespace nocc::oltp;

namespace {
// some helper functions

SpinLock exit_lock;  // race between two signals

void printTraceExit(int sig) {
  nocc::util::print_stacktrace();
}

void sigsegv_handler(int sig) {
  exit_lock.Lock();
  fprintf(stderr, "[NOCC] Meet a segmentation fault!\n");
  printTraceExit(sig);
  nocc::cluster_running = false;
  exit(-1);
}

void sigint_handler(int sig) {
  exit_lock.Lock();
  fprintf(stderr, "[NOCC] Meet an interrupt!\n");
  // printTraceExit(sig);
  nocc::cluster_running = false;
  exit(-1);
}

void sigabrt_handler(int sig) {
  exit_lock.Lock();
  fprintf(stderr, "[NOCC] Meet an assertion failure!\n");
  printTraceExit(sig);
  exit(-1);
}

vector<string> split_ws(const string &s) {
  vector<string> r;
  istringstream iss(s);
  copy(istream_iterator<string>(iss),
       istream_iterator<string>(),
       back_inserter<vector<string>>(r));
  return r;
}

} // anonymous namespace

int main(int argc, char **argv) {
  /* install the event handler if necessary */
  signal(SIGSEGV, sigsegv_handler);
  signal(SIGABRT, sigabrt_handler);
  signal(SIGINT,  sigint_handler);

  /* parse arguments for whole system */
  config.parse_sys_args(argc, argv);
  config.parse_sys_xml();
  config.printConfig();

  /* initialize view */
  view.init_view();
  view.print_view();


  /*********************/
  coroutine_num = config.getNumRoutines();
  txn_nthreads = config.getNumTxnThreads();
  distributed_ratio = config.getDistRatio();
  total_partition = config.getNumPrimaries();
  current_partition = config.getServerID();
  backup_nthreads = config.getNumBackupThreads();
  query_nthreads = config.getNumQueryThreads();
  nthreads = txn_nthreads + query_nthreads;
  scale_factor = config.getScaleFactorPerPartition();
  sync_seconds = config.getEpochSeconds();
  /*********************/


  /* initialize logger */
  uint64_t log_sz = 0;
  if (config.isUseLogger()) {
    // const uint32_t log_area_k = 1024 * 400;
    // const uint32_t log_area_k = 1024 * 1;  // freshness > 1 epoch
    const uint32_t log_area_k = 1024 * 0.4;  // freshness = 1 epoch
    int num_rep = config.getTPFactor() +
                  config.getAPFactor() + config.getGPFactor();
    logArea.init(config.getNumServers(), txn_nthreads, num_rep, log_area_k);
    log_sz = logArea.size();
  }

  /* initialize RDMA */
  rdmaConfig.init_rdma(log_sz);
  printf("[Main] log_area_sz: %ld bytes\n", log_sz);

  /* set logger */
  logArea.setBase(rdmaConfig.getLogBase());

  /*********************/
  cm = rdmaConfig.getRdmaCtrl();
  store_buffer = rdmaConfig.getStoreBuf();
  ring_padding = rdmaConfig.getRingPadding();
  ringsz = rdmaConfig.getRingSize();
  /**********************/

  const string &bench_type = config.getBenchType();
  void (*test_fn)( int argc, char **argv) = NULL;

  // XXX: fix this
  if (bench_type == "ch") {
    test_fn = nocc::oltp::ch::ChTest;
  } else if (bench_type == "mindex") {
    test_fn = nocc::oltp::mindex::MIndexTest;
  } else if (bench_type == "micro") {
    test_fn = nocc::oltp::wiki::WikiTest;
  } else if (bench_type == "ldbc") {
    test_fn = nocc::oltp::ldbc::LDBCTest;
  } else {
    printf("error bench type\n");
    assert(false);
  }

  vector<string> bench_toks = split_ws(config.getBenchOptions());
  int app_argc = 1 + bench_toks.size();
  char **app_argv = new char *[app_argc];
  app_argv[0] = (char *) bench_type.c_str();
  for (size_t i = 1; i <= bench_toks.size(); i++)
    app_argv[i] = (char *) bench_toks[i - 1].c_str();
  test_fn(app_argc, app_argv);
  delete[] app_argv;

  return 0;
}

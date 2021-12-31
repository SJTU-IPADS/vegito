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

#ifndef NOCC_DB_DBLOGGER_H_
#define NOCC_DB_DBLOGGER_H_

#include "framework/log_area.h"
#include "framework/framework_cfg.h"

#include "framework/rpc.h"
#include "framework/routine.h"
#include "framework/rdma_sched.h"
#include "framework/log_cleaner.h"

#include "util/temp_log.h"
#include "all.h"
#include "rdmaio.h"

#include <vector>

//                        logger memory structure
//  |--------------------------------node_area---------------------------------|
//  ||--------------------------thread_area---------------------------|        |
//  |||-----------log_area-----------|                                |        |
//  ||||-META-||---BUF---||-PADDING-||                                |        |
//  {<([      ][         ][         ])([      ][         ][         ])>  <....>} {....}


#define RPC_LOGGING 10
#define RPC_LOGGING_COMMIT 11

// TODO: our assumption is the number of columns <= 30, [0 ... 29]
#define LOG_OP_I (1ul << 31)  // insert
#define LOG_OP_D ((1ul << 31) | (1ul << 30)) // delete

using namespace rdmaio;
using namespace nocc::framework;

namespace nocc  {

using namespace oltp;

namespace db{

struct TXHeader {
  uint64_t  magic_num;
  uint64_t  global_seq;
  uint64_t  tx_id;
};

struct TXTailer {
  uint64_t  magic_num; 
  uint64_t  seq;
  uint64_t ts_vec;
};

struct EntryMeta {
  uint8_t table_id;
  uint8_t clean;
  int16_t pid;
  uint32_t size;
  uint64_t key;
  uint64_t seq;
  uint32_t op_bit; 
};

class DBLogger {

 public:
  enum LogStatus {
    LOG_SUCC = 0,
    LOG_NOT_COMPLETE,
    LOG_NOT_USE,
    LOG_TIMEOUT,
    LOG_ERR
  };

  struct RequestHeader{
    uint32_t length;
    uint64_t offsets[MAX_SERVERS];
  };

  // TODO: currently, no checking whether ack is right
  struct ReplyHeader{
    uint32_t ack;
  };

  // wid: worker_id, tid: rdma tid
  DBLogger(int wid, int tid, RdmaCtrl *rdma, RDMA_sched* rdma_sched);
  DBLogger(int wid, int tid, RdmaCtrl *rdma, Rpc *rpc_handler);
  DBLogger(int wid, int tid, RdmaCtrl *rdma, RDMA_sched* rdma_sched, Rpc *rpc_handler);

  void thread_local_init();

  //global_seq == 0 means each entry in the log needs a seq corresponding 
  void log_begin(uint cor_id, uint64_t global_seq);
  void log_begin(uint cor_id, int tx_id, uint64_t global_seq);
  char* get_log_entry(uint cor_id, int tableid, uint64_t key, uint32_t size, 
                      uint32_t op_bit, int partition_id = -1);
  void close_entry(uint cor_id, uint64_t seq = 0);
  int log_backups(uint cor_id, yield_func_t &yield, uint64_t seq, uint64_t ts = 0);
  int log_setup_(uint cor_id, yield_func_t &yield);
  uint64_t poll_tailer_(int remote_id, int log_id, uint64_t need_sz,  
                        yield_func_t &yield) const;
  int log_backups_ack(uint cor_id);
  int log_end(uint cor_id);
  void log_abort(uint cor_id);

  // RPC handler
  void logging_handler(int id,int cid,char *msg,void *arg);
  void logging_commit_handler(int id,int cid,char *msg,void *arg);

  // print functions, for debugging
  void print_total_mem();
  void print_node_area_mem(int node_id);
  void print_thread_area_mem(int node_id, int worker_id);
  char* print_log(char* ptr);
  char* print_log_header(char *ptr);
  char* print_log_entry(char *ptr);
  char* print_log_tailer(char *ptr);

  void set_send_log_cnt(uint64_t *ptr) { send_log_cnt_ = ptr; }

  // global constants  
  static LogCleaner *log_cleaner_;
  static char* check_log_completion(volatile char* ptr, uint64_t *msg_size = NULL);
  static void clean_log(int log_id, int p_id, char* log_ptr, char* tailer_ptr, 
                        uint64_t write_epoch);

  static void set_log_cleaner(LogCleaner *log_cleaner) {
    assert(log_cleaner_ == NULL);
    log_cleaner_ = log_cleaner;
  }

 private:

  DBLogger(int worker_id, int thread_id, RdmaCtrl *rdma, int logger_type, 
           RDMA_sched *rdma_sched, Rpc *rpc_handler);

  inline uint64_t translate_offset(uint64_t offset, int log_id) const {
    assert(offset < buf_sz_ + padding_);
    return offset + logArea.getBufferBase(config.getServerID(), worker_id_, log_id);
  }

  static const uint32_t HEADER_MAGIC_ = 73;
  static const uint32_t TAILER_MAGIC_ = 74;
  static const uint32_t ACK_MAGIC_ = 0x888888;
  
  // To avoid ring buffer bug when logger tailer == header
  static const uint32_t Q_HOLE_SZ_ = 1024;

  static bool inited_;

  const int num_nodes_;
  const int num_logs_;
  const int mac_id_;
  const int worker_id_;
  const int thread_id_;
  const int logger_type_;  // LOGGER_USE_RPC
  RdmaCtrl &rdma_;

  RDMA_sched * const rdma_sched_;
  Rpc        * const rpc_handler_;

  // used for ring buffer when backup thread cleaner is present
  char * const base_;        // ptr to start of RDMA buffer
  const uint32_t buf_sz_;
  const uint32_t padding_;

  uint64_t *send_log_cnt_;

  std::vector<Qp *> qp_vec_;
  std::vector<TempLog> temp_logs_; 

  // the remaining size for each machine
  std::vector<std::vector<int>> rest_sz_;
  // the offsets of remote machine ring buffer, empty: header == tailer
  std::vector<std::vector<uint64_t>> log_header_;
  std::vector<uint64_t *> log_tailer_;  // indexed by log_id
};

} // namespace db
} //namespace nocc

#endif //NOCC_DB_DBLOGGER_H_

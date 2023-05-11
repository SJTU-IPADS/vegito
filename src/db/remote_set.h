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

#ifndef NOCC_TX_REMOTE_SET
#define NOCC_TX_REMOTE_SET

#include "all.h"
#include "config.h"
#include "db_statistics_helper.h"
#include "memstore/memstore.h"

#include "framework/rpc.h"

#include <stdint.h>

enum RPC_ID {
  RPC_READ = 1,
  RPC_LOCK,
  RPC_VALIDATE,
  RPC_RELEASE,
  RPC_COMMIT,
  RPC_LOCKED_READS, // FaSST style reads
  RPC_R_VALIDATE
};

enum REQ_TYPE {
  REQ_READ = 0,
  REQ_READ_IDX,
  REQ_INSERT,
  REQ_INSERT_IDX,
  REQ_RANGE_SEARCH,
  REQ_WRITE,
  REQ_READ_LOCK
};

namespace nocc {

  using namespace oltp;

  namespace db {

    class RemoteSet {
    public:
      // class related structs
#include "remote_set_structs.h"

      /* class methods  ******************************************************/

      RemoteSet(Rpc *rpc,int cid,int tid);

      // This call will read all value in parallal, assuming we know all the remote read set
      void do_reads(yield_func_t &yield);
      int  do_reads(int tx_id = 73);
      bool get_results(int);

      bool get_results_readlock(int); // get the result of read lock request
      bool get_result_imm(int idx, char **ptr,int size);          // get the reuslt of immediate read
      bool get_result_imm_batch(int start_idx,RemoteReqObj *reqs,int num);

      int  add(REQ_TYPE type,int pid,int8_t tableid,uint64_t key);

      /* return the index of the cached entries */
      int  add(REQ_TYPE type,int pid,int8_t tableid,uint64_t *key,int klen);
      int  add_imm(REQ_TYPE type,int pid,int8_t tableid,uint64_t key); // direct send reqs
      int  add_batch_imm(REQ_TYPE type,RemoteReqObj *reqs,int num);   // direct send reqs in a batch way

      /* num, num of items required to return */
      void  add_range(int pid,int8_t tableid,uint64_t *min,uint64_t *max,int klen,int num);

      void promote_to_write(int id,char *val,int len);

      void write(int8_t tableid, uint64_t key,  char *val,int len);
      /* this is used to encode some addition meta data in the message */
      char* get_meta_ptr()  {
        return ((char *)request_buf_) + sizeof(RequestHeader);
      }

      // These codes actually shall be refined later
      bool lock_remote(yield_func_t &yield);

      /* for fasst */
      int add_update_request(int pid,int8_t tableid,uint64_t key);
      int add_read_request(int pid,int8_t tableid,uint64_t key);

      bool validate_remote(yield_func_t &yield);

      void release_remote();

      void commit_remote();

      void clear(int meta_size = 0);
      void reset();

      void update_read_buf();
      void update_write_buf();

      void clear_for_reads();

      void set_lockpayload(uint64_t payload);
      int  get_broadcast_num() { return read_server_num_;}

      char *get_req_buf();

      /* helper functions */
      inline bool _check_res(int); /* check whether a validation or lock result is successfull */

      void report() {  REPORT(lock); }

      /* Class members *******************************************************/
      int max_length_;
      int elems_; // elems in the local cache
      uint16_t write_items_;
      uint16_t read_items_;
      RemoteSetItem *kvs_;

      Rpc *rpc_handler_;

      volatile char *request_buf_;
      volatile char *request_buf_end_;

      volatile char *lock_request_buf_;
      volatile char *lock_request_buf_end_;

      volatile char *write_back_request_buf_;
      volatile char *write_back_request_buf_end_;

      /* used to receive objs reads */
      char *reply_buf_;
      char *reply_buf_end_;
      int   reply_buf_size_;

    public:
      // used to receive lock & validate results
      // this is exposed to users to allow more works
      char *reply_buf1_;
      uint64_t max_time_;
      int meta_size_;
      uint cor_id_;
      uint tid_;
      uint64_t count_;
      bool need_validate_;
      std::set<int> server_set_;

    private:
      void print_write_server_list();
      int read_servers_[MAX_SERVER_TO_SENT];
      int read_server_num_;
      int write_servers_[MAX_SERVER_TO_SENT];
      int write_server_num_;

      // some statictics
      LAT_VARS(lock);
    };

  }
}
#endif

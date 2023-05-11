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

#ifndef NOCC_DB_DBRPC_H
#define NOCC_DB_DBRPC_H

#include "msg_handler.h"
#include "config.h"
#include "util/util.h"
#include "routine.h"

#define MAX_RPC_REGISTERED 16
//#define MAX_INFLIGHT_REPLY 64

#include <set>
#include <functional>

using namespace rdmaio;

namespace nocc {

  struct rpc_info {
    int8_t   rpc_id;
    int8_t   first_node_id;
    uint64_t counter;
    void print_info() {
      fprintf(stdout,"RPC id %d, node %d\n",rpc_id,first_node_id);
    }
  };

  namespace oltp {

    struct BufferedMessageStruct {
      uint8_t pid;
      char  *msg;
      int8_t id;
      uint8_t cid;
      BufferedMessageStruct (int pid,char *m,int id,int cid) : pid(pid),msg(m),id(id),cid(cid) {}
    };

    /* arg shall be DBRpc itself */
    // callback type
    typedef std::function<void(int,int,char *,void *)> rpc_func_t;

    // Header of each RPC request
    struct   rpc_header  {
      struct rpc_meta {
        uint32_t type : 2;
        uint32_t rpc_id :  5;
        uint32_t payload : 18;
        uint32_t cid     : 7;   // coroutine id
      } meta;

#ifdef RPC_CHECKSUM
      uint64_t  counter;
      uint64_t  checksum;
#endif
    } __attribute__ ((aligned (sizeof(uint64_t))));


    class Rpc {
    public:

      enum TYPE {
        REQ = 0,
        Y_REQ, // differ from req, the handler may yield
        REPLY
      };

      Rpc(MsgHandler *,int tid, Routine *r = nullptr);
      void register_callback(rpc_func_t callback,int id);
      /* This init shall be called before started, and after RThreadlocalinit */
      void init();

      void thread_local_init();// thread local inits
      void report();

      void  clear_reqs();
      char *get_req_buf();
      /* used for getting a buffer that is larger than the max msg buffer supported */
      char *get_req_buf(int size);
      void  inline set_msg(char *msg)
      { msg_buf_ = msg - sizeof(uint64_t) - sizeof(struct rpc_header); }

      //    bool  new_req(int server_id);
      bool  poll_comps();
      bool  poll_comp_callback(char *msg,int size);

      /* on success, shall return how many servers needed to get reply */
      // !! **size** fields below does not include sizeof(rpc_header) and padding.
      // It is the **raw** msg payload.

      // API for broadcasting
      int   send_reqs(int rpc_id,int size,int *server_lists, int server_num,int cid); /* need not reply */
      int   send_reqs(int rpc_id,int size,char *reply_buf,int *server_lists,
                      int server_num, int cid,int type = 0);  /* shall receive reply*/

      int   prepare_multi_req(char *reply_buf,int num_of_replies,int cid);
      int   append_req(char *msg,int rpc_id,int size,int server_id,int cid);
      // ud can use doorbell batching, which is different from RC based RPC
      int   append_req_ud(char *msg,int rpc_id,int size,int server_id,int cid);
      int   end_req_ud() { return message_handler_->flush_pending(); }// doorbell batching requires additional processing

      // assuming the underlying msg channel is UD
      int   send_reqs_ud(int rpc_id,int size,int *server_lists,int server_num,int cid);

      void   force_replies(int reply_num) {        handled_replies_  = 0;required_replies_ = reply_num; }

      void  rpc_as_reply() { handled_replies_ += 1; }

      /* utils for send replies */
      char *get_reply_buf();
      void  send_reply(int size,int server_id,int cid);
      void  send_reply(int size,int server_id,char *buf,int cid);


    private:
      /* 0 measn that this request is an rpc request */
      int   _send_reqs(int rpc_id,int size,int *server_list,int num,int cid,int type = 0);

      /* Local RDMA buffer */
      uint8_t   current_req_buf_slot_;
      bool inited_;
      //    int coroutine_num_;
    public:
      MsgHandler *message_handler_;
      volatile char *msg_buf_; /* current in-used req buf*/

      char *reply_msg_bufs_[MAX_INFLIGHT_REPLY];
      uint16_t   current_reply_buf_slot_;
      char *reply_msg_buf_; /* current in-used reply buf*/

      rpc_func_t callbacks_[MAX_RPC_REGISTERED];

      /* If the state of rpc is true, we shall not handle this rpc */
      bool states[MAX_RPC_REGISTERED];
      int  total_rpc_registed;

      /* used for receiving messages */
      char *reply_buf_;
      char *processed_buf_;

      int thread_id_;

    public:

      int handled_replies_;
      int required_replies_;

      /* some little statictics */
      size_t nrpc_processed_;

      /* used for debug*/
      uint64_t routine_counters_[16];
      uint64_t received_counters_[16];

      uint64_t nrpc_polled_; // average rpc handled per poll_comps
#ifdef TIMER
      Breakdown_Timer rpc_timer;
      Breakdown_Timer post_timer;
      Breakdown_Timer minor_timer;
#endif
    private:

      Routine *const routine_;

    };
  };
};
#endif

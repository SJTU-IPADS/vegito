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

#include "all.h"
#include "config.h"

#include "rpc.h"
#include "ralloc.h"
#include "ring_msg.h"

#include "util/util.h"
#include "util/mapped_log.h"

#include "routine.h"

#include <unistd.h>
#include <assert.h>
#include <string.h>

#include <queue>

#include <arpa/inet.h> //used for checksum

#ifdef RPC_TIMEOUT_FLAG
#include<sys/time.h>
#endif

//__thread char temp_msg_buf[MAX_MSG_SIZE];
__thread char *msg_buf = NULL;

#define unlikely(x) __builtin_expect(!!(x), 0)
#define likely(x)   __builtin_expect(!!(x), 1)

extern size_t current_partition;
extern int    coroutine_num;

extern size_t nthreads; //For debug!! shall be cleaned

using namespace rdmaio;

extern __thread MappedLog local_log;

// some helper functions
static inline uint64_t rad_checksum(void *vdata,size_t length,int rpc_id,int payload);

namespace nocc {

  __thread char        **reply_bufs_ ;
  __thread int          *reply_counts_;
  __thread int          *reply_size_;

#ifdef RPC_TIMEOUT_FLAG
  extern __thread struct  timeval   *routine_timeouts_;
#endif

#ifdef RPC_VERBOSE
  __thread struct rpc_info *rpc_infos_;
#endif


  namespace oltp {

    Rpc::Rpc (MsgHandler *msg,int tid, Routine *r)
      : message_handler_(msg),
        reply_buf_(NULL),
        handled_replies_(0),
        required_replies_(0),
        nrpc_processed_(0),
        nrpc_polled_(0),
        inited_(false),
        thread_id_(tid),
        routine_(r)
    {
      assert(sizeof(rpc_header::meta) == sizeof(uint32_t));
      clear_reqs();

      for(uint i = 0;i < 16;++i) {
        routine_counters_[i] = 0;
        received_counters_[i] = 0;
      }

#ifdef TIMER
      // clear timers
      rpc_timer.report();
      post_timer.report();
      minor_timer.report();
#endif
    }

    void Rpc::thread_local_init() {
#ifdef RPC_TIMEOUT_FLAG
      routine_timeouts_ = new struct timeval[16];
#endif
#ifdef RPC_VERBOSE
      rpc_infos_ = new rpc_info[16];
#endif

      reply_bufs_       = new char*[1 + coroutine_num];
      reply_counts_     = new int[1 +   coroutine_num];
      reply_size_       = new int[1 +   coroutine_num];

      for(uint i = 0;i < 1 + coroutine_num;++i){
        reply_counts_[i] = 0;
        reply_bufs_[i]   = NULL;
        reply_size_[i]   = 0;
      }

    }

    void Rpc::init() {
      //    fprintf(stdout,"thread %d init\n",thread_id_);
      /* alloc a local buffer */
      if(!inited_) {

        //      msg_bufs_ = new char*
        //for(uint i = 0;i < coroutine_num + 1;++i) {
        for(uint i = 0;i < MAX_INFLIGHT_REPLY;++i) {
          reply_msg_bufs_[i] = (char *)Rmalloc(MAX_REMOTE_SET_SIZE);
          if(reply_msg_bufs_[i] != NULL)
            memset(reply_msg_bufs_[i],0,MAX_REMOTE_SET_SIZE);
          else assert(false);
        }
        //      this->current_req_buf_slot_ = 0;
        this->current_reply_buf_slot_ = 0;
        inited_ = true;
      }
    }

    void Rpc::register_callback(rpc_func_t callback, int id) {
      callbacks_[id] = callback;
    }

    void Rpc::clear_reqs() {
    }

    char *Rpc::get_req_buf() {
      assert(false);
      return NULL;
    }

    int Rpc::prepare_multi_req(char *reply_buf,int num_of_replies,int cid) {
      assert(reply_counts_[cid] == 0);
      reply_counts_[cid] = num_of_replies;
      reply_bufs_[cid]   = reply_buf;
#if USE_UD_MSG == 1
      message_handler_->prepare_pending();
#endif

      return 0;
    }

    int Rpc::append_req(char *msg,int rpc_id,int size,int server_id,int cid) {

      set_msg(msg);

      // set the RPC header
      volatile struct rpc_header *header = (volatile struct rpc_header *)(msg_buf_ + sizeof(uint64_t));
      header->meta.type = 0;
      header->meta.rpc_id  = rpc_id;
      header->meta.payload = size;
      header->meta.cid = cid;

      int total_size = size + sizeof(struct rpc_header);

#if USE_UD_MSG == 0
      *((uint64_t *)msg_buf_) = total_size;
      *((uint64_t *)(msg_buf_ + sizeof(uint64_t) + total_size)) = total_size;

      Qp::IOStatus res = message_handler_->send_to(server_id,(char *)msg_buf_,total_size);
#else
      Qp::IOStatus res = message_handler_->send_to(server_id,(char *)(msg_buf_ + sizeof(uint64_t)),total_size);
#endif
      assert(res == Qp::IO_SUCC);

      return 0;
    }

    // __attribute__((optimize("O0")))
    int Rpc::append_req_ud(char *msg,int rpc_id,int size,int server_id,int cid) {

#if USE_UD_MSG != 1
      assert(false);
#endif
      set_msg(msg);

      // set the RPC header
      volatile struct rpc_header *header = (volatile struct rpc_header *)(msg_buf_ + sizeof(uint64_t));
      header->meta.type = 0;
      header->meta.rpc_id  = rpc_id;
      header->meta.payload = size;
      header->meta.cid = cid;

      int total_size = size + sizeof(struct rpc_header);

      Qp::IOStatus res = message_handler_->post_pending(server_id,(char *)(msg_buf_ + sizeof(uint64_t)),total_size);
      assert(res == Qp::IO_SUCC);
      return 0;
    }


    // __attribute__((optimize("O0")))
    int Rpc::_send_reqs(int rpc_id,int size,int *server_lists, int num,int cid,int type) {
      assert(rpc_id <= 32);
#ifdef TIMER
      minor_timer.start();
#endif

#if USE_UD_MSG == 0
      /* align to 8*/
      int total_size = size + sizeof(struct rpc_header);
      total_size = total_size + sizeof(uint64_t) - total_size % sizeof(uint64_t);
      if(unlikely(total_size + sizeof(uint64_t) + sizeof(uint64_t) >= MAX_MSG_SIZE)) {
        fprintf(stdout,"invalid msg size %d\n",size);
        //assert(total_size + sizeof(uint64_t) + sizeof(uint64_t) < MAX_MSG_SIZE);
        assert(false);
      }
#endif

#if RPC_LOG
      if(thread_id_ != nthreads) {
        char *log_buf = next_log_entry(&local_log,32);
        assert(log_buf != NULL);
        sprintf(log_buf,"Send to %d\n",server_lists[0]);
      }
#endif
      // set the rpc header
      volatile struct rpc_header *header = (volatile struct rpc_header *)(msg_buf_ + sizeof(uint64_t));
      header->meta.type = type;
      header->meta.rpc_id  = rpc_id;
      header->meta.payload = size;
      header->meta.cid = cid;

#ifdef RPC_CHECKSUM
      header->checksum = rad_checksum((char *)msg_buf_ + sizeof(uint64_t) + sizeof(struct rpc_header),size,
                                      rpc_id,size);
      header->counter = routine_counters_[cid];
      //      if(thread_id_ != nthreads) fprintf(stdout,"send counter %lu\n",routine_counters_[cid]);
      routine_counters_[cid] += 1;
#endif

#ifdef TIMER
      minor_timer.end();
#endif

#if USE_UD_MSG == 0 // use RC to post msgs
      *((uint64_t *)msg_buf_) = total_size;
      *((uint64_t *)(msg_buf_ + sizeof(uint64_t) + total_size)) = total_size;

#ifdef TIMER
      post_timer.start();
#endif
      auto ret = message_handler_->broadcast_to(server_lists,num,(char *)msg_buf_, total_size);
      assert(ret == Qp::IO_SUCC);
#else // UD case
      assert(num > 0); // the request server shall be > 0
      auto ret = message_handler_->broadcast_to(server_lists,num,(char *)msg_buf_ + sizeof(uint64_t),
                                                size + sizeof(rpc_header));
      //auto ret = message_handler_->send_to(server_lists[0],(char *)msg_buf_ + sizeof(uint64_t),size + sizeof(rpc_header));
      
      assert(ret == Qp::IO_SUCC);
#endif

#ifdef TIMER
      post_timer.end();
#endif

      return num;
    }

    int Rpc::send_reqs_ud(int rpc_id,int size,int *server_ids,int num,int cid) {
      // set the rpc header
      volatile struct rpc_header *header = (volatile struct rpc_header *)(msg_buf_ + sizeof(uint64_t));
      header->meta.rpc_id  = rpc_id;
      header->meta.payload = size;
      header->meta.cid = cid;

#ifdef RPC_CHECKSUM
      header->checksum = rad_checksum((char *)msg_buf_ + sizeof(uint64_t) + sizeof(struct rpc_header),size,
                                      rpc_id,size);
      header->counter = routine_counters_[cid];
      //      if(thread_id_ != nthreads) fprintf(stdout,"send counter %lu\n",routine_counters_[cid]);
      routine_counters_[cid] += 1;
#endif

#ifdef TIMER
      minor_timer.end();
#endif
      Qp::IOStatus ret = message_handler_->broadcast_to(server_ids,num,(char *)msg_buf_ + sizeof(uint64_t),
                                                size + sizeof(rpc_header));
     
      assert(ret == Qp::IO_SUCC);

      return 0;
    }

    int // __attribute__((optimize("O0")))
    Rpc::send_reqs(int rpc_id,int size, char *reply_buf,int *server_ids,int num,
                     int cid,int type) {
      //assert(reply_counts_[cid] == 0);
      //assert(reply_bufs_[cid] == NULL);
      reply_counts_[cid] = num;
      reply_bufs_[cid]   = reply_buf;

#ifdef RPC_TIMEOUT_FLAG
      // only reply needs to count the start time
      gettimeofday(&(routine_timeouts_[cid]),NULL);
#endif

#ifdef RPC_VERBOSE
      //assert(rpc_id != 0);
      rpc_infos_[cid].rpc_id = rpc_id;
      rpc_infos_[cid].first_node_id = server_ids[0];
#endif

      return _send_reqs(rpc_id,size,server_ids,num,cid,type);
    }

    // __attribute__((optimize("O0")))
    int Rpc::send_reqs(int rpc_id,int size,int *server_lists,int num,int cid) {

      return _send_reqs(rpc_id,size,server_lists,num,cid);
    }

    bool Rpc::poll_comp_callback(char *msg,int from) {

      nrpc_polled_ += 1;
      struct rpc_header *header = (struct rpc_header *) msg;

      if(header->meta.type == 0) {
        // normal rpcs
        try {
#ifdef TIMER
          rpc_timer.start();
#endif
#if 0
          if(thread_id_ != nthreads) {
            char *log_buf = next_log_entry(&local_log,64);
            assert(log_buf != NULL);
            sprintf(log_buf,"server rpc id %d %p\n",header->meta.rpc_id,msg);
          }
#endif

          callbacks_[header->meta.rpc_id](from,header->meta.cid,msg + sizeof(struct rpc_header),
                                          (void *)this);
          nrpc_processed_ += 1;
#ifdef TIMER
          rpc_timer.end();
#endif
        } catch (...) {
          fprintf(stdout,"call rpc failed @thread %d, cid %d, rpc_id %d\n",
                  thread_id_,header->meta.cid,header->meta.rpc_id);
          assert(false);
        }

      } else if (header->meta.type == 1) {
        assert(false);
      } else if (header->meta.type == 2) {
        // This is a reply
        assert(header->meta.cid < 1 + coroutine_num && header->meta.cid > 0);

#if 0
        char *log_buf = next_log_entry(&local_log,48);
        assert(log_buf != NULL);
        sprintf(log_buf,"cid %d count size %d, msg size %d\n",
                header->meta.cid,reply_counts_[header->meta.cid],header->meta.payload);
#endif

        if(reply_counts_[header->meta.cid] == 0) {
          fprintf(stdout,"tid %d error, cid %d, size %d\n",thread_id_,header->meta.cid,header->meta.payload);
          assert(false);
        }

        processed_buf_ = reply_bufs_[header->meta.cid];
        assert(processed_buf_ != NULL);

        assert(reply_size_[header->meta.cid] + header->meta.payload < MAX_MSG_SIZE);

        memcpy(processed_buf_,msg + sizeof(struct rpc_header),header->meta.payload);
        reply_bufs_[header->meta.cid] += header->meta.payload;
        reply_size_[header->meta.cid] += header->meta.payload;



        reply_counts_[header->meta.cid] -= 1;
        if(reply_counts_[header->meta.cid] == 0){
          reply_bufs_[header->meta.cid] = NULL;
          reply_size_[header->meta.cid] = 0;
          if (routine_)
            routine_->add_to_routine_list(header->meta.cid);
        }
      } else {
        // unknown msg type
        assert(false);
      }
      return true;
    }

    bool Rpc::poll_comps() {
      assert(false);
      return false;
#if 0        
      uint64_t polled = 0;

      int total_servers = message_handler_->get_num_nodes();
      // thread local init
      if(NULL == msg_buf) msg_buf = new char[MAX_MSG_SIZE];

      for(uint i = 0;i < total_servers;++i) {

        char *msg;
        if ( (msg = message_handler_->try_recv_from(i)) != NULL) {
          // printf("get message from: %u\n", i);
          poll_comp_callback(msg,i);
          message_handler_->ack_msg(i);
        }
      RECV_END:
        ; /*pass */
      }

      nrpc_polled_ = polled;
      return true;
#endif
    }


    char *Rpc::get_reply_buf() {
      reply_msg_buf_ = reply_msg_bufs_[current_reply_buf_slot_ % MAX_INFLIGHT_REPLY];
      current_reply_buf_slot_ += 1;
      return reply_msg_buf_ + sizeof(uint64_t) + sizeof(struct rpc_header); /* meta data + rpc type */
    }

    void Rpc::send_reply(int size, int server_id,char *buf,int cid) {
      reply_msg_buf_ = buf - sizeof(uint64_t) - sizeof(struct rpc_header);
      return send_reply(size,server_id,cid);
    }

    void __attribute__((optimize("O0")))
    Rpc::send_reply(int size, int server_id,int cid) {
      // printf("sned reply to %d\n", server_id);
#if USE_UD_MSG == 0
      /* again, round up */
      int total_size = size + sizeof(struct rpc_header);
      total_size = total_size + sizeof(uint64_t) -  total_size % sizeof(uint64_t);
      assert(total_size + sizeof(uint64_t) + sizeof(uint64_t) < MAX_MSG_SIZE);
#endif

      struct rpc_header *header = (struct rpc_header *) (reply_msg_buf_ + sizeof(uint64_t));
      //header->reqs =  2;
      header->meta.type = 2;
      header->meta.payload = size;
      header->meta.cid = cid;

#ifdef RPC_CHECKSUM
      header->checksum = rad_checksum((char *)reply_msg_buf_ + sizeof(uint64_t) + sizeof(rpc_header),size,
                                      current_partition,size);
#endif

#if USE_UD_MSG == 0
      *((uint64_t *)reply_msg_buf_) = total_size;
      *((uint64_t *)(reply_msg_buf_ + sizeof(uint64_t) + total_size)) = total_size;
      assert(total_size < MAX_REMOTE_SET_SIZE);

      //#if TEST_LOG
#if 0
        char *log_buf = next_log_entry(&local_log,32);
        assert(log_buf != NULL);
        sprintf(log_buf,"Reply size %lu, rpc %lu\n",size,header->counter);
        assert(false);
#endif

      //message_handler_->force_sync(&server_id,1);
      Qp::IOStatus res = message_handler_->send_to(server_id,reply_msg_buf_,total_size);
      assert(res == Qp::IO_SUCC);
#else
#if 0
      Qp::IOStatus res = message_handler_->send_to(server_id,
                                                   (char *)reply_msg_buf_ + sizeof(uint64_t),
                                                 size + sizeof(rpc_header));
#else
      Qp::IOStatus res = message_handler_->post_pending(server_id,
                                                        (char *)reply_msg_buf_ + sizeof(uint64_t),
                                                        size + sizeof(rpc_header));
#endif
      //assert(res == Qp::IO_SUCC);
#endif

#if 0
      //#if TEST_LOG
      char *log_buf = next_log_entry(&local_log,32);
      assert(log_buf != NULL);
      sprintf(log_buf,"Reply size %lu, \n",size);
#endif

    }

    void Rpc::report() {
      fprintf(stdout,"%lu rpc processed\n",nrpc_processed_);
#ifdef TIMER
      fprintf(stdout,"RPC cycles: %f\n",rpc_timer.report());
      fprintf(stdout,"post cycles: %f\n",post_timer.report());
      fprintf(stdout,"minor cycles: %f\n",minor_timer.report());
#endif
      //message_handler_->check();
    }
    /* end namespace rpc */
  };

};


// some helper functions
static inline uint64_t ip_checksum(void* vdata,size_t length) {
  // Cast the data pointer to one that can be indexed.
  char* data = (char*)vdata;

  // Initialise the accumulator.
  uint64_t acc=0xffff;

  // Handle any partial block at the start of the data.
  unsigned int offset=((uintptr_t)data)&3;
  if (offset) {
    size_t count=4-offset;
    if (count>length) count=length;
    uint32_t word=0;
    memcpy(offset+(char*)&word,data,count);
    acc+=ntohl(word);
    data+=count;
    length-=count;
  }

  // Handle any complete 32-bit blocks.
  char* data_end=data+(length&~3);
  while (data!=data_end) {
    uint32_t word;
    memcpy(&word,data,4);
    acc+=ntohl(word);
    data+=4;
  }
  length&=3;

  // Handle any partial block at the end of the data.
  if (length) {
    uint32_t word=0;
    memcpy(&word,data,length);
    acc+=ntohl(word);
  }

  // Handle deferred carries.
  acc=(acc&0xffffffff)+(acc>>32);
  while (acc>>16) {
    acc=(acc&0xffff)+(acc>>16);
  }

  // If the data began at an odd byte address
  // then reverse the byte order to compensate.
  if (offset&1) {
    acc=((acc&0xff00)>>8)|((acc&0x00ff)<<8);
  }

  // Return the checksum in network byte order.
  return htons(~acc);
}


static inline uint64_t rad_checksum(void *vdata,size_t length,int rpc_id,int payload) {
  uint64_t pre = ip_checksum(vdata,length); //pre-compute the checksum of the content
  return pre + rpc_id + payload; // compute the checksum of the header
}

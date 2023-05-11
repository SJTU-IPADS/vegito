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

#include "si_ts_manager.h"
#include "dbsi.h"
#include "ralloc.h"
#include "rdmaio.h"
#include "util/util.h"

#include <pthread.h>

extern size_t current_partition;
extern size_t nthreads;

using namespace std::placeholders;

static ts_manage_func_t moniter;
static ts_manage_func_t poller;

void  *pthread_call_wrapper (void *arg) {
  return moniter(arg);
}

void *pthread_call_wrapper1 (void *arg) {
  return poller(arg);
}

#define unlikely(x) __builtin_expect(!!(x), 0)


using namespace rdmaio;

namespace nocc {

  namespace db {

#define MSG_CHANNEL_SZ 1024

    uint64_t *headers = NULL;
    uint64_t *tailers = NULL;
    char **msg_channels = NULL;

    __thread char *local_write_buffer = NULL;

    TSManager::TSManager(RdmaCtrl *cm,uint64_t addr,int id,int master_id,int wid)
      : cm_(cm),
        ts_addr_(addr),
        id_(id),
        master_id_(master_id),
        worker_id_(wid),
        fetched_ts_buffer_(NULL)
    {
      local_timestamp_ = 3;
      last_ts_ = local_timestamp_ - 1;

      /* maybe some sanity checks? */
      /* start the monitor */
#if 0
#ifndef SI_VEC
      if(current_partition == master_id_)
#endif
        {
          moniter = std::bind(&TSManager::ts_monitor,this,_1);
          pthread_t tid;
          pthread_create(&tid, NULL, pthread_call_wrapper, NULL);
        }

#endif

      this->total_partition = cm_->get_num_nodes();
      RThreadLocalInit();

      tv_size_ = this->total_partition * sizeof(uint64_t);

      headers = new uint64_t[nthreads];
      tailers = new uint64_t[nthreads];
      msg_channels = new char *[nthreads];

      for(uint i = 0;i < nthreads;++i) {
        headers[i] = 0;
        tailers[i] = 0;
        msg_channels[i] = new char[MSG_CHANNEL_SZ];
        memset(msg_channels[i],0,MSG_CHANNEL_SZ);
      }

      // create qps
      {
        int use_port = 0;

        int dev_id = cm->get_active_dev(use_port);
        int port_idx = cm->get_active_port(use_port);

        cm->thread_local_init();
        cm->open_device(dev_id);
        cm->register_connect_mr(dev_id); // register memory on the specific device

        for(uint i = 0;i < cm->get_num_nodes();++i) {
          Qp *qp1 = cm->create_rc_qp(worker_id_,i,dev_id,port_idx);
        }

        //fprintf(stdout,"[WORKER %d] start connect qps\n",worker_id_);
        while(1) {
          int connected = 0;
          for(uint i = 0;i < cm->get_num_nodes();++i) {
            Qp *qp = cm->create_rc_qp(worker_id_,i,dev_id,port_idx);
            if(qp->inited_) connected += 1;
            else {
              if(qp->connect_rc()) {
                connected += 1;
              }
              else {
              }
            }
            // printf("num_node:%d, connected:%d\n", i, connected);
          }
          if(connected == cm->get_num_nodes()) break;
          else {
            //fprintf(stdout,"[WORKER %d] connect %d\n",connected);
            sleep(1);
          }
        }

        // end create qps
      }


      {
        poller = std::bind(&TSManager::timestamp_poller,this,_1);
        pthread_t tid;
        pthread_create(&tid,NULL,pthread_call_wrapper1,NULL);
      }
#if 1
      while(fetched_ts_buffer_ == NULL) {
        asm volatile("" ::: "memory");
      }
#endif
    }

    void *TSManager::timestamp_poller(void *) {
      // Maybe bind?
#ifndef SI_TX
      assert(false);
#endif
      RThreadLocalInit();
      assert(total_partition < 64);
      uint64_t *local_buffer = (uint64_t *)Rmalloc(sizeof(uint64_t) *  64);

      uint64_t *fetched_ts = new uint64_t[total_partition];
      uint64_t *target_ts  = new uint64_t[total_partition];

      /*
        First init the timestamp manager
      */
      for(uint i = 0;i < total_partition;++i)
        fetched_ts[i] = last_ts_;

      fetched_ts_buffer_ = (char *)fetched_ts;
      char *temp = (char *)target_ts;

      Qp *qp = cm_->get_rc_qp(worker_id_,master_id_);

      while(true) {
        /* keep fetching */
        qp->rc_post_send(IBV_WR_RDMA_READ,(char *)local_buffer,tv_size_,0,IBV_SEND_SIGNALED);
        auto ret = qp->poll_completion();
        assert(ret == Qp::IO_SUCC);
        /* ensure an atomic fetch */
        memcpy(temp,local_buffer,tv_size_);
        char *swap = temp;
        temp = fetched_ts_buffer_;
        fetched_ts_buffer_ = swap;
      }
    }

    void TSManager::post_commit_ts(uint64_t ts,int tid) {
#ifdef SI_VEC
    retry:
      /* the monitor has not processed all the stuff yet */
      if(unlikely(headers[tid] - tailers[tid] >= MSG_CHANNEL_SZ)) {
        asm volatile("" ::: "memory");
        goto retry;
      }
      char *msg_channel = msg_channels[tid];
      uint64_t *m_ptr = (uint64_t *)(msg_channel + (headers[tid] % MSG_CHANNEL_SZ));
      headers[tid] += sizeof(uint64_t);
      asm volatile("" ::: "memory");
      *m_ptr = ts;
#else
      if(NULL == local_write_buffer) {
        RThreadLocalInit();
        local_write_buffer = (char *)Rmalloc(sizeof(uint64_t));
      }
      *((uint64_t *)local_write_buffer) = ts;
      /* i still think this is a problem */
#endif
    }

    void *TSManager::ts_monitor(void *arg) {
      /*
      //RDMAFunctionWrapper rw(rdma_handler_);
      #ifdef SI_VEC
      // timestamp vector version. i think it is bad
      uint64_t current_ts = last_ts_;
      RThreadLocalInit();
      uint64_t *local_buffer  = (uint64_t *)Rmalloc(sizeof(uint64_t) * 64);
      int cur_buf_slot = 0;

      static char *ts_buffer = rdma_handler_->getBuffer();
      //    assert(false);
      while(true) {
      // actually we will use a simple design, we will wait till the ts has advanced enough to commit the tx
      asm volatile("" ::: "memory");
      for(uint i = 0;i < nthreads;++i) {
      assert(headers[i] >= tailers[i]);
      if((headers[i] > tailers[i]) ) {
      uint64_t *m_ptr = (uint64_t *)(msg_channels[i] + tailers[i] % MSG_CHANNEL_SZ);
      if(*m_ptr != last_ts_ + 1) {
      continue;
      }
      // gotten one
      *m_ptr = 0;
      asm volatile("" ::: "memory");
      tailers[i] += sizeof(uint64_t);
      last_ts_ += 1;
      #if 0
      *(uint64_t *)ts_buffer = last_ts_;
      #else
      #if 1
      local_buffer[(cur_buf_slot) % 64] = last_ts_;
      // casual write
      rw.casualWrite(_QP_ENCODE_ID(master_id_,worker_id_), (char *)(&(local_buffer[(cur_buf_slot) % 64])),
      ts_addr_ + current_partition * sizeof(uint64_t), sizeof(uint64_t));
      cur_buf_slot += 1;
      #else
      *local_buffer = last_ts_;
      rdma_handler_->write(_QP_ENCODE_ID(master_id_,worker_id_), (char *)local_buffer,
      ts_addr_ + current_partition * sizeof(uint64_t),sizeof(uint64_t));
      #endif
      #endif
      }
      // end iterating thread local data
      }
      }
      #else
      // one timestamp version
      // update it's local timestamp
      assert(false);
      #endif
      */
      return NULL;
    }

    void TSManager::get_timestamp(char *buffer, int tid) {
#if 0
      /* first version, we will let each transaction to fetch timestamp from remote */
#if 1
      //RDMAQueues::Status ret = rdma_handler_->read(_QP_ENCODE_ID(master_id_,tid),buffer,ts_addr_,tv_size_);
      //    assert(ret == RDMAQueues::IO_SUCC);
#else
      *((uint64_t *)buffer) = last_ts_;
#endif

#else
      memcpy(buffer,fetched_ts_buffer_,tv_size_);
#endif
      return ;
    }
  };
};

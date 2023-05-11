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

#include "db_one_remote.h"

#include "ralloc.h"               // for Rmalloc
#include "framework/bench_worker.h"  // for worker->indirect_yield

#include "util/util.h"

#define MAX_ELEMS 32   // max elems the remote item fetch contains
#define DOORBELL 1

using namespace rdmaio;

namespace nocc {

  extern __thread oltp::BenchWorker* worker;

  namespace db {

#define MAX_VAL_SZ (1024 - sizeof(RRWSet::Meta) - sizeof(RRWSet::Meta)) // max value per record
    // VAL buffer format: | meta (read phase) | payload | meta (for validation) |

    RRWSet::RRWSet(rdmaio::RdmaCtrl *cm,RDMA_sched *sched,MemDB *db,int tid,int cid,int meta)
      :tid_(tid),cor_id_(cid),db_(db),meta_len_(meta),sched_(sched),
       elems_(0),read_num_(0)
    {
      assert(cm != NULL);

      // get qp vector for one-sided operations
      auto num_nodes = cm->get_num_nodes();
      for(uint i = 0;i < num_nodes;++i) {
        rdmaio::Qp *qp = cm->get_rc_qp(tid_,i,2);
        assert(qp != NULL);
        qps_.push_back(qp);
      }

      // init local remote key-value cache
      kvs_ = new RemoteSetItem[MAX_ELEMS];
      for(uint i = 0;i < MAX_ELEMS;++i) {
        kvs_[i].pid = 0;
        kvs_[i].key = 0;
        kvs_[i].off = 0;
        kvs_[i].val = (char *)Rmalloc(MAX_VAL_SZ + sizeof(Meta));
      }

      INIT_LAT_VARS(post);
    }


    int RRWSet::add(int pid,int tableid,uint64_t key,int len) {

      // fetch the remote items using the mem store
      auto off = db_->stores_[tableid]->RemoteTraverse(key,qps_[pid]);

      // fetch the data record
      // FIXME!, we need to ensure that the value returned is consistent by checking \
      the checksums of each data record
      // it seems that it may cause higher abort rate
      qps_[pid]->rc_post_send(IBV_WR_RDMA_READ,kvs_[elems_].val,len,off,
                              IBV_SEND_SIGNALED,cor_id_);

#if CACHING == 0
      auto ret = qps_[pid]->poll_completion();
      assert(ret == Qp::IO_SUCC);
#else
      // add to the pending qp list
      sched_->add_pending(cor_id_,qps_[pid]);
#endif
      // add the record to the read write set
      kvs_[elems_].pid     = pid;
      kvs_[elems_].key     = key;
      kvs_[elems_].off     = off;
      kvs_[elems_].len     = len;
      kvs_[elems_].tableid = tableid;
      kvs_[elems_].ro      = false;

      return elems_++;
    }

    bool RRWSet::validate_reads(yield_func_t &yield) {

      if(read_num_ > 0) {
        // send out read requests
        for(uint i = 0;i < elems_;++i) {

          if(!kvs_[i].ro) continue;

          // prepare targets
          auto pid = kvs_[i].pid;
          auto off = kvs_[i].off;
          auto val = kvs_[i].val;

          qps_[pid]->rc_post_send(IBV_WR_RDMA_READ,val + MAX_VAL_SZ + sizeof(Meta),sizeof(Meta),
                                  off,IBV_SEND_SIGNALED,cor_id_);

          sched_->add_pending(cor_id_,qps_[pid]);

        } // end iterating validation values

        worker->indirect_yield(yield); // yield for waiting for NIC's completion

        // parse the result
        for(uint i = 0;i < elems_;++i) {

          if(!kvs_[i].ro) continue;

          auto val = (char *)(kvs_[i].val);

          Meta *meta_p = (Meta *)(val);                             // meta before
          Meta *meta_a = (Meta *)(val + sizeof(Meta) + MAX_VAL_SZ); // meta after

          if(meta_p->seq != meta_a->seq) {
            return false;
          }

        } // end check results
      }   // end read validation using one-sided reads
      return true;
    }

    int RRWSet::write_all_back(int meta_off,int data_off,char *buffer) {

      // FIXME!!: assume meta format:
      //{
      // uint64_t lock;
      // uint64_t seq;
      //}

      char *cur_ptr = buffer; // iterating pointer of the buffer
      auto flag = 0 | IBV_SEND_SIGNALED; // the flag for rdma operations

      for(uint i = 0;i < elems_;++i) {

        auto pid = kvs_[i].pid;
        auto off = kvs_[i].off;
        auto val = kvs_[i].val;
        auto len = kvs_[i].len;

        // set the data
        memcpy(cur_ptr,val,len + meta_len_);

        // set the meta
        Meta *meta = (Meta *)(cur_ptr + meta_off);
        meta->lock = 0;
        meta->seq += 2;

        // write the data payload
        auto qp = qps_[pid];
        int  flag = 0;
        if(qp->first_send()) flag = IBV_SEND_SIGNALED;
        if(qp->need_poll())  qp->poll_completion();

        qps_[pid]->rc_post_send(IBV_WR_RDMA_WRITE,cur_ptr + data_off,len,off + data_off, \
                                flag,cor_id_); // FIXME: is it ok to not signaled this?

        // write the meta data, including release the locks and other things
        qps_[pid]->rc_post_send(IBV_WR_RDMA_WRITE,cur_ptr + meta_off,meta_len_,off + meta_off, \
                                IBV_SEND_INLINE,cor_id_);
        cur_ptr = cur_ptr + meta_len_ + len;
      }
      return 0;
    }

  }; // namespace db
};   // namespace nocc

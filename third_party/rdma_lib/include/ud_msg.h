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

#ifndef RDMA_UD_MSG_
#define RDMA_UD_MSG_

#include "rdmaio.h"
#include "msg_handler.h"


namespace rdmaio {

  namespace udmsg {

    const int MAX_RECV_SIZE = 2048;
    const uint64_t UD_MAX_DOORBELL_SIZE = 16;

    const int SEND_QP_IDX = 1; // send qp index starts at 1,2,3...
    const int RECV_QP_IDX = 0; // receive qp starts at 0, since there may be multiple send_qps

    bool bootstrap_ud_qps(RdmaCtrl *cm,int tid,int total,int dev_id,int port_idx,int send_qp_num);

    class UDMsg : public MsgHandler {

    public:

      UDMsg(RdmaCtrl *cm,int tid,int total,int max_recv_num,msg_func_t func,int dev_id,int port_id,int send_num);

      Qp::IOStatus send_to(int key,char *msg,int len);
      Qp::IOStatus send_to(int node_id,int tid,char *msg,int len);
      Qp::IOStatus broadcast_to(int *node_ids, int num_of_node, char *msg,int len);

      Qp::IOStatus prepare_pending();
      Qp::IOStatus post_pending(int node,char *msg,int len);
      Qp::IOStatus post_pending(int node_id,int tid,char *msg,int len);
      Qp::IOStatus flush_pending();

      // force a sync among all current in-flight messages, return when all these msgs are ready
      void force_sync(int *node_id,int num_of_node);

      // Return true if one message is received
      bool  try_recv_from(int from_mac,char *buffer);

      virtual void  poll_comps();

      // if we receive one
      void  ack_msg();
      
      int   get_num_nodes() { return num_nodes_; }
      int   get_thread_id() { return thread_id_; }

      virtual void check();
      virtual void report();

    private:
      Qp::IOStatus post_pending_helper(int key,char *msg,int len);

      RdmaCtrl *cm_;   // RDMA communication manager
      Qp *recv_qp_;    // qp used to recv msg

      int send_qp_idx_; // current in-used send qp
      int total_send_qps_; // total number of send qp in-used

      int num_nodes_; // num of nodes in the cluster
      int my_node_id_;   // my node id
      int thread_id_; // tid

      // related to qp infos
      int recv_head_;
      int idle_recv_num_;
      int max_idle_recv_num_ = 1;
      int max_recv_num_ = 0;


      // recv data structures
      struct ibv_recv_wr rr_[MAX_RECV_SIZE];
      struct ibv_sge sge_[MAX_RECV_SIZE];
      struct ibv_wc wc_[MAX_RECV_SIZE];

      struct ibv_recv_wr *bad_rr_;
      int recv_buf_size_; // calculated during init

      msg_func_t callback_;

      //-------------------------------------

      // sender's data structure
      ibv_send_wr sr_[UD_MAX_DOORBELL_SIZE];
      ibv_sge     ssge_[UD_MAX_DOORBELL_SIZE];
      struct ibv_send_wr *bad_sr_;

      // used for pending reqs
      volatile int current_idx_;
      Qp *send_qp_;


      // some statistics info used to profile poll completion
      uint64_t total_costs_;
      uint64_t pre_total_costs_;

      uint64_t counts_;
      uint64_t pre_counts_;

      //-------------------------------------
      // private helper functions
      void init();
      void post_recvs(int recv_num);
    };
  };  // end namespace ud_msg
};



#endif

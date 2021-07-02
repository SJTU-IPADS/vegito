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

/* This file is used to implement OCC's remote operations */
/* OCC different from other concurrency control such that it is stateful, thus */

#include "dbtx.h"

//__thread std::map<uint64_t,DBTX::RemoteSet *> buffered_items_;
__thread std::map<uint64_t,DBTX*> *buffered_items_ = NULL;
__thread bool ro_helper_inited = false;

extern __thread RemoteHelper *remote_helper;

void RemoteHelper::thread_local_init() {

  if(false == ro_helper_inited) {
    buffered_items_ = new std::map<uint64_t ,DBTX *> ();
    ro_helper_inited = true;
    for(uint i = 0;i < total_servers_;++i) {
      for(uint j = 0;j < routines_ ;++j) {
        auto temp_tx = new DBTX(db_);
        temp_tx->init_temp();
        buffered_items_->insert(std::make_pair(_QP_ENCODE_ID(i,j + 1),temp_tx));
      }
    }    
  }
}

/* implementation of remote helper */
RemoteHelper::RemoteHelper (MemDB *db,int servers,int cn)
  : db_(db), total_servers_(servers), routines_(cn) {
  
}

void RemoteHelper::begin(uint64_t id) {
#if 0
  thread_local_init();
  temp_ptr_ = new RemoteItemSet();
  buffered_items_->insert(std::make_pair(id,temp_ptr_));
#endif
  //  fprintf(stdout,"ro begin %d %d\n",_QP_DECODE_MAC(id),_QP_DECODE_INDEX(id));
  thread_local_init();
  assert(buffered_items_->find(id) != buffered_items_->end());
  temp_tx_ = (*buffered_items_)[id];
  temp_tx_->reset_temp();
  //temp_tx_->local_ro_begin();
}

void
RemoteHelper::add(MemNode *node,uint64_t seq) {
  //  temp_ptr_->add(node,seq);
}

uint16_t RemoteHelper::validate(uint64_t id) {
  assert(buffered_items_->find(id) != buffered_items_->end());
  temp_tx_ = (*buffered_items_)[id];
  //fprintf(stdout,"ro validate %d %d, %d\n",_QP_DECODE_MAC(id),_QP_DECODE_INDEX(id),temp_tx_->report_rw_set());
  //return temp_tx_->end_ro();
  if(temp_tx_->end_ro())
    return temp_tx_->report_rw_set() + 1;
  else
    return 0;
}

void DBTX::ro_val_rpc_handler(int id,int cid,char *msg,void *arg) {
  
  char *reply_msg = rpc_handler_->get_reply_buf();
  *((uint16_t *) reply_msg) = remote_helper->validate(_QP_ENCODE_ID(id,cid + 1));
  //fprintf(stdout,"check reply buf %u\n",*((uint16_t *)reply_msg));
  rpc_handler_->send_reply(sizeof(uint16_t),id,cid);
}

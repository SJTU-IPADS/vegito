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

#include "dbtx.h"
#include "util/mapped_log.h"

//#include "oltp/tpce_mixin.h"

#define unlikely(x) __builtin_expect(!!(x), 0)
extern size_t current_partition;

#define RPC_LOCK_MAGIC_NUM 73


#define TEST_LOG 0
extern __thread MappedLog local_log;

void DBTX::get_naive_rpc_handler(int id,int cid,char *msg,void *arg) {
  assert(false);

  RemoteSet::RequestItem *header = (RemoteSet::RequestItem *)msg;

  char *reply_buf = rpc_handler_->get_reply_buf();
  RemoteSet::ReplyItem *r_header = (RemoteSet::ReplyItem *)reply_buf;
  assert(header->tableid <= 20);
  int vlen = txdb_->_schemas[header->tableid].vlen;
  auto node = txdb_->stores_[header->tableid]->GetWithInsert(header->key);

 retry:
  auto seq = node->seq;
  asm volatile("" ::: "memory");
  uint64_t *tmp_val = node->value;
  asm volatile("" ::: "memory");
  if(seq == 1 || node->seq != seq)
    goto retry;

  if(unlikely(tmp_val == NULL)) {
    assert(false);
  }
  else {
    memcpy((char *)reply_buf + sizeof(RemoteSet::ReplyItem),(char *)tmp_val + META_LENGTH, vlen);
  }

  r_header->seq = seq;
  r_header->node = node;
  r_header->idx  = header->idx;

  rpc_handler_->send_reply(vlen + sizeof(RemoteSet::ReplyItem),id,cid);
}

void DBTX::get_rpc_handler(int id,int cid,char *msg,void *arg) {

  RemoteSet::RequestHeader *g_header = (RemoteSet::RequestHeader *)msg;

  /* prepare reply pointer */
  char *reply_msg = rpc_handler_->get_reply_buf();
  char *reply_msg_t = reply_msg + sizeof(RemoteSet::ReplyHeader);
  int num_item_fetched(0);

  /* init traverse */
  char *traverse_ptr = msg + sizeof(RemoteSet::RequestHeader);
  int num_items = g_header->num;
  assert(num_items > 0 && num_items <= 15);

#if TEST_LOG
  char *log_buf = next_log_entry(&local_log,32);
  assert(log_buf != NULL);
  sprintf(log_buf,"Server read %lu, \n",num_items);
#endif

  struct RemoteSet::ReplyHeader *r_header = (struct RemoteSet::ReplyHeader *)reply_msg;

  for(int i = 0;i < num_items;++i) {
    RemoteSet::RemoteSetRequestItem *header = (RemoteSet::RemoteSetRequestItem *)traverse_ptr;
    traverse_ptr += sizeof(RemoteSet::RemoteSetRequestItem);
    if(header->pid != current_partition) {
      continue;
    }
    /* Fetching objects */
    switch(header->type) {
    case REQ_READ: {

      RemoteSet::RemoteSetReplyItem *reply_item = (RemoteSet::RemoteSetReplyItem *) reply_msg_t;
      int vlen = txdb_->_schemas[header->tableid].vlen;
      reply_item->payload = vlen;
      uint64_t seq(0);

      MemNode *node;
      if(txdb_->_schemas[header->tableid].klen == sizeof(uint64_t)) {
        // normal fetch
#if LONG_KEY == 1
        // node = txdb_->stores_[header->tableid]->GetWithInsert((uint64_t)(header->key.short_key));
        node = txdb_->stores_[header->tableid]->Get((uint64_t)(header->key.short_key));
#else
        node = txdb_->stores_[header->tableid]->GetWithInsert((uint64_t)(header->key));
#endif
      } else {
#if LONG_KEY == 1
        node = txdb_->stores_[header->tableid]->GetWithInsert( (uint64_t )( &(header->key.long_key)));
#else
        assert(false);
#endif
      }

      assert(node != NULL);

      retry:
      seq = node->seq;
      asm volatile("" ::: "memory");
      uint64_t *tmp_val = node->value;
      asm volatile("" ::: "memory");
      if(seq == 1 || node->seq != seq)
        goto retry;

      if(unlikely(tmp_val == NULL)) {
        seq = 0;
      }
      else {
        memcpy(reply_msg_t + sizeof(RemoteSet::RemoteSetReplyItem),(char *)tmp_val + META_LENGTH, vlen);
      }
      reply_item->seq = seq;
      assert(seq > 0);
      reply_item->node = node;
      reply_msg_t += (sizeof(RemoteSet::RemoteSetReplyItem) + vlen);
      reply_item->idx = i;
      num_item_fetched += 1;
    }
      break;
    case REQ_READ_IDX: {

      RemoteSet::RemoteSetReplyItem *reply_item = (RemoteSet::RemoteSetReplyItem *)reply_msg_t;
#if LONG_KEY == 1
      MemNode *node = txdb_->_indexs[header->tableid]->GetWithInsert((uint64_t)(&(header->key.long_key)));
#else
      MemNode *node = NULL;
      assert(false);
#endif
      //      assert(node->seq != 0);
      reply_item->seq = node->seq;
      reply_item->node = node;
      reply_item->idx  = i;
      reply_item->payload = 0;
      reply_msg_t += (sizeof(RemoteSet::RemoteSetReplyItem));
      num_item_fetched += 1;
    }
      break;
    case REQ_INSERT: {
      RemoteSet::RemoteSetReplyItem *reply_item = (RemoteSet::RemoteSetReplyItem *)reply_msg_t;
#if LONG_KEY == 1
      MemNode *node = txdb_->stores_[header->tableid]->GetWithInsert((uint64_t)(&(header->key.long_key)));
#else
      MemNode *node = NULL;
      assert(false);
#endif
      reply_item->seq = node->seq;
      reply_item->node = node;
      reply_item->idx  = i;
      reply_item->payload = 0;
      reply_msg_t += (sizeof(RemoteSet::RemoteSetReplyItem));
      num_item_fetched += 1;
    }
      break;
    case REQ_INSERT_IDX: {

      RemoteSet::RemoteSetReplyItem *reply_item = (RemoteSet::RemoteSetReplyItem *)reply_msg_t;
#if LONG_KEY == 1
      MemNode *node = txdb_->_indexs[header->tableid]->GetWithInsert((uint64_t)(&(header->key.long_key)));
#else
      MemNode *node = NULL;
      assert(false);
#endif
      assert(node->value == NULL);

      reply_item->seq  = node->seq;
      reply_item->node = node;
      reply_item->idx  = i;
      reply_item->payload = 0;
      reply_msg_t += (sizeof(RemoteSet::RemoteSetReplyItem));
      num_item_fetched += 1;
    }
      break;
    default:
      assert(false);
    }
  }
  //  assert(num_item_fetched > 0);
  if(unlikely(num_item_fetched == 0)) {
    char *traverse_ptr = msg + sizeof(RemoteSet::RequestHeader);

    for(int i = 0;i < num_items;++i) {
      RemoteSet::RemoteSetRequestItem *header = (RemoteSet::RemoteSetRequestItem *)traverse_ptr;
      fprintf(stdout,"%d fetch pid\n",header->pid);
      traverse_ptr += sizeof(RemoteSet::RemoteSetRequestItem);
    }
    //    assert(false);
    //  }
    //fprintf(stdout,"from tx %d, num %d\n",g_header->tx_id,g_header->num);
    assert(false);
  }
  /* send reply */
  r_header->num_items_ = num_item_fetched;
  r_header->payload_   = reply_msg_t - reply_msg;
  r_header->partition_id_ = current_partition;
  rpc_handler_->send_reply(r_header->payload_,id,cid);
}


void DBTX::get_lock_rpc_handler(int id,int cid,char *msg,void *arg) {

  RemoteSet::RequestHeader *header = (RemoteSet::RequestHeader *)msg;

  // prepare reply pointer
  char *reply_msg = rpc_handler_->get_reply_buf();

  char *reply_msg_t = reply_msg + sizeof(RemoteSet::ReplyHeader);
  int num_item_fetched(0);

  // init traverse ptr
  char *traverse_ptr = msg + sizeof(RemoteSet::RequestHeader);

  int num_items = header->num;
  //fprintf(stdout,"rad num items %d\n",num_items);
  struct RemoteSet::ReplyHeader *r_header = (struct RemoteSet::ReplyHeader *)reply_msg;
#if 1
  for(int i = 0;i < num_items;++i) {

    RemoteSet::RemoteSetRequestItem *header = (RemoteSet::RemoteSetRequestItem *)traverse_ptr;
    traverse_ptr += sizeof(RemoteSet::RemoteSetRequestItem);
    if(header->pid != current_partition) {
      continue;
    }
    // Fetching objects
    switch(header->type) {
    case REQ_READ: {

      RemoteSet::RemoteSetReplyItem *reply_item = (RemoteSet::RemoteSetReplyItem *) reply_msg_t;
      int vlen = txdb_->_schemas[header->tableid].vlen;
      reply_item->payload = vlen;
      uint64_t seq(0);
      MemNode *node;
      if(txdb_->_schemas[header->tableid].klen == sizeof(uint64_t)) {
        // normal fetch
#if LONG_KEY == 1
        node = txdb_->stores_[header->tableid]->GetWithInsert((uint64_t)(header->key.short_key));
#else
        node = txdb_->stores_[header->tableid]->GetWithInsert((uint64_t)(header->key));
#endif
      } else {
#if LONG_KEY == 1
        node = txdb_->stores_[header->tableid]->GetWithInsert( (uint64_t )( &(header->key.long_key)));
#else
        assert(false);
#endif
      }
      assert(node != NULL);
      uint64_t counter = 0;
      retry:
      seq = node->seq;
      asm volatile("" ::: "memory");
      uint64_t *tmp_val = node->value;
      asm volatile("" ::: "memory");
      if(seq == 1 || node->seq != seq) {
        goto retry;
      }

      if(unlikely(tmp_val == NULL)) {
        seq = 0;
      }
      else {
        memcpy(reply_msg_t + sizeof(RemoteSet::RemoteSetReplyItem),(char *)tmp_val + META_LENGTH, vlen);
      }

      reply_item->seq = seq;
      reply_item->node = node;
      reply_msg_t += (sizeof(RemoteSet::RemoteSetReplyItem) + vlen);
      reply_item->idx = i;
      num_item_fetched += 1;

      if(unlikely(seq == 0)){
        fprintf(stdout,"Tableid %d\n",header->tableid);
        assert(false);
      }
    }
      break;
    case REQ_READ_LOCK: {

      RemoteSet::RemoteSetReplyItem *reply_item = (RemoteSet::RemoteSetReplyItem *) reply_msg_t;
      int vlen = txdb_->_schemas[header->tableid].vlen;
      reply_item->payload = vlen;
      uint64_t seq(0);
      MemNode *node;

      if(txdb_->_schemas[header->tableid].klen == sizeof(uint64_t)) {
        // normal fetch
#if LONG_KEY == 1
        node = txdb_->stores_[header->tableid]->GetWithInsert((uint64_t)(header->key.short_key));
#else
        node = txdb_->stores_[header->tableid]->GetWithInsert((uint64_t)(header->key));
#endif
      } else {
#if LONG_KEY == 1
        node = txdb_->stores_[header->tableid]->GetWithInsert( (uint64_t )( &(header->key.long_key)));
#else
        assert(false);
#endif
      }

      assert(node != NULL && node->value != NULL);
      //
#if 1
      volatile uint64_t *lockptr = &(node->lock);
      if( unlikely( (*lockptr != 0) ||
                    !__sync_bool_compare_and_swap(lockptr,0,
                                                  ENCODE_LOCK_CONTENT(id,thread_id,cid + 1))))
        {
          reply_item->seq = 0;
        } else {
        reply_item->seq = node->seq;
        assert(node->seq != 1);
        memcpy(reply_msg_t + sizeof(RemoteSet::RemoteSetReplyItem),
               (char *)(node->value) + META_LENGTH, vlen);
      }
#endif

      reply_item->node = node;
      reply_msg_t += (sizeof(RemoteSet::RemoteSetReplyItem) + vlen);
      reply_item->idx = i;
      num_item_fetched += 1;
    }
      break;
    }
    // end itereating items
  }
  assert(num_item_fetched > 0);
#endif
  /* send reply */
  r_header->num_items_ = num_item_fetched;
  r_header->payload_   = reply_msg_t - reply_msg;
  r_header->partition_id_ = current_partition;
  rpc_handler_->send_reply(r_header->payload_,id,cid);
}


void DBTX::release_rpc_handler(int id,int cid,char *msg,void *arg) {
  /* release rpc handler shall be the same */
  RemoteSet::RequestHeader *header = (RemoteSet::RequestHeader *)msg;
  //assert(header->padding == 73);
  char *traverse_ptr = msg + sizeof(RemoteSet::RequestHeader);
  int num_items = header->num;
  // fprintf(stdout,"try release  %d, %p\n",num_items, msg);
  for(int i = 0;i < num_items;++i) {
    //    RemoteSet::RemoteSetRequestItem *header = (RemoteSet::RemoteSetRequestItem *)traverse_ptr;
    //    traverse_ptr += sizeof(RemoteSet::RemoteSetRequestItem);
    RemoteSet::RemoteLockItem *lheader = (RemoteSet::RemoteLockItem *)traverse_ptr;
    traverse_ptr += sizeof(RemoteSet::RemoteLockItem);
    if(lheader->pid != current_partition) {
      continue;
    }
    // fprintf(stdout,"release %p for %d, %d/%d\n",lheader->node,cid, i, num_items);
#if 1
    /* release the item */
    volatile uint64_t *lockptr = &(lheader->node->lock);
    __sync_bool_compare_and_swap(lockptr,ENCODE_LOCK_CONTENT(id,thread_id,cid + 1),
                                 0);
#endif
  }
}


void DBTX::validate_rpc_handler(int id,int cid,char *msg,void *arg) {
  //assert(false);
  RemoteSet::RequestHeader *header = (RemoteSet::RequestHeader *)msg;

  char *reply_msg = rpc_handler_->get_reply_buf();

  /* initilizae with lock success */
  ((RemoteSet::ReplyHeader *) reply_msg)->num_items_ = 1;

  char *traverse_ptr = msg + sizeof(RemoteSet::RequestHeader);
  int num_items = header->num;

  for(int i = 0;i < num_items;++i) {
    RemoteSet::RemoteSetRequestItem *header = (RemoteSet::RemoteSetRequestItem *)traverse_ptr;
    traverse_ptr += sizeof(RemoteSet::RemoteSetRequestItem);
    if(header->pid != current_partition) {
      continue;
    }
    //    fprintf(stdout,"validate %p for %d\n",header->node,cid);
    /* possibly not happen due to readset != writeset */
    //    assert(header->node->lock == ENCODE_LOCK_CONTENT(id,thread_id,cid + 1));

    if(header->node->seq != header->seq) {
      /* validation failed */
      //      fprintf(stdout,"tableid %d ,seq %lu, needed %lu\n",header->tableid,header->node->seq,
      //	      header->seq);
      ((RemoteSet::ReplyHeader *) reply_msg)->num_items_ = 0;
      break;
    }
    //fprintf(stdout,"process %d %lu done\n",header->tableid,header->key);
  }
  //  fprintf(stdout,"lock request result %d\n",*((RemoteSet::ReplyHeader *) reply_msg));
  rpc_handler_->send_reply(sizeof(RemoteSet::ReplyHeader),id,cid);
}

void DBTX::commit_rpc_handler2(int id,int cid,char *msg,void *arg) {
  //  fprintf(stdout,"commit @%d\n",cid);
  int num_items = (*((RemoteSet::RequestHeader *) msg)).num;
  uint64_t desired_seq = (*((RemoteSet::RequestHeader *) msg)).padding;
  //assert(desired_seq == 73);
  //  fprintf(stdout,"try commit %d\n",num_items);
  char *traverse_ptr = msg + sizeof(RemoteSet::RequestHeader);
  for(uint i = 0;i < num_items;++i) {
    RemoteSet::RemoteWriteItem *header = (RemoteSet::RemoteWriteItem *)traverse_ptr;
    traverse_ptr += sizeof(RemoteSet::RemoteWriteItem);
    if(header->pid != current_partition) {
      traverse_ptr += header->payload;
      continue;
    }

#if 1
    /* now we simply using memcpy */
    /* dbrad shall install a new vrsion */
    char *new_val;
    if(header->payload == 0) {
      /* a delete case */
      new_val = NULL;
    } else {
      new_val = (char *)malloc(header->payload + META_LENGTH);
      memcpy(new_val + META_LENGTH,traverse_ptr,header->payload);
    }
    //    fprintf(stdout,"try commit %d %d, node %p\n",cid,thread_id,header->node);
    uint64_t old_seq = header->node->seq;
    header->node->seq   = 1;
    asm volatile("" ::: "memory");
    header->node->value = (uint64_t *)new_val;
    asm volatile("" ::: "memory");
    //    header->node->seq  = header->seq + 2;
    header->node->seq = old_seq + 2;
    asm volatile("" ::: "memory");
    /* release the lock */
#if 0
    if(__sync_bool_compare_and_swap( (uint64_t *)(&(header->node->lock)),
                                     ENCODE_LOCK_CONTENT(id,thread_id,cid + 1),
                                     0) != true){
      //fprintf(stdout,"locked by mac %d tid %d, cid %d\n",
      //DECODE_LOCK_MAC(header->node->lock),DECODE_LOCK_TID(header->node->lock),
      //DECODE_LOCK_CID(header->node->lock));
      assert(false);
    }
#else
    //    assert(false);
    header->node->lock = 0;
#endif

#endif
    traverse_ptr += header->payload;
  }
}

void DBTX::lock_rpc_handler(int id,int cid,char *msg,void *arg) {
  /* unlike lock rpc handler in rpc, dbrad needs further check versions */
  RemoteSet::RequestHeader *header = (RemoteSet::RequestHeader *)msg;

  char *reply_msg = rpc_handler_->get_reply_buf();

  /* initilizae with lock success */
  ((RemoteSet::ReplyHeader *) reply_msg)->num_items_ = 1;

  char *traverse_ptr = msg + sizeof(RemoteSet::RequestHeader);
  int num_items = header->num;

#if TEST_LOG
  char *log_buf = next_log_entry(&local_log,32);
  assert(log_buf != NULL);
  sprintf(log_buf,"Server lock %lu, \n",num_items);
#endif

#if 1
  //fprintf(stdout,"try lock %d\n",num_items);
  uint64_t max_time(0);
  for(int i = 0;i < num_items;++i) {
    RemoteSet::RemoteLockItem *lheader = (RemoteSet::RemoteLockItem *)traverse_ptr;
    traverse_ptr += sizeof(RemoteSet::RemoteLockItem);
    if(lheader->pid != current_partition) {
      continue;
    }
#if 1
    //fprintf(stderr,"node %p from %d\n",lheader->node,lheader->pid);
    /* lock the item */
    volatile uint64_t *lockptr = &(lheader->node->lock);
    /* 73 is a magic number to avoid races */
    // fprintf(stdout,"lock %p for (%d, %d, %d)\n",lheader->node,id, thread_id,cid + 1);
    if ( unlikely( (*lockptr != 0) ||
                  !__sync_bool_compare_and_swap(lockptr,0,
                                                ENCODE_LOCK_CONTENT(id,thread_id,cid + 1))))
      {
        /* lock failed */
        // fprintf(stdout,"fail %p for %d, %lx\n",lheader->node,cid,*lockptr);
        ((RemoteSet::ReplyHeader *) reply_msg)->num_items_ = 0;
        break;
      }
#endif

    // further check sequence numbers
    if(unlikely( lheader->node->seq != lheader->seq)) {
      // validation failed, value has been changed
      ((RemoteSet::ReplyHeader *) reply_msg)->num_items_ = 0;
      break;
    }

    /* end iterating request items */
  }

  /* re-use payload field to set the max time */
  ((RemoteSet::ReplyHeader *)(reply_msg))->payload_ = max_time;
#endif
  rpc_handler_->send_reply(sizeof(RemoteSet::ReplyHeader),id,cid);
}

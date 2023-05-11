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

#include "config.h"
#include "dbrad.h"
#include "util/mapped_log.h"

#define unlikely(x) __builtin_expect(!!(x), 0)
extern size_t current_partition;
extern size_t total_partition;

#undef  RAD_LOG
#define RAD_LOG 0

#define MAX(x,y) (((x) > (y ))? (x):(y))

extern __thread MappedLog local_log;

using namespace nocc::db;

void DBRad::fast_get_rpc_handler(int id, int cid, char *msg, void *arg) {

  RemoteSet::RequestHeader *header = (RemoteSet::RequestHeader *)msg;

  // prepare reply pointer
  char *reply_msg = rpc_handler_->get_reply_buf();

  char *reply_msg_t = reply_msg + sizeof(RemoteSet::ReplyHeader);
  int num_item_fetched(0);

  // init traverse
  char *traverse_ptr = msg + sizeof(RemoteSet::RequestHeader);

  int num_items = header->num;
  //  fprintf(stdout,"rad num items %d\n",num_items);
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
#if 0
        counter += 1;
        if(counter > 100000)
          assert(false);
#endif
        goto retry;
      }

      if(unlikely(tmp_val == NULL)) {
        seq = 0;
      }
      else {
        memcpy(reply_msg_t + sizeof(RemoteSet::RemoteSetReplyItem),(char *)tmp_val + RAD_META_LEN, vlen);
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
      //      fprintf(stdout,"%d get node %p\n",thread_id,reply_item->node);
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

      //fprintf(stdout,"get key %lu\n",header->key.short_key);
      assert(node != NULL && node->value != NULL);
      //
#if 1
      volatile uint64_t *lockptr = &(node->lock);
      if( unlikely( (*lockptr != 0) ||
                    !__sync_bool_compare_and_swap(lockptr,0,
                                                  ENCODE_LOCK_CONTENT(id,thread_id,cid + 1))))
        {
          //assert(false);
          reply_item->seq = 0;
        } else {
        reply_item->seq = node->seq;
        assert(node->seq != 1);
        memcpy(reply_msg_t + sizeof(RemoteSet::RemoteSetReplyItem),
               (char *)(node->value) + RAD_META_LEN, vlen);
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
  //  last_rpc_mark_[cid] = RPC_READ;
}

void DBRad::fast_validate_rpc_handler(int id, int cid, char *msg, void *arg) {

  RemoteSet::RequestHeader *header = (RemoteSet::RequestHeader *)msg;

  char *reply_msg = rpc_handler_->get_reply_buf();

  /* initilizae the reply, 1 means successfull */
  ((RemoteSet::ReplyHeader *) reply_msg)->num_items_ = 1;

  char *traverse_ptr = msg + sizeof(RemoteSet::RequestHeader);
  int num_items = header->num;

  for(int i = 0;i < num_items;++i) {

    RemoteSet::RemoteSetRequestItem *header = (RemoteSet::RemoteSetRequestItem *)traverse_ptr;
    traverse_ptr += sizeof(RemoteSet::RemoteSetRequestItem);
    if(header->pid != current_partition || header->type != REQ_READ) {
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

void //__attribute__((optimize("O0")))
DBRad::get_rpc_handler(int id,int cid,char *msg,void *arg) {

#if RAD_LOG
  char *log_buf = next_log_entry(&local_log,10);
  assert(log_buf != NULL);
  sprintf(log_buf,"get start\n");
#endif


  /* no suprises, get rpc handler is exactly the same as OCC's rpc handler  */
  RemoteSet::RequestHeader *header = (RemoteSet::RequestHeader *)msg;
  assert(header->cor_id == cid);
  /* prepare reply pointer */
  char *reply_msg = rpc_handler_->get_reply_buf();

  char *reply_msg_t = reply_msg + sizeof(RemoteSet::ReplyHeader);
  int num_item_fetched(0);

  /* init traverse */
#if 1
  char *traverse_ptr = msg + sizeof(RemoteSet::RequestHeader);
#else
  /* for test message num */
  char *traverse_ptr = msg + sizeof(RemoteSet::RequestHeader) + sizeof(uint64_t) * total_partition;
#endif
  int num_items = header->num;
  //assert(num_items <= 4);
  assert(num_items > 0);
  //  fprintf(stdout,"rad num items %d\n",num_items);
  struct RemoteSet::ReplyHeader *r_header = (struct RemoteSet::ReplyHeader *)reply_msg;

  for(uint i = 0;i < num_items;++i) {
  //for(uint i = 0;false;) {
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

      //fprintf(stdout,"reply %p to key %lu\n",node,header->key.short_key);
      assert(node != NULL);
      retry:
      asm volatile("" ::: "memory");
      seq = node->seq;
      asm volatile("" ::: "memory");
      uint64_t *tmp_val = node->value;
      asm volatile("" ::: "memory");
      if(seq == 1 || node->seq != seq)
        goto retry;

      if(unlikely(tmp_val == NULL)) {
        seq = 0;
        assert(false);
      }
      else {
        memcpy(reply_msg_t + sizeof(RemoteSet::RemoteSetReplyItem),(char *)tmp_val + RAD_META_LEN, vlen);
      }

      reply_item->seq = seq;
      reply_item->node = node;
      reply_msg_t += (sizeof(RemoteSet::RemoteSetReplyItem) + vlen);
      reply_item->idx = i;
      num_item_fetched += 1;
      assert(seq != 0);
#if RAD_LOG
      char *log_buf = next_log_entry(&local_log,64);
      assert(log_buf != NULL);
      sprintf(log_buf,"try get %d,%d,%d, lock %p, table %d\n",id,thread_id,cid,&(node->lock),
              header->tableid);
#endif
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
      reply_item->seq = node->seq;
      reply_item->node = node;
      reply_item->idx  = i;
      reply_item->payload = 0;
      reply_msg_t += (sizeof(RemoteSet::RemoteSetReplyItem));
      num_item_fetched += 1;
    }
      break;
    case REQ_READ_LOCK:
      assert(false);
      break;
    default:
      assert(false);
    }

  }
  //  assert(num_item_fetched > 0 && num_item_fetched <= 256);
  assert(num_item_fetched > 0);
  /* send reply */
  r_header->num_items_ = num_item_fetched;
  r_header->payload_   = reply_msg_t - reply_msg;
  r_header->partition_id_ = current_partition;
  rpc_handler_->send_reply(r_header->payload_,id,cid);

  //  last_rpc_mark_[cid] = RPC_READ;
#if RAD_LOG
  log_buf = next_log_entry(&local_log,9);
  assert(log_buf != NULL);
  sprintf(log_buf,"get ends\n");
#endif

}


void //__attribute__((optimize("O0")))
DBRad::lock_rpc_handler(int id,int cid, char *msg,void *arg) {

#if RAD_LOG
  char *log_buf = next_log_entry(&local_log,10);
  assert(log_buf != NULL);
  sprintf(log_buf,"lock start\n");
#endif


  // unlike lock rpc handler in OCC, dbrad needs further check versions
  RemoteSet::RequestHeader *header = (RemoteSet::RequestHeader *)msg;

  char *reply_msg = rpc_handler_->get_reply_buf();

  // initilizae with lock success
  ((RemoteSet::ReplyHeader *) reply_msg)->num_items_ = 1;

  char *traverse_ptr = msg + sizeof(RemoteSet::RequestHeader);
  int num_items = header->num;

  uint64_t max_time(0);
  for(uint i = 0;i < num_items;++i) {

    RemoteSet::RemoteLockItem *lheader = (RemoteSet::RemoteLockItem *)traverse_ptr;
    traverse_ptr += sizeof(RemoteSet::RemoteLockItem);

    if(lheader->pid != current_partition) {
      continue;
    }
#if 1
    MemNode *node = lheader->node;
    volatile uint64_t *lockptr = &(node->lock);

    // lock the item
#if RAD_LOG
    char *log_buf = next_log_entry(&local_log,64);
    assert(log_buf != NULL);
    sprintf(log_buf,"lock ptr %p, @%d,%d,%d\n",lheader->node,id,thread_id,cid);
#endif

    // 73 is a magic number to avoid races
    if( unlikely(
                 (*lockptr != 0) ||
                 (!__sync_bool_compare_and_swap(lockptr,0,
                                                ENCODE_LOCK_CONTENT(id,thread_id,cid + 1)))
                  )
        ) {

        uint64_t lock_val = *lockptr;
#if RAD_LOG
        char *log_buf = next_log_entry(&local_log,64);
        assert(log_buf != NULL);
        sprintf(log_buf,"lock ptr %p,fail %lu, check val %d,%d,%d\n",lheader->node,lock_val,
                DECODE_LOCK_MAC(lock_val),DECODE_LOCK_TID(lock_val),DECODE_LOCK_CID(lock_val));
#endif


        assert(lock_val != ENCODE_LOCK_CONTENT(id,thread_id,cid + 1));
        // lock failed
        ((RemoteSet::ReplyHeader *) reply_msg)->num_items_ = 0;
        break;
      }
#if RAD_LOG
    {
        char *log_buf = next_log_entry(&local_log,64);
        assert(log_buf != NULL);
        sprintf(log_buf,"lock ptr %p, @%d,%d,%d succeed\n",lheader->node,id,thread_id,cid);
    }
#endif
    //assert(lheader->node->lock == ENCODE_LOCK_CONTENT(id,thread_id,cid + 1));
#if 0
    // record the previous lock history
    if(lock_check_status[cid].find((uint64_t)lockptr) == lock_check_status[cid].end()) {
      lock_check_status[cid].insert(std::make_pair((uint64_t)lockptr,false));
    } else
      lock_check_status[cid][(uint64_t)lockptr] = false;
#endif

    // further check sequence numbers
    if(unlikely( lheader->node->seq != lheader->seq)) {
      // validation failed, value has been changed
      ((RemoteSet::ReplyHeader *) reply_msg)->num_items_ = 0;
      break;
    }
#endif

#ifdef RAD_TX
    max_time = MAX(max_time,lheader->node->read_ts);
#endif
    /* end iterating request items */
  }
  /* re-use payload field to set the max time */
  assert(num_items > 0);
  //  ((RemoteSet::ReplyHeader *) reply_msg)->num_items_ = num_items;
  ((RemoteSet::ReplyHeader *)(reply_msg))->payload_ = max_time;
  rpc_handler_->send_reply(sizeof(RemoteSet::ReplyHeader),id,cid);

  //last_rpc_mark_[cid] = RPC_LOCK;

#if RAD_LOG
  log_buf = next_log_entry(&local_log,10);
  assert(log_buf != NULL);
  sprintf(log_buf,"lock ends\n");
#endif

}


void
DBRad::release_rpc_handler(int id,int cid,char *msg,void *arg) {

#if RAD_LOG
  char *log_buf = next_log_entry(&local_log,64);
  assert(log_buf != NULL);
  sprintf(log_buf,"try release ptr ,%d,%d,%d\n",id,thread_id,cid);
#endif

  /* release rpc handler shall be the same */
  RemoteSet::RequestHeader *header = (RemoteSet::RequestHeader *)msg;
  char *traverse_ptr = msg + sizeof(RemoteSet::RequestHeader);
  int num_items = header->num;
  assert(num_items > 0);

  for(uint i = 0;i < num_items;++i) {

    RemoteSet::RemoteLockItem *lheader = (RemoteSet::RemoteLockItem *)traverse_ptr;
    traverse_ptr += sizeof(RemoteSet::RemoteLockItem);
    if(lheader->pid != current_partition) {
      continue;
    }
#if 1
    // release the item
    volatile uint64_t *lockptr = &(lheader->node->lock);
    bool s_res = __sync_bool_compare_and_swap(lockptr,ENCODE_LOCK_CONTENT(id,thread_id,cid + 1),0);

#if RAD_LOG
    char *log_buf = next_log_entry(&local_log, 64);
    assert(log_buf != NULL);
    sprintf(log_buf,"try release at %d,%d,%d @%p, %d,val %lu\n",id,thread_id,cid,lockptr,s_res,*lockptr);
#endif

#endif
  }

  //last_rpc_mark_[cid] = RPC_RELEASE;
}


void //__attribute__((optimize("O0")))
DBRad::commit_rpc_handler2(int id,int cid,char *msg,void *arg) {

  RemoteSet::RequestHeader *r_header = (RemoteSet::RequestHeader *)msg;

  int num_items = r_header->num;
  //int num_items = r_header->tx_id;
  uint64_t desired_seq = r_header->padding;

  char *traverse_ptr = msg + sizeof(RemoteSet::RequestHeader);
  assert(num_items > 0);

  for(uint i = 0;i < num_items;++i) {
  //for(uint i = 0;false; ) {

    RemoteSet::RemoteWriteItem *header = (RemoteSet::RemoteWriteItem *)traverse_ptr;
    traverse_ptr += sizeof(RemoteSet::RemoteWriteItem);

    if(header->pid != current_partition ) {
      //fprintf(stdout,"git pid %d\n",header->pid);
      //assert(false);
      traverse_ptr += header->payload;
      continue;
    }
#if 0
    MemNode *node = txdb_->stores_[header->tableid]->GetWithInsert((uint64_t)(header->key));
    if(node != header->node) {
      fprintf(stdout,"fetched node %p, real %p for key %lu\n",header->node,node,header->key);
      assert(false);
    }
#endif

    char *new_val;
    if(header->payload == 0) {
      new_val = NULL;
    }
    else {
      new_val = (char *)malloc(header->payload + RAD_META_LEN);
      memcpy(new_val + RAD_META_LEN,traverse_ptr, header->payload);
    }
    uint64_t old_seq = header->node->seq;
#if 1
    header->node->seq   = 1;
    asm volatile("" ::: "memory");
    /* now we simply using memcpy */
    /* dbrad shall install a new vrsion */
    uint64_t *cur    = header->node->value;
    /* Using value switch */
    uint64_t *oldptr = header->node->old_value;

    if(cur != NULL) {
      _RadValHeader * hptr = (_RadValHeader *)cur;
      hptr->oldValue = oldptr;
      //hptr->version  = header->seq;
      hptr->version = old_seq;
    } else {

    }
    header->node->old_value = cur;
    header->node->value = (uint64_t *)new_val;
    assert(header->node->seq < desired_seq);
    asm volatile("" ::: "memory");
    header->node->seq = desired_seq;
    asm volatile("" ::: "memory");
#endif
    /* release the lock */
    header->node->lock = 0;
    asm volatile("" ::: "memory");
    //assert(header->node->lock != ENCODE_LOCK_CONTENT(id,thread_id,cid + 1));
    //last_lock_ptr_[cid] = (char *) (&(header->node->lock));

    //assert(lock_check_status[cid].find((uint64_t)last_lock_ptr_[cid]) != lock_check_status[cid].end());
    //lock_check_status[cid][(uint64_t)last_lock_ptr_[cid]] = true;
#if RAD_LOG
    //    fprintf(stdout,"try release %p at %d\n",header->node,thread_id);
    char *log_buf = next_log_entry(&local_log,64);
    assert(log_buf != NULL);
    sprintf(log_buf,"commit release node %p,%d,%d,%d\n",header->node,id,thread_id,cid);
#endif
    // promote the pointer
    traverse_ptr += header->payload;
  }
  //fprintf(stdout,"commit done\n");
  //last_msg_counts_[cid] = r_header->padding;
  //last_rpc_mark_[cid] = RPC_COMMIT;
}

void DBRad::commit_rpc_handler(int id,int cid,char *msg,void *arg) {

  assert(false); /* this method is abandomed */
#if 0
  int32_t total_size   = (*((RemoteSet::CommitHeader *) msg)).total_size;
  uint64_t desired_seq = (*((RemoteSet::CommitHeader *) msg)).commit_seq;

  msg += sizeof(RemoteSet::CommitHeader);
  int processed = 0;

  assert(total_size > sizeof(RemoteSet::ReplyHeader));
  while(processed < total_size) {

    RemoteSet::ReplyHeader *r_header = (RemoteSet::ReplyHeader *) msg;
    if(r_header->partition_id_ != current_partition) {
      msg += r_header->payload_;
      processed += r_header->payload_;
      continue;
    }

    msg += sizeof(RemoteSet::ReplyHeader);

    for(uint j = 0; j < r_header->num_items_;++j) {
      /* install local writes */
      RemoteSet::RemoteWriteItem *header = (RemoteSet::RemoteWriteItem *)msg;
      //      memcpy(header->node->value
#if 0
      if(header->seq != header->node->seq) {
        fprintf(stdout,"heade %lu, node %lu\n",header->seq,header->node->seq);
        assert(false);
      }
#else
      assert(header->seq == header->node->seq);
#endif
      assert(header->seq == header->node->seq);
      //      assert(desired_seq > header->seq);
      header->node->seq   = 1;
      asm volatile("" ::: "memory");
      /* now we simply using memcpy */
      /* dbrad shall install a new vrsion */
      uint64_t *cur    = header->node->value;
      /* Using value switch */
      uint64_t *oldptr = header->node->old_value;

      if(cur != NULL) {
        _RadValHeader * hptr = (_RadValHeader *)cur;
        hptr->oldValue = oldptr;
        hptr->version  = header->seq;
      } else {

      }
      /* TODO, may set the version */
      char *new_val = (char *)malloc(header->payload + RAD_META_LEN);
      memcpy(new_val + RAD_META_LEN,msg + sizeof(RemoteSet::RemoteSetReplyItem), header->payload);
      header->node->old_value = cur;
      header->node->value = (uint64_t *)new_val;
      assert(desired_seq > header->node->seq);
      asm volatile("" ::: "memory");
      header->node->seq = desired_seq;
      asm volatile("" ::: "memory");
      /* release the lock */
#if 0
      if(__sync_bool_compare_and_swap( (uint64_t *)(&(header->node->lock)),
                                       _QP_ENCODE_ID(id,this->thread_id + 1),
                                       0) != true){
        fprintf(stdout,"locked by %d idx %d\n",_QP_DECODE_MAC(header->node->lock),_QP_DECODE_INDEX(header->node->lock));
        assert(false);
      }
#else
      assert(false);
      header->node->lock = 0;
#endif
      //header->node->lock = 0; // optimized way
      msg += (header->payload + sizeof(RemoteSet::RemoteSetReplyItem));
    }
    break;
  }
  assert(processed < total_size);
#endif
  /* end proessing commit message */
}

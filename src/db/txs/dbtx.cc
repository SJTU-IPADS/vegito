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
#include "all.h"
#include "config.h"

#include "framework/bench_worker.h"
#include "framework/framework_cfg.h"

#include <unistd.h>
#include <algorithm>
#include <cstdlib>

/* for std::bind */
#include <functional>

#define MAXSIZE 1024

// 1: use copy to install the new value
// 0: replace the old value with the new value pointer
#define COPY 1

// 1: naive RPC
// 2: naive + RPC batching
// 3: naive + RPC batching + speculative execuation
#define NAIVE 0       // does not batch RPC, send it one-by-one

using namespace nocc::oltp;
using namespace nocc::framework;
using namespace std;

extern size_t current_partition;
extern RdmaCtrl *cm;

#define unlikely(x) __builtin_expect(!!(x), 0)
#define likely(x)   __builtin_expect(!!(x), 1)

/* Read-write sets */
/*******************/

__thread RemoteHelper *remote_helper;
namespace nocc{

  extern __thread BenchWorker* worker;
  namespace db{
    class DBTX::ReadSet  {

public:
  int elems;
  ReadSet(int tid);
  ~ReadSet();
  inline void Reset();
  inline uint64_t Add(uint64_t *ptr);
  inline void AddNext(uint64_t *ptr, uint64_t value);
  inline bool Validate();

private:
  struct RSSeqPair {
    uint64_t seq; //seq got when read the value
    uint64_t *seqptr; //pointer to the global memory location
  };

  //This is used to check the insertion problem in range query
  //FIXME: Still have the ABA problem
  struct RSSuccPair {
    uint64_t next; //next addr
    uint64_t *nextptr; //pointer to next[0]
  };

  int max_length;
  RSSeqPair *seqs;

  int rangeElems;
  RSSuccPair *nexts;

  void Resize();
  int tid;
};

DBTX::ReadSet::ReadSet(int t):
  tid(t)
{
  max_length = MAXSIZE;
  elems = 0;
  seqs = new RSSeqPair[max_length];

  rangeElems = 0;
  nexts = new RSSuccPair[max_length];

  //pretouch to avoid page fault in the rtm
  for(int i = 0; i < max_length; i++) {
    seqs[i].seq = 0;
    seqs[i].seqptr = NULL;
    nexts[i].next = 0;
    nexts[i].nextptr = NULL;
  }
}

DBTX::ReadSet::~ReadSet()
{
  delete[] seqs;
  delete[] nexts;
}

inline void DBTX::ReadSet::Reset()
{
  elems = 0;
  rangeElems = 0;
}

void DBTX::ReadSet::Resize()
{
  //    printf("Read Set Resize %d  by %d\n",elems,tid);
  max_length = max_length * 2;

  RSSeqPair *ns = new RSSeqPair[max_length];
  for(int i = 0; i < elems; i++) {
    ns[i] = seqs[i];
  }
  delete[] seqs;
  seqs = ns;


  RSSuccPair* nts = new RSSuccPair[max_length];
  for(int i = 0; i < rangeElems; i++) {
    nts[i] = nexts[i];
  }
  delete[] nexts;
  nexts = nts;

}


inline void DBTX::ReadSet::AddNext(uint64_t *ptr, uint64_t value)
{
  //if (max_length < rangeElems) printf("ELEMS %d MAX %d\n", rangeElems, max_length);
  assert(rangeElems <= max_length);

  if(rangeElems == max_length) {
    Resize();
  }

  int cur = rangeElems;
  rangeElems++;

  nexts[cur].next = value;
  nexts[cur].nextptr = ptr;
}


inline uint64_t DBTX::ReadSet::Add(uint64_t *ptr)
{
  if (max_length < elems) printf("ELEMS %d MAX %d\n", elems, max_length);
  assert(elems <= max_length);

  if(elems == max_length) {
    Resize();
    //assert(false);
  }

  int cur = elems;
  elems++;

  //    fprintf(stdout,"add seq %llu\n",*ptr);
  // since we are update steps 2 for optimistic replication
  //  seqs[cur].seq = (*ptr + ((*ptr) % 2 ));
  seqs[cur].seq = *ptr;
  seqs[cur].seqptr = ptr;
  return seqs[cur].seq;
}

inline bool DBTX::ReadSet::Validate()
{
  //This function should be protected by rtm or mutex
  //Check if any tuple read has been modified
  for(int i = 0; i < elems; i++) {
    assert(seqs[i].seqptr != NULL);
    if(seqs[i].seq != *seqs[i].seqptr) {
      //      fprintf(stdout,"Record seq %lu, now %lu\n",seqs[i].seq, *seqs[i].seqptr);
      return false;
    }
  }

  //Check if any tuple has been inserted in the range
  for(int i = 0; i < rangeElems; i++) {
    assert(nexts[i].nextptr != NULL);
    if(nexts[i].next != *nexts[i].nextptr) {
      //      fprintf(stdout,"Record seq %lu, now %lu\n",nexts[i].next, *nexts[i].nextptr);
      return false;
    }
  }
  return true;
}

/* End readset's definiation */

class DBTX::DeleteSet {
public:
  struct DItem {
    int tableid;
    uint64_t key;
    MemNode *node;
  };

  int elems;
  int max_length;
  DItem *kvs;

  DeleteSet();
  inline void Reset();
  inline void Add(DItem &item);
  inline void Execute();
  inline bool Prepare();
};

DBTX::DeleteSet::DeleteSet() {
  max_length = MAXSIZE;
  elems = 0;
  kvs = new DItem[max_length];
}

inline void DBTX::DeleteSet::Reset() {
  elems = 0;
}

inline void DBTX::DeleteSet::Add(DBTX::DeleteSet::DItem &item) {
  kvs[elems].key = item.key;
  kvs[elems].tableid = item.tableid;
  kvs[elems].node = item.node;
}

inline bool DBTX::DeleteSet::Prepare() {
  return true;
}

inline void DBTX::DeleteSet::Execute() {
  /* Fix me, current we does not consider concurrency in execute function */
  for(uint i = 0;i < elems;++i) {
    if(kvs[i].node->value != NULL) {
      kvs[i].node->value = NULL;
      //      kvs[i].node->seq = 0;
    }
    else
      assert(false);
  }
}

/* End delete set's implementation */


class DBTX::RWSet  {
public:
  struct RWSetItem {
    int32_t tableid;
    uint64_t key;
    int len;
    MemNode  *node;
    uint64_t *addr;  /* Pointer to the new value buffer */
    uint64_t seq;
    bool ro;
    uint32_t op_bit;
  };

  DBTX *dbtx_;
  int max_length;
  RWSetItem *kvs;

  int elems;
  int current;

  RWSet();
  ~RWSet();

  inline void Reset();
  inline void Resize();
  inline void SetDBTX(DBTX *dbtx);
  inline void Add(RWSetItem &item);

  // commit helper functions
  inline bool LockAllSet();
  inline void ReleaseAllSet();
  inline bool CheckLocalSet();

  inline int  CommitLocalWrite();

  inline void GC();

  bool inline IsLocked(uint64_t *ptr) {
    return (*ptr) != 0;
  }
};

DBTX::RWSet::RWSet()
{
  max_length = MAXSIZE;
  elems = 0;
  kvs = new RWSetItem[max_length];
  dbtx_ = NULL;

  for (int i = 0; i < max_length; i++) {
    kvs[i].tableid = -1;
    kvs[i].key = 0;
    kvs[i].seq = 0;
    kvs[i].addr = NULL;
    kvs[i].len = 0;
    kvs[i].ro = false;
    kvs[i].node = NULL;
  }
}


DBTX::RWSet::~RWSet()
{  delete [] kvs;
}

inline void DBTX::RWSet::GC() {
  for(uint i = 0;i < elems;++i) {
    if(kvs[i].addr != NULL) free(kvs[i].addr);
  }
}

inline void DBTX::RWSet::Add(RWSetItem &item) {

  //  assert(item.tableid != 0);
  if (max_length < elems) printf("ELEMS %d MAX %d\n", elems, max_length);
  //assert(elems <= max_length);
  if (elems == max_length) {
    Resize();
  //assert(false);
  }

  int cur = elems;
  elems++;

  //  kvs[cur].pid = item.pid;
  kvs[cur].tableid = item.tableid;
  kvs[cur].key = item.key;
  kvs[cur].len = item.len;
  kvs[cur].ro = item.ro;

  kvs[cur].seq = item.seq;
  //  kvs[cur].loc = item.loc;
  kvs[cur].addr = item.addr;
  //  kvs[cur].off = item.off;
  kvs[cur].node = item.node;
}

inline void DBTX::RWSet::SetDBTX(DBTX *dbtx)
{
  dbtx_ = dbtx;
}

inline void DBTX::RWSet::Reset()
{
  elems = 0;
  current = -1;
}

inline void DBTX::RWSet::Resize()
{
  max_length = max_length * 2;

  RWSetItem *nkvs = new RWSetItem[max_length];
  for (int i = 0; i < elems; i++) {
    nkvs[i] = kvs[i];
  }

  delete []kvs;
  kvs = nkvs;
}

inline bool
DBTX::RWSet::LockAllSet() {

  for(int i = 0;i < elems;++i) {
    if(kvs[i].ro) {
      continue;
    }

    //    uint64_t *lockptr = (uint64_t *)(kvs[i].loc);
    volatile uint64_t *lockptr = &(kvs[i].node->lock);

    //    fprintf(stdout,"lock %d  %lu\n",kvs[i].tableid,kvs[i].key);
    if( unlikely( (*lockptr != 0) ||
		  !__sync_bool_compare_and_swap(lockptr,0,
						ENCODE_LOCK_CONTENT(current_partition,dbtx_->thread_id,
								    dbtx_->cor_id_ + 1)))) {
      //      fprintf(stdout,"prev value %lu at %lu tab %d current %d\n",*lockptr,kvs[i].off,kvs[i].tableid,current_partition);
      return false;
    }
  current = i;
  //    if(kvs[i].node->seq != kvs[i].seq) {
  //          return false;
  //    }
  }
  return true;
}

inline void
DBTX::RWSet::ReleaseAllSet() {

  for(int i = 0;i <= current;++i) {
    if(kvs[i].ro)
      continue;
    volatile uint64_t *lockptr = &(kvs[i].node->lock);
    //    *lockptr = 0;
    __sync_bool_compare_and_swap( lockptr,
                                  ENCODE_LOCK_CONTENT(current_partition,dbtx_->thread_id,
                                                      dbtx_->cor_id_ + 1),0);
  }
}

inline bool DBTX::RWSet::CheckLocalSet() {

  for(uint i = 0;i < elems;++i) {

    uint64_t seq = kvs[i].node->seq;

    if(seq != kvs[i].seq) {
      //      fprintf(stdout,"abort by table %d %lu=>%lu\n",kvs[i].tableid,kvs[i].seq,seq);
      //assert(false);
      return false;
    }
  }
  return true;
}

inline int
DBTX::RWSet::CommitLocalWrite() {

  int counter = 0;
  for(uint i = 0;i < elems;++i) {

    if(kvs[i].ro)
      continue;
    /* local writes */
    kvs[i].node->seq = 1;

    asm volatile("" ::: "memory");
#if COPY == 1
    memcpy( (char *)(kvs[i].node->value) + META_LENGTH, (char *)(kvs[i].addr) + META_LENGTH,kvs[i].len);
#else
    kvs[i].node->value = kvs[i].addr;
#endif
    asm volatile("" ::: "memory");

    // update the sequence
    kvs[i].node->seq = kvs[i].seq + 2;

    /* By the way, release the lock */
    asm volatile("" ::: "memory");
    kvs[i].node->lock = 0;
  }
  return counter;
}


/* Main transaction layer definations  */
void DBTX::ThreadLocalInit()
{
  if ( false == localinit) {
    readset = new ReadSet(thread_id);
    rwset = new RWSet();
    remoteset = new RemoteSet(rpc_handler_,cor_id_,thread_id);
    //    remote_helper_->thread_local_init();
    localinit = true;
  }
}

void DBTX::init_temp() {
  readset = new ReadSet(thread_id);
  rwset = new RWSet();
}

DBTX::DBTX(MemDB *db) : txdb_(db),localinit(false) ,TXHandler(73) { }

DBTX::DBTX(MemDB* tables,int t_id,Rpc *rpc,int c_id, LogTSManager *tsm)
  :txdb_(tables),
   thread_id(t_id),
   rpc_handler_(rpc),
   localinit(false),
   TXHandler(c_id),
   db_tsm_(tsm)
{
  /* register RPC handler */
  using namespace  std::placeholders;
#if FASST == 1
  rpc_handler_->register_callback(std::bind(&DBTX::get_lock_rpc_handler,this,_1,_2,_3,_4),RPC_READ);
#else
#if NAIVE == 0
  rpc_handler_->register_callback(std::bind(&DBTX::get_rpc_handler,this,_1,_2,_3,_4),RPC_READ);
#else
  rpc_handler_->register_callback(std::bind(&DBTX::get_naive_rpc_handler,this,_1,_2,_3,_4),RPC_READ);
#endif // register RPC for normal's case
#endif // register RPC for fasst's case
  rpc_handler_->register_callback(std::bind(&DBTX::lock_rpc_handler,this,_1,_2,_3,_4),RPC_LOCK);
  rpc_handler_->register_callback(std::bind(&DBTX::release_rpc_handler,this,_1,_2,_3,_4),RPC_RELEASE);
  rpc_handler_->register_callback(std::bind(&DBTX::validate_rpc_handler,this,_1,_2,_3,_4),RPC_VALIDATE);
  rpc_handler_->register_callback(std::bind(&DBTX::commit_rpc_handler2,this,_1,_2,_3,_4),RPC_COMMIT);

  rpc_handler_->register_callback(std::bind(&DBTX::ro_val_rpc_handler,this,_1,_2,_3,_4),RPC_R_VALIDATE);

  nreadro_locked = 0;
}

uint64_t DBTX::get_cached(int tableid, uint64_t key, char **val) {
  for(uint i = 0;i < remoteset->elems_;++i) {
    if(remoteset->kvs_[i].tableid == tableid && remoteset->kvs_[i].key == key) {
      //      fprintf(stdout,"return key %lu\n",key);
      *val = (char *)(remoteset->kvs_[i].val);
      return remoteset->kvs_[i].seq;
    }
  }
  return 0;
}

uint64_t DBTX::get_cached(int idx,char **val) {
  assert(idx < remoteset->elems_);
  *val = (char *)(remoteset->kvs_[idx].val);
  return remoteset->kvs_[idx].seq;
}



uint64_t DBTX::get_ro_versioned(int tableid,uint64_t key,char *val,uint64_t version,yield_func_t &yield) {
  /* this shall be only called for remote operations */
  /* since this is occ, the version is not useful */
#if 0
  MemNode *node = NULL;
  int len = txdb_->_schemas[tableid].vlen;
  //  fprintf(stdout,"start getwith %p\n",txdb_);
  node = txdb_->stores_[tableid]->GetWithInsert(key);
  assert(node != NULL);
 retry:
  uint64_t seq = node->seq;
  asm volatile("" ::: "memory");
  uint64_t *tmpVal = node->value;

  if(likely(tmpVal != NULL)) {
    if(unlikely(seq == 1))
      goto retry;
    asm volatile("" ::: "memory");
    memcpy(val,(char *)tmpVal + META_LENGTH,len );
    asm volatile("" ::: "memory");
    if( unlikely(node->seq != seq) ) {
      goto retry;
    }

  } else {
    //    fprintf(stdout,"key %lu, tableid %d\n",key,tableid);
    //    assert(false);
    //    seq = 0;
    return 1;
  }
  remote_helper_->add(node,seq);
#endif
  return remote_helper->temp_tx_->get_ro(tableid,key,val,yield);
}

uint64_t DBTX::get_ro(int tableid,uint64_t key,char *val,yield_func_t &yield) {

  MemNode *node = NULL;
  int len = txdb_->_schemas[tableid].vlen;
  // node = txdb_->stores_[tableid]->GetWithInsert(key);
  node = txdb_->stores_[tableid]->Get(key);

 retry:
  uint64_t seq = node->seq;
  asm volatile("" ::: "memory");
  uint64_t *tmpVal = node->value;

  if(likely(tmpVal != NULL)) {
    if(unlikely(seq == 1))
      goto retry;
    asm volatile("" ::: "memory");
    memcpy(val,(char *)tmpVal + META_LENGTH,len );
    asm volatile("" ::: "memory");
    if( unlikely(node->seq != seq) ) {
      goto retry;
    }

  } else {
    //    fprintf(stdout,"key %lu, tableid %d\n",key,tableid);
    //    assert(false);
    return 1;
  }

#if 1
  RWSet::RWSetItem item;

  item.tableid = tableid;
  item.key = key;
  item.seq = seq;
  item.len = len;
  item.ro  = false;
  item.node = (MemNode *)node;

  rwset->Add(item);
#endif
  return seq;
}

uint64_t DBTX::get(int tableid, uint64_t key, char **val,int len) {

#if 0
  for(uint i = 0;i < rwset->elems;++i) {
    if (rwset->kvs[i].tableid == tableid && rwset->kvs[i].key == key) {
      *val = (char *)rwset->kvs[i].addr;
      return rwset->kvs[i].seq;
    }
  }
#endif
  char *_addr = (char *) std::malloc(len + META_LENGTH);
  MemNode *node = NULL;
  // node = txdb_->stores_[tableid]->GetWithInsert(key);
  node = txdb_->stores_[tableid]->Get(key);

 retry:
  uint64_t seq = node->seq;

  asm volatile("" ::: "memory");
  if(unlikely(seq == 1)) goto retry;
  uint64_t *tmpVal = node->value;

  if(unlikely(tmpVal == NULL)) {
    return 1;
  }
  //  asm volatile("" ::: "memory");
  memcpy(_addr + META_LENGTH,(char *)tmpVal + META_LENGTH,len);
  asm volatile("" ::: "memory");
  if( unlikely(node->seq != seq) ) {
    goto retry;
  }

#if 1
  RWSet::RWSetItem item;

  item.tableid = tableid;
  item.key = key;
  item.seq = seq;
  item.len = len;
  item.ro  = true;
  item.addr = (uint64_t *)_addr;
  //item.loc  = tmpVal;
  item.node = (MemNode *)node;

  rwset->Add(item);
#endif
  *val = ((char *)_addr + META_LENGTH);
  return seq;
}


uint64_t DBTX::get(char *n, char **val,int len) {

#if 0
  for(uint i = 0;i < rwset->elems;++i) {
    if (rwset->kvs[i].tableid == tableid && rwset->kvs[i].key == key) {
      *val = (char *)rwset->kvs[i].addr;
      return rwset->kvs[i].seq;
    }
  }
#endif
  //  fprintf(stdout,"get tab %d key %lu\n",tableid,key);
  char *_addr = (char *) std::malloc(len + META_LENGTH);
  MemNode *node = (MemNode *)n;
  //  node = txdb_->stores_[tableid]->GetWithInsert(key);

 retry:
  uint64_t seq = node->seq;
  return seq;
#if 1
  asm volatile("" ::: "memory");
  if(unlikely(seq == 1)) goto retry;
  uint64_t *tmpVal = node->value;

  if(unlikely(tmpVal == NULL)) {
    assert(false);
    return 0;
  }

  //  asm volatile("" ::: "memory");
  memcpy(_addr + META_LENGTH,(char *)tmpVal + META_LENGTH,len);
  asm volatile("" ::: "memory");
  if( unlikely(node->seq != seq) ) {
    goto retry;
  }

#if 1
  RWSet::RWSetItem item;

  //  item.tableid = tableid;
  //  item.key = key;
  item.seq = seq;
  item.len = len;
  item.ro  = true;
  item.addr = (uint64_t *)_addr;
  //item.loc  = tmpVal;
  item.node = (MemNode *)node;

  rwset->Add(item);
#endif
  *val = ((char *)_addr + META_LENGTH);
#endif
  return seq;
}



// void DBTX::insert_index(int tableid, uint64_t key,char *val) {
// 
//   MemNode *node = txdb_->_indexs[tableid]->GetWithInsert(key);
//   RWSet::RWSetItem item;
//   if(tableid == 4) assert(node->value == NULL);
// 
//   item.tableid = tableid;
//   item.key     = key;
//   item.node    = node;
//   item.addr    = (uint64_t *)val;
//   item.seq     = node->seq;
//   item.ro      = false;
//   rwset->Add(item);
// }

void DBTX::insert_index(int tableid, uint64_t key,char *val) {
  int len = sizeof(uint64_t) + sizeof(uint64_t);
  int vlen = META_LENGTH + len;

  RWSet::RWSetItem item;
  item.addr = (uint64_t *) (new char[vlen]);
  memset(item.addr,0,META_LENGTH);
  memcpy( (char *)item.addr + META_LENGTH, val,len);

 retry:
#if COPY == 1
  MemNode *node = txdb_->stores_[tableid]->Put(key,(uint64_t *)(item.addr));
#else
  MemNode *node = txdb_->stores_[tableid]->GetWithInsert(key);
#endif

  item.tableid = tableid;
  item.key = key;
  item.seq = node->seq;
  item.len = len;
  item.ro = false;
  item.node = node;

  rwset->Add(item);

}

inline static void 
prepare_log(int cor_id, DBLogger* db_logger, 
            const DBTX::RWSet::RWSetItem& item) {
  assert(item.op_bit != 0);
  char* val = db_logger->get_log_entry(cor_id, item.tableid, 
                                       item.key, item.len, item.op_bit);
  memcpy(val, (char*)item.addr + META_LENGTH, item.len);
  db_logger->close_entry(cor_id, item.seq + 2); 
} 

void DBTX::insert(int tableid, uint64_t key, char *val, int len) {

  int vlen = META_LENGTH + len;

  RWSet::RWSetItem item;
  item.addr = (uint64_t *) (new char[vlen]);
  memset(item.addr,0,META_LENGTH);
  memcpy( (char *)item.addr + META_LENGTH, val,len);

#if COPY == 1
  MemNode *node = txdb_->stores_[tableid]->Put(key,(uint64_t *)(item.addr));
#else
  MemNode *node = txdb_->stores_[tableid]->GetWithInsert(key);
#endif

  item.tableid = tableid;
  item.key = key;
  item.seq = node->seq;
  item.len = len;
  item.ro = false;
  item.node = node;
  item.op_bit = LOG_OP_I; 

  rwset->Add(item);
  if(db_logger_) {
    prepare_log(cor_id_, db_logger_, item);
  }
}

void DBTX::stream_insert(int tableid, uint64_t key, const char *val, int len) {

  int vlen = META_LENGTH + len;

  RWSet::RWSetItem item;
  item.addr = (uint64_t *) (new char[vlen]);
  memset(item.addr,0,META_LENGTH);
  memcpy( (char *)item.addr + META_LENGTH, val,len);

  item.tableid = tableid;
  item.key = key;
  // item.seq = node->seq;
  item.seq = 0;
  item.len = len;
  item.ro = false;
  // item.node = node;
  item.op_bit = LOG_OP_I; 

  // rwset->Add(item);
  if(db_logger_) {
    prepare_log(cor_id_, db_logger_, item);
  }
}

void DBTX::delete_index(int tableid,uint64_t key) {
#if 0
  DeleteSet::DItem item;
  item.tableid = tableid;
  item.node = txdb_->_indexs[tableid]->GetWithInsert(key);
  if(unlikely(item.node->value == NULL)) return;
  delset->Add(item);
#endif
  // MemNode *node = txdb_->_indexs[tableid]->GetWithInsert(key);
  MemNode *node = txdb_->_indexs[tableid]->Get(key);
  if(unlikely(node->value == NULL)) {
    //fprintf(stdout,"index %d null, check symbol %s using %p\n",
    //	      tableid,(char *)( &(((uint64_t *)key)[2])),node);
    //      assert(false);
    // possible found by the workload
    return;
  }
  RWSet::RWSetItem item;
  item.tableid = tableid;
  item.key     = key;
  item.node    = node;
  item.addr    = NULL;
  item.seq     = node->seq;
  item.ro      = false;
  rwset->Add(item);
}

void DBTX::delete_by_node(int tableid,char *node) {
#if 0
  DeleteSet::DItem item;
  item.tableid = tableid;
  item.node = (MemNode *)node;
  delset->Add(item);
#endif
  for(uint i = 0;i < rwset->elems;++i) {
    if((char *)(rwset->kvs[i].node) == node) {
      RWSet::RWSetItem &item = rwset->kvs[i];
      item.ro = false;
      delete item.addr;
      item.addr = NULL;
      item.len = 0;
      if(db_logger_){
        // printf("delete_by_node size:%d ,key:%lu\n",item.len, item.key);
        prepare_log(cor_id_, db_logger_, item);
      }
      return;
    }
  }
  RWSet::RWSetItem item;

  item.tableid = tableid;
  item.key     = 0;
  item.len     = 0;
  item.node    = (MemNode *)node;
  item.addr    = NULL; // NUll means logical delete
  item.seq     = item.node->seq;
  item.ro      = false;
  rwset->Add(item);
  if(db_logger_) {
    // printf("delete_by_node size:%d ,key:%lu\n",item.len, item.key);
    prepare_log(cor_id_, db_logger_, item);
  }
}

void DBTX::delete_(int tableid,uint64_t key) {
#if 0
  MemNode *node = txdb_->stores_[tableid]->GetWithInsert(key);

  DeleteSet::DItem item;
  item.tableid = tableid;
  item.key = key;
  item.node = node;

  delset->Add(item);
#endif

  for(uint i = 0;i < rwset->elems;++i) {
    if(tableid == rwset->kvs[i].tableid &&
       rwset->kvs[i].key == key) {
      RWSet::RWSetItem &item = rwset->kvs[i];
      item.ro = false;
      delete item.addr;
      item.addr = NULL;
      item.len = 0;
      item.op_bit = LOG_OP_D;
      static_assert(LOG_OP_D != 0);
      if(db_logger_){
        // printf("delete_ size:%d ,key:%lu\n",item.len, item.key);
        prepare_log(cor_id_, db_logger_, item);
      }
      return;
    }
  }

  // MemNode *node = txdb_->stores_[tableid]->GetWithInsert(key);
  MemNode *node = txdb_->stores_[tableid]->Get(key);
  RWSet::RWSetItem item;

  item.tableid = tableid;
  item.key     = key;
  item.len     = 0;
  item.node    = node;
  item.addr    = NULL; // NUll means logical delete
  item.seq     = node->seq;
  item.ro      = false;
  item.op_bit  = LOG_OP_D;
  rwset->Add(item);
  if(db_logger_){
    // printf("delete_ size:%d ,key:%lu\n",item.len, item.key);
    prepare_log(cor_id_, db_logger_, item);
  }
}

void DBTX::write() {
  assert(false);
  RWSet::RWSetItem &item = rwset->kvs[rwset->elems - 1];
  item.ro = false;
  if(db_logger_){
    // printf("write() size:%d\n",item.len);
    prepare_log(cor_id_, db_logger_, item);
  }
}

void DBTX::write(int tableid,uint64_t key,char *val,int len, uint32_t op) {

  // first search the TX's r/w set
  for (uint i = 0; i < rwset->elems; i++) {
    if (rwset->kvs[i].tableid == tableid && rwset->kvs[i].key == key) {
      RWSet::RWSetItem &item = rwset->kvs[i];
      item.ro = false;
      assert(op != 0 && op != LOG_OP_I && op != LOG_OP_D);
      item.op_bit = op;
      if(db_logger_){
        // printf("write(...) size:%d ,key:%lu\n",item.len, item.key);
        prepare_log(cor_id_, db_logger_, item);
      }
      return;
    }
  }

  fprintf(stderr,"@%d, local write operation not in the readset! tableid %d key %lu\n",
    thread_id,tableid,key);
  exit(-1);
}

void DBTX::local_ro_begin() {

  //if ( false == localinit) {
  //    readset = new ReadSet(thread_id);
  //    rwset = new RWSet();
  // local TX handler does not init remote-set
  //    localinit = true;
  //}
  ThreadLocalInit();
  readset->Reset();
  rwset->Reset();
  abort_ = false;
}

void DBTX::reset_temp() {
  readset->Reset();
  rwset->Reset();

}

void DBTX::begin(int tx_id, DBLogger *db_logger) {
  ThreadLocalInit();
  readset->Reset();
  rwset->Reset();
  remoteset->clear();
  //  delset->Reset();

  abort_ = false;

  db_logger_ = db_logger;
  if (db_logger_)
    db_logger->log_begin(cor_id_, tx_id, 0);

}

void DBTX::_begin(DBLogger *db_logger,TXProfile *p ) {

  ThreadLocalInit();
  readset->Reset();
  rwset->Reset();
  remoteset->clear();
  //  delset->Reset();

#ifdef DEBUG
  profile = p;
#endif
  abort_ = false;

  db_logger_ = db_logger;
  if (db_logger_)
    db_logger->log_begin(cor_id_, 0);
}

int DBTX::add_to_remote_set(int tableid, uint64_t key, int pid) {
  return remoteset->add(REQ_READ,pid,tableid,key);
}

int  DBTX::add_to_remote_set(int tableid, uint64_t *key, int klen, int pid) {
  return remoteset->add(REQ_READ,pid,tableid,key,klen);
}

int DBTX::remote_read_idx(int tableid, uint64_t *key, int klen, int pid) {
  return remoteset->add(REQ_READ_IDX,pid,tableid,key,klen);
}

int DBTX::remote_insert(int tableid,uint64_t *key, int klen, int pid) {
  return remoteset->add(REQ_INSERT,pid,tableid,key,klen);
}

int DBTX::remote_insert_idx(int tableid, uint64_t *key, int klen, int pid) {
  return remoteset->add(REQ_INSERT_IDX,pid,tableid,key,klen);
}

void DBTX::remote_write(int r_idx,char *val,int len,uint32_t op) {
  if(db_logger_){
    // printf("remote_write size: %d\n", len);
    RemoteSet::RemoteSetItem& item = remoteset->kvs_[r_idx];
    char* logger_val = db_logger_->get_log_entry(cor_id_, item.tableid, item.key, len, op, item.pid);
    memcpy(logger_val, val, len);
    db_logger_->close_entry(cor_id_, item.seq + 2);
  }
  remoteset->promote_to_write(r_idx,val,len);
}

void DBTX::do_remote_reads(yield_func_t &yield) {
  remoteset->do_reads(yield);
}

int  DBTX::do_remote_reads() {
  return remoteset->do_reads();
}

void DBTX::get_remote_results(int num_results) {
  remoteset->get_results(num_results);
  //remoteset->clear_for_reads();
}

bool DBTX::end_ro() {
#if OCC_RO_CHECK == 0
  return true;
#endif
  if(abort_)
    return false;
  rwset->SetDBTX(this);

#if 1
  if(!readset->Validate()) {
    goto ABORT;
  }
#endif
  //assert(rwset->elems != 0);
  if (!rwset->CheckLocalSet()) {
#ifdef DEBUG
    profile->abortLocalValidate += 1;
#endif
    goto ABORT;
  }
  return true;
 ABORT:
  return false;
}

bool DBTX::debug_validate() {
  return readset->Validate();
}

int DBTX::report_rw_set() {
  return rwset->elems;
}

#if 0
bool DBTX::end_fasst(yield_func_t &yield) {

  if(abort_) {
    assert(false);
    return false;
  }
  rwset->SetDBTX(this);

#if 1
  if(unlikely(!rwset->LockAllSet()))  {
    //assert(false);
#ifdef DEBUG
    profile->abortLocalLock += 1;
#endif
    goto ABORT;
  }
#endif

#if 1
  if(!readset->Validate()) {
    goto ABORT;
  }
#endif

#if 1
  if (!rwset->CheckLocalSet()) {
#ifdef DEBUG
    profile->abortLocalValidate += 1;
#endif
    goto ABORT;
  }
#endif

  if(db_logger_){
    db_logger_->log_backups(cor_id_, yield, timestamp);
    worker->indirect_must_yield(yield);
    db_logger_->log_end(cor_id_);
  }
  if(remoteset->elems_ > 0)
    remoteset->update_read_buf();

  remoteset->commit_remote();
  rwset->CommitLocalWrite();
  return true;

 ABORT:
  if(db_logger_){
    db_logger_->log_abort(cor_id_);
  }
  //remoteset->clear_for_reads();
  if(remoteset->elems_ > 0)
    remoteset->update_read_buf();

  remoteset->release_remote();
  rwset->ReleaseAllSet();
  return false;
}
#endif

bool
DBTX::end(yield_func_t &yield) {
  //assert(false);
  uint64_t epoch_tx = 0;
  if(abort_) {
    assert(false);
    return false;
  }
  remoteset->need_validate_ = true; // let remoteset to send the validate
  rwset->SetDBTX(this);

  if(!remoteset->lock_remote(yield)) {
    goto ABORT;
  }
#if 1
  if(unlikely(!rwset->LockAllSet()))  {
    //assert(false);
#ifdef DEBUG
    profile->abortLocalLock += 1;
#endif
    goto ABORT;
  }
#endif

  // gossip: collect epoch from remote machines
  // then, sync the latest epoch to them
  if (config.getEpochType() == 4) {
    epoch_tx = BenchWorker::get_txn_epoch();
#if 1
    uint64_t old_epoch = epoch_tx;
    for (int server : remoteset->server_set_) {
      uint64_t ts = db_tsm_->get_epoch(server);
      if (ts > epoch_tx) epoch_tx = ts;
      ++gossip_cnt_;
    }

    // for debug
    // if (epoch_tx - old_epoch > 1) {
    //   printf("new %lu, old %lu, diff %lu\n", epoch_tx, old_epoch, epoch_tx - old_epoch);
    // }
    assert(epoch_tx - old_epoch <= 1);
   
    for (int server : remoteset->server_set_) {
      uint64_t ts = db_tsm_->get_epoch_buffer(server);
      if (ts != epoch_tx) {
        db_tsm_->sync_epoch(server, epoch_tx);
        ++gossip_cnt_;
      }
    }
#endif
  }

#ifdef OCC_RETRY
  if(!remoteset->validate_remote(yield)) {
    goto ABORT;
  }
#else
  remoteset->clear_for_reads();
#endif
  //    fprintf(stdout,"remote set validate pass\n");
#if 0
  if(!readset->Validate()) {
    goto ABORT;
  }
#endif

#if 1
  if (!rwset->CheckLocalSet()) {
#ifdef DEBUG
    profile->abortLocalValidate += 1;
#endif
    goto ABORT;
  }
#endif

  if (db_logger_) {
    uint64_t commit_ts = 0;
    switch (config.getEpochType()) {
     case 0:
      break;
     case 1:
      assert(db_tsm_);
      commit_ts = db_tsm_->get_global_ts();
      break;
     case 2:
      assert(db_tsm_);
      commit_ts = db_tsm_->get_vec_ts(); 
      break;
     case 3:
      assert(db_tsm_);
      commit_ts = db_tsm_->get_local_ts();
      break;
     case 4:
      // commit_ts = BenchWorker::get_txn_epoch();
      commit_ts = epoch_tx;
      break;
     default:
      assert(false);
    }

#if 1
    int num_logs = db_logger_->log_backups(cor_id_, yield, timestamp, commit_ts);
    if (num_logs != 0)
      worker->indirect_must_yield(yield);
    db_logger_->log_end(cor_id_);
#else
    // for debug, not send log, directly close log
    // worker->indirect_must_yield(yield);
    db_logger_->log_abort(cor_id_);
#endif
  }
  
  remoteset->commit_remote();
  rwset->CommitLocalWrite();
  return true;

 ABORT:
  //  assert(false);
  //  fprintf(stdout,"abort @%d\n",thread_id);
  if(db_logger_){
    db_logger_->log_abort(cor_id_);
  }
  remoteset->clear_for_reads();
  remoteset->release_remote();
  rwset->ReleaseAllSet();
  //rwset->GC();
  return false;
}

bool
DBTX::stream_end(yield_func_t &yield) {
  //assert(false);
  uint64_t epoch_tx = 0;
  if(abort_) {
    assert(false);
    return false;
  }
  // gossip: collect epoch from remote machines
  // then, sync the latest epoch to them
  if (config.getEpochType() == 4) {
    epoch_tx = BenchWorker::get_txn_epoch();
    uint64_t old_epoch = epoch_tx;
#if 0
    for (int server : remoteset->server_set_) {
      uint64_t ts = db_tsm_->get_epoch(server);
      if (ts > epoch_tx) epoch_tx = ts;
      ++gossip_cnt_;
    }
#endif
    assert(epoch_tx - old_epoch <= 1);
  
#if 0 
    for (int server : remoteset->server_set_) {
      uint64_t ts = db_tsm_->get_epoch_buffer(server);
      if (ts != epoch_tx) {
        db_tsm_->sync_epoch(server, epoch_tx);
        ++gossip_cnt_;
      }
    }
#endif
  }

  if (db_logger_) {
    uint64_t commit_ts = 0;
    switch (config.getEpochType()) {
     case 0:
      break;
     case 1:
      assert(db_tsm_);
      commit_ts = db_tsm_->get_global_ts();
      break;
     case 2:
      assert(db_tsm_);
      commit_ts = db_tsm_->get_vec_ts(); 
      break;
     case 3:
      assert(db_tsm_);
      commit_ts = db_tsm_->get_local_ts();
      break;
     case 4:
      // commit_ts = BenchWorker::get_txn_epoch();
      commit_ts = epoch_tx;
      break;
     default:
      assert(false);
    }

#if 1
    int num_logs = db_logger_->log_backups(cor_id_, yield, timestamp, commit_ts);
    if (num_logs != 0)
      worker->indirect_must_yield(yield);
    db_logger_->log_end(cor_id_);
#else
    // for debug, not send log, directly close log
    // worker->indirect_must_yield(yield);
    db_logger_->log_abort(cor_id_);
#endif
  }
  
  return true;
}

void DBTX::abort() {
  if(db_logger_){
    db_logger_->log_abort(cor_id_);
  }
}

DBTXIterator::DBTXIterator(DBTX* tx, int tableid,bool sec)
{
  tx_ = tx;
  if(sec) {
    iter_ = (tx_->txdb_->_indexs[tableid])->GetIterator();
  } else {
    iter_ = (tx->txdb_->stores_[tableid])->GetIterator();
  }
  cur_ = NULL;
  prev_link = NULL;
}


bool DBTXIterator::Valid()
{
  return cur_ != NULL && cur_->value != NULL;
}


uint64_t DBTXIterator::Key()
{
  return iter_->Key();
}

char* DBTXIterator::Value()
{
  return (char *)val_;
}

char* DBTXIterator::Node() {
  return (char *)cur_;
}

void DBTXIterator::Next()
{
  bool r = iter_->Next();

  while(iter_->Valid()) {

    cur_ = iter_->CurNode();
    {

      RTMScope rtm(NULL);
      val_ = cur_->value;

      if(prev_link != iter_->GetLink()) {
	tx_->readset->AddNext(iter_->GetLink(), iter_->GetLinkTarget());
	prev_link = iter_->GetLink();
      }
      tx_->readset->Add(&cur_->seq);

      if(DBTX::ValidateValue(val_)) {
      	//	fprintf(stdout,"normal case return\n");
	      return;
      }

    }
    iter_->Next();
  }
  cur_ = NULL;
}

void DBTXIterator::Prev()
{
  bool b = iter_->Prev();
  if (!b) {
    //    tx_->abort = true;
    cur_ = NULL;
    return;
  }

  while(iter_->Valid()) {

    cur_ = iter_->CurNode();
    {

      RTMScope rtm(NULL);
      val_ = cur_->value;

      tx_->readset->AddNext(iter_->GetLink(), iter_->GetLinkTarget());
      tx_->readset->Add(&cur_->seq);

      if(DBTX::ValidateValue(val_)) {
	      return;
      }
    }
    iter_->Prev();
  }
  cur_ = NULL;
}

void DBTXIterator::Seek(uint64_t key)
{
  //Should seek from the previous node and put it into the readset
  iter_->Seek(key);

  //No keys is equal or larger than key
  if(!iter_->Valid()) {
#if 1
    return;
#else
    assert(cur_ == NULL);
    RTMScope rtm(NULL);

    //put the previous node's next field into the readset

    printf("Not Valid!\n");
    //sleep(20);
    //assert(false);
    tx_->readset->AddNext(iter_->GetLink(), iter_->GetLinkTarget());
    return;
#endif
  }
  
  cur_ = iter_->CurNode();

  //Second, find the first key which value is not NULL
  while(iter_->Valid()) {
    // cur_ = iter_->CurNode();

    {
      RTMScope rtm(NULL);
      //Avoid concurrently insertion
      tx_->readset->AddNext(iter_->GetLink(), iter_->GetLinkTarget());

      //	  printf("end\n");
      //Avoid concurrently modification
      //      tx_->readset->Add(&cur_->seq);
      val_ = cur_->value;

      if(DBTX::ValidateValue(val_)) {
        return;
      }
      //assert(false);
    }

    iter_->Next();
    cur_ = iter_->CurNode();
  }
  cur_ = NULL;
}

// Position at the first entry in list.
// Final state of iterator is Valid() iff list is not empty.
void DBTXIterator::SeekToFirst()
{
  //Put the head into the read set first

  iter_->SeekToFirst();

  assert(iter_->Valid());

#if 0
  if(!iter_->Valid()) {
    assert(cur_ == NULL);
    RTMScope rtm(NULL);

    //put the previous node's next field into the readset

    tx_->readset->AddNext(iter_->GetLink(), iter_->GetLinkTarget());


    return;
  }
#endif
  
  cur_ = iter_->CurNode();

  while(iter_->Valid()) {
    {

      RTMScope rtm(NULL);
      val_ = cur_->value;

      tx_->readset->AddNext(iter_->GetLink(), iter_->GetLinkTarget());
      tx_->readset->Add(&cur_->seq);

      if(DBTX::ValidateValue(val_)) {
	return;
      }

    }

    iter_->Next();
    cur_ = iter_->CurNode();
  }
  cur_ = NULL;
}

// Position at the last entry in list.
// Final state of iterator is Valid() iff list is not empty.
void DBTXIterator::SeekToLast()
{
  assert(0);
}


DBTXTempIterator::DBTXTempIterator (DBTX *tx,int tableid,bool sec) {
  tx_ = tx;
  if(sec) {
    iter_ = (tx_->txdb_->_indexs[tableid])->GetIterator();
  } else
    iter_ = (tx_->txdb_->stores_[tableid])->GetIterator();
  cur_ = NULL;
  prev_link = NULL;
}


bool DBTXTempIterator::Valid() {
  /* Read will directly read from the latest one */
  //  if(cur_ != NULL)
  //    assert(cur_->seq != 0);
  return cur_ != NULL && val_ != NULL;
}

uint64_t DBTXTempIterator::Key()
{
  return iter_->Key();
}

char *DBTXTempIterator::Value() {
  return (char *)val_ ;
}

char *DBTXTempIterator::Node() {
  return (char *)cur_;
}

void DBTXTempIterator::Next() {
  bool r = iter_->Next();
  //    assert(r);
  while(iter_->Valid()) {
    cur_ = iter_->CurNode();
    {
      RTMScope rtm(NULL);
      val_ = cur_->value;
      if(prev_link != iter_->GetLink() ) {
	prev_link = iter_->GetLink();
      }
      if(ValidateValue(val_) )
	return;
    }
    iter_->Next();
  }
  cur_ = NULL;
}

void DBTXTempIterator::Prev() {

  bool b = iter_->Prev();
  if(!b) {
    //      tx_->abort = true;
    cur_ = NULL;
    return;
  }

  while(iter_->Valid()) {
    cur_ = iter_->CurNode();
    {
      RTMScope rtm(NULL);
      val_ = cur_->value;
      if(ValidateValue(val_))
	return;
    }
    iter_->Prev();
  }
  cur_ = NULL;
}

void DBTXTempIterator::Seek(uint64_t key) {

  iter_->Seek(key);

  if(!iter_->Valid()) {
    cur_ == NULL;
    //    fprintf(stderr,"seek fail..\n");
    return ;
  }
  
  cur_ = iter_->CurNode();

  while (iter_->Valid()) {
    {
      RTMScope rtm(NULL) ;
      val_ = cur_->value;
      if(ValidateValue(val_)) {
	//	fprintf(stdout,"one time succeed\n");
#if 0
	if(!Valid()) {
	  fprintf(stderr,"one time error!\n");
	  exit(-1);
	}
#endif
	return;
      }
    }
    iter_->Next();
    cur_ = iter_->CurNode();

  }

  cur_ = NULL;
}

void DBTXTempIterator::SeekToFirst() {
  /* TODO ,not implemented. seems not needed */
}

void DBTXTempIterator::SeekToLast() {
  /* TODO ,not implemented */
}

}
}

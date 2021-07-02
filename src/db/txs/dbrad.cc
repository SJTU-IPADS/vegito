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

#include <time.h>
#include <sys/time.h>

#include "config.h"

#include "dbrad.h"
#include "framework/bench_worker.h"

#include "rdmaio.h"
#include "ralloc.h"

#include "util/mapped_log.h"

#define unlikely(x) __builtin_expect(!!(x), 0)
#define likely(x)   __builtin_expect(!!(x), 1)

#define MAX(x,y) (  (x) > (y) ? (x) : (y))

/* Max item a write set shall have */
#define MAX_SIZE 1024

using namespace nocc::oltp;

extern __thread MappedLog local_log;


//__thread DBRad::WriteSet* DBRad::rwset = NULL;
//__thread RemoteSet   *DBRad::remoteset = NULL;
//__thread bool DBRad::localinit = false;

/* partition id */
extern size_t current_partition ;
/* total number of partition */
extern size_t total_partition;

#define THREAD_ID_OFFSET (54)
#define TID_TS_OFFSET (0)
#define SECOND_OFFSET (22)
#define SECOND_MASK (0xffffffff)

/* The counter is carefully choosn so that increasing it will not overlap second */
#define COUNTER_MASK (0x3fffff)
#define THREAD_ID_MASK (0x3ff)
#define TIMESTAMP_MASK (0x3fffffffffffff)

#define GEN_TIME(sec,counter) (  ((sec) << SECOND_OFFSET) | (counter) )
#define GEN_TID(tid,time) ( ((tid << THREAD_ID_OFFSET ) | (time) ) )
#define GET_TIMESTAMP(tid) ( (tid) & TIMESTAMP_MASK)
#define GET_TID(tid) ( (tid) >> THREAD_ID_OFFSET)
#define GET_SEC(tid) ( ((tid) & TIMESTAMP_MASK) >> SECOND_OFFSET)


  inline void __lock_ts(volatile uint64_t *lock) {
//  fprintf(stdout,"check read lock val %lu %p\n",*lock,lock);
    //  assert(__sync_bool_compare_and_swap(lock,0,1) == true);
    //  return;
    for(;;) {
      if( unlikely( ( (*lock) != 0) ||
                    !__sync_bool_compare_and_swap(lock,0,1) )
          ) {
        //      fprintf(stdout,"failed ,lock %lu\n",*lock);
        //      sleep(1);
        continue;
      } else
        return;
    }
          }

          inline void __release_ts(volatile uint64_t *lock) {
            barrier();
            *lock = 0;
          }

          namespace nocc {

            extern __thread BenchWorker* worker;
            extern __thread coroutine_func_t *routines_;
            extern __thread TXProfile *profile;

            namespace db {

              std::mutex mtx;
              volatile __thread bool rpc_registered = false;

              // extern __thread uint *next_coro_id_arr_;

              void DBRad::GlobalInit() {

              }

              int radGetMetalen() {
                return RAD_META_LEN;
              }

              /* Write set's definiation */
              class DBRad::WriteSet {

              public:
                struct  WriteSetItem {
                  int32_t tableid;
                  uint64_t key;
                  int len ;
                  MemNode  *node;
                  uint64_t *addr;  /* Pointer to the new value buffer */
                  uint64_t seq;
                  bool ro;
                } ;

                DBRad *tx_;
                int max_len;
                WriteSetItem *kvs;

                /* How many items in write set*/
                int elems;
                /* How many items which has been locked */
                int current;

                WriteSet();
                ~WriteSet();

                inline void Reset();
                inline void SetTX(DBRad *dbtx);
                inline void Add(WriteSetItem &item);

                /* commit helper functions */
                inline bool LockAllSet(uint64_t &);
                inline void ReleaseAllSet();

                inline int  CommitLocalWrite(uint64_t commit_ts);
                inline void RemoveDuplicate();

                inline void GC();

                bool inline IsLocked(uint64_t *ptr) {
                  return (*ptr) != 0;
                }
              };

              DBRad::WriteSet::WriteSet() {
                max_len = MAX_SIZE;
                elems = 0;
                kvs   = new WriteSetItem[max_len];
                tx_   = NULL;

                for(uint i = 0;i < max_len;++i) {
                  memset(&(kvs[i]),0,sizeof(WriteSetItem));
                }
              }

              DBRad::WriteSet :: ~WriteSet() {
                delete []kvs;
                kvs = NULL;
              }

              inline void DBRad::WriteSet::GC () {
                for(uint i = 0;i < elems;++i) {
                  if(kvs[i].addr != NULL)
                    free(kvs[i].addr);
                }
              }

              inline void DBRad::WriteSet::RemoveDuplicate () {
                //if (elems <= 1) return;

                std::sort(kvs, kvs + elems, [](const WriteSetItem &a, const WriteSetItem &b) -> bool {
                    return a.node < b.node;
                  });

                int nelems = 1;

                for (int i = 1; i < elems; i++) {
                  //      if (kvs[i-1].tableid != kvs[i].tableid || kvs[i-1].key != kvs[i].key) {
                  if(kvs[i-1].node != kvs[i].node) {
                    kvs[nelems] = kvs[i];
                    nelems++;
                  }
                }
                elems = nelems;
              }

              inline void DBRad::WriteSet::Add(WriteSetItem &item) {
                if(max_len < elems) printf("ELEMS %d MAX %d\n",elems, max_len);
                if(elems == max_len) assert(false);

                int cur = elems;
                elems ++;
                kvs[cur].tableid = item.tableid;
                kvs[cur].key = item.key;
                kvs[cur].len = item.len;
                kvs[cur].node = item.node;
                kvs[cur].addr = item.addr;
                kvs[cur].seq  = item.seq;
                kvs[cur].ro   = item.ro;
              }


              inline void DBRad::WriteSet::SetTX(DBRad *dbtx) {
                tx_ = dbtx;
              }

              inline void DBRad::WriteSet::Reset() {
                elems = 0;
                current = -1;
              }

              inline bool //__attribute__((optimize("O0")))
              DBRad::WriteSet::LockAllSet(uint64_t &tentative_timestamp) {

                uint64_t max_ts = tentative_timestamp;
                //    assert(elems <= 1);

                for(uint i = 0;i < elems;++i) {
                  /* Lock + check */
                  if(kvs[i].ro) {
                    continue;
                  }

                  volatile uint64_t *lockptr = &(kvs[i].node->lock);
                  //uint64_t lock_val = kvs[i].node->lock;
#if 1

                  if( unlikely( (*lockptr != 0) ||
                                !__sync_bool_compare_and_swap(lockptr,
                                                              0,ENCODE_LOCK_CONTENT(current_partition,tx_->thread_id,
                                                                                    tx_->cor_id_ + 1))))
                    {
                      //assert(*lockptr != ENCODE_LOCK_CONTENT(current_partition,tx_->thread_id,tx_->cor_id_ + 1));
                      //          char *log_buf = next_log_entry(&local_log,32);
                      //assert(log_buf != NULL);
                      //sprintf(log_buf,"aborted at %d %d\n",tx_->thread_id,tx_->cor_id_);
                      //assert(false);
                      return false;
                    } else {
                    //        char *log_buf = next_log_entry(&local_log,32);
                    //assert(log_buf != NULL);
                    //sprintf(log_buf,"success at %d %d\n",tx_->thread_id,tx_->cor_id_);
                  }
#endif
                  /* lock process to i */
                  current = i;
#if 1
                  if(kvs[i].node->seq != kvs[i].seq ) {
                    //  assert(false);
                    return false;
                  }
#endif
                  // Calculate timestamp
#ifdef RAD_TX
                  max_ts = MAX(kvs[i].node->read_ts,max_ts);
#endif
                }

                tentative_timestamp = max_ts + 1;
                return true;
              }

              inline void DBRad::WriteSet::ReleaseAllSet() {

                // This shall be int, since current is -1 if no lock succeed, uint will cause an overvlow
                for(int i = 0;i <= current;++i) {
                  if(kvs[i].ro)
                    continue;
#if 1
                  kvs[i].node->lock = 0;
#else
                  assert(kvs[i].node->lock == ENCODE_LOCK_CONTENT(current_partition,
                                                                  tx_->thread_id,tx_->cor_id_ + 1));
#endif
                }
              }


              inline int DBRad::WriteSet::CommitLocalWrite(uint64_t commit_ts) {

                for(uint i = 0;i < elems;++i) {
                  if(kvs[i].ro) {
                    continue;
                  }

                  // Forbiding concurrent read access
                  kvs[i].node->seq = 1;
                  asm volatile("" ::: "memory");
#if 1
                  uint64_t *cur    = kvs[i].node->value;
                  /* Using value switch */
                  uint64_t *oldptr = kvs[i].node->old_value;

                  if(likely(cur != NULL)) {
                    _RadValHeader * hptr = (_RadValHeader *)cur;
                    hptr->oldValue = oldptr;
                    hptr->version  = kvs[i].seq;
                  } else {

                  }
                  // TODO, may set the version
                  kvs[i].node->old_value = cur;
                  kvs[i].node->value = kvs[i].addr;
                  // TODO, shall be set to commit version
#else
                  memcpy( (char *)cur + RAD_META_LEN, (char *)(kvs[i].addr) + RAD_META_LEN, kvs[i].len);
#endif
                  asm volatile("" ::: "memory");
                  //kvs[i].node->seq = kvs[i].seq + 2;
                  kvs[i].node->seq = commit_ts;
                  //      kvs[i].node->seq = kvs[i].seq + 2;
                  assert(commit_ts > kvs[i].seq);
                  asm volatile("" ::: "memory");
                  //
                  // Release the lock
                  kvs[i].node->lock = 0;
                }
                return 0;
              }


              DBRad::DBRad(MemDB *tables,int t_id,Rpc *rpc,int c_id)
                : txdb_(tables), thread_id(t_id),rpc_handler_(rpc),
                  abort_(false),
                  localinit(false),
                  TXHandler(c_id)
              {
                /* register rpc handlers */
                using namespace std::placeholders;

                if(rpc != NULL) {

#if 0
                  for(uint i = 0;i < 16;++i) {
                    last_rpc_mark_[i] = RPC_COMMIT;
                    last_msg_counts_[i] = 0;
                  }
#endif
                  mtx.lock();
                  if(!rpc_registered) {
                    fprintf(stdout,"[DBRAD] register callbacks @%ld\n",thread_id);
                    // avoids multiple binds
#if FASST == 0
                    rpc_handler_->register_callback(std::bind(&DBRad::get_rpc_handler,this,_1,_2,_3,_4),RPC_READ);
#else
                    rpc_handler_->register_callback(std::bind(&DBRad::fast_get_rpc_handler,this,_1,_2,_3,_4),
                                                    RPC_READ);
                    rpc_handler_->register_callback(std::bind(&DBRad::fast_validate_rpc_handler,
                                                              this,_1,_2,_3,_4),RPC_VALIDATE);
#endif
                    rpc_handler_->register_callback(std::bind(&DBRad::lock_rpc_handler,this,_1,_2,_3,_4),RPC_LOCK);
                    rpc_handler_->register_callback(std::bind(&DBRad::release_rpc_handler,this,_1,_2,_3,_4),RPC_RELEASE);
                    rpc_handler_->register_callback(std::bind(&DBRad::commit_rpc_handler2,this,_1,_2,_3,_4),RPC_COMMIT);
                     asm volatile("" ::: "memory");
                    rpc_registered = true;
                  }
                  mtx.unlock();

                } else {
                  // rpc not registered!
                  assert(false);
                }
                assert(rpc_registered == true);
                TXHandler::nreadro_locked = 0;
              }

              inline uint64_t
              DBRad::_get_ro_versioned_helper(int tableid, uint64_t key, char *val, uint64_t version,yield_func_t &yield) {

                int vlen = txdb_->_schemas[tableid].vlen;
                MemNode *node = txdb_->stores_[tableid]->GetWithInsert(key);
                uint64_t ret = 0;

              read_retry:
#if 0
                node->read_ts = MAX(version,node->read_ts);
                //    asm volatile("" ::: "memory");
                //    if(unlikely(node->read_ts < version))
                //      goto read_retry;
#else
                //    fprintf(stdout,"sizeof %lu\n",sizeof(struct MemNode));
                //    assert(sizeof(struct MemNode) == CACHE_LINE_SZ * 2);
                uint64_t origin = 0;
#ifdef RAD_TX
                origin = node->read_ts;
#endif
                uint64_t tentative_timestamp = MAX(version,origin);
                //    fprintf(stdout,"origin val %lu => %lu\n",origin,tentative_timestamp);
#if 0
                uint64_t res = __sync_val_compare_and_swap((uint64_t *)(&(node->read_ts)),origin,
                                                           tentative_timestamp);
                if(res != origin && res < version)
                  goto read_retry;
#else
                //    fprintf(stdout,"origin lock %lu\n",node->read_lock);
                //    Acquire((__rtm_spin_lock_t *) (&(node->read_lock)));
                //    fprintf(stdout,"now lock %lu\n",node->read_lock);
                if(1)
                  {
#ifdef RAD_TX
                    __lock_ts(&(node->read_lock));
                    node->read_ts = tentative_timestamp;
                    __release_ts(&(node->read_lock));
#endif
                  }
                //    Xrelease((__rtm_spin_lock_t *)(&(node->read_lock)));

                //    sleep(1);
#endif
                //    asm volatile("" ::: "memory");
                //    fprintf(stdout,"now val %lu\n",node->read_ts);
                //    exit(-1);
#endif

              lock_retry:
                /* check the locks */
                uint64_t seq = node->seq;
                //           worker->yield_next(yield); // dummy skip
                //fprintf(stdout,"yield back\n");
#if 1
                asm volatile("" ::: "memory");
                if(unlikely(node->lock != 0 && seq < version)) {
                  /* maybe need refinements, or blocking */
                  /* wait for one-time lock to be released */
                  {
                    nreadro_locked += 1;
                    uint64_t retry_counter = 0;
                    while(true) {
                      /* being locked status */
                      // yield(routines_[next_coro_id_arr_[this->cor_id_]]);
                      //yield(routines_[MASTER_ROUTINE_ID]);
                      worker->yield_next(yield);
                      asm volatile("" ::: "memory");
                      uint64_t n_seq = node->seq;
                      uint64_t lock  = node->lock;
                      //	  asm volatile("" ::: "memory");
                      if(n_seq != seq || lock == 0) {
                        break;
                      }
#if 0
                      retry_counter += 1;
                      if(retry_counter > 9999999) {
                        fprintf(stdout,"tableid %d lock %lu, seq %lu, old %lu, node %p, stuch @%d\n",tableid,
                                lock,node->seq,seq,node,thread_id);
                        fprintf(stdout,"it is locked by mac %d, thread %d, my %d\n",_QP_DECODE_MAC(node->lock),
                                _QP_DECODE_INDEX(node->lock) - 1,thread_id);
                        assert(false);
                      }
#endif
                    }
                    /* end wait process */
                  }
                }
#endif
              retry:
                seq = node->seq;
                //    fprintf(stdout,"read node seq %lu, key %lu\n",seq,key);
                /* traverse the read linked list to find the correspond records */

                if(seq <= version ) {
                  /* simple case, read the current value */
                  asm volatile("" ::: "memory");
                  uint64_t *tmpVal = node->value;

                  if(likely(tmpVal != NULL)) {
                    memcpy(val, (char *)tmpVal + RAD_META_LEN,vlen);
                    asm volatile("" ::: "memory");
                    if(node->seq != seq || seq == 1)
                      goto retry;
                    ret = seq;
                  } else {
                    /* read a deleted value, currently not supported */
                    //assert(false);
                    return 0;
                  }
                } else {
                  /* traverse the old reader's list */
                  // if( seq - version <= 500 ) worker->ntxn_strict_counts_ += 1;
                  /* this is the simple case, and can always success  */
                  char *old_val = (char *)(node->old_value);
                  asm volatile("" ::: "memory");
                  if(seq == 1 || node->seq != seq) goto retry;
                  _RadValHeader *rh = (_RadValHeader *)old_val;
                  while(old_val != NULL && rh->version > version) {
                    old_val = (char *)(rh->oldValue);
                    rh = (_RadValHeader *)old_val;
                  }
                  if(unlikely(old_val == NULL)) {
                    /* cannot find one */
                    //	if(tableid == 17) {
                    //	fprintf(stdout,"seq %lu, version %lu node seq %lu, tab %d\n",seq,version,node->seq,
                    //		tableid);
                    //	fprintf(stdout,"tableid %d, seq %lu  %lu\n",tableid,node->seq,version);
                    //assert(false);
                    //	}
                    return 0;
                  }

                  /* cpy */
                  memcpy(val,(char *)old_val + RAD_META_LEN,vlen);
                  assert(rh->version != 0 && rh->version != 1);
                  ret = rh->version;
                  /* in this case, we do not need to check the lock */
                }
                /* */
                //    assert(ret != 1);
                return ret;
              }

              uint64_t DBRad::get_cached(int tableid,uint64_t key,char **val) {
                for(uint i = 0;i < remoteset->elems_;++i) {
                  if(remoteset->kvs_[i].tableid == tableid && remoteset->kvs_[i].key == key) {
                    *val = (char *)(remoteset->kvs_[i].val);
                    return remoteset->kvs_[i].seq;
                  }
                }
                return 0;
              }

              uint64_t DBRad::get_cached(int idx,char **val) {
                assert(idx < remoteset->elems_);
                *val = (char *)(remoteset->kvs_[idx].val);
                return remoteset->kvs_[idx].seq;
              }

              uint64_t DBRad::get_ro(int tableid,uint64_t key,char *val,yield_func_t &yield) {
                return _get_ro_versioned_helper(tableid,key,val,timestamp,yield);
              }

              uint64_t DBRad::get_ro_versioned(int tableid, uint64_t key, char *val, uint64_t version,yield_func_t &yield) {
                return _get_ro_versioned_helper(tableid,key,val,version,yield);
              }

              uint64_t DBRad::get(int tableid, uint64_t key, char **val,int len) {

                int vlen = len + RAD_META_LEN;
                vlen = vlen + 64 - 64 % vlen;
                char *_addr = (char *)malloc(len + RAD_META_LEN);

                MemNode *node = NULL;
                node = txdb_->stores_[tableid]->GetWithInsert(key);
              retry:
                uint64_t seq = node->seq;
                asm volatile("" ::: "memory");
                uint64_t *tmpVal = node->value;

                if(tmpVal != NULL) {
                  memcpy(_addr + RAD_META_LEN, (char *)tmpVal + RAD_META_LEN , len);
                  asm volatile("" ::: "memory");
                  if(seq == 1 || node->seq != seq)
                    goto retry;

                } else {
                  /* An invalid value which means that read does not success */
                  //fprintf(stdout,"get key %lu for tabl %d\n",key,tableid);
                  //assert(false);

                  // possible in TPC-E
                  return 1;
                }

                WriteSet::WriteSetItem item;
                item.tableid = tableid;
                item.key = key;
                item.len = len;
                item.node = node;
                item.addr = (uint64_t *)_addr;
                item.seq = seq;
                item.ro  = true;
                *val = ( (char *)_addr + RAD_META_LEN);
                timestamp = MAX(item.seq,timestamp);
                rwset->Add(item);
                return seq;
              }

              inline void prepare_log(int cor_id, DBLogger* db_logger, const DBRad::WriteSet::WriteSetItem& item){
                  char* val = db_logger->get_log_entry(cor_id, item.tableid, item.key, item.len);
                  // printf("%p %p %d %lu\n", val, (char*)item.addr + RAD_META_LEN, item.len, item.key);
                  memcpy(val, (char*)item.addr + RAD_META_LEN, item.len);
                  db_logger->close_entry(cor_id); 
              } 

              void DBRad::insert_index(int tableid, uint64_t key, char *val) {

                MemNode *node = txdb_->_indexs[tableid]->GetWithInsert(key);
                //    fprintf(stdout,"here insert %p\n",node);
                WriteSet::WriteSetItem item;

                item.tableid = tableid;
                item.key     = key;
                item.node    = node;
                item.addr    = (uint64_t *)val;
                item.seq     = node->seq;
                item.ro      = false;
                rwset->Add(item);
              }

              void DBRad::insert(int tableid, uint64_t key, char *val, int len) {

                int vlen = RAD_META_LEN + len;
                MemNode *node = txdb_->stores_[tableid]->GetWithInsert(key);
                //    fprintf(stdout,"insert seq %lu\n",node->seq);
                //assert(node->value == NULL && node->seq == 0);

                WriteSet::WriteSetItem item;

                item.tableid = tableid;
                item.key     = key;
                item.len     = len;
                item.node    = node;
                item.addr    = (uint64_t *)(new char[vlen]);
                memcpy( (char *)item.addr + RAD_META_LEN, val,len);
                item.seq     = node->seq;
                item.ro      = false;
                rwset->Add(item);
                if(db_logger_){
                  // printf("insert size:%d ,key:%lu\n",item.len, item.key);
                  prepare_log(cor_id_, db_logger_, item);
                }
              }

              void DBRad::delete_index(int tableid,uint64_t key) {
                MemNode *node = txdb_->_indexs[tableid]->GetWithInsert(key);
                if(unlikely(node->value == NULL)) {
                  //fprintf(stdout,"index %d null, check symbol %s using %p\n",
                  //	      tableid,(char *)( &(((uint64_t *)key)[2])),node);
                  //      assert(false);
                  // possible found by the workload
                  return;
                }
                WriteSet::WriteSetItem item;
                item.tableid = tableid;
                item.key     = key;
                item.node    = node;
                item.addr    = NULL;
                item.seq     = node->seq;
                item.ro      = false;
                rwset->Add(item);
                timestamp = MAX(item.seq,timestamp);
              }

              void DBRad::delete_by_node(int tableid, char *node) {

                for(uint i = 0;i < rwset->elems;++i) {
                  if((char *)(rwset->kvs[i].node) == node) {
                    WriteSet::WriteSetItem &item = rwset->kvs[i];
                    item.ro = false;
                    delete item.addr;
                    item.addr = NULL;
                    item.len = 0;
                    timestamp = MAX(timestamp,item.seq);
                    if(db_logger_){
                      // printf("delete_by_node size:%d ,key:%lu\n",item.len, item.key);
                      prepare_log(cor_id_, db_logger_, item);
                    }
                    return;
                  }

                }
                WriteSet::WriteSetItem item;

                item.tableid = tableid;
                item.key     = 0;
                item.len     = 0;
                item.node    = (MemNode *)node;
                item.addr    = NULL; // NUll means logical delete
                item.seq     = item.node->seq;
                item.ro      = false;
                rwset->Add(item);
                timestamp = MAX(timestamp,item.seq);
                if(db_logger_){
                  // printf("delete_by_node size:%d ,key:%lu\n",item.len, item.key);
                  prepare_log(cor_id_, db_logger_, item);
                }
              }

              void DBRad::delete_(int tableid, uint64_t key) {

                //    assert(false);
                for(uint i = 0;i < rwset->elems;++i) {
                  if(tableid == rwset->kvs[i].tableid &&
                     rwset->kvs[i].key == key) {
                    WriteSet::WriteSetItem &item = rwset->kvs[i];
                    item.ro = false;
                    delete item.addr;
                    item.addr = NULL;
                    item.len = 0;
                    timestamp = MAX(timestamp,item.seq);
                    if(db_logger_){
                      // printf("delete_ size:%d\n",item.len);
                      prepare_log(cor_id_, db_logger_, item);
                    }
                    return;
                  }
                }

                MemNode *node = txdb_->stores_[tableid]->GetWithInsert(key);
                WriteSet::WriteSetItem item;

                item.tableid = tableid;
                item.key     = key;
                item.len     = 0;
                item.node    = node;
                item.addr    = NULL; // NUll means logical delete
                item.seq     = node->seq;
                item.ro      = false;
                timestamp = MAX(timestamp,item.seq);
                rwset->Add(item);
                if(db_logger_){
                  // printf("delete_ size:%d\n",item.len);
                  prepare_log(cor_id_, db_logger_, item);
                }
              }

              void DBRad::write() {
                WriteSet::WriteSetItem& item = rwset->kvs[rwset->elems - 1];
                item.ro = false;
                timestamp = MAX(timestamp,item.seq);
                if(db_logger_){
                  // printf("write() size:%d\n",item.len);
                  prepare_log(cor_id_, db_logger_, item);
                }
              }

              void DBRad::write(int tableid,uint64_t key,char *val,int len) {
                for (uint i = 0; i < rwset->elems; i++) {
                  if (rwset->kvs[i].tableid == tableid && rwset->kvs[i].key == key) {
                    WriteSet::WriteSetItem& item = rwset->kvs[i];
                    item.ro = false;
                    timestamp = MAX(timestamp,item.seq);
                    if(db_logger_){
                      // printf("write(...) size:%d ,key:%lu\n",item.len, item.key);
                      prepare_log(cor_id_, db_logger_, item);
                    }
                    return;
                  }
                }
                fprintf(stderr,"@%lu, local write operation not in the readset! tableid %d key %lu, not supported \n",
                        thread_id,tableid,key);
                exit(-1);
              }

              void DBRad::ThreadLocalInit() {

                if (false == localinit) {
                  rwset = new WriteSet();
                  remoteset = new RemoteSet(rpc_handler_,cor_id_,thread_id);
                  localinit = true;
                }
              }

              uint64_t DBRad::_getTxId() {
                return 0;
              }

              uint64_t DBRad::get_timestamp() {
                struct timeval tv;
                gettimeofday(&tv,NULL);
                timestamp = tv.tv_sec * 1000000 + tv.tv_usec;
                return timestamp;
              }

              void DBRad::reset() {
                ThreadLocalInit();
                rwset->Reset();
              }

              void DBRad::local_ro_begin() {
                ThreadLocalInit();
                rwset->Reset();
                remoteset->clear();
                abort_ = false;
                //  timestamp = _getTxId();
                timestamp = get_timestamp();
              }

              void DBRad::_begin(DBLogger *db_logger, TXProfile *p ) {

                ThreadLocalInit();
                rwset->Reset();
                remoteset->clear();
                abort_ = false;
                //  timestamp = _getTxId();
                timestamp = get_timestamp();
                db_logger_ = db_logger;
                // printf("begin!!!!\n");
                if(db_logger_)db_logger->log_begin(cor_id_, 1);
#if 0
                remoteset->add_meta(sizeof(uint64_t) * total_partition);
#endif
              }

              bool DBRad::check_in_rwset(int tableid, uint64_t node) {
                for(uint i = 0;i < rwset->elems;++i) {
                  if(rwset->kvs[i].tableid == tableid
                     && ((char *)(rwset->kvs[i].node) == (char *)node)) {
                    return true;
                  }
                }
                return false;
              }


              bool DBRad::end_fasst(yield_func_t &yield) {

                rwset->SetTX(this);

                if(unlikely(!remoteset->validate_remote(yield))) {
                  goto ABORT;
                }
                if(unlikely(!rwset->LockAllSet(timestamp) ) ) {
                  //      assert(false);
                  goto ABORT;
                }

                // calculate the maxium remote seq
                //for(uint i = 0;i < remoteset->elems_;++i) {
                //      timestamp = MAX(timestamp,remoteset->kvs_[i].seq);
                //}

                //timestamp += 1;
                //    fprintf(stdout,"commit all start\n");
                if(db_logger_){
                  db_logger_->log_backups(cor_id_, yield, timestamp);
                  worker->indirect_must_yield(yield);
                  db_logger_->log_end(cor_id_);
                }
                rwset->CommitLocalWrite(timestamp);
                remoteset->max_time_ = timestamp;
                remoteset->commit_remote();
                return true;
              ABORT:
                //assert(false);
                remoteset->release_remote();
                if(db_logger_){
                  db_logger_->log_abort(cor_id_);
                }
                //rwset->ReleaseAllSet();
                //rwset->GC();
                return false;
              }

              bool
              DBRad::end(yield_func_t &yield) {

                if(abort_) {
                  assert(false);
                  return false;
                }

                rwset->SetTX(this);
                //    fprintf(stdout,"lock all start\n");

                // lock remote sets
                if(!remoteset->lock_remote(yield)) {
                  goto ABORT;
                }
                // record the max remote timestamp
                //timestamp = MAX(timestamp,remoteset->max_time_);
                //remoteset->need_validate_ = true;
                //remoteset->validate_all(yield);
                //    rwset->RemoveDuplicate();
                if(!rwset->LockAllSet(timestamp) ) {
                  //assert(remoteset->elems_ == 0);
                  goto ABORT;
                }

                // calculate the maximum remote seq
#if 1
                for(uint i = 0;i < remoteset->elems_;++i) {
                  timestamp = MAX(timestamp,remoteset->kvs_[i].seq);
                }
#endif

                //timestamp += 1;
                if(db_logger_) {
                  db_logger_->log_backups(cor_id_, yield, timestamp);
                  worker->indirect_must_yield(yield);
                  db_logger_->log_end(cor_id_);
                }
                rwset->CommitLocalWrite(timestamp);
                remoteset->max_time_ = timestamp;
                remoteset->commit_remote();
                //rwset->GC();
                return true;
              ABORT:
                remoteset->release_remote();
                rwset->ReleaseAllSet();
                if(db_logger_){
                  db_logger_->log_abort(cor_id_);
                }
                //rwset->GC();
                //profile->abortTotal += 1;
                return false;
              }

              void
              DBRad::abort() {
                if(db_logger_){
                  db_logger_->log_abort(cor_id_);
                }
              }

              RadIterator::RadIterator (DBRad *tx,int tableid,bool sec) {
                tx_ = tx;
                if(sec) {
                  iter_ = (tx_->txdb_->_indexs[tableid])->GetIterator();
                } else
                  iter_ = (tx_->txdb_->stores_[tableid])->GetIterator();
                cur_ = NULL;
                prev_link = NULL;
              }


              bool RadIterator::Valid() {
                /* Read will directly read from the latest one */
                //  if(cur_ != NULL)
                //    assert(cur_->seq != 0);
                return cur_ != NULL && val_ != NULL;
              }

              uint64_t RadIterator::Key()
              {
                return iter_->Key();
              }

              char *RadIterator::Value() {
                return (char *)val_ ;
              }

              char *RadIterator::Node() {
                return (char *)cur_;
              }

              void RadIterator::Next() {
                bool r = iter_->Next();
                //assert(r);
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

              void RadIterator::Prev() {

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

              void RadIterator::Seek(uint64_t key) {

                iter_->Seek(key);
                cur_ = iter_->CurNode();

                if(!iter_->Valid()) {
                  assert(cur_ == NULL) ;
                  //fprintf(stderr,"seek fail..\n");
                  //sleep(10);
                  return ;
                }

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

              void RadIterator::SeekToFirst() {
                /* TODO ,not implemented. seems not needed */
              }

              void RadIterator::SeekToLast() {
                /* TODO ,not implemented */
              }

              int  DBRad::add_to_remote_set(int tableid,uint64_t key,int pid) {
                return remoteset->add(REQ_READ,pid,tableid,key);
              }

              int  DBRad::add_to_remote_set(int tableid, uint64_t *key, int klen, int pid) {
                return remoteset->add(REQ_READ,pid,tableid,key,klen);
              }


              int DBRad::remote_read_idx(int tableid,uint64_t *key,int klen,int pid) {
                return remoteset->add(REQ_READ_IDX,pid,tableid,key,klen);
              }

              int DBRad::remote_insert(int tableid,uint64_t *key, int klen, int pid) {
                return remoteset->add(REQ_INSERT,pid,tableid,key,klen);
              }

              int DBRad::remote_insert_idx(int tableid, uint64_t *key, int klen, int pid) {
                return remoteset->add(REQ_INSERT_IDX,pid,tableid,key,klen);
              }

              void DBRad::remote_write(int tableid,uint64_t key,char *val,int len) {
                assert(false);
              }

              void DBRad::remote_write(int r_id,char *val,int len) {
                assert(remoteset->cor_id_ == cor_id_);
                if(db_logger_){
                  RemoteSet::RemoteSetItem& item = remoteset->kvs_[r_id];
                  char* logger_val = db_logger_->get_log_entry(cor_id_, item.tableid, item.key, len, item.pid);
                  memcpy(logger_val, val, len);
                  db_logger_->close_entry(cor_id_); 
                }
                remoteset->promote_to_write(r_id,val,len);
              }

              void DBRad::do_remote_reads(yield_func_t &yield) {
                remoteset->do_reads(yield);
              }

              int DBRad::do_remote_reads() {
                return remoteset->do_reads();
              }

              void DBRad::get_remote_results(int num_results) {
                remoteset->get_results(num_results);
                remoteset->clear_for_reads();
              }
            }

          }//end namespace nocc

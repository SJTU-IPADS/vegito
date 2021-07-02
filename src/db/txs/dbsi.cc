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
#include "rdmaio.h"
#include "ralloc.h"

#include "framework/bench_worker.h"

#include <time.h>
#include <sys/time.h>

#include "dbsi.h"

#define MAX(x,y) (  (x) > (y) ? (x) : (y))

using namespace nocc::db;
using namespace nocc::oltp;


//__thread DBSI::WriteSet* DBSI::rwset = NULL;
//__thread bool DBSI::localinit = false;

__thread uint64_t  local_seconds;

#define MAX_SIZE 1024

extern size_t current_partition ;
extern size_t nthreads;
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

namespace nocc {
    extern __thread BenchWorker* worker;

    namespace db {

        extern __thread coroutine_func_t *routines_;

        void DBSI::GlobalInit() {

        }

        int SIGetMetalen() {
            return SI_META_LEN;
        }

        /* Write set's definiation */
        class DBSI::WriteSet {

        public:
            struct  WriteSetItem {
                int32_t tableid;
                uint64_t key;
                int len ;
                MemNode  *node;
                uint64_t *addr;  /* Pointer to the new value buffer */
                uint64_t seq;
                bool ro;
            };

            DBSI *tx_;
            int max_len;
            WriteSetItem *kvs;

            /* How many items in write set*/
            int elems;
            /* How many items which has been locked */
            int current;

            WriteSet();
            ~WriteSet();

            inline void Reset();
            inline void SetTX(DBSI *dbtx);
            inline void Add(WriteSetItem &item);

            /* commit helper functions */
            inline bool LockAllSet(uint64_t );
            inline void ReleaseAllSet();

            inline int  CommitLocalWrite(uint64_t commit_ts);

            inline void GC();

            bool inline IsLocked(uint64_t *ptr) {
                return (*ptr) != 0;
            }
        };

        DBSI::WriteSet::WriteSet () {
            max_len = MAX_SIZE;
            elems = 0;
            kvs   = new WriteSetItem[max_len];
            tx_   = NULL;

            for(uint i = 0;i < max_len;++i) {
                memset(&(kvs[i]),0,sizeof(WriteSetItem));
            }
        }

        DBSI::WriteSet :: ~WriteSet() {
            delete []kvs;
            kvs = NULL;
        }

        inline void DBSI::WriteSet::GC () {
            for(uint i = 0;i < elems;++i) {
                if(kvs[i].addr != NULL)
                    free(kvs[i].addr);
            }
        }

        inline void DBSI::WriteSet::Add(WriteSetItem &item) {
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


        inline void DBSI::WriteSet::SetTX(DBSI *dbtx) {
            tx_ = dbtx;
        }

        inline void DBSI::WriteSet::Reset() {
            elems = 0;
            current = -1;
        }

        inline bool DBSI::WriteSet::LockAllSet(uint64_t commit_ts) {

            for(uint i = 0;i < elems;++i) {
                //    fprintf(stdout,"tab %d\n",kvs[i].tableid);
                /* Lock + check */
                if(kvs[i].ro )
                    continue;
                volatile uint64_t *lockptr = &(kvs[i].node->lock);

                if( unlikely( (*lockptr != 0) ||
                             //		    !__sync_bool_compare_and_swap(lockptr,0,_QP_ENCODE_ID(current_partition,tx_->thread_id + 1)))) {
                             !__sync_bool_compare_and_swap(lockptr,0,
                                                           ENCODE_LOCK_CONTENT(current_partition,tx_->thread_id,
                                                                               tx_->cor_id_ + 1)))) {

                                                               return false;
                                                           }
                /* lock process to i */
                current = i;
#if 1
                /* it exceed my timestamp */
#ifdef SI_VEC
                //      if( SI_GET_COUNTER(kvs[i].node->seq) > tx_->ts_buffer_[SI_GET_SERVER(kvs[i].node->seq)])
                //	return false;
                if(kvs[i].node->seq != kvs[i].seq) {
                    //fprintf(stdout,"desired %lu, real %lu\n",kvs[i].seq,kvs[i].node->seq);
                    //sleep(1);
                    return false;
                }
#else
                assert( SI_GET_COUNTER(kvs[i].node->seq) <= ts_buffer_[SI_GET_SERVER(kvs[i].node->seq)]);
                if( kvs[i].node->seq > tx_->ts_buffer_[0])
                    return false;
#endif
#else

                if(kvs[i].node->seq != kvs[i].seq) {
                    return false;
                }
#endif
            }
            return true;
        }

        inline void DBSI::WriteSet::ReleaseAllSet() {
            /* This shall be int, since current is -1 if no lock succeed, uint will cause an overvlow */
            for(int i = 0;i <= current;++i) {
                if(kvs[i].ro)
                    continue;
                kvs[i].node->lock = 0;
            }
        }


        inline int DBSI::WriteSet::CommitLocalWrite(uint64_t commit_ts) {

            for(uint i = 0;i < elems;++i) {
                if(kvs[i].ro)
                    continue;
                /* Forbiding concurrent read access */
                kvs[i].node->seq = 1;
                //fprintf(stdout,"write record %p\n",kvs[i].node);
                asm volatile("" ::: "memory");
#if 1
                uint64_t *cur    = kvs[i].node->value;
                /* Using value switch */
                uint64_t *oldptr = kvs[i].node->old_value;

                if(likely(cur != NULL)) {
                    _SIValHeader * hptr = (_SIValHeader *)cur;
                    hptr->oldValue = oldptr;
                    hptr->version  = kvs[i].seq;
                    if(unlikely(kvs[i].seq == 0)) {
                        fprintf(stdout,"tableid %d\n",kvs[i].tableid);
                        assert(false);
                    }
                    assert(hptr->version != 0);
                } else {
                    // insertion
                    //assert(oldptr == NULL);
                    kvs[i].node->value = kvs[i].addr;
                }
                /* TODO, may set the version */
                kvs[i].node->old_value = cur;
                kvs[i].node->value = kvs[i].addr;
                // TODO, shall be set to commit version
#else
                memcpy( (char *)cur + SI_META_LEN, (char *)(kvs[i].addr) + SI_META_LEN, kvs[i].len);
#endif
                asm volatile("" ::: "memory");
                //    kvs[i].node->seq = kvs[i].seq + 2;
                kvs[i].node->seq = commit_ts;
                //      assert(commit_ts > kvs[i].seq);
                asm volatile("" ::: "memory");
                /* Release the lock*/
                kvs[i].node->lock = 0;
            }
            return 0;
        }


        DBSI::DBSI(MemDB *tables,int t_id,Rpc *rpc,TSManager *tm,int c_id)
        : txdb_(tables), thread_id(t_id),rpc_handler_(rpc),
        abort_(false),ts_manager_(tm),
        TXHandler(c_id) // farther class
        {
            /* register rpc handlers */
            using namespace std::placeholders;
            rpc_handler_->register_callback(std::bind(&DBSI::get_rpc_handler,this,_1,_2,_3,_4),RPC_READ);
            rpc_handler_->register_callback(std::bind(&DBSI::lock_rpc_handler,this,_1,_2,_3,_4),RPC_LOCK);
            rpc_handler_->register_callback(std::bind(&DBSI::release_rpc_handler,this,_1,_2,_3,_4),RPC_RELEASE);
            //rpc_handler_->register_callback(std::bind(&DBSI::commit_rpc_handler,this,_1,_2,_3),RPC_COMMIT);
            rpc_handler_->register_callback(std::bind(&DBSI::commit_rpc_handler2,this,_1,_2,_3,_4),RPC_COMMIT);
            TXHandler::nreadro_locked = 0;
            assert(ts_manager_ != NULL);

            // get qp vector
#if 1
            auto rdma_cm = tm->cm_;
            for(uint i = 0;i < rdma_cm->get_num_nodes();++i) {
                qp_vec_.push_back(rdma_cm->get_rc_qp(t_id + nthreads + 2,i));
            }
#endif
            localinit = false;
        }

        inline uint64_t
        DBSI::_get_ro_versioned_helper(int tableid, uint64_t key, char *val, uint64_t version,yield_func_t &yield) {

            int vlen = txdb_->_schemas[tableid].vlen;
            MemNode *node = txdb_->stores_[tableid]->GetWithInsert(key);
            uint64_t ret = 0;

            uint64_t *ts_vec = (uint64_t *)version;
        retry:
            uint64_t seq = node->seq;
            if(unlikely(seq == 0)) {
                //fprintf(stdout,"tableid %d timestamp %lu real %lu\n",tableid,*ts_vec,ts_manager_->last_ts_);
                //assert(false);
                return 0;
            }
            /* traverse the read linked list to find the correspond records */
#ifdef SI_VEC
            if(SI_GET_COUNTER(seq) <= ts_vec[SI_GET_SERVER(seq)] ) {
#else
                if(seq <= ts_vec[0]) {
#endif
                    /* simple case, read the current value */
                    asm volatile("" ::: "memory");
                    uint64_t *tmpVal = node->value;

                    if(likely(tmpVal != NULL)) {
                        memcpy(val, (char *)tmpVal + SI_META_LEN,vlen);
                        asm volatile("" ::: "memory");
                        if(node->seq != seq || seq == 1) {
                            goto retry;
                        }
                        ret = seq;
                    } else {
                        /* read a deleted value, currently not supported */
                        return 0;
                    }
                } else {
                    /* traverse the old reader's list */
                    /* this is the simple case, and can always success  */
                    char *old_val = (char *)(node->old_value);
                    _SIValHeader *rh = (_SIValHeader *)old_val;
#ifdef SI_VEC
                    while(old_val != NULL && SI_GET_COUNTER(rh->version) > ts_vec[SI_GET_SERVER(rh->version)]) {
#else
                        while(old_val != NULL && rh->version > ts_vec[0]) {
#endif
                            old_val = (char *)(rh->oldValue);
                            rh = (_SIValHeader *)old_val;
                        }
                        if(old_val == NULL) {
                            /* cannot find one */
                            //fprintf(stdout,"ts %lu, seq %lu %d\n",ts_vec[0],seq,tableid);
                            //assert(false);
                            return 0;
                        }
                        /* cpy */
                        memcpy(val,(char *)old_val + SI_META_LEN,vlen);
                        assert(rh->version != 0);
                        ret = rh->version;
                        /* in this case, we do not need to check the lock */
                    }
                    return ret;
                }

                uint64_t DBSI::get_cached(int tableid,uint64_t key,char **val) {
                    for(uint i = 0;i < remoteset->elems_;++i) {
                        if(remoteset->kvs_[i].tableid == tableid && remoteset->kvs_[i].key == key) {
                            *val = (char *)(remoteset->kvs_[i].val);
                            return remoteset->kvs_[i].seq;
                        }
                    }
                    return 0;

                }
                uint64_t DBSI::get_ro(int tableid,uint64_t key,char *val,yield_func_t &yield) {
                    return _get_ro_versioned_helper(tableid,key,val,(uint64_t )ts_buffer_,yield);
                }

                uint64_t DBSI::get_ro_versioned(int tableid, uint64_t key, char *val, uint64_t version,yield_func_t &yield) {
                    return _get_ro_versioned_helper(tableid,key,val,version,yield);
                }

                uint64_t
                DBSI::get(int tableid, uint64_t key, char **val,int len) {

                    int vlen = len + SI_META_LEN;
                    vlen = vlen + 64 - vlen % 64;
                    char *_addr = (char *)malloc(vlen);

                    MemNode *node = NULL;
                    node = txdb_->stores_[tableid]->GetWithInsert(key);

                retry:
                    uint64_t seq = node->seq;
                    asm volatile("" ::: "memory");
#if 1
                    if( SI_GET_COUNTER(seq) <= ts_buffer_[SI_GET_SERVER(seq)]) {
                        //if(tableid == 1) { fprintf(stdout,"get latst version %p\n",node);}
                        uint64_t *temp_val = node->value;

                        if(unlikely(temp_val == NULL)){
                            /* actually it is possible */
                            //	fprintf(stdout,"seq 1 table %d\n",tableid);
                            //	assert(false);
                            return 1;
                        }
                        /* reads the current value */
                        memcpy(_addr + SI_META_LEN,(char *)temp_val + SI_META_LEN,len);
                        asm volatile("" ::: "memory");
                        if(node->seq != seq || seq == 1) {
                            goto retry;
                        }
                    } else {
                        //if(tableid == 1) { fprintf(stdout,"traverse old version\n");}
                        /* traverse the old reader's list */
                        char *old_val = (char *)(node->old_value);
                        _SIValHeader *rh = (_SIValHeader *)old_val;
#ifdef SI_VEC
                        while(old_val != NULL && SI_GET_COUNTER(rh->version) > ts_buffer_[SI_GET_SERVER(rh->version)]) {
#else
                            while(old_val != NULL && rh->version > ts_buffer_[0]) {
#endif
                                old_val = (char *)(rh->oldValue);
                                rh = (_SIValHeader *)old_val;
                            }
                            //	assert(old_val != NULL);
                            if(unlikely(old_val == NULL)){
                                //fprintf(stdout,"seq %lu,current vec %lu\n",seq,ts_buffer_[0]);
                                seq = 1;
                                goto READ_END;
                            }
                            seq = rh->version;
                            memcpy(_addr + sizeof(_SIValHeader),(char *)old_val + sizeof(_SIValHeader),len);
                        }
#else
                        /* normal occ get */
                        assert(false);
#endif
                    READ_END:
                        assert(seq != 0);
                        WriteSet::WriteSetItem item;
                        item.tableid = tableid;
                        item.key = key;
                        item.len = len;
                        item.node = node;
                        item.addr = (uint64_t *)_addr;
                        item.seq = seq;
                        item.ro  = true;
                        *val = ( (char *)_addr + SI_META_LEN);
                        assert(seq != 0);
                        rwset->Add(item);
                        return seq;
                    }


                    inline void prepare_log(int cor_id, DBLogger* db_logger, const DBSI::WriteSet::WriteSetItem& item){
                        char* val = db_logger->get_log_entry(cor_id, item.tableid, item.key, item.len);
                        // printf("%p %p %d %lu\n", val, (char*)item.addr + META_LENGTH, item.len, item.key);
                        memcpy(val, (char*)item.addr + SI_META_LEN, item.len);
                        db_logger->close_entry(cor_id);
                    }
                    
                    void DBSI::insert(int tableid, uint64_t key, char *val, int len) {

                        int vlen = SI_META_LEN + len;
                        //  vlen = vlen + 64 - vlen % 64;

                        MemNode *node = txdb_->stores_[tableid]->GetWithInsert(key);
                        //assert(node->value == NULL && node->seq == 0);

                        WriteSet::WriteSetItem item;

                        item.tableid = tableid;
                        item.key     = key;
                        item.len     = len;
                        item.node    = node;
                        item.addr    = (uint64_t *)(new char[vlen]);
                        memcpy( (char *)item.addr + SI_META_LEN, val,len);
                        item.seq     = node->seq;
                        item.ro      = false;
                        rwset->Add(item);
                        if(db_logger_){
                          // printf("insert size:%d ,key:%lu\n",item.len, item.key);
                          prepare_log(cor_id_, db_logger_, item);
                        }
                    }

                    void DBSI::delete_(int tableid, uint64_t key) {

                        for(uint i = 0;i < rwset->elems;++i) {
                            if(rwset->kvs[i].tableid == tableid && rwset->kvs[i].key == key) {
                                WriteSet::WriteSetItem &item = rwset->kvs[i];
                                item.ro = false;
                                // DZY:: does here miss : "delete item.addr;"" ?
                                item.addr = NULL;
                                item.len = 0;
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
                        rwset->Add(item);
                        if(db_logger_){
                            // printf("delete_ size:%d\n",item.len);
                            prepare_log(cor_id_, db_logger_, item);
                        }
                    }

                    void DBSI::write() {
                        WriteSet::WriteSetItem &item = rwset->kvs[rwset->elems - 1];
                        item.ro = false;
                        if(db_logger_){
                          // printf("write() size:%d\n",item.len);
                          prepare_log(cor_id_, db_logger_, item);
                        }
                    }

                    void DBSI::write(int tableid,uint64_t key,char *val,int len) {
                        for (uint i = 0; i < rwset->elems; i++) {
                            if (rwset->kvs[i].tableid == tableid && rwset->kvs[i].key == key) {
                                WriteSet::WriteSetItem& item = rwset->kvs[i];
                                item.ro = false;
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

                    void DBSI::ThreadLocalInit() {
                        if (false == localinit) {
                            rwset = new WriteSet();
                            remoteset = new RemoteSet(rpc_handler_,cor_id_,thread_id);
                            ts_buffer_ = (uint64_t *)(Rmalloc(sizeof(uint64_t ) * (ts_manager_->total_partition + 1)));
                            //assert(ts_buffer_ != NULL);
                            localinit = true;

                        }
                    }

                    void DBSI::local_ro_begin() {
                        ThreadLocalInit();
                        int ts_size = total_partition * sizeof(uint64_t);
                        ts_manager_->get_timestamp((char *)ts_buffer_,thread_id);
                    }

                    void DBSI::_begin(DBLogger *db_logger,TXProfile *p) {
                        // init rw sets
#if 1
                        ThreadLocalInit();
                        rwset->Reset();
                        abort_ = false;

                        // get timestamp
                        int ts_size = total_partition * sizeof(uint64_t);
                        ts_manager_->get_timestamp((char *)ts_buffer_,thread_id);

                        remoteset->clear(ts_size);
#endif
                        db_logger_ = db_logger;
                        // printf("begin!!!!\n");
                        if(db_logger_)db_logger->log_begin(cor_id_, 1);
                    }

                    bool
                    DBSI::end(yield_func_t &yield) {

                        if(abort_) {
                            assert(false);
                            return false;
                        }

                        uint64_t commit_ts,encoded_commit_ts;
                        uint64_t *commit_ts_vec = NULL;
                        rwset->SetTX(this);

                        /* lock remote sets */
                        if(!remoteset->lock_remote(yield)) {
                            goto ABORT;
                        }

                        if(unlikely(!rwset->LockAllSet(timestamp) ) )
                            goto ABORT;

                        /* get commit ts */
                        commit_ts = ts_manager_->commit_ts();

#ifdef SI_VEC
                        encoded_commit_ts = SI_ENCODE_TS(current_partition,commit_ts);
                        uint64_t ts_vec[MAX_SERVERS];
                        ts_manager_->get_timestamp((char *) ts_vec, 0);
                        // the second argu is meanless
                        ts_vec[current_partition] = commit_ts;
                        commit_ts_vec = ts_vec;
#else
                        encoded_commit_ts = commit_ts;
#endif
                        if(db_logger_){
                            assert(false); // XXX(SSJ): deal with ts
                            // db_logger_->log_backups(cor_id_, yield, encoded_commit_ts, commit_ts_vec);
                            worker->indirect_must_yield(yield);
                            db_logger_->log_end(cor_id_);
                        }
                        rwset->CommitLocalWrite(encoded_commit_ts);
                        remoteset->max_time_ = encoded_commit_ts;
                        remoteset->commit_remote();
#if 1
                        // update the commit timestamp
                        while(ts_manager_->last_ts_ != commit_ts - 1) {
                            asm volatile("" ::: "memory");
                        }
                        // this is the distributed SI case
                        // an RDMA write is needed
                        {
                            // must be **synchronously** posted, otherwise a later one may be overwritten by other
                            // threads
                            *ts_buffer_ = commit_ts;
                            Qp *qp = qp_vec_[0];
                            auto send_flag = 0;
                            auto ret = qp->rc_post_send(IBV_WR_RDMA_WRITE,(char *)ts_buffer_,sizeof(uint64_t),
                                                        current_partition * sizeof(uint64_t),IBV_SEND_SIGNALED);
                            assert(ret == Qp::IO_SUCC);

                            qp->poll_completion(); // FIXME!! no error detection now
                        }

                        asm volatile("" ::: "memory");
                        ts_manager_->last_ts_ += 1;
#else
#endif
                        return true;
                    ABORT:
                        remoteset->release_remote();
                        rwset->ReleaseAllSet();
                        if(db_logger_){
                            db_logger_->log_abort(cor_id_);
                        }
                        return false;
                    }

                    void DBSI::abort() {
                        if(db_logger_){
                            db_logger_->log_abort(cor_id_);
                        }
                    }

                    SIIterator::SIIterator (DBSI *tx,int tableid,bool sec) {
                        tx_ = tx;
                        if(sec) {
                            iter_ = (tx_->txdb_->_indexs[tableid])->GetIterator();
                        } else {
                            iter_ = (tx_->txdb_->stores_[tableid])->GetIterator();
                        }
                        cur_ = NULL;
                        prev_link = NULL;
                    }

                    bool SIIterator::Valid() {
                        /* maybe we need a snapshot one ?*/
                        return cur_ != NULL && cur_->seq != 0 ;
                    }

                    uint64_t SIIterator::Key()
                    {
                        return iter_->Key();
                    }

                    char *SIIterator::Node() {
                        return (char *)cur_;
                    }

                    char *SIIterator::Value() {
                        return (char *)val_ ;
                    }

                    void SIIterator::Next() {

                        bool r = iter_->Next();

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

                    void SIIterator::Prev() {

                        bool b = iter_->Prev();
                        if(!b) {
                            //  tx_->abort = true;
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

                    void SIIterator::Seek(uint64_t key) {

                        iter_->Seek(key);
                        cur_ = iter_->CurNode();

                        if(!iter_->Valid()) {
                            assert(cur_ == NULL) ;
                            //    fprintf(stderr,"seek fail..\n");
                            return ;
                        }

                        while (iter_->Valid()) {
                            {
                                RTMScope rtm(NULL) ;
                                val_ = cur_->value;
                                if(ValidateValue(val_)) {
                                    //	fprintf(stdout,"one time succeed\n");
                                    return;
                                }
                            }
                            iter_->Next();
                            cur_ = iter_->CurNode();

                        }

                        cur_ = NULL;
                    }

                    void SIIterator::SeekToFirst() {
                        /* TODO ,not implemented. seems not needed */
                    }

                    void SIIterator::SeekToLast() {
                        /* TODO ,not implemented */
                    }

                    int  DBSI::add_to_remote_set(int tableid,uint64_t key,int pid) {
                        return remoteset->add(REQ_READ,pid,tableid,key);
                    }

                    void DBSI::remote_write(int tableid,uint64_t key,char *val,int len) {
                        // TODO!!
                    }

                    void DBSI::do_remote_reads(yield_func_t &yield) {
                        // not supported any more
                        assert(false);
                        remoteset->do_reads(yield);
                    }

                    int
                    DBSI::do_remote_reads() {
                        // add timestamp to rpc's meta data
                        char *remote_ts = remoteset->get_meta_ptr();
                        memcpy(remote_ts,ts_buffer_,total_partition * sizeof(uint64_t));
                        //fprintf(stdout,"remote reads %d\n",remoteset->elems_);
                        return remoteset->do_reads();
                    }

                    void DBSI::get_remote_results(int num_results) {
                        remoteset->get_results(num_results);
                        remoteset->clear_for_reads();
                    }

                    void DBSI::insert_index(int tableid, uint64_t key, char *val) {

                        MemNode *node = txdb_->_indexs[tableid]->GetWithInsert(key);
                        WriteSet::WriteSetItem item;

                        item.tableid = tableid;
                        item.key     = key;
                        item.node    = node;
                        item.addr    = (uint64_t *)val;
                        item.seq     = node->seq;
                        item.ro      = false;
                        rwset->Add(item);
                    }

                    void DBSI::delete_index(int tableid,uint64_t key) {
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
                    }

                    void DBSI::delete_by_node(int tableid,char *node) {
                        for(uint i = 0;i < rwset->elems;++i) {
                            if((char *)(rwset->kvs[i].node) == node) {
                                WriteSet::WriteSetItem &item = rwset->kvs[i];
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
                        WriteSet::WriteSetItem item;

                        item.tableid = tableid;
                        item.key     = 0;
                        item.len     = 0;
                        item.node    = (MemNode *)node;
                        item.addr    = NULL; // NUll means logical delete
                        item.seq     = item.node->seq;
                        item.ro      = false;
                        rwset->Add(item);
                        if(db_logger_){
                            // printf("delete_by_node size:%d ,key:%lu\n",item.len, item.key);
                            prepare_log(cor_id_, db_logger_, item);
                        }
                    }

                    int DBSI::add_to_remote_set(int tableid,uint64_t *key,int klen,int pid) {
                        return remoteset->add(REQ_READ,pid,tableid,key,klen);
                    }

                    int DBSI::remote_read_idx(int tableid,uint64_t *key,int klen,int pid) {
                        return remoteset->add(REQ_READ_IDX,pid,tableid,key,klen);
                    }

                    int DBSI::remote_insert(int tableid,uint64_t *key, int klen,int pid) {
                        return remoteset->add(REQ_INSERT,pid,tableid,key,klen);
                    }

                    int DBSI::remote_insert_idx(int tableid,uint64_t *key, int klen,int pid) {
                        return remoteset->add(REQ_INSERT_IDX,pid,tableid,key,klen);
                    }

                    void DBSI::remote_write(int idx,char *val,int len) {
                        assert(remoteset->cor_id_ == cor_id_);
                        if(db_logger_){
                            // printf("remote_write size: %d\n", len);
                            RemoteSet::RemoteSetItem& item = remoteset->kvs_[idx];
                            char* logger_val = db_logger_->get_log_entry(cor_id_, item.tableid, item.key, len, item.pid);
                            memcpy(logger_val, val, len);
                            db_logger_->close_entry(cor_id_);
                        }
                        remoteset->promote_to_write(idx,val,len);
                    }

                    uint64_t DBSI::get_cached(int idx,char **val) {
                        if(unlikely(remoteset->kvs_[idx].seq == 0))
                            return 1;
                        assert(idx < remoteset->elems_);
                        *val = (char *)(remoteset->kvs_[idx].val);
                        return remoteset->kvs_[idx].seq;
                    }
                }
            }

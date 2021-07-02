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

#include "db_logger.h"
#include "req_buf_allocator.h"
#include "framework/bench_worker.h"
#include "framework/view_manager.h"
#include "framework/log_area.h"

extern size_t txn_nthreads;
extern size_t current_partition;
extern size_t coroutine_num;

using namespace nocc::framework;
using namespace std;

namespace nocc {

extern __thread db::RPCMemAllocator *msg_buf_alloctors;
extern __thread BenchWorker *worker;

namespace db {

LogCleaner* DBLogger::log_cleaner_ = NULL;  // XXX: ???

DBLogger::DBLogger(int worker_id, int thread_id, RdmaCtrl *rdma, int logger_type, 
                   RDMA_sched *rdma_sched, Rpc *rpc_handler)
  : rdma_(*rdma), 
    num_nodes_(config.getNumServers()), 
    num_logs_(logArea.num_log()),
    mac_id_(current_partition), 
    worker_id_(worker_id),
    thread_id_(thread_id), 
    logger_type_(logger_type),
    rdma_sched_(rdma_sched), 
    rpc_handler_(rpc_handler), 
  	base_((char*) rdma->conn_buf_),
    log_tailer_(num_logs_, nullptr),
    buf_sz_(logArea.getLogBufferSize()), 
    padding_(logArea.getLogPadding()),
    log_header_(num_logs_, vector<uint64_t>(num_nodes_, 0)),
    send_log_cnt_(nullptr)
  {

  //computer the start location
	//DZY: conn_buf_ is the start pointer of our memory region

	//TODO: header_ptr_
  for (int i = 0; i < num_logs_; ++i) {
    char *ptr = (base_ + logArea.getMetaBase(mac_id_, worker_id_, i));
    memset(ptr, 0, logArea.getLogAreaSize());
    log_tailer_[i] = (uint64_t *) ptr;
  }

	temp_logs_.reserve(coroutine_num + 1);
	for(int i = 0; i <= coroutine_num; ++i) {
    if (logger_type_ > 0)
      temp_logs_.emplace_back(num_nodes_, num_logs_, 
                              num_nodes_ * sizeof(DBLogger::ReplyHeader),
		  			                  sizeof(uint64_t) + sizeof(rpc_header) 
                              + sizeof(DBLogger::RequestHeader));
    else
      temp_logs_.emplace_back(num_nodes_, num_logs_, num_nodes_ * sizeof(char));
  }

}

DBLogger::DBLogger(int worker_id, int thread_id, RdmaCtrl *rdma, RDMA_sched* rdma_sched)
  : DBLogger(worker_id, thread_id, rdma, 0, rdma_sched, nullptr) { }

DBLogger::DBLogger(int worker_id, int thread_id, RdmaCtrl *rdma, Rpc *rpc_handler)
  : DBLogger(worker_id, thread_id, rdma, 1, nullptr, rpc_handler) { }

DBLogger::DBLogger(int worker_id, int thread_id, RdmaCtrl *rdma, 
                   RDMA_sched* rdma_sched, Rpc *rpc_handler)
  : DBLogger(worker_id, thread_id, rdma, 2, rdma_sched, rpc_handler) { }

void DBLogger::thread_local_init() {
  using namespace std::placeholders;  // _1, _2, ...

  if (logger_type_ == 1) {
    rpc_handler_->
      register_callback(bind(&DBLogger::logging_handler, this, _1, _2, _3, _4),
                        RPC_LOGGING);
  }

  if (logger_type_ > 0) {
    rpc_handler_->
      register_callback(bind(&DBLogger::logging_commit_handler, 
                             this, _1, _2, _3, _4),
                        RPC_LOGGING_COMMIT);
  }

  if (logger_type_ == 0 || logger_type_ == 2) {
    assert(rdma_sched_);
    for(uint i = 0; i < num_nodes_; ++i) {
    	Qp *qp = rdma_.get_rc_qp(thread_id_,i,1);
    	assert(qp != nullptr);
    	qp_vec_.push_back(qp);
    }
  }
}

void DBLogger::log_begin(uint cor_id, int tx_id, uint64_t global_seq) {
  assert(cor_id >= 1 && cor_id <= coroutine_num);
  // printf("log_begin: cor_id:%d\n", cor_id);
  
  TempLog *temp_log = &temp_logs_[cor_id];
  
  temp_log->open();
  
  for (int i = 0; i < num_logs_; ++i) {
    temp_log->add_dest(view.get_backup_mac(mac_id_, i), i);
  }
  
  TXHeader *header = (TXHeader*)temp_log->append_entry(sizeof(TXHeader));
  // reserved operation for future TXHeader fields,
  header->magic_num = HEADER_MAGIC_;
  header->global_seq = global_seq;
  header->tx_id = tx_id;
  temp_log->close_entry();
}

void DBLogger::log_begin(uint cor_id, uint64_t global_seq) {
  assert(cor_id >= 1 && cor_id <= coroutine_num);
  // printf("log_begin: cor_id:%d\n", cor_id);
  
  TempLog *temp_log = &temp_logs_[cor_id];
  
  temp_log->open();
  
  for (int i = 0; i < num_logs_; ++i) {
    temp_log->add_dest(view.get_backup_mac(mac_id_, i), i);
  }
  
  TXHeader *header = (TXHeader*)temp_log->append_entry(sizeof(TXHeader));
  // reserved operation for future TXHeader fields,
  header->magic_num = HEADER_MAGIC_;
  header->global_seq = global_seq;
  header->tx_id = -1;
  temp_log->close_entry();
}

// XXX: temprarily we think remote_id as partition id
char* DBLogger::get_log_entry(uint cor_id, int table_id, 
                              uint64_t key, uint32_t size, uint32_t op_bit,
                              int partition_id) {

	TempLog *temp_log = &temp_logs_[cor_id];
	char *cur = temp_log->append_entry(sizeof(EntryMeta) + size);

	EntryMeta* entry_meta = (EntryMeta *) cur;
  entry_meta->clean    = false;
	entry_meta->table_id = table_id;
	entry_meta->size 		= size;
	entry_meta->key 		= key;
  entry_meta->pid     = (partition_id == -1)? current_partition : partition_id;
  entry_meta->op_bit  = op_bit;

	// the data is in the remote
	// we assume that local log is in NVRAM
	// so nothing to do for local data
	if(unlikely(partition_id >= 0)) {
    // 2PC log on primary
    if (num_logs_ > 0)
      temp_log->add_dest(view.partition_to_mac(partition_id), 0); 

    // HA log
    for (int i = 0; i < num_logs_; ++i) {
      temp_log->add_dest(view.get_backup_mac(partition_id, i), i);
    }
	}

	return (cur + sizeof(EntryMeta));
}

void DBLogger::close_entry(uint cor_id, uint64_t seq){

	TempLog *temp_log = &temp_logs_[cor_id];

	EntryMeta* entry_meta = (EntryMeta*) temp_log->current();
	entry_meta->seq = seq;
	// print_log_entry(temp_log->current_);
	temp_log->close_entry();
}

int DBLogger::log_backups(uint cor_id, yield_func_t &yield, uint64_t seq, uint64_t ts) {
  // int ret = LOG_SUCC;
  
  TempLog *temp_log = &temp_logs_[cor_id];
  
  temp_log->log_size_round_up();
  
  TXTailer *tailer = (TXTailer*)temp_log->append_entry(sizeof(TXTailer));
  tailer->magic_num = TAILER_MAGIC_;
  tailer->seq = seq;
  
  int ret = log_setup_(cor_id, yield);
  assert(ret == LOG_SUCC);
  
  uint64_t* log_size_ptr = (uint64_t *) temp_log->start();
  *log_size_ptr = temp_log->log_size();
  	if(temp_log->empty()) {
  	// printf("no backups!-------------------------\n");
  	return ret;
  }

  tailer->ts_vec = ts; 

  int length = temp_log->total_size();
  // printf("total_size: %d\n", length);

  if (logger_type_ == 1) {
    vector<int> mac_vec = temp_log->mac_vec();
    auto request_header = (DBLogger::RequestHeader*) 
        (temp_log->start() - sizeof(DBLogger::RequestHeader));
    request_header->length = length;

   // XXX: fix it!
#if 0
    copy(temp_log->log_off.begin(), temp_log->log_off.end(),
         request_header->offsets);
#else
    assert(false);
#endif
  
    // printf("remote_log_offsets: ");
    // for(int i = 0; i < temp_log->remote_mac_num_; i++){
    // 	printf("%u ", request_header->offsets[i]) 	;
    // }
    // printf("\n");
  
    rpc_handler_->set_msg((char*) request_header);
    rpc_handler_->send_reqs(RPC_LOGGING,
                            length + sizeof(DBLogger::RequestHeader), 
                            temp_log->reply_buf(),
                            mac_vec.data(), mac_vec.size(), cor_id);
  } else {
  
    for (uint64_t mac_log_id : temp_log->mac_log_ids()) {
      int remote_id = TempLog::decode_mac_id(mac_log_id);
      int log_id = TempLog::decode_log_id(mac_log_id);
      Qp* qp = qp_vec_[remote_id];
      // printf("remote_mac:%d, log_offset:%lu, length:%d\n", remote_id,temp_log->log_off[remote_id],length);
#if 1 // 0 for debug.1
      Qp::IOStatus ret = qp->rc_post_send(
                            IBV_WR_RDMA_WRITE, temp_log->start(),
                            length,
  					                temp_log->log_off[remote_id][log_id],
                            IBV_SEND_SIGNALED, cor_id);
      assert(ret == Qp::IO_SUCC);
      rdma_sched_->add_pending(cor_id,qp);
#endif
                  // qp->poll_completion();
    }
  }
  // printf("log_backups: cor_id:%u\n", cor_id);
  return temp_log->mac_log_ids().size();
}

int DBLogger::log_setup_(uint cor_id, yield_func_t &yield) {
	int ret = LOG_SUCC;

	TempLog *temp_log = &temp_logs_[cor_id];

	uint64_t log_total_size = temp_log->total_size();
	assert(log_total_size <= padding_);

  for (uint64_t mac_log_id : temp_log->mac_log_ids()) {
    int remote_id = TempLog::decode_mac_id(mac_log_id);
    int log_id = TempLog::decode_log_id(mac_log_id);

    uint64_t header = log_header_[log_id][remote_id];
    uint64_t tailer = log_tailer_[log_id][remote_id];
    uint64_t rest_sz = buf_sz_ - (header + buf_sz_ - tailer) % buf_sz_;

		int whole_temp_log_size = log_total_size + sizeof(uint64_t);
    if (rest_sz < whole_temp_log_size + Q_HOLE_SZ_) {
#if 1  // 0 for debug.2
      poll_tailer_(remote_id, log_id, whole_temp_log_size, yield);
#endif
    } // have enough space

		temp_log->log_off[remote_id][log_id] = translate_offset(header, log_id);
		temp_log->ack_off[remote_id][log_id] 
      = translate_offset(header + log_total_size, log_id);

		log_header_[log_id][remote_id] = (header + whole_temp_log_size) % buf_sz_;
	}

	return ret;
}

uint64_t DBLogger::poll_tailer_(int remote_id, int log_id, uint64_t need_sz, 
                                yield_func_t &yield) const {
  volatile uint64_t *ptr = (volatile uint64_t*) &log_tailer_[log_id][remote_id];
  uint64_t rest_sz;
  int loop_count = 0;
  do {
    if (loop_count != 0) {
      // printf("[WARNING] log is full! n_id %d, t_id %d, l_id %d\n",
      //        remote_id, worker_id_, log_id);
    }
    ++loop_count;
    if(loop_count % 50 == 0) {
      worker->yield_next(yield);
    }
  
  	uint64_t header = log_header_[log_id][remote_id]; 
  	volatile uint64_t tailer = *ptr;
  
    assert(tailer < buf_sz_);

    rest_sz = buf_sz_ - (header + buf_sz_ - tailer) % buf_sz_;
  } while (rest_sz < need_sz + Q_HOLE_SZ_);

  return rest_sz;
}

int DBLogger::log_end(uint cor_id) {

	int ret = LOG_SUCC;

	TempLog *temp_log = &temp_logs_[cor_id];
	if(!temp_log->empty()) {
    if (logger_type_ > 0) {
      vector<int> mac_vec = temp_log->mac_vec();
      char* req_buf = msg_buf_alloctors[cor_id].get_req_buf() 
                      + sizeof(rpc_header) + sizeof(uint64_t);
      auto request_header = (DBLogger::RequestHeader*) req_buf;
      
      request_header->length = sizeof(uint64_t);
      // XXX: fix it!
#if 0
      copy(temp_log->ack_off.begin(), temp_log->ack_off.end(),
           request_header->offsets);
#else
      assert(false);
#endif
      
      *((uint64_t*)(req_buf + sizeof(DBLogger::RequestHeader))) = 
        *((uint64_t*)temp_log->start());
      
      rpc_handler_->set_msg((char*) request_header);
      rpc_handler_->send_reqs(RPC_LOGGING_COMMIT,
                              sizeof(uint64_t) + sizeof(DBLogger::RequestHeader),
                              mac_vec.data(), mac_vec.size(), cor_id);
    } else {

      for(uint64_t mac_log_id : temp_log->mac_log_ids()) {
        int remote_id = TempLog::decode_mac_id(mac_log_id);
        int log_id = TempLog::decode_log_id(mac_log_id);
        Qp* qp = qp_vec_[remote_id];
        ++(*send_log_cnt_);
#if 1 // 0 for debug.3
        Qp::IOStatus ret = 
          qp->rc_post_send(IBV_WR_RDMA_WRITE, 
                           temp_log->start(), sizeof(uint64_t),
                           temp_log->ack_off[remote_id][log_id],
                           IBV_SEND_SIGNALED | IBV_SEND_INLINE);
        assert(ret == Qp::IO_SUCC);
#endif
        // rdma_sched_->add_pending(cor_id,qp);
      }
  
      // for(int remote_id : temp_log->mac_backups_) {
      //   Qp* qp = qp_vec_[remote_id];
      //   if(qp->poll_completion() != Qp::IO_SUCC) {
      //     assert(false);
      //     return LOG_TIMEOUT;
      //   }
      // }
    }
  }
  // printf("log_end : %u\n", cor_id);
  temp_log->close();

  return ret;
}

void DBLogger::log_abort(uint cor_id) {
	temp_logs_[cor_id].close();
}

void DBLogger::logging_handler(int id,int cid,char *msg,void *arg) {
  assert(logger_type_ == 1);

	DBLogger::RequestHeader* request_header = (DBLogger::RequestHeader*)msg;
	uint64_t offset = request_header->offsets[mac_id_];
	// print_log_header(msg + sizeof(uint64_t) + sizeof(DBLogger::RequestHeader));

	memcpy(base_ + offset, msg + sizeof(DBLogger::RequestHeader),
		request_header->length);

	char* reply_msg = rpc_handler_->get_reply_buf();
	DBLogger::ReplyHeader* reply_header = (DBLogger::ReplyHeader*)reply_msg;
	reply_header->ack = ACK_MAGIC_;
	rpc_handler_->send_reply(sizeof(DBLogger::ReplyHeader),id,cid);
}

void DBLogger::logging_commit_handler(int id,int cid,char *msg,void *arg){
	DBLogger::RequestHeader* request_header = (DBLogger::RequestHeader*)msg;
	uint64_t offset = request_header->offsets[mac_id_];
	char* ptr = base_ + offset;

	memcpy(ptr, msg + sizeof(DBLogger::RequestHeader),request_header->length);
#if USE_BACKUP_STORE && !USE_BACKUP_BENCH_WORKER
	char* head_ptr = ptr - *((uint64_t*)ptr) - sizeof(uint64_t);
	char* tail_ptr = check_log_completion(head_ptr);
	assert(tail_ptr != NULL);
	clean_log(head_ptr,tail_ptr);
#endif
}

char* DBLogger::check_log_completion(volatile char* ptr, uint64_t *msg_size){
	uint64_t header_log_size = *((uint64_t*)ptr);
	// printf("header_log_size:%lx, offset:%lx\n",header_log_size, (uint64_t)(ptr - (uint64_t)cm_->conn_buf_));
	if(header_log_size >= logArea.getLogPadding())
		return NULL;
	uint64_t tailer_log_size = *((uint64_t*)(ptr + header_log_size + sizeof(uint64_t)));
	// printf("tailer_log_size:%lx, offset:%lx\n",tailer_log_size, (uint64_t)(ptr + header_log_size + sizeof(uint64_t) - (uint64_t)cm_->conn_buf_));
	if(header_log_size != tailer_log_size)
		return NULL;

	TXHeader* header = (TXHeader*)(ptr + sizeof(uint64_t));
	TXTailer* tailer = (TXTailer*)(ptr + sizeof(uint64_t) + header_log_size - sizeof(TXTailer));

	if(header->magic_num != HEADER_MAGIC_ ||
			tailer->magic_num != TAILER_MAGIC_)
		return NULL;

	if(msg_size)*msg_size = header_log_size;
	return (char*)tailer;
}

void DBLogger::clean_log(int log_id, int p_id, char* log_ptr, char* tailer_ptr, 
                         uint64_t write_epoch) {
	TXHeader* header = (TXHeader*)(log_ptr + sizeof(uint64_t));
  assert(header->magic_num == HEADER_MAGIC_);
	TXTailer* tailer = (TXTailer*)(tailer_ptr);
  assert(tailer->magic_num == TAILER_MAGIC_);

  uint64_t tx_id = header->tx_id;

	bool use_global_seq = header->global_seq;
	uint64_t global_seq = tailer->seq;

	char* ptr = log_ptr + sizeof(uint64_t) + sizeof(TXHeader);
  bool need_version = log_cleaner_->need_version();
	while(ptr + ROUND_UP_BASE <= tailer_ptr) {
		EntryMeta* entry_meta = (EntryMeta*)ptr;
    assert(!entry_meta->clean);
		int table_id = entry_meta->table_id;
		uint32_t size = entry_meta->size;
    uint32_t op_bit = entry_meta->op_bit;
		uint64_t seq = use_global_seq ? global_seq : entry_meta->seq;
		uint64_t key = entry_meta->key;
    int pid = entry_meta->pid;
		char* value_ptr = ptr + sizeof(EntryMeta);

    if (pid == p_id) {
  		// printf("table_id:%d\n", table_id);
  		// printf("key:%lu, 0x%lx\n", key, key);
  		// printf("size:%d\n", size);
  		// printf("seq:%lu\n", seq);
  		// printf("value:%lu\n", *((uint64_t*)value_ptr));
      if (need_version)
  		  log_cleaner_->clean_log(log_id, pid, tx_id, table_id, key, seq, value_ptr, size,
                                op_bit, write_epoch);
      else
        log_cleaner_->clean_log(table_id, key, seq, value_ptr, size);
    }
    entry_meta->clean = true;
		ptr += sizeof(EntryMeta) + size;
	}
}

void DBLogger::print_total_mem(){
	printf("base_ptr:%p\n", base_ + logArea.getBase());
	for(int node_id = 0; node_id < num_nodes_; node_id++){
		print_node_area_mem(node_id);
	}
}

void DBLogger::print_node_area_mem(int node_id){
	for(int worker_id = 0 ; worker_id < txn_nthreads; worker_id++){
		print_thread_area_mem(node_id, worker_id);
	}
}

void DBLogger::print_thread_area_mem(int node_id, int worker_id){
	printf("print thread area memory->node:%d, thread:%d\n",node_id,worker_id);
	char* meta_ptr = base_ + logArea.getMetaBase(node_id, worker_id, 0);
	char* buf_ptr = base_ + logArea.getBufferBase(node_id, worker_id, 0);
	printf("[\n");
	while(1){
		buf_ptr = print_log(buf_ptr);
		if(buf_ptr == NULL)break;
		// printf("buf:%p\n", buf_ptr);
	}
	printf("]\n");
}

char* DBLogger::print_log(char* ptr) {
	char* tailer_ptr = check_log_completion(ptr);
	if(tailer_ptr == NULL)return NULL;
	printf("{\n");
	ptr += sizeof(uint64_t);
	ptr = print_log_header(ptr);
	// printf("tailer:%p\n", tailer_ptr);
	// printf("buf:%p\n", buf_ptr);
	printf("(\n");
	while(ptr + ROUND_UP_BASE <= tailer_ptr){
		ptr = print_log_entry(ptr);
		printf(",\n");
		// printf("buf:%p\n", buf_ptr);
	}
	printf(")\n");
	ptr = print_log_tailer(tailer_ptr);
	ptr += sizeof(uint64_t);
	printf("}\n");
	return ptr;
}

char* DBLogger::print_log_header(char *ptr) {
	TXHeader* header = (TXHeader*)ptr;
	printf("magic_num:%lu\n", header->magic_num);
	printf("global_seq:%lu\n", header->global_seq);
	return ptr += sizeof(TXHeader);
}

char* DBLogger::print_log_entry(char *ptr) { 
	EntryMeta* entry_meta = (EntryMeta*)ptr;
	printf("table_id:%d\n", entry_meta->table_id);
	printf("key:%lu, 0x%lx\n", entry_meta->key, entry_meta->key);
	printf("size:%d\n", entry_meta->size);
	printf("seq:%lu\n", entry_meta->seq);
	ptr += sizeof(EntryMeta);
	if(entry_meta->size == sizeof(uint64_t)){
		printf("value:%lu\n", *((uint64_t*)ptr));
	}
	return ptr + entry_meta->size;
}

char* DBLogger::print_log_tailer(char *ptr) {
	TXTailer* tailer = (TXTailer*)ptr;
	printf("magic_num:%lu\n", tailer->magic_num);
	printf("seq:%lu\n", tailer->seq);
	return ptr += sizeof(TXTailer);
}
	
}
}

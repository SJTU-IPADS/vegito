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

#ifndef NOCC_OLTP_LOG_CLEANER_H
#define NOCC_OLTP_LOG_CLEANER_H

#include "memstore/memdb.h"
#include "backup_store/backup_db.h"
#include "graph/ddl.h"
#include "graph/graph_store.h"

#include "view_manager.h"
#include <map>
#include <vector>

namespace nocc{
namespace oltp{

// the abstract class for cleaning the log entry
// be called after the receive each log entries
class LogCleaner {
 public:
  LogCleaner(bool version = false);
  // should be 0 on success,-1 on error
  virtual int clean_log(int table_id, uint64_t key, uint64_t seq, 
                        char *val,int length) = 0;
  virtual int clean_log(int log_id, int partition_id, int tx_id,
                        int table_id, uint64_t key, uint64_t seq, 
                        char *val,int length, uint32_t op_bit, 
                        uint64_t write_epoch) { assert(false); }

  virtual void balance_index() { }

  virtual void prepare_balance(int num_workers) { }
  virtual void parallel_balance(int worker_id) { }
  virtual void end_balance() { }

  virtual void balance_index(const std::vector<int> &threads) { }

  void add_backup_store(BackupDB *backup_store);

  void add_graph_store(graph::GraphStore *graph, graph::RGMapping *mapping);

  bool need_version() const { return need_version_; }

 protected:
  bool need_version_;
  std::vector<BackupDB *> backup_map_;  // id -> backupDB
  std::vector<BackupDB *> backup_map2_;  // id -> backupDB

  std::vector<graph::GraphStore *> graph_stores_;
  std::vector<graph::RGMapping *> mappings_;
};

}  // namespace oltp
}  // namespace nocc
#endif

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

/*
 * This file contains a db class built from backup_store
 */
#ifndef BACKUP_DB_H_
#define BACKUP_DB_H_

#include <stdint.h>
#include <vector>
#include <unordered_map>

#include "backup_store.h"
#include "backup_index.h"

#define ENCODE_IDX_ID(tid, cid)\
  (uint64_t)(((uint64_t)tid << 32) | (uint64_t)cid)
#define DECODE_IDX_TID(id)  ((uint64_t)id >> 32)  // table id
#define DECODE_IDX_CID(id)  ((uint64_t)id << 32 >> 32)  // column id

enum BackupIndexType {
  NO_BINDEX = 0,
  BINDEX_BTREE,
  // BINDEX_BTREE_ARRAY,  // String b+ tree
  BINDEX_HASH,
  BINDEX_PALMTREE // Only used in vice index or secondary index
};

enum BackupStoreType {
  BSTORE_KV,
  BSTORE_ROW,
  BSTORE_COLUMN,
  BSTORE_COLUMN2
};

class BackupDB {

 public:
  struct Column {
    size_t vlen;
    bool updatable;
    size_t page_size;  // uint64_t(-1) or 0 means infinity, unit: items
    BackupIndexType sec_index;
  };

  struct Schema {
    int table_id;
    size_t klen;
    std::vector<Column> cols;  //  each column
    uint64_t max_items;

    // Which class of underlying store is used
    hash_fn_t hash_fn;
    BackupIndexType index_type;  // primary index
    BackupIndexType sec_index_type;  // secondary index on key
    BackupStoreType store_type;
  };

  BackupDB(int pid, bool ap) 
    : p_id_(pid), is_ap_(ap),
      indices_(20, nullptr) { 
    sec_idx_id_.reserve(20);
    sec_idx_.reserve(20);    
  }

  inline int getPID() const { return p_id_; }
  inline bool isAP() const { return is_ap_; }
  void AddSchema(int tableid, Schema schema);

  // graph
  void AddEdge(int tableid);
  inline uint64_t *getEdge(int tableid, uint64_t row_id) const { 
    return stores_[tableid]->getEdge(row_id);
  }

  char *Get(int tableid, uint64_t key, int columnID, uint64_t version, uint64_t *walk_cnt = nullptr) const;
  char *GetByOffset(int tableid, uint64_t offset, int columnID, uint64_t version) const;

  void UpdateOffset() {
    for (BackupStore *db : stores_) {
      if (db)
        db->updateHeader();
    }
  }

  // return row offsert
  uint64_t Insert(int tableid, uint64_t key, char *value, uint64_t ver = 0) const;
  void Update(int tableid, uint64_t key, char *value, 
              const std::vector<int> &cols, 
              int64_t seq, uint64_t version) const;

  void Put(int tableid, uint64_t key, char *value, bool lock,
           int64_t seq, uint64_t version) const;

  void CopyFrom(const BackupDB &db);

  BackupStore *getStore(int tableid) const { return stores_[tableid]; }
  inline BackupIndex *getIndex(int tableid) const { return indices_[tableid]; }

  BackupIndex *getSecIndex(int tableid, int colid = key_idx_) const; 

  uint64_t getOffset(int tableid, uint64_t key, uint64_t ver = -1) const; 

  const std::vector<BackupStore *> &get_stores() const { return stores_; }

  bool verify_sec_index() {
    bool flag = false;
    for (const SecIndexInfo &info : sec_idx_) {
        info.index->VerifyIndex(); 
        flag = true;
    } 
    assert(flag);
    return flag;
  }

  // aborted function
#if 0
  void balance_index(const std::vector<int> &cpus) {
    assert(false);
    for (auto &table_pair : sec_indices_) {
      for (auto &col_pair : table_pair.second) {
        const SecIndexInfo &info = col_pair.second;
        info.index->Balance(cpus); 
      }
    } 
  }
#endif

  void balance_index() {
    for (const SecIndexInfo &info : sec_idx_) {
      info.index->Balance(); 
    } 
#if BTREE_COUNT == 1 // 1 for verify btree index
    if (verify_sec_index()) {
      printf("Secondary index verified!\n");
    }
#endif
  }

  // clean the pages whose version < `version`
  void GC(uint64_t version) {
    for (BackupStore *db : stores_) {
      if (db)
        db->gc(version);
    }
  }
  
  void prepare_balance(int num_workers) { 
    for (const SecIndexInfo &info : sec_idx_) {
      info.index->PrapareBalance(num_workers); 
    } 
  }

  void parallel_balance(int worker_id) {
    for (const SecIndexInfo &info : sec_idx_) {
      info.index->ParallelBalance(worker_id); 
    } 
  }
  
  void end_balance() { 
    for (const SecIndexInfo &info : sec_idx_) {
      info.index->EndBalance(); 
    } 
#if FRESHNESS  // for freshness
    for (BackupStore *db : stores_) {
      if (db)
        db->updateHeader();
    }
#endif

#if BTREE_COUNT == 1 // 1 for verify btree index
    if (verify_sec_index()) {
      printf("Secondary index verified!\n");
    }
#endif
  }
 private:

  inline bool checkValid(BackupNode *node, uint64_t version) const {
    assert(node);
    assert(node->value_offset != -1);
    if (node->min_version > version) return false;

    return true;
  }

  struct SecIndexInfo {
    size_t field_off;
    size_t vlen;
    BackupIndex *index;

    SecIndexInfo()
      : field_off(0), vlen(0), index(nullptr) { }

    SecIndexInfo(size_t off, size_t v, BackupIndex *ptr)
      : field_off(off), vlen(v), index(ptr) { }
  };

  static const int key_idx_ = 1000;  // XXX: hard code, number of columns not larger than 1000

  const int p_id_;
  const bool is_ap_;

  // std::vector<int> schemas_;
  std::vector<BackupStore *> stores_;
  // std::unordered_map<int, BackupIndex *> indices_;
  std::vector<BackupIndex *> indices_;
  // std::unordered_map<int, 
  //                    std::unordered_map<int, SecIndexInfo>> sec_indices_;
  std::vector<uint64_t> sec_idx_id_;  // vector of <table_id << 32| column_id>
  std::vector<SecIndexInfo> sec_idx_;
};

#endif


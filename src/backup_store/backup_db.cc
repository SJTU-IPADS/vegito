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

#include "backup_db.h"

#include "backup_index.h"
#include "backup_index_hash.h"
#include "backup_tree.h"

#include "backup_store.h"
#include "backup_store_kv.h"
#include "backup_store_row.h"
#include "backup_store_col.h"
#include "backup_store_col2.h"

#include "kv_template.h"

#include "util/util.h"
#include "util/timer.h"

#include "custom_config.h"

#include "framework/backup_worker.h"
#include "framework/framework_cfg.h"

#include <vector>
#include <stdexcept>
using namespace std;
using namespace nocc::util;
using namespace nocc::framework;

namespace nocc {
  extern __thread nocc::oltp::LogWorker *backup_worker;
  extern size_t backup_nthreads;
}

void BackupDB::AddSchema(int tableid, Schema schema) {
  schema.table_id = tableid;
  // schemas_[tableid] = schema;
  if (tableid >= stores_.size()) {
    stores_.resize(tableid + 1);
  }

  vector<size_t> vlens;
  for (int i = 0; i < schema.cols.size(); ++i)
    vlens.push_back(schema.cols[i].vlen);

  switch(schema.index_type) {
    case BINDEX_BTREE:
      indices_[tableid] = (BackupIndex *) new BackupTree();
      break;
    case BINDEX_HASH:
      indices_[tableid] =
          static_cast<BackupIndex *>(new BackupIndexHash(schema.hash_fn,
                                                         schema.max_items));
      break;
    default:
      printf("Not implement this backup index type: %d\n", schema.index_type);
      assert(false);
  }

  switch(schema.store_type) {
    case BSTORE_KV: {
      BackupStoreKV *kv = new BackupStoreKV(vlens, schema.max_items);
      stores_[tableid] = static_cast<BackupStore *>(kv);
      break;
    }
    case BSTORE_ROW: {
      BackupStoreRow *row_store = new BackupStoreRow(vlens,
                                                     schema.max_items);
      stores_[tableid] = static_cast<BackupStore *>(row_store);
      break;
    }
    case BSTORE_COLUMN: {
      BackupStoreCol *col_store = new BackupStoreCol(schema);
      stores_[tableid] = static_cast<BackupStore *>(col_store);
      break;
    }
    case BSTORE_COLUMN2: {
      BackupStoreCol2 *col2_store = new BackupStoreCol2(schema);
      stores_[tableid] = static_cast<BackupStore *>(col2_store);
      break;
    }
    default:
      printf("Not implement this backup store type: %d\n", schema.store_type);
      assert(false);
  }

  if (schema.sec_index_type != NO_BINDEX) {
    BackupIndex *ksec_index = nullptr;
    switch (schema.sec_index_type) {
      case BINDEX_BTREE:
        ksec_index = (BackupIndex *) new BackupTree();
        break;
  
      default:
        printf("Not implement this backup index type: %d\n", schema.sec_index_type);
        assert(false);
    }
  
    int kidx = key_idx_;
    sec_idx_id_.emplace_back(ENCODE_IDX_ID(tableid, kidx));
    sec_idx_.emplace_back(uint64_t(0), schema.klen, ksec_index);
    // sec_indices_[tableid][kidx] = { uint64_t(0), schema.klen, ksec_index }; 
  }

  size_t field_off = 0;
  for (int i = 0; i < schema.cols.size(); ++i, field_off += vlens[i]) {
    const BackupDB::Column &col = schema.cols[i];
    BackupIndex *index = nullptr;

    if (col.sec_index == NO_BINDEX) continue;

    switch (col.sec_index) {
      case BINDEX_BTREE:
        index = (BackupIndex *) new BackupTree();
        break;
#if 0 
      case BINDEX_HASH:
        index =
            static_cast<BackupIndex *>(new BackupIndexHash(schema.hash_fn,
                                                           schema.max_items));
        break;
#endif
      default:
        printf("Not implement this backup index type: %d\n", col.sec_index);
        assert(false);
    }

    sec_idx_id_.emplace_back(ENCODE_IDX_ID(tableid, i));
    sec_idx_.emplace_back(field_off, vlens[i], index);
    // sec_indices_[tableid][i] = { field_off, vlens[i], index }; 

  }
  
}

#if 0
BackupIndex::Iterator *BackupDB::getSecIter(int tid, int cid) const {
  BackupIndex *sec_index = sec_indices_[tid][cid];
  if (!sec_index) return NULL;
  return sec_index->GetIterator();
}
#endif

void BackupDB::Insert(int tableid, uint64_t key, char *value, uint64_t ver) const {
  // prepare store and index
  BackupStore *store = stores_[tableid];
  BackupIndex *index = indices_[tableid];
  assert(store && index);

  BackupNode *index_node = index->GetWithInsert(key);
  // assert(index_node && index_node->value_offset == -1);

#if 0 
  if (ver >= 2) {
    store->getFakeOffset();
    return;
  }
#endif

  uint64_t offset = store->getNewOffset();
  // if (ver >= 2) return;
  int64_t seq = 2;  // init
#if 1 // 0 for debug
  // if (tableid != 6 || ver == 0) 
  // if (tableid ==6 && ver != 0) ver = -ver;
  {
  store->insert(offset, key, value, seq, ver);
  }
  // if (tableid ==6 && ver != 0) ver = -ver;
#endif

  // update index
  index_node->value_offset = offset;
  index_node->min_version = ver; // TODO: perf

  // if (tableid == 6 && ver != 0) return;
  // insert secondary index
  // auto mit = sec_indices_.find(tableid);
  // if (mit == sec_indices_.end()) return;

  // assert(is_ap_);

  // const unordered_map<int, SecIndexInfo> &sec_indices = mit->second;
  // for (auto it = sec_indices.begin(); it != sec_indices.end(); ++it) 
  for (int i = 0; i < sec_idx_id_.size(); ++i)
  {
    uint64_t index_id = sec_idx_id_[i];
    if (DECODE_IDX_TID(index_id) != tableid) continue;

    int colid = DECODE_IDX_CID(index_id);
    const SecIndexInfo &info = sec_idx_[i];

    if (colid == key_idx_) {
      // secondary index on key
      assert(info.vlen == 8);
      if (ver == 0 || !config.isLazyIndex() || !is_ap_) {
        info.index->Put(key, index_node);
      } else {
        // lazy insert
        // printf("insert to tid %d cid %d\n", tableid, colid);
        info.index->LazyInsert(key, index_node); 
      }
    } else {
      assert(false);  // not support lazy

      // get offset and get value
      if (info.vlen == 4) {
        int32_t ikey = *(int32_t *) (value + info.field_off);
        info.index->Put((uint64_t)ikey, index_node);
      } else if (info.vlen == 8) {
        int64_t ikey = *(int64_t *) (value + info.field_off);
        info.index->Put((uint64_t)ikey, index_node);
      } else {
        assert(false);
      } 
    }
  }
 
}

void BackupDB::Update(int tableid, uint64_t key, char *value, 
                      const std::vector<int> &cols, 
                      int64_t seq, uint64_t version) const {
  assert(seq > 2);

  // prepare store and index
  BackupStore *store = stores_[tableid];
  BackupIndex *index = indices_[tableid];
  assert(store && index);

  BackupNode *index_node = index->Get(key);
  assert(index_node && index_node->value_offset != -1);

  lock32(&index_node->lock);
  {
    // get the offset and update storage
    uint64_t offset = index_node->value_offset;
    // store->putByOffset(offset, key, value, seq, version);
    store->update(offset, cols, value, seq, version);
  }
  unlock32(&index_node->lock);

  // TODO: insert secondary index
}

void BackupDB::Put(int tableid, uint64_t key, char *value, bool lock,
                   int64_t seq, uint64_t version) const {
  assert(false);

  assert(seq > 2);

  // prepare store and index
  BackupStore *store = stores_[tableid];
  BackupIndex *index = indices_[tableid];
  assert(store && index);

  BackupNode *index_node = NULL;
  index_node = index->Get(key);

  assert(index_node && index_node->value_offset != -1);

  if (lock)
    lock32(&index_node->lock);

  // get the offset
  uint64_t offset = index_node->value_offset;

  // insert into storage
  store->putByOffset(offset, key, value, seq, version);

  if (lock)
    unlock32(&index_node->lock);

  // TODO: insert secondary index
}

char *BackupDB::Get(int tableid, uint64_t key, int colid, uint64_t version, uint64_t *walk_cnt) const {
  // prepare store and index
  BackupStore *store = stores_[tableid];
  BackupIndex *index = indices_[tableid];
  assert(store && index);

  BackupNode *index_node = index->Get(key);
#if 0
  bool isValid = checkValid(index_node, version);
  if (!isValid) return nullptr;
#endif
  return store->getByOffset(index_node->value_offset, colid, version, walk_cnt);
}

char *BackupDB::GetByOffset(int tableid, uint64_t offset, int colid, uint64_t version) const {
  // prepare store and index
  BackupStore *store = stores_[tableid];
  assert(store);

  return store->getByOffset(offset, colid, version);
}

uint64_t BackupDB::getOffset(int tableid, uint64_t key, uint64_t ver) const {
  uint64_t off = 0;
  BackupIndex *index = indices_[tableid];
  if (!index) assert(false);
  BackupNode *node = index->Get(key);
  assert(node);
#if 1
  off = node->value_offset;
  assert(off != -1);
  // if (node->min_version > ver) return -1;
#endif
  return off;
}


BackupIndex *BackupDB::getSecIndex(int tableid, int colid) const {
  int i = 0;
  uint64_t id = ENCODE_IDX_ID(tableid, colid);
  for (i = 0; i < sec_idx_id_.size(); ++i) {
    if (sec_idx_id_[i] == id) break;
  }
  if (i == sec_idx_id_.size()) return nullptr;

  BackupIndex *res = sec_idx_[i].index;
  assert(res != nullptr);
  return res;
}

void BackupDB::CopyFrom(const BackupDB &db) {
  for (int i = 0; i <  stores_.size(); ++i) {
    BackupStore *tbl = stores_[i];
    if (i == 4 || !tbl) continue;  // XXX: ignore NEWO
    uint64_t off = tbl->copy(db.stores_[i]);

    printf("[BackupDB::CopyFrom] Copy table %d with %lu rows\n", 
           i, off);
  }
}

#if 0
char *BackupDB::Get(int tableid, uint64_t key, uint64_t version) const {
  // prepare store and index
  BackupStore *store = stores_[tableid];
  BackupIndex *index = indices_[tableid];
  assert(store != NULL && index != NULL);

  BackupNode *index_node = index->Get(key);
  bool isValid = checkValid(tableid, index_node, version);
  if (!isValid)
    return NULL;
  return store->getByOffset(index_node->value_offset, version);
}

bool BackupDB::Exist(int tableid, uint64_t key, uint64_t version) const {
  // prepare store and index
  BackupStore *store = stores_[tableid];
  BackupIndex *index = indices_[tableid];
  assert(store != NULL && index != NULL);

  BackupNode *index_node = index->Get(key);
  bool isValid = checkValid(tableid, index_node, version);

  return isValid;
}
#endif

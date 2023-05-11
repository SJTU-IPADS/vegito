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

#ifndef B_KV_TEMPLATE_H_
#define B_KV_TEMPLATE_H_

#include "backup_store.h"
#include <vector>

// T is value type
template <class T>
class KV : public BackupStore {
 public:
  KV(std::vector<size_t> v, uint64_t max_items)
    : BackupStore(v, max_items) { }

 private:
  struct Node {
    uint64_t ver;  // version
    Node *prev;    // previous version
    T val;         // value
    
    Node(uint64_t v, Node *p) 
      : ver(v), prev(p) { }
  };

  std::vector<Node *> nodes_; 

#if 0
 public:

  // for insert or update 
  virtual void putByOffset(uint64_t offset, uint64_t key, char *val,
                           int64_t seq, uint64_t version);
  // get cursor
  virtual std::unique_ptr<RowCursor> getRowCursor(uint64_t ver) const {
    RowCursor *c = new Cursor(*this, ver);
    return std::unique_ptr<RowCursor>(c);
  }
  
  virtual uint64_t locateCol(int col_id, uint64_t width) const;


  virtual char *getByOffset(uint64_t offset, uint64_t version);
  virtual char *getByOffset(uint64_t offset, int columnID, uint64_t version);
  

 private:
  struct ValueNode {
    uint64_t ver = 0;
    ValueNode *prev = nullptr;
    char val[0];  // flexible array

    ValueNode(uint64_t v, ValueNode *p)
      : ver(v), prev(p) { }
  };

  struct RowMeta {
    uint64_t key;
    int64_t seq = -1;
    uint64_t min_ver;
    ValueNode *value = nullptr;
  } __attribute__ ((aligned (CACHE_LINE_SZ)));

  ValueNode *getValueNode_(uint64_t v, ValueNode *p) const;

  const uint64_t val_len_;

  std::vector<RowMeta> meta_;
  
 public:

  class Cursor : public BackupStore::RowCursor {
   public:
    Cursor(const KV &store, uint64_t ver);
    virtual void seekOffset(uint64_t begin, uint64_t end);
    virtual bool nextRow();
    virtual uint64_t key() const {
      return kv_.meta_[cur_].key;
    }
    virtual char *value() const;

   private:
    const KV &kv_;
    const uint64_t ver_;

    // offset
    uint64_t begin_;
    uint64_t end_;
    uint64_t cur_;
  };

  friend class Cursor;
#endif
};

#endif  // B_KV_TEMPLATE_H_


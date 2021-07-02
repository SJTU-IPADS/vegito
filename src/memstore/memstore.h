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

#ifndef DRTMMEMSTORE
#define DRTMMEMSTORE

#include "all.h"
#include "util/spinlock.h"

#include "rdmaio.h"

#include <stdlib.h>

#define MEMSTORE_MAX_TABLE 16

struct /* alignas(CACHE_LINE_SZ) */ MemNode {
  volatile uint64_t lock;
  uint64_t seq;
  uint64_t* value; // pointer to the real value (for row-stored)
                   // 1: logically delete 2: Node is removed from memstore
  union {
    uint64_t  off;
    uint64_t* old_value;
  };
#ifdef RAD_TX
  uint64_t read_ts;
  volatile uint64_t read_lock;
#endif

  // uint64_t padding[4];

  inline MemNode(uint64_t *v)
    : lock(0), seq(0), value(v)
#ifdef RAD_TX
    , read_ts(0), read_lock(0)
#endif
  { }

  inline MemNode()
    : MemNode(nullptr) { }

  inline void init(uint64_t *v) {
    lock = 0;
    seq  = 0;
    value = v;
#ifdef RAD_TX
    read_ts = 0;
    read_lock(0);
#endif
  }
};


class Memstore {
 public:
  class Iterator {
  public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    Iterator() {}

    virtual bool Valid() = 0;

    // REQUIRES: Valid()
    virtual MemNode* CurNode() = 0;

    virtual uint64_t Key() = 0;

    // Advances to the next position.
    // REQUIRES: Valid()
    virtual bool Next() = 0;

    // Advances to the previous position.
    // REQUIRES: Valid()
    // REQUIRES: Valid()
    virtual bool Prev() = 0;

    // Advance to the first entry with a key >= target
    virtual void Seek(uint64_t key) = 0;

    virtual void SeekPrev(uint64_t key) = 0;

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    virtual void SeekToFirst() = 0;

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    virtual void SeekToLast() = 0;

    virtual uint64_t* GetLink() = 0;

    virtual uint64_t GetLinkTarget() = 0;
  };

  virtual Iterator *GetIterator() { return NULL;}
  // XXX: this method "Put" is called only during data loading
  virtual MemNode* Put(uint64_t k, uint64_t* val) = 0;
  virtual MemNode* Get(uint64_t key) = 0;
  virtual bool     CompareKey(uint64_t k0,uint64_t k1) { return true;}

  MemNode* GetWithInsert(uint64_t key,char *val = NULL) {
    return _GetWithInsert(key,val);
  }
  virtual MemNode* _GetWithInsert(uint64_t key,char *val) = 0;

  // return the remote offset of the true value
  virtual uint64_t RemoteTraverse(uint64_t key,rdmaio::Qp *qp) {
    assert(false); // If supported, then shall re-write
    return 0;
  }

  virtual void Print() { }
};

#endif

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

#ifndef BACKUP_INDEX_H_
#define BACKUP_INDEX_H_

#include "all.h"

#include <memory>
#include <vector>

struct BackupNode {
  uint64_t value_offset;  // for column-stored
  // uint64_t pedding[7];
  uint32_t lock;
  uint32_t min_version;
  // uint64_t pedding2[6];

  BackupNode()
    : lock(0), value_offset(-1) { }
} ;
// __attribute__ ((aligned (CACHE_LINE_SZ)));

typedef uint64_t (*hash_fn_t) (uint64_t);

class BackupIndex {
 public:
  class Iterator {
   public:

    virtual uint64_t CurOffset() { assert(false); }

    virtual uint64_t Key() = 0;

    // Advances to the next position.
    // REQUIRES: Valid()
    virtual bool Next() = 0;

    // Advance to the first entry with a key >= target
    virtual void Seek(uint64_t key) = 0 ;

    // The returned iterator is not valid.
    virtual bool Valid() { assert(false); }

    // REQUIRES: Valid()
    virtual BackupNode* CurNode() { assert(false); };

    // Advances to the previous position.
    // REQUIRES: Valid()
    virtual bool Prev() { assert(false); }

    virtual void SeekPrev(uint64_t key) { assert(false); };

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    virtual void SeekToFirst() { assert(false); };

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    virtual void SeekToLast() { assert(false); };

  };

  virtual std::unique_ptr<Iterator> getIterator(uint64_t ver) { 
    return nullptr; 
  }
    
  virtual void VerifyIndex() const { assert(false); };

  // virtual Iterator *GetIterator() = 0;
  virtual BackupNode* Get(uint64_t key) = 0;
  virtual BackupNode* GetWithInsert(uint64_t key) = 0;
  virtual void Put(uint64_t key, BackupNode *node) { }

  virtual void LazyInsert(uint64_t key, BackupNode *node) {
    assert(false);
  }

  // single thread
  virtual void Balance() { }
  
  virtual void PrapareBalance(int num_worker) { }
  virtual void ParallelBalance(int worker_id) { }
  virtual void EndBalance() { }

  virtual void Balance(const std::vector<int> &cpus) { }

 };

#endif

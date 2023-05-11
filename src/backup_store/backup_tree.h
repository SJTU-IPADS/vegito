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

#ifndef BACKUP_TREE_H_
#define BACKUP_TREE_H_

#include "backup_index.h"
#include "structure/stx/btree_map.h"

#include "util/rtm.h"

class BackupTree : public BackupIndex {
 public:
   BackupTree() { }
   virtual ~BackupTree() { }
   virtual BackupNode* Get(uint64_t key) {
    RTMScope rtm(&rtmlock_);
     auto it = map_.find(key);
     if (it == map_.end()) return nullptr;

     return it->second;
   }

   virtual BackupNode* GetWithInsert(uint64_t key) {
     RTMScope rtm(&rtmlock_);
     auto it = map_.find(key);
     if (it != map_.end()) {
       return it->second;
     } else {
       BackupNode *node = new BackupNode();
       map_.insert(key, node);
       return node;
     }
   }

   virtual void LazyInsert(uint64_t key, BackupNode *node) {
     map_.insert_lazy(key, node);
   }
   
   virtual void Balance() {
     map_.balance();
   }

   virtual void Balance(const std::vector<int> &cpus) {
     map_.balance(cpus);
   }

   virtual void PrapareBalance(int num_worker) {
     map_.prepare_balance(num_worker);
   }

   virtual void ParallelBalance(int worker_id) {
     map_.parallel_balance(worker_id);
   }

   virtual void EndBalance() {
     map_.end_balance();
   }

   virtual void Put(uint64_t key, BackupNode *node) {
     RTMScope rtm(&rtmlock_);
     map_.insert(key, node);
   }
 
   virtual std::unique_ptr<Iterator> getIterator(uint64_t ver) { 
     Iterator *it = new Iter(*this, ver, &rtmlock_);
     return std::unique_ptr<Iterator> (it);
   }

   virtual void VerifyIndex() const override {
     map_.verify();
   } 

 private:
/** Generates default traits for a B+ tree used as a map. It estimates leaf and
 * inner node sizes by assuming a cache line size of 256 bytes. */
  template <typename _Key, typename _Data>
  class map_traits {
   public:
    /// If true, the tree will self verify it's invariants after each insert()
    /// or erase(). The header must have been compiled with BTREE_DEBUG defined.
    static const bool selfverify = false;

    /// If true, the tree will print out debug information and a tree dump
    /// during insert() or erase() operation. The header must have been
    /// compiled with BTREE_DEBUG defined and key_type must be std::ostream
    /// printable.
    static const bool debug = false;

    /// Number of slots in each leaf of the tree. Estimated so that each node
    /// has a size of about 256 bytes.
    static const int leafslots = BTREE_MAX(8, 256 / (sizeof(_Key) + sizeof(_Data)));

    /// Number of slots in each inner node of the tree. Estimated so that each node
    /// has a size of about 256 bytes.
    static const int innerslots = BTREE_MAX(8, 256 / (sizeof(_Key) + sizeof(void*)));

    /// As of stx-btree-0.9, the code does linear search in find_lower() and
    /// find_upper() instead of binary_search, unless the node size is larger
    /// than this threshold. See notes at
    /// http://panthema.net/2013/0504-STX-B+Tree-Binary-vs-Linear-Search
    static const size_t binsearch_threshold = 256;
    
    /// Used for lazy insert
    static const bool lazy = true;
    static const int  lazyslots = 20;
  };

  using Map = stx::btree_map<uint64_t, BackupNode *, std::less<uint64_t>, 
                             map_traits<uint64_t, BackupNode *>>;

  Map map_;
  SpinLock rtmlock_;
 
 // iterator 
 public:
  class Iter : public Iterator {
   public:
    // The returned iterator is not valid.
    Iter(const BackupTree &tree, uint64_t ver, SpinLock *lock)
      : tree_(tree), ver_(ver), tree_lock_(lock),
        iter_(tree_.map_.begin()) {
    }

    virtual uint64_t Key() {
      return iter_->first; 
    }
    
    virtual bool Next() {
      while (1) {
        ++iter_; 
        if (iter_ == tree_.map_.end()) return false;
        BackupNode *node = iter_->second;
        assert(node);
        if (node->min_version <= ver_) return true;
      }

      assert(false);  // not reachable
    }

    virtual uint64_t CurOffset() {
      return iter_->second->value_offset;
    }

    virtual void Seek(uint64_t key) {
      RTMScope rtm(tree_lock_);
      iter_ = tree_.map_.find(key);
      assert(iter_ != tree_.map_.end());
    }

  private:
    const BackupTree &tree_;
    const uint64_t ver_;
    Map::const_iterator iter_;
    SpinLock *tree_lock_;
    // LeafNode* node_;
    // uint64_t seq_;
    // int leaf_index;
    // uint64_t key_;
    // BackupNode *value_;
    // Intentionally copyable
  };
};

#endif  // BACKUP_TREE_H_

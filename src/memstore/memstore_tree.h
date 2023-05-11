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

#pragma once

#include "memstore.h"
#include "structure/stx/btree_map.h"

#include "util/rtm.h"

#include <iostream>

// split Put and Get
class MemstoreTree : public Memstore {

 public:
  virtual MemNode* Get(uint64_t key) {
    MemNode *node = nullptr;
#if 0
    {
      Map::const_iterator it;
      RTMScope rtm(&rtmlock_);
      it = map_.find(key);
      if (it == map_.end()) assert(false);
      node = it->second;
    }
#else
    {
      RTMScope rtm(&rtmlock_);
      node = map_.get(key);
    }
#endif
    return node;
  }

  virtual MemNode *Put(uint64_t k, uint64_t *val) {
#if 1
    // MemNode *node = new MemNode(val);
    // MemNode *node = new MemNode();
    if (nodes_ == nullptr || node_i_ == max_nodes) {
      nodes_ = new MemNode[max_nodes];
      node_i_ = 0;
    }

    assert(node_i_ < max_nodes);

    MemNode *node = &nodes_[node_i_++];
    node->init(val);
    assert(node->seq == 0);
#else
    char *buf = new char[sizeof(MemNode)];
    // void *buf = malloc(sizeof(MemNode));
    MemNode *node = new (buf) MemNode(val);
#endif

    if (hint_leaf == nullptr) {
      map_.prepare_leaf_node(&hint_leaf); 
    }

    {
      RTMScope rtm(&rtmlock_);
      auto res =  map_.insert(k, node, &hint_leaf);
      // assert(res.second);
      // assert(node->seq == 0);
    }

    assert(node->seq == 0);

    return node;
  }

  virtual MemNode* _GetWithInsert(uint64_t key,char *val) { 
    assert (false);
    return nullptr;
  }
  
  virtual Iterator *GetIterator() { return new Iter(*this); }

  virtual void Print() {
    map_.print(std::cout);
  } 
 
 private:

  static const int max_nodes = 8;
  thread_local static MemNode *nodes_;
  thread_local static int node_i_;
  thread_local static void *hint_leaf;

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
    // static const int leafslots = BTREE_MAX(8, 256 / (sizeof(_Key) + sizeof(_Data)));
    static const int leafslots = 15;

    /// Number of slots in each inner node of the tree. Estimated so that each node
    /// has a size of about 256 bytes.
    // static const int innerslots = BTREE_MAX(8, 256 / (sizeof(_Key) + sizeof(void*)));
    static const int innerslots = 15;

    /// As of stx-btree-0.9, the code does linear search in find_lower() and
    /// find_upper() instead of binary_search, unless the node size is larger
    /// than this threshold. See notes at
    /// http://panthema.net/2013/0504-STX-B+Tree-Binary-vs-Linear-Search
    static const size_t binsearch_threshold = 256;
    
    /// Used for lazy insert
    static const bool lazy = false;
    static const int  lazyslots = 0;
  };

  using Map = stx::btree_map<uint64_t, MemNode *, std::less<uint64_t>, 
                             map_traits<uint64_t, MemNode *>>;

  Map map_;
  SpinLock rtmlock_;
 
 // iterator 
 public:
  class Iter : public Iterator {
   public:
    // The returned iterator is not valid.
    Iter(MemstoreTree &tree)
      : tree_(tree), iter_(tree_.map_.begin()), 
        valid_(tree_.map_.size() != 0) { }
    
    virtual bool Valid() { return valid_; }

    // REQUIRES: Valid()
    virtual MemNode* CurNode() { 
      assert(valid_);
      MemNode *node = iter_->second;
      assert(node);
      return node; 
    }

    virtual uint64_t Key() { return iter_->first; }

    // Advances to the next position.
    // REQUIRES: Valid()
    virtual bool Next() {
      RTMScope rtm(&tree_.rtmlock_);
      ++iter_;
      if (iter_ == tree_.map_.end()) valid_ = false;

      return true;
    }

    // Advances to the previous position.
    // REQUIRES: Valid()
    virtual bool Prev() {
      RTMScope rtm(&tree_.rtmlock_);
      if (iter_ == tree_.map_.begin()) valid_ = false;
      --iter_;

      return true;
    }

    // Advance to the first entry with a key >= target
    virtual void Seek(uint64_t key) { 
      RTMScope rtm(&tree_.rtmlock_);
      iter_ = tree_.map_.lower_bound(key);
      valid_ =  (iter_ != tree_.map_.end());
    }

    virtual void SeekPrev(uint64_t key) { assert(false); }

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    virtual void SeekToFirst() { 
      iter_ = tree_.map_.begin(); 
      valid_ =  (iter_ != tree_.map_.end());
    }

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    virtual void SeekToLast() { assert(false); }

    virtual uint64_t* GetLink() {
      // assert(false);
      return (uint64_t *) iter_.get_seq_ptr();
    }

    virtual uint64_t GetLinkTarget() {
      // assert(false);
      return *(iter_.get_seq_ptr());
    }

   private:
    MemstoreTree &tree_;
    Map::const_iterator iter_;
    bool valid_;
  };

};

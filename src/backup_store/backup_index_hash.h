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

#ifndef BACKUP_INDEX_HASH_H_
#define BACKUP_INDEX_HASH_H_

#include "backup_index.h"
#include "util/rtm.h"
#include <unordered_map>

class BackupIndexHash : public BackupIndex {

 public:
  BackupIndexHash(hash_fn_t hash_fn, size_t max_items);
  ~BackupIndexHash();

  virtual BackupNode* Get(uint64_t key);
  virtual BackupNode* GetWithInsert(uint64_t key);

  virtual Iterator *GetIterator() { assert(false); }

 private:
  const hash_fn_t hash_fn_;
  const size_t max_items_;
  BackupNode *nodes_;
};

class BackupMap : public BackupIndex {

 public:
  BackupMap(size_t max_items) { 
    map_.reserve(max_items);
  }

  ~BackupMap() { }

  virtual BackupNode* Get(uint64_t key) {
    RTMScope rtm(&lock_);
    auto it = map_.find(key);
    if (it == map_.end()) return nullptr;
    return it->second;
  }
  virtual BackupNode* GetWithInsert(uint64_t key) {
    RTMScope rtm(&lock_);
    BackupNode *node = nullptr;
    auto it = map_.find(key);
    if (it == map_.end()) {
      node = new BackupNode();
      map_[key] = node;
    } else {
      node = it->second;
    }
    return node;
  }

  virtual Iterator *GetIterator() { assert(false); }

 private:
  SpinLock lock_;
  std::unordered_map<uint64_t, BackupNode *> map_;
};

class BackupHash : public BackupIndex {
  
 public:
  typedef BackupNode node_t;

  BackupHash(int len, char *arr) 
    : entrysize((((sizeof(node_t) - 1) >> 3) + 1) << 3),
      length(len),
      Logical_length(length * 1/CLUSTER_H),
      indirect_length(length * 1/2),
      total_length(length + indirect_length),
      header_size(sizeof(HeaderNode)),
      data_size(sizeof(DataNode) + entrysize),
      size(indirect_length * header_size +  length * data_size),
      free_indirect(Logical_length),
      free_data(indirect_length),
      array(arr? arr : (char *) malloc(size))
  {
    
  }

  ~BackupHash() { }

  virtual BackupNode* Get(uint64_t key) {
    return (BackupNode *) get_(key);
  }

  virtual BackupNode* GetWithInsert(uint64_t key) {
    return (BackupNode *) getWithInsert_(key);
  }

  virtual Iterator *GetIterator() { assert(false); }

 private: 

  static const int CLUSTER_H = 8;

  struct HeaderNode {
    uint64_t next;
    uint64_t keys[CLUSTER_H];
    uint64_t indexes[CLUSTER_H];
  };
  struct DataNode {
    uint64_t key;
    bool valid;
  };

  static inline uint64_t MurmurHash64A (uint64_t key, unsigned int seed)  {

    const uint64_t m = 0xc6a4a7935bd1e995;
    const int r = 47;
    uint64_t h = seed ^ (8 * m);
    const uint64_t * data = &key;
    const uint64_t * end = data + 1;

    while(data != end)  {
      uint64_t k = *data++;
      k *= m;
      k ^= k >> r;
      k *= m;
      h ^= k;
      h *= m;
    }

    const unsigned char * data2 = (const unsigned char*)data;

    switch(8 & 7)   {
    case 7: h ^= uint64_t(data2[6]) << 48;
    case 6: h ^= uint64_t(data2[5]) << 40;
    case 5: h ^= uint64_t(data2[4]) << 32;
    case 4: h ^= uint64_t(data2[3]) << 24;
    case 3: h ^= uint64_t(data2[2]) << 16;
    case 2: h ^= uint64_t(data2[1]) << 8;
    case 1: h ^= uint64_t(data2[0]);
      h *= m;
    };

    h ^= h >> r;
    h *= m;
    h ^= h >> r;

    return h;
  }

  inline uint64_t GetHash(uint64_t key) const {
    return MurmurHash64A(key, 0xdeadbeef) % Logical_length;
  }

  inline DataNode * getDataNode(int i) const {
    return (DataNode *)(array + getDataNode_loc(i));
  }

  inline uint64_t getDataNode_loc(int i) const {
    return indirect_length * header_size +  (i-indirect_length) * data_size;
  }

  inline uint64_t getHeaderNode_loc(int i) const {
    return i * header_size;
  }

  void insert_(uint64_t key, void* val) {
    if(free_data == total_length){
      //	printf("fail when inserting %lldd\n",key);
      assert(false);
    }
    uint64_t hash = GetHash(key);
    HeaderNode * node =(HeaderNode *) (array+hash*header_size);
    while(node->next !=0){
      node =(HeaderNode *) (array+(node->next)*header_size);
    }
    for(int i=0;i<CLUSTER_H;i++){
      if(node->indexes[i]==0){
        DataNode * free_node=getDataNode(free_data);
        free_node->key=key;
        memcpy((void*)(free_node+1),val,entrysize);
        node->keys[i]=key;
        node->indexes[i]=free_data;
        free_data ++ ;
        return ;
      }
    }
    if(free_indirect == indirect_length){
      //	printf("fail when allocating indirect node,key is %lld\n",key);
      assert(false);
    }
    node->next = free_indirect;
    node =(HeaderNode *) (array+free_indirect*header_size);
    free_indirect++;
    DataNode * free_node=getDataNode(free_data);
    free_node->key=key;
    memcpy((void*)(free_node+1),val,entrysize);
    node->keys[0]=key;
    node->indexes[0]=free_data;
    free_data ++ ;
    return;
  }

  uint64_t read_(uint64_t key) {
    uint64_t hash = GetHash(key);
    HeaderNode * node =(HeaderNode *) (array+hash*header_size);
    int count=0;
    while(true){
      count++;
      for(int i=0;i<CLUSTER_H;i++){
        if(node->keys[i]==key && node->indexes[i]!=0){
          DataNode* datanode=getDataNode(node->indexes[i]);
          return count;
        }
      }
      if(node->next != 0)
        node = (HeaderNode *) (array+(node->next)*header_size);
      else{
        assert(false);
      }
    }
  }

  char *get_(uint64_t key) {
    uint64_t index = GetHash(key);
    uint64_t loc   = getHeaderNode_loc(index);

    char *res = nullptr;

    HeaderNode* node = (HeaderNode*)(array + loc);

    while(1) {
      for (uint i = 0; i < CLUSTER_H; i++) {
        if (node->keys[i] == key && node->indexes[i] != 0) {
          loc = getDataNode_loc(node->indexes[i]);
          loc += sizeof(DataNode);
          res =  (char *)(array + loc);
          return res;
        }
      }

      if (node->next != 0 ) {
        // jump to another node
        loc = getHeaderNode_loc(node->next);
        node = (HeaderNode *) (array + loc);
      } else {
        return nullptr;  // not find
      }
    }

    return res;
  }

  char *getWithInsert_(uint64_t key) {

    uint64_t index = GetHash(key);
    uint64_t loc   = getHeaderNode_loc(index);

    char *res = nullptr;

    HeaderNode* node = (HeaderNode*)(array + loc);

    // RTMScope rtm(&lock_);
    RTMScope rtm(nullptr);
    while(1) {

      for (uint i = 0; i < CLUSTER_H; i++) {
        if (node->keys[i] == key && node->indexes[i] != 0) {
          loc = getDataNode_loc(node->indexes[i]);
          loc += sizeof(DataNode);
          res =  (char *)(array + loc);
          return res;
        }
      }

      if (node->next != 0 ) {
        // jump to another node
        loc = getHeaderNode_loc(node->next);
        node = (HeaderNode *) (array + loc);

      } else {
        // get failed ,start inserting
        if(free_data == total_length) {
          fprintf(stdout,"no free space for insertion!\n");
          assert(false);
          return res;
        }
        for(uint i = 0;i < CLUSTER_H;++i) {
          if(node->indexes[i] == 0) {
            // find the slot entry
            DataNode *free_node = getDataNode(free_data);
            free_node->key = key;
            node->keys[i] = key;
            node->indexes[i] = free_data;
            free_data++;
            res = (char *)((char *)free_node + sizeof(DataNode));
            return res;
          }
        }

        // donot find the slot, create one
        if(free_indirect == indirect_length) {
          fprintf(stderr,"fail when allocating indirect node");
          exit(-1);
        }
        node->next = free_indirect;
        node = (HeaderNode *)(array + free_indirect * header_size);
        free_indirect++;
        DataNode * free_node = getDataNode(free_data);
        free_node->key = key;
        node->keys[0] = key;
        node->indexes[0] = free_data;
        free_data++ ;
        res =  (char *)((char *)free_node + sizeof(DataNode));
        return res;
        // end insertion
      }

      // end while(1)
    }
    return res;
  }

  void delete_(uint64_t key) {
    assert(false);
  }

  const uint64_t length;
  const uint64_t Logical_length;
  const uint64_t indirect_length;
  const uint64_t total_length;
  const uint64_t entrysize;
  const uint64_t header_size;
  const uint64_t data_size;
  const uint64_t size;

  const char * array;

  uint64_t free_indirect;
  uint64_t free_data;

  SpinLock lock_;
};
   
#endif

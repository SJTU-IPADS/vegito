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
 * This file contains a db class built from memstore
 */


#ifndef MEM_DB
#define MEM_DB

#include <stdint.h>
#include <vector>

#include "memstore.h"

#include "rdmaio.h"

/* For main table */
#include "memstore_bplustree.h"
#include "memstore_tree.h"
/* For string index */
#include "memstore_uint64bplustree.h"
#include "memstore_hash.h"

#define MAX_TABLE_SUPPORTED 32

enum TABLE_CLASS {
  TAB_BTREE,
  TAB_BTREE1,
  // String b+ tree
  TAB_SBTREE,
  TAB_HASH,
  TAB_USER
};

class MemDB {

 public:

  struct TableSchema {
    int klen;

    // We didn't support varible length of value
    int vlen;

    // The size of meta data
    // int meta_len;

    // Total legnth, = round( meta_len + vlen ) up to cache_line size
    int total_len;

    // Which class of underlying store is used
    TABLE_CLASS c;
  };

  TableSchema _schemas[MAX_TABLE_SUPPORTED]; // table's meta data infor
  Memstore * stores_[MAX_TABLE_SUPPORTED];   // primary tables
  MemstoreUint64BPlusTree *_indexs[MAX_TABLE_SUPPORTED]; // secondary indexes
  char *store_buffer_;  // RDMA buffer used to store the memory store
  

  /*
    Do not give the same store_buffer to different MemDB instances!
  */
  MemDB(char *s_buffer = NULL): store_buffer_(s_buffer) { }

  void AddSchema(int tableid, TABLE_CLASS c, int klen,int vlen,int meta_len = 0);

  void AddSchema(int tableid, Memstore *store, int klen, int vlen);

  /**
     Important!
     If the remote accesses are enabled, then each node shall ensure the order
     of addschema is the same.
     For example,
     if we have 2 tables, A,B.
     Node 0 shall call AddSchema(A),AddSchema(B).
     Node 1 shall call AddSchema(A),AddSchema(B), the same order.

     Further, the store_buffer_ offset shall be the same.
     We do this for convenience.
     Otherwise nodes need sync about the offsets of the RDMA region in the table.
   */
  void EnableRemoteAccess(int tableid,rdmaio::RdmaCtrl *cm);

  uint64_t *Get(int tableid,uint64_t key);
  void      Put(int tableid,uint64_t key,uint64_t *value);
};

#endif

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

#include "memdb.h"
#include "rdma_hashext.h"

#include "util/util.h"

#include <vector>
#include <stdexcept>
using namespace std;
using namespace nocc::util;

extern size_t scale_factor;

void MemDB::AddSchema(int tableid,TABLE_CLASS c,  int klen, int vlen, int meta_len) {

  int total_len = meta_len + vlen;

  // round up to the cache line size
  auto round_size = CACHE_LINE_SZ * 2;
  total_len = nocc::util::Round<int>(total_len,round_size);

  switch(c) {
  case TAB_BTREE:
    // stores_[tableid] = new MemstoreBPlusTree();
    stores_[tableid] = new MemstoreTree();
    break;
  case TAB_BTREE1:
    stores_[tableid] = new MemstoreUint64BPlusTree(klen);
    break;
  case TAB_SBTREE:
    /* This is a secondary index, it does not need to set schema */
    /* for backward compatability */
    assert(false);
    break;
  case TAB_HASH: {
    auto tabp = new drtm::memstore::RdmaHashExt(1024 * 1024 * 4,store_buffer_); //FIXME!! hard coded
    stores_[tableid] = tabp;
    // update the store buffer
    if(store_buffer_ != NULL)store_buffer_ += tabp->size;
  }
    break;
  default:
    fprintf(stderr,"Unsupported store type! tab %d, type %d\n",tableid,c);
    exit(-1);
  }
  _schemas[tableid].c = c;
  _schemas[tableid].klen = klen;
  _schemas[tableid].vlen = vlen;
  // _schemas[tableid].meta_len = meta_len;
  _schemas[tableid].total_len = total_len;
}

void MemDB::AddSchema(int tableid, Memstore *store, int klen, int vlen) {
  assert(store != nullptr);
  stores_[tableid] = store;
  _schemas[tableid].c = TAB_USER;
  _schemas[tableid].klen = klen;
  _schemas[tableid].vlen = vlen;
  // _schemas[tableid].meta_len = meta_len;
  _schemas[tableid].total_len = vlen;
}

void MemDB::EnableRemoteAccess(int tableid,rdmaio::RdmaCtrl *cm) {
  assert(_schemas[tableid].c == TAB_HASH); // now only HashTable support remote accesses
  assert(store_buffer_ != NULL);           // the table shall be allocated on an RDMA region
  drtm::memstore::RdmaHashExt *tab = (drtm::memstore::RdmaHashExt *)(stores_[tableid]);
  tab->enable_remote_accesses(cm);
}

uint64_t *MemDB::Get(int tableid,uint64_t key) {
  MemNode *mn = NULL;
#if 0
  switch(_schemas[tableid].c) {
  case TAB_SBTREE: {
    assert(false);
    mn = _indexs[tableid]->Get(key);
  }
    break;
  default:
    /* otherwise, using store */
    mn = stores_[tableid]->Get(key);
    break;
  }
#else
  mn = stores_[tableid]->Get(key);
#endif
  if(mn == NULL)
    return NULL;
  return mn->value;

}


// XXX: only for initialization
void MemDB::Put(int tableid, uint64_t key, uint64_t *value) {
#if 0
  switch(_schemas[tableid].c) {
  case TAB_SBTREE: {
    assert(false);
    MemNode *mn = _indexs[tableid]->Put(key,value);
    mn->seq = 2;
  }
    break;
  default:
    MemNode *mn = stores_[tableid]->Put(key,value);
    mn->seq = 2;
    break;
  }
#else
  MemNode *mn = stores_[tableid]->Put(key,value);
  mn->seq = 2;
#endif
}

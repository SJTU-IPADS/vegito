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

#include "backup_store_kv.h"
#include "util/util.h"
#include <numeric>
#include <cstring>
using namespace std;
using namespace nocc::util;

inline BackupStoreKV::ValueNode *
BackupStoreKV::getValueNode_(uint64_t v, ValueNode *p) const {
#if 1
  void *buf = malloc(sizeof(ValueNode) + val_len_);
  ValueNode *ret = new (buf) ValueNode(v, p);
  return ret;
#else
  if (p == nullptr) {
    
    void *buf = malloc(sizeof(ValueNode) + val_len_);
    ValueNode *ret = new (buf) ValueNode(v, p);
    return ret;
  } else {
    p->ver = v;
    return p;
  }
#endif

}

BackupStoreKV::BackupStoreKV(vector<size_t> v, size_t max_items)
    : BackupStore(v, max_items), meta_(max_items_),
      val_len_(accumulate(v.begin(), v.end(), 0)) { }

BackupStoreKV::~BackupStoreKV() { }

void BackupStoreKV::insert(uint64_t off, uint64_t k, char *v, 
                           uint64_t seq, uint64_t ver) {
  assert(off < max_items_);
 
  RowMeta &meta = meta_[off];
  assert(meta.seq == -1);

  meta.key = k;
  meta.min_ver = ver;

  if (val_len_ != 0) {
    ValueNode *node = meta.value;
    if (node == nullptr || node->ver < ver) {
      // insert into the first position
      ValueNode *new_node = getValueNode_(ver, node);
      meta.value = new_node;
      node = new_node;
    }
  
    assert(node != nullptr && node->ver == ver);
  
    // start to fill with value
    memcpy(node->val, v, val_len_);
  }

  meta.seq = seq;
}

void BackupStoreKV::update(uint64_t off, const std::vector<int> &cids, 
                           char *v, uint64_t seq, uint64_t ver) {
  if (val_len_ == 0) return;
  assert(off < max_items_);
 
  RowMeta &meta = meta_[off];
  assert(meta.seq != -1 && meta.seq != seq);
  if (meta.seq >= seq) return;  // XXX: fix it, set seq on each columns
  
  ValueNode *node = meta.value;
  if (node == nullptr || node->ver < ver) {
    // insert into the first position
    ValueNode *new_node = getValueNode_(ver, node);
    meta.value = new_node;
    node = new_node;
  }
  assert(node != nullptr && node->ver == ver);

  // start to fill with value
  for (int cid : cids) {
    uint64_t voff = val_off_[cid];
    uint64_t vlen = val_lens_[cid];
    
    copy_val_(&node->val[voff], &v[voff], vlen);
  }
  meta.seq = seq;
}

void BackupStoreKV::putByOffset(uint64_t offset, uint64_t key, char *val,
                                int64_t seq, uint64_t version) {

  assert(offset < max_items_);
 
  RowMeta &meta = meta_[offset];
#if 1  // 0 for debug
  if (meta.seq == -1) {
    // insert the new item
    meta.key = key;
    meta.min_ver = version;
  }

  assert(meta.key == key && meta.seq != seq);
  
  if (val_len_ == 0) {
    meta.seq = seq;  
    return;
  }
#endif
  if (meta.seq >= seq) return;

  ValueNode *node = meta.value;
  if (node == nullptr || node->ver < version) {
    // insert into the first position
    ValueNode *new_node = getValueNode_(version, node);
    meta.value = new_node;
    node = new_node;
  }

  assert(node != nullptr && node->ver == version);

  // start to fill with value
  assert(val_len_);
  memcpy(node->val, val, val_len_);

  meta.seq = seq;
}

uint64_t BackupStoreKV::locateCol(int col_id, uint64_t width) const {
  assert(val_lens_[col_id] == width);
  return val_off_[col_id];
}

// "version" is stable version
char *BackupStoreKV::getByOffset(uint64_t offset, uint64_t version) {
  assert(offset < header_);
  ValueNode *node = meta_[offset].value;
  for ( ; node != nullptr; node = node->prev) {
    if (node->ver <= version)  // not newer than the stable version
      return node->val;
  }
  return nullptr;
}

char *BackupStoreKV::getByOffset(uint64_t offset, int columnID, 
                                 uint64_t version, uint64_t *walk_cnt) {
  assert(columnID < val_lens_.size());
  char *ptr = getByOffset(offset, version);
  if (ptr == nullptr)
    return nullptr;

  size_t field_off = 0;
  for (int i = 0; i < columnID; ++i)
    field_off += val_lens_[i];
  return (ptr + field_off);
}

BackupStoreKV::Cursor::Cursor(const BackupStoreKV &store, uint64_t ver)
  : kv_(store), ver_(ver), begin_(0), end_(store.header_), cur_(-1) { }

void BackupStoreKV::Cursor::seekOffset(uint64_t begin, uint64_t end) {
  uint64_t header = kv_.header_;
  if (end > header) end = header;
  if (begin > end)  begin = end;
  begin_ = begin;
  end_ = end;
  cur_ = begin_ - 1;
}

bool BackupStoreKV::Cursor::nextRow() {
  ++cur_;
  if (cur_ >= end_) return false;
  if (kv_.meta_[cur_].min_ver > ver_) return false;
  return true;
}

char *BackupStoreKV::Cursor::value() const {
  ValueNode *node = kv_.meta_[cur_].value;
  for ( ; node != nullptr; node = node->prev) {
    if (node->ver <= ver_) break;
  }
  assert(node);
  return node->val;
}

void BackupStoreKV::copy_(const BackupStoreKV &that) {
  val_lens_ = that.val_lens_;

  // val_len_ = that.val_len_;
  header_ = that.header_;

  meta_.resize(max_items_);
  
  assert(false);  // TODO

  // for (int i = 0; i < header_; ++i) {
  //   meta_[i].key = that.meta_[i].key;
  //   ValueNode *thatValue = that.meta_[i].value;
  //   ValueNode *new_val = getValueNode_();
  //   memcpy(new_val, thatValue, sizeof(ValueNode) + val_len_);
  //   meta_[i].value = new_val;
  // }
}

// an ugly version for test
BackupStoreKV::BackupStoreKV(const BackupStoreKV &that)
  : BackupStore(vector<size_t>(), that.max_items_), val_len_(that.val_len_) {
  copy_(that);
  uint64_t each_len = sizeof(ValueNode) + val_len_;
  double total = header_ * each_len / 1024.0 / 1024.0;
  printf("copy %ld items, each %ld bytes, total %lf MB\n", header_, each_len, total);
}


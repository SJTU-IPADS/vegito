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

#include "backup_store_col2.h"
#include "util/util.h"
#include "custom_config.h"

using namespace std;
using namespace nocc::util;

#define COL_USE_EPOCH 1

BackupStoreCol2::BackupStoreCol2(BackupDB::Schema s)
    : BackupStore(vector<size_t>(), s.max_items),
      // meta_(max_items_), 
      seq_(max_items_, -1),
      cols_(s.cols),
      key_col_(max_items_),
      fixCols_(s.cols.size(), nullptr),
      flexCols_(s.cols.size(), nullptr)
{
  // each column
  for (int i = 0; i < cols_.size(); i++) {
    size_t vlen = cols_[i].vlen;
    val_lens_.push_back(vlen);

    // fixCols_[i] = new char[vlen * max_items_];

    if (cols_[i].updatable) {
      flexCols_[i] = new char[(sizeof(ValueNode) + vlen) * max_items_];
    } else {
      fixCols_[i] = new char[vlen * max_items_];
    }
  }
}

void BackupStoreCol2::insert(uint64_t off, uint64_t k, char *v,
                            uint64_t seq, uint64_t ver) {
  _put(off, k, v, seq, ver, true);
}

void BackupStoreCol2::update(uint64_t off, const std::vector<int> &cids, 
                            char *v, uint64_t seq, uint64_t ver) {
  _put(off, uint64_t(-1), v, seq, ver, false);
}

void BackupStoreCol2::_put(uint64_t offset, uint64_t key, char *val,
                                 int64_t seq, uint64_t version, bool insert) {
  assert(offset < max_items_);

  // RowMeta &meta = meta_[offset];
  int64_t &meta_seq = seq_[offset];

  if (insert) {
    if (meta_seq == -1) {
      // meta.min_ver = version;
      key_col_[offset] = key;

      // TODO: offset
    }
    
    assert(key_col_[offset] == key);
  }
  assert(meta_seq != seq);
  if (meta_seq >= seq) return;

  for (int i = 0; i < cols_.size(); ++i) {
    const BackupDB::Column &col = cols_[i];
    if (meta_seq >= 0 && !col.updatable) break;

    size_t vlen = col.vlen;
    char *dst = nullptr;
    if (col.updatable) {
      char *buf = flexCols_[i] + (sizeof(ValueNode) + vlen) * offset;
      ValueNode *node = (ValueNode *) buf;
      if (meta_seq == -1) {
        node->ver = version;
        node->prev = nullptr;
      } 
#if COL_USE_EPOCH == 1
      else if (node->ver < version)  // XXX: best performance!
#else
      else if (node->ver <= version)  // for every version
#endif
      {
        ValueNode *new_n = (ValueNode *) malloc(sizeof(ValueNode) + vlen);
        memcpy(new_n, node, sizeof(ValueNode) + vlen);
#if UPDATE_STAT
        stat_.num_copy += vlen;    // size
#endif
        node->prev = new_n;
        node->ver = version;
      }

      assert(node->ver == version);
      dst = buf + sizeof(ValueNode); 
    } else {
      dst = fixCols_[i] + vlen * offset;
    }
    memcpy(dst, val, vlen);
    val += vlen;
  }

  meta_seq = seq;
}

char *BackupStoreCol2::getByOffset(uint64_t offset, uint64_t version) {
  printf("Function not implemented\n");
  assert(false);
  return nullptr;
}

char *BackupStoreCol2::getByOffset(uint64_t offset, int col_id,
                                  uint64_t version, uint64_t *walk_cnt) {
  char *val = nullptr;
  const BackupDB::Column &col = cols_[col_id];

  if (col.updatable) {
    char *buf = flexCols_[col_id] + (sizeof(ValueNode) + col.vlen) * offset;
    ValueNode *node = (ValueNode *) buf;
    for ( ; node != nullptr; node = node->prev) {
      if (node->ver <= version) {
        val = node->val;
        break;
      }
    }
    assert(node);
  } else {
    val = fixCols_[col_id] + col.vlen * offset;
  }

  assert(val);
  return val;
}

const std::vector<uint64_t> &BackupStoreCol2::getKeyCol() const {
  return key_col_;
}

char *BackupStoreCol2::getFixCol(int col_id) const {
  const BackupDB::Column &col = cols_[col_id];
  assert(!col.updatable);
  return fixCols_[col_id];
}

size_t BackupStoreCol2::getItemNum(uint64_t lver) const {
  assert(false);
  return 0;
  // size_t ret = insertable_? hist_headers_[lver] : max_items_;
  // assert (ret != 0);
  // return ret;
}

BackupStoreCol2::Cursor::Cursor(const BackupStoreCol2 &store,
                                int col_id, uint64_t ver)
  : col_(store), col_id_(col_id), update_(store.cols_[col_id].updatable),
    ver_(ver), base_(update_? store.flexCols_[col_id] : store.fixCols_[col_id]),
    field_width_(store.get_field_width_(col_id)),
    begin_(0), end_(store.header_), cur_(-1), ptr_(nullptr) { 
  assert(base_ != nullptr);    
}

void BackupStoreCol2::Cursor::seekOffset(uint64_t begin, uint64_t end) {
  uint64_t header = col_.header_;
  if (end > header) end = header;
  if (begin > end)  begin = end;
  begin_ = begin;
  end_ = end;
  cur_ = begin_ - 1;
}

bool BackupStoreCol2::Cursor::nextRow(uint64_t *walk_cnt) {
  ++cur_;
  if (cur_ >= end_) return false;
  // if (col_.meta_[cur_].min_ver > ver_) return false;

  if (!update_) {
    ptr_ = base_ + field_width_ * cur_;
    return true;
  }

  // can update 
  ValueNode *node = (ValueNode *) (base_ + field_width_ * cur_);
  for ( ; node != nullptr; node = node->prev) {
    if (walk_cnt) ++(*walk_cnt);
    if (node->ver <= ver_) break;
    // if (node->prev == nullptr) break;
  }
  if (!node) return false;   // XXX: real ture?
  assert(node);
  ptr_ = node->val;

  return true;
}

char *BackupStoreCol2::Cursor::value() const {
  return ptr_;
}

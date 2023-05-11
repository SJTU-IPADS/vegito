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

#include "backup_store_row.h"
#include "util/util.h"
#include "custom_config.h"
#include <numeric>
#include <cstring>
using namespace std;
using namespace nocc::util;

const int item_per_page = 32;

inline BackupStoreRow::RowPage *
BackupStoreRow::getRowPage_(uint64_t v, RowPage *n) const {
  void *buf = malloc(sizeof(RowPage) + pg_sz_);

  RowPage *ret = new (buf) RowPage(v, n);
  if (pg_items_ != 1 && n != nullptr) {
    memcpy(ret->bytes, n->bytes, pg_sz_);
  }

  return ret;
}

inline void BackupStoreRow::updateRecord_(RowPage* page, uint64_t offset_in_page,
                                          uint64_t key, char* val) {
  RowRecord *record = reinterpret_cast<RowRecord *>(page->bytes + offset_in_page);
  record->key = key;
  memcpy(record->val, val, val_len_);
}

inline uint64_t BackupStoreRow::getOffsetInPage_(uint64_t item_offset) const {
  return (item_offset % pg_items_) *
            (sizeof(RowRecord) + val_len_);
}

BackupStoreRow::BackupStoreRow(BackupDB::Schema s)
    : BackupStore(s.max_items), 
      pg_items_(item_per_page), meta_(max_items_),
      max_pages_(max_items_ % pg_items_ ? max_items_ / pg_items_  + 1 
                                             : max_items_ / pg_items_),
      pg_sz_(pg_items_ * (sizeof(RowPage) + val_len_)),
      pages_(max_pages_, nullptr), page_locks_(max_pages_, 0)
{
  // each column
  val_len_ = 0; 
  for (int i = 0; i < s.cols.size(); i++) {
    size_t vlen = s.cols[i].vlen;
    val_off_.push_back(val_len_);
    val_lens_.push_back(vlen);
    val_type_.push_back(s.cols[i].vtype);

    val_len_ += vlen;
  }
#if 0  // pre-alloc
  for (int i = 0; i < pages_.size(); ++i) {
    pages_[i] = getRowPage_(-1, nullptr);
  }
#endif
}

BackupStoreRow::~BackupStoreRow() {
  // TODO free memory
}

// version is upper bound
// concurrency control is managed by db layer
void BackupStoreRow::putByOffset(uint64_t offset, uint64_t key, char *val,
                                 int64_t seq, uint64_t version) {
  assert(offset < max_items_);

  RowMeta &meta = meta_[offset];
  if (meta.seq == -1) {
    // insert the new item
    meta.key = key;
    meta.min_ver = version;
    meta.pgn = offset / pg_items_;
    meta.pgo = getOffsetInPage_(offset);
  }
  
  assert(meta.key == key && meta.seq != seq);
  if (meta.seq >= seq) return;

  // Calculate page number and offset in page
  uint64_t page_num = meta.pgn;
  uint64_t offset_in_page = meta.pgo;

  RowPage *page = pages_[page_num];
#if 0  // pre-alloc
  assert(page != nullptr);
  if (page->ver == -1) page->ver = version;
#endif
  if (page == nullptr || page->ver < version) {
    lock32(&page_locks_[page_num]);
    page = pages_[page_num];
    // allocate new page
    if (page == nullptr || page->ver < version) {
      RowPage *new_page = getRowPage_(version, page);
      pages_[page_num] = new_page;
      page = new_page;
    }
    unlock32(&page_locks_[page_num]);
  }

  assert(page && page->ver == version);
  // copy data - new record
  updateRecord_(page, offset_in_page, key, val);

  meta.seq = seq;
}

uint64_t BackupStoreRow::locateCol(int col_id, uint64_t width) const {
  uint64_t field_off = 0;
  assert(val_lens_[col_id] == width);
  for (int i = 0; i < col_id; ++i)
    field_off += val_lens_[i];
  return field_off;
}

// "version" is stable version
char *BackupStoreRow::getByOffset(uint64_t offset, uint64_t version) {
  assert(offset < header_);

  uint64_t page_num = offset / pg_items_;
  uint64_t offset_in_page = getOffsetInPage_(offset);
  RowPage *page = pages_[page_num];
  for (; page != nullptr; page = page->prev) {
    if (page->ver <= version)
      return reinterpret_cast<RowRecord *>(page->bytes + offset_in_page)->val;
  }

  return nullptr;
}

char *BackupStoreRow::getByOffset(uint64_t offset, int columnID, 
                                  uint64_t version, uint64_t *walk_cnt) {
  assert(offset < header_);

  char *ptr = getByOffset(offset, version);
  if (ptr == nullptr)
    return nullptr;

  size_t field_off = 0;
  for (int i = 0; i < columnID; ++i)
    field_off += val_lens_[i];
  return (ptr + field_off);
}

BackupStoreRow::Cursor::Cursor(const BackupStoreRow &store, uint64_t ver)
  : row_(store), ver_(ver), begin_(0), end_(store.header_), cur_(-1), 
    cur_pgn_(-1), cur_page_(nullptr) { }

void BackupStoreRow::Cursor::seekOffset(uint64_t begin, uint64_t end) {
  uint64_t header = row_.header_;
  if (end > header) end = header;
  if (begin > end)  begin = end;
  begin_ = begin;
  end_ = end;
  cur_ = begin_ - 1;
}

bool BackupStoreRow::Cursor::nextRow() {
  ++cur_;
  if (cur_ >= end_) return false;

  const RowMeta &meta = row_.meta_[cur_];
  if (meta.min_ver > ver_) return false;

  // TODO: locate page 
  if (meta.pgn != cur_pgn_) {
    // relocate page
    RowPage *page = row_.pages_[meta.pgn];
    for ( ; page != nullptr; page = page->prev) {
      if (page->ver <= ver_) break;
    }
    assert(page);
    cur_page_ = page->bytes;
  } 

  cur_pgo_ = meta.pgo;
  return true;
}

char *BackupStoreRow::Cursor::value() const {
  RowRecord *r = (RowRecord *) (cur_page_ + cur_pgo_);
  return r->val;
}

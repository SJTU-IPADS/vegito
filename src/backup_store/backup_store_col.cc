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

#include "backup_store_col.h"
#include "util/util.h"
#include "custom_config.h"

using namespace std;
using namespace nocc::util;


namespace {
const uint32_t MAX_POOL = 1;
thread_local uint32_t pool_off = MAX_POOL - 1;
thread_local char *pool = nullptr;

thread_local uint64_t cached_ver;
thread_local uint32_t cached_page_num[30];
thread_local void *cached_page[30] = { nullptr };

}

// NOTE: page_sz is number of objects, instead of bytes
inline BackupStoreCol::Page *BackupStoreCol::
getNewPage_(uint64_t page_sz, uint64_t vlen, uint64_t ver, Page *prev) {
# if 0  // 1 for debug, 1.039682 M
  if (prev) {
    prev->ver = ver;
    return prev;
  } 
#endif
  char *buf = nullptr;
  uint32_t pg_sz = sizeof(Page) + vlen * page_sz;
  // uint32_t pg_sz = 4 * 1024;
 
#if 0 
  if (pool_off == MAX_POOL - 1) {
    pool = (char *) malloc(MAX_POOL * pg_sz);
    pool_off = 0;
  } else {
    pool += pg_sz;
    pool_off += 1;
  }
  buf = pool;
#else
  buf = (char *) malloc(pg_sz);
#endif
  Page *ret = new (buf) Page(ver, prev);

  // stat_.num_copy += vlen * page_sz;

  if (page_sz != 1 && prev != nullptr) {
    memcpy(ret->content, prev->content, page_sz * vlen);
#if UPDATE_STAT
    stat_.num_copy += vlen * page_sz;    // size
    // ++stat_.num_copy;    // count
#endif
  }

  return ret;
}

BackupStoreCol::BackupStoreCol(BackupDB::Schema s, 
                               const vector<uint32_t> *split)
    : BackupStore(s.max_items),
      table_id_(s.table_id),
      seq_(max_items_, -1),  
      cols_(s.cols),
      key_col_(max_items_), 
      fixCols_(s.cols.size(), nullptr),
      flexCols_(s.cols.size())
{
  // each column
  val_len_ = 0; 
  for (int i = 0; i < cols_.size(); i++) {
    size_t vlen = cols_[i].vlen;
    val_off_.push_back(val_len_);
    val_lens_.push_back(vlen);
    val_type_.push_back(cols_[i].vtype);

    val_len_ += vlen;
    if (cols_[i].page_size > max_items_)
      cols_[i].page_size = max_items_;
    if (cols_[i].page_size == 0)
      cols_[i].page_size = 1;
  }

  pcols_.reserve(cols_.size());
  if (split == nullptr) {
    for (int i = 0; i < cols_.size(); ++i) {
      pcols_.emplace_back(i, 0); 
      split_.emplace_back(i);
      split_vlen_.emplace_back(val_lens_[i]);
    }
  } else {
    assert(split->size() > 0);
    for (int s = 0; s < split->size(); ++s) {
      int begin = (*split)[s];
      int end = (s == split->size() - 1)? cols_.size() : (*split)[s + 1];
      uint64_t off = 0;
      for (int i = begin; i < end; ++i) {
        pcols_.emplace_back(s, off);
        off += val_lens_[i];
      }
      split_.emplace_back(s);
      split_vlen_.emplace_back(off);
    }
  }
  assert(pcols_.size() == cols_.size());
  
  for (int i = 0; i < cols_.size(); i++) {
    size_t vlen = val_lens_[i];
    // divided into 2 types according to "updatable"
    if (cols_[i].updatable) {
      size_t page_sz = cols_[i].page_size;
      int page_num = (max_items_ + page_sz - 1) / page_sz;
      assert(page_num != 0);
      flexCols_[i].locks.assign(page_num, 0);
      flexCols_[i].pages.assign(page_num, nullptr);
      flexCols_[i].old_pages.assign(page_num, nullptr);
      for (int p = 0; p < page_num; ++p) {
        Page *page = getNewPage_(page_sz, vlen, -1, nullptr);
        flexCols_[i].pages[p] = page;
        flexCols_[i].old_pages[p] = page;
      }
    } else {
      fixCols_[i] = new char[vlen * max_items_];
    }
  }

}

BackupStoreCol::~BackupStoreCol() {
  for (int i = 0; i < cols_.size(); i++) {
    size_t page_sz = cols_[i].page_size;
    if (cols_[i].updatable) {
      int page_num = (max_items_ + page_sz - 1) / page_sz;
      for (int p = 0; p < page_num; ++p)
        free(flexCols_[i].pages[p]);
    } else {
      delete[] fixCols_[i];
    }
  }
}


inline BackupStoreCol::Page *BackupStoreCol::
findWithInsertPage_(int colID, uint64_t pg_num, uint64_t version) {
  const BackupDB::Column &col = cols_[colID];

  FlexCol &flex = flexCols_[colID];
  assert(pg_num < flex.pages.size());
  Page *page = flex.pages[pg_num];
  if (page->ver == -1) {
    page->ver = version;  // a initialized page
    page->min_ver = version;
  } else if (version > page->ver) {
#if 1
    lock32(&flex.locks[pg_num]);
    page = flex.pages[pg_num];

    if (version > page->ver) {
      Page *newPage = getNewPage_(col.page_size, col.vlen, version, page);
      flex.pages[pg_num] = newPage;
      page = newPage;
    }

    unlock32(&flex.locks[pg_num]);
#else
    page->ver = version;
#endif
  }

  assert(page && page->ver == version);
  return page;
}

inline BackupStoreCol::Page *BackupStoreCol::
findPage(int colID, uint64_t pg_num, uint64_t version,
         uint64_t *walk_cnt) {
  const BackupDB::Column &col = cols_[colID];

  FlexCol &flex = flexCols_[colID];
  assert(pg_num < flex.pages.size());
  Page *page = flex.pages[pg_num];
  if (page == nullptr || page->min_ver > version) {
    // assert(false);
    return nullptr;
  }

  for ( ; page; page = page->prev) {
    if (page->ver <= version) {
      assert(page);
      // if (walk_cnt) ++(*walk_cnt);
      return page; 
    }
    if (walk_cnt) ++(*walk_cnt);
  }

  assert(false);
  return nullptr;
}

void BackupStoreCol::insert(uint64_t off, uint64_t k, char *v,
                            uint64_t seq, uint64_t ver) {

  int64_t dv = (int64_t) ver;  // for debug
  assert(off < max_items_);

  // if (dv < 0) ver = -ver;
  int64_t &meta_seq = seq_[off];
  assert(meta_seq == -1);
  // if (dv < 0) return;

  key_col_[off] = k;
  // assert(k != 0);  // for LDBC
  
  for (int i = 0; i < cols_.size(); ++i) {
    const BackupDB::Column &col = cols_[i];

    size_t vlen = col.vlen;
    char *dst = nullptr;
    if (col.updatable) {
      int pg_num = off / col.page_size;
      // int pg_num = 0;
#if 0  // the same
      Page *page = findWithInsertPage_(i, pg_num, ver);
#else
      FlexCol &flex = flexCols_[i];
      Page *page = flex.pages[pg_num];

      if (page->ver == -1) {
        page->ver = ver;  // a initialized page
        page->min_ver = ver;
      } else if (ver > page->ver) {
#if 1 // 0 for debug
        lock32(&flex.locks[pg_num]);
        page = flex.pages[pg_num];
        if (ver > page->ver) {
          Page *newPage = getNewPage_(col.page_size, col.vlen, ver, page);
          flex.pages[pg_num] = newPage;
          page = newPage;
        }
        unlock32(&flex.locks[pg_num]);
#else
        page->ver = ver;
#endif
      }

#endif
      assert (page && page->ver == ver);
      dst = page->content + (off % col.page_size) * vlen;
    } else {
      dst = fixCols_[i] + off * vlen;
    }
    copy_val_(dst, v, vlen);

#if UPDATE_STAT
    ++stat_.num_update;
#endif
    v += vlen;  // XXX: FIXME! for the length of value
  }

  meta_seq = seq;
}

void BackupStoreCol::update(uint64_t off, const std::vector<int> &cids, 
                            char *v, uint64_t seq, uint64_t ver) {
  if (val_len_ == 0) return;
  assert(off < max_items_);
  
  // return;  // for debug, thr: 1.231274 M

  int64_t &meta_seq = seq_[off];
  assert(meta_seq != -1 && meta_seq != seq);

  if (meta_seq >= seq) return;  // XXX: fix it, set seq on each columns

  for (int i : cids) {
    // continue;  // for debug, thr: 1.177976 M
    const BackupDB::Column &col = cols_[i];

    uint64_t vlen = col.vlen;
    uint64_t voff = val_off_[i];
    
    char *dst = nullptr;
    assert(col.updatable);

    int pg_num = off / col.page_size;
    
    // continue;  // for debug, thr: 1.142011 M

#if 0  // the same, reduce function call
    Page *page = findWithInsertPage_(i, pg_num, ver);
#else
    FlexCol &flex = flexCols_[i];
    Page *page = flex.pages[pg_num];
    assert(page->ver != -1);

#if 1  // 0 for debug, thr: 1.071205 M 
    if (ver > page->ver) {
      lock32(&flex.locks[pg_num]);
      page = flex.pages[pg_num];
      if (ver > page->ver) {
#if 1   // 0 for debug, 0: 1.057807 M
        Page *newPage = getNewPage_(col.page_size, col.vlen, ver, page);
        flex.pages[pg_num] = newPage;
        page = newPage;
#else
        page->ver = ver;
#endif
      }
      unlock32(&flex.locks[pg_num]);

    }
#else
    page->ver = ver;
#endif

#endif


    assert (page && page->ver == ver);
    dst = page->content + (off % col.page_size) * vlen;
    memcpy(dst, &v[voff], vlen);

#if UPDATE_STAT
    ++stat_.num_update;
#endif
  }

  meta_seq = seq;
}

void BackupStoreCol::update(uint64_t off, int cid, char *v, uint64_t ver) {
  if (val_len_ == 0) return;
  assert(off < max_items_);

  // continue;  // for debug, thr: 1.177976 M
  const BackupDB::Column &col = cols_[cid];

  uint64_t vlen = col.vlen;
  
  char *dst = nullptr;
  assert(col.updatable);

  int pg_num = off / col.page_size;
  
#if 0  // the same, reduce function call
  Page *page = findWithInsertPage_(i, pg_num, ver);
#else
  FlexCol &flex = flexCols_[cid];
  Page *page = flex.pages[pg_num];
  assert(page->ver != -1);

#if 1  // 0 for debug, thr: 1.071205 M 
  if (ver > page->ver) {
    lock32(&flex.locks[pg_num]);
    page = flex.pages[pg_num];
    if (ver > page->ver) {
#if 1   // 0 for debug, 0: 1.057807 M
      Page *newPage = getNewPage_(col.page_size, col.vlen, ver, page);
      flex.pages[pg_num] = newPage;
      page = newPage;
#else
      page->ver = ver;
#endif
    }
    unlock32(&flex.locks[pg_num]);
  }
#else
  page->ver = ver;
#endif

#endif

  assert (page && page->ver == ver);
  dst = page->content + (off % col.page_size) * vlen;
  memcpy(dst, v, vlen);

#if UPDATE_STAT
  ++stat_.num_update;
#endif
}

void BackupStoreCol::gc(uint64_t ver) {
  if (ver == 0) return;

  uint64_t clean_sz = 0;
  uint64_t clean_pg = 0;

  // uint64_t header = header_;
  uint64_t header = stable_header_;
  for (int i = 0; i < cols_.size(); i++) {
    if (!cols_[i].updatable) continue;
    vector<Page *> &old_pages = flexCols_[i].old_pages;
    int pgsz = cols_[i].page_size;
    size_t vlen = cols_[i].vlen;
    int used_page = (header + pgsz - 1) / pgsz;
    assert (used_page <= old_pages.size());
    for (int pi = 0; pi < used_page; ++pi) {
      Page *p = old_pages[pi];
      if (p->ver >= ver) continue;

      while (p->ver < ver && p->next && p->next->ver <= ver) {
        Page *next = p->next;
        next->prev = nullptr;
        free(p);
        p = next;
        clean_sz += sizeof(Page) + vlen * pgsz;
        ++clean_pg;
      }
      old_pages[pi] = p;
    }
  }
#if 0
  if (clean_sz != 0) {
    printf("GC ver %lu table_id %d page %lu, size %lu\n", 
           ver, table_id_, clean_pg, clean_sz);
  }
#endif
}

#if 0
void BackupStoreCol::putByOffset(uint64_t offset, uint64_t key, char *val,
                                 int64_t seq, uint64_t version) {

  assert(offset < max_items_);

  int64_t &meta_seq = seq_[offset];
  if (meta_seq == -1) {
    // min_ver_[offset] = version;  // for cache corherence
    key_col_[offset] = key;
  }
  
  assert(key_col_[offset] == key && meta_seq != seq);
  if (cols_.size() == 0) {
    meta_seq = seq;
    return;
  }

  if (meta_seq >= seq) return;

  for (int i = 0; i < cols_.size(); ++i) {
    const BackupDB::Column &col = cols_[i];
    if (meta_seq >= 0 && !col.updatable) break;

    size_t vlen = col.vlen;
    char *dst = nullptr;
    if (col.updatable) {
      int pg_num = offset / col.page_size;
#if 0  // 0 for debug
      Page *page = findWithInsertPage_(i, pg_num, version);
#else
      FlexCol &flex = flexCols_[i];
      Page *page = flex.pages[pg_num];

      if (page->ver == -1) {
        page->ver = version;  // a initialized page
        page->min_ver = version;
      } else if (version > page->ver) {
#if 1
        lock32(&flex.locks[pg_num]);
        page = flex.pages[pg_num];
        if (version > page->ver) {
          Page *newPage = getNewPage_(col.page_size, col.vlen, version, page);
          flex.pages[pg_num] = newPage;
          page = newPage;
        }
        unlock32(&flex.locks[pg_num]);
#else
        page->ver = version;
#endif
      }

#endif
      assert (page && page->ver == version);
      dst = page->content + (offset % col.page_size) * vlen;
    } else {
      dst = fixCols_[i] + offset * vlen;
    }
    memcpy(dst, val, vlen);

    // ++stat_.num_update;
    val += vlen;
  }

  meta_seq = seq;
}
#endif

char *BackupStoreCol::getByOffset(uint64_t offset, int col_id,
                                  uint64_t version, uint64_t *walk_cnt) {
  char *val = nullptr;
  const BackupDB::Column &col = cols_[col_id];
  assert(offset < max_items_);
  // if (offset >= max_items_) return nullptr;

  Page *page = nullptr;
  if (col.updatable) {
    uint64_t pg_num = offset / col.page_size;
#if 1
    if (cached_page[col_id] != nullptr 
        && cached_ver == version && cached_page_num[col_id] == pg_num) {
      page = (Page *) cached_page[col_id];
    } else {
      cached_ver = version;
      page = findPage(col_id, pg_num, version, walk_cnt);
      cached_page[col_id] = page;
      cached_page_num[col_id] = pg_num;
    }
#else
    page = findPage(col_id, pg_num, version, walk_cnt);
#endif

    if (!page) return nullptr;
    val = page->content + (offset % col.page_size) * col.vlen;
  } else {
    val = fixCols_[col_id] + col.vlen * offset;
  }

  return val;
}

const vector<uint64_t> &BackupStoreCol::getKeyCol() const {
  return key_col_;
}

size_t BackupStoreCol::getCol(int col_id, uint64_t start_off, size_t size,
                             uint64_t lver, vector<char *> &pages) {
  const BackupDB::Column &col = cols_[col_id];
  pages.clear();
  assert(col.updatable);
  int start_pg = start_off / col.page_size;
  int end_pg = (start_off + size + col.page_size - 1) / col.page_size;
  for (int i = start_pg; i < end_pg; i++) {
    Page *p = findPage(col_id, i, lver);
    assert(p != nullptr);
    pages.push_back(p->content);
  }
  return col.page_size;
}

size_t BackupStoreCol::getPageSize(int col_id) const {
  return cols_[col_id].page_size;
}

char *BackupStoreCol::locateValue(int cid, char *col, size_t offset) {
  size_t vlen = cols_[cid].vlen;
  size_t voff = offset % cols_[cid].page_size;
  return (col + vlen * voff);
}

BackupStoreCol::Cursor::Cursor(const BackupStoreCol &store,
                                int col_id, uint64_t ver)
  : col_(store), col_id_(col_id), ver_(ver),
    update_(store.cols_[col_id].updatable), 
    vlen_(store.cols_[col_id].vlen),
    pgsz_(store.cols_[col_id].page_size), 
    begin_(0), end_(store.stable_header_), cur_(-1),
    ptr_(nullptr), base_(update_? nullptr : store.fixCols_[col_id]),
    pgn_(-1), pgi_(pgsz_ - 1),
    pages_(store.flexCols_[col_id].pages) { }

void BackupStoreCol::Cursor::seekOffset(uint64_t begin, uint64_t end) {
  uint64_t header = col_.stable_header_;
  if (end > header) end = header;
  if (begin > end)  begin = end;
  begin_ = begin;
  end_ = end;
  cur_ = begin_ - 1;

  if (update_) {
    pgi_ = begin_ % pgsz_ - 1;
    if (pgi_ == -1) {
      pgi_ = pgsz_ - 1;
      pgn_ = begin_ / pgsz_ - 1;
    } else {
      pgn_ = begin_ / pgsz_;
    }
  }
}

#if 1  // 1 for MVCS, 0 for pure col
bool BackupStoreCol::Cursor::nextRow(uint64_t *walk_cnt) {
  ++cur_;
  if (cur_ >= end_) return false;
  // if (col_.min_ver_[cur_] > ver_) return false;  // for cache corherence

#if 1
  if (unlikely(!update_)) {
    ptr_ = base_ + vlen_ * cur_;
    return true;
  }

  ++pgi_;
  if (pgi_ == pgsz_) {
    pgi_ = 0;
    ++pgn_; 
  }

  if (pgi_ == 0 || base_ == nullptr) {
    // relocate page
    Page *p = pages_[pgn_];
    if (p->min_ver > ver_) return false;

    for ( ; p != nullptr; p = p->prev) {
      if (walk_cnt) ++(*walk_cnt);
      if (p->ver <= ver_) break;
    }
    assert(p);
    base_ = p->content;
  }
  ptr_ = base_ + vlen_ * pgi_;
#else
  // if (!ptr_)
  //   ptr_ = pages_[0]->content;
#endif
  return true;
}
# else
bool BackupStoreCol::Cursor::nextRow(uint64_t *walk_cnt) {
  ++cur_;
  if (unlikely(cur_ >= end_)) return false;
 
  if (unlikely(!update_)) {
    ptr_ = base_ + vlen_ * cur_;
    return true;
  }

  ++pgi_;
  if (unlikely(pgi_ == pgsz_)) {
    // pgi_ = 0;
    ++pgn_; 
  } 

  if (base_ == nullptr) { 
    Page *p = pages_[0];
    base_ = p->content;
  }

  ptr_ = base_ + vlen_ * cur_;
  return true;
}
#endif

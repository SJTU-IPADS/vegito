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

#ifndef BACKUP_STORE_H_
#define BACKUP_STORE_H_

#include <vector>
#include <cstdint>
#include <cstddef>
#include <cassert>

#include <memory>

#include <cstring>

#include "util/util.h"

struct StoreProf {
  uint64_t num_update;
  uint64_t num_copy;

  StoreProf()
    : num_update(0), num_copy(0) { }
};

enum BackupStoreDataType {
  INVALID = 0,
  BOOL = 1,
  CHAR = 2,
  SHORT = 3,
  INT = 4,
  LONG = 5,
  FLOAT = 6,
  DOUBLE = 7,
  STRING = 8,
  BYTES = 9,
  INT_LIST = 10,
  LONG_LIST = 11,
  FLOAT_LIST = 12,
  DOUBLE_LIST = 13,
  STRING_LIST = 14,
  DATE = 15,
  DATETIME = 16,
  LONGSTRING = 17,
  TEXT = 18
};

// multi-version store
class BackupStore {

 public:
  // ATTENTION: add lock in the index level!
  virtual void insert(uint64_t off, uint64_t k, char *v, 
                      uint64_t seq, uint64_t ver) {
    assert(false);
  }

  virtual void update(uint64_t off, const std::vector<int> &cids, char *v, 
                      uint64_t seq, uint64_t ver) {
    assert(false);
  }

  virtual void update(uint64_t off, int cid, char *v, uint64_t ver) {
    assert(false);
  }

  uint64_t copy(const BackupStore *store) {
    uint64_t max_ver = uint64_t(-1);
    auto row_cursor = store->getRowCursor(max_ver);
    assert(row_cursor.get());

    uint64_t off = 0;
    for ( ; row_cursor->nextRow(); ++off) {
      this->insert(off, row_cursor->key(), row_cursor->value(), 2, 0);
    }

    return off;
  }

  virtual void putByOffset(uint64_t offset, uint64_t key, char *val,
                           int64_t seq, uint64_t version) { assert(false); };

  uint64_t getNewOffset() {
    uint64_t ret = nocc::util::FAA(&header_, 1);
    assert(ret < max_items_);
    return ret;
  }

#if 0  
  uint64_t getFakeOffset() {
    // volatile static uint64_t tmp = 0;
    uint64_t ret = nocc::util::FAA(&fake_header_, 0);
    // uint64_t ret = nocc::util::FAA(&tmp, 0);
    // assert(ret < max_items_);
    return ret;
  }
#endif

  uint64_t getOffsetHeader() const { return stable_header_; }
  void updateHeader() { stable_header_ = header_; }

  static const int MAX_EDGE_ = 16;  // array
  // static const int MAX_EDGE_ = 2;  // linked list
  void addEdge() {
    edges_ = new uint64_t[MAX_EDGE_ * max_items_];
  }

  uint64_t *getEdge(uint64_t row_id) const { return &edges_[MAX_EDGE_ * row_id]; }

  // return offset in a row and check width
  virtual uint64_t locateCol(int col_id, uint64_t width) const { return 0; };

  virtual char *getByOffset(uint64_t offset, uint64_t version) { 
    assert(false);
    return nullptr;
  }

  std::vector<size_t> &get_val_lens() {return val_lens_;}
  std::vector<size_t> &get_val_offs() {return val_off_;}
  std::vector<BackupStoreDataType> &get_val_types() { return val_type_; }

  virtual char *getByOffset(uint64_t offset,
                            int columnID, uint64_t version,
                            uint64_t *walk_cnt = nullptr) = 0;

  const StoreProf &get_profile() const { return stat_; }

  // clean the pages whose version < `version`
  virtual void gc(uint64_t version) { }

  // schema
  std::vector<size_t> val_lens_;
  std::vector<size_t> val_off_;
  std::vector<BackupStoreDataType> val_type_;

#if 0
  class Cursor {
   public:
    virtual void seekOffset(uint64_t begin, uint64_t end) = 0;
    virtual bool nextRow() = 0;
    virtual uint64_t key() const = 0;
    virtual char *value(int col_id) const = 0; 
  };
#endif

  class RowCursor {
   public:
    virtual void seekOffset(uint64_t begin, uint64_t end) = 0;
    virtual bool nextRow() = 0;
    virtual uint64_t key() const = 0;
    virtual char *value() const = 0; 
  };

  class ColCursor {
   public:
    virtual void seekOffset(uint64_t begin, uint64_t end) = 0;
    virtual bool nextRow(uint64_t *walk_cnt = nullptr) = 0;
    virtual uint64_t key() const = 0;
    virtual char *value() const = 0;

    virtual uint64_t cur() const = 0;  // current offset
    virtual char *base() const = 0;  // base ptr of static columns
  };

  // col_ids: movable columns
  virtual char *col(int col_id, uint64_t *len = nullptr) const 
    { assert(false); }
  virtual const std::vector<uint64_t> &getKeyCol() const { assert(false); }

#if 0
  virtual Cursor *getCursor(std::vector<int> col_ids, uint64_t ver) const { 
    return nullptr; 
  }
#endif

  virtual std::unique_ptr<RowCursor> getRowCursor(uint64_t ver) const { 
    return nullptr; 
  }

  virtual std::unique_ptr<ColCursor> 
  getColCursor(int col_id, uint64_t ver) const {
    return nullptr; 
  }

 protected:
  // XXX: fix here
#if 0
  struct RowMeta {
    uint64_t key;
    uint64_t min_ver;
    std::vector<int64_t> field_seq;

    RowMeta() : min_ver(-1) { }
  } CACHE_ALIGNED;
#endif

  BackupStore(uint64_t max_items)
    : max_items_(max_items), header_(0), stable_header_(0),
      // fake_header_(0),
      edges_(nullptr) {
#if 0
      meta_(max_items_) { 
    for (RowMeta &m : meta_) {
      m.field_seq.assign(v.size(), -1);
    } 
#endif
  }

  static inline void copy_val_(char *dst, const char *src, uint64_t sz) {
    switch (sz) {
      case 1:
        *(uint8_t *) dst = *(uint8_t *) src;
        break;
      case 2:
        *(uint16_t *) dst = *(uint16_t *) src;
        break;
      case 4:
        *(uint32_t *) dst = *(uint32_t *) src;
        break;
      case 8:
        *(uint64_t *) dst = *(uint64_t *) src;
        break;
      default:
        memcpy(dst, src, sz);
        break;
    }
  }

  // meta data about logic table
  const uint64_t max_items_;

  // std::vector<RowMeta> meta_;

  StoreProf stat_;

#if 1  // cache for performance!
  uint64_t padding_[8];
  // volatile uint64_t fake_header_;
  volatile uint64_t header_;
  uint64_t padding2_[8];
  volatile uint64_t stable_header_;
  uint64_t padding3_[7];
  uint64_t *edges_;
#endif
};

#endif  // BACKUP_STORE_H_

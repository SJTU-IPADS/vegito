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

#ifndef BACKUP_STORE_ROW_H_
#define BACKUP_STORE_ROW_H_

#include "all.h"
#include "backup_store.h"
#include "backup_db.h"
#include <vector>

class BackupStoreRow : public BackupStore {
 public:
  BackupStoreRow(BackupDB::Schema s);

  ~BackupStoreRow();

  // for insert or update 
  virtual void putByOffset(uint64_t offset, uint64_t key, char *val,
                           int64_t seq, uint64_t version);

  // get cursor
  virtual std::unique_ptr<RowCursor> getRowCursor(uint64_t ver) const {
    RowCursor *c = new Cursor(*this, ver);
    return std::unique_ptr<RowCursor>(c);
  }

  virtual uint64_t locateCol(int col_id, uint64_t width) const;

  virtual char *getByOffset(uint64_t offset, uint64_t version);
  virtual char *getByOffset(uint64_t offset, int columnID, uint64_t version,
                            uint64_t *walk_cnt = nullptr);

 private:
  struct RowRecord {
    uint64_t key;
    char val[0]; // flexible array
  };

  struct RowPage {
    uint64_t ver;
    RowPage *prev; // versions are desc ordered
    char bytes[0]; // flexible array

    RowPage(uint64_t v, RowPage *n)
      : ver(v), prev(n) { }
  };

  struct RowMeta {
    uint64_t key;
    int64_t seq = -1;
    uint64_t min_ver;

    uint64_t pgn;  // page number
    uint64_t pgo;  // offset in page in bytes
  } __attribute__ ((aligned (CACHE_LINE_SZ)));

  RowPage *getRowPage_(uint64_t v, RowPage *n) const;

  void updateRecord_(RowPage* page, uint64_t offset_in_page,
                     uint64_t key, char* val);

  uint64_t getOffsetInPage_(uint64_t item_offset) const;

  const uint64_t pg_items_;  // in item
  uint64_t val_len_;  // in bytes
  const uint64_t max_pages_;
  const uint64_t pg_sz_;  // in bytes

  std::vector<RowMeta> meta_;
  std::vector<RowPage *> pages_;
  std::vector<uint32_t> page_locks_;

 public:
  // Cursor always seeks with stable version, so it is lock-free
  class Cursor : public BackupStore::RowCursor {
   public:
    Cursor(const BackupStoreRow &store, uint64_t ver);
    virtual void seekOffset(uint64_t begin, uint64_t end);
    virtual bool nextRow();
    virtual uint64_t key() const {
      return row_.meta_[cur_].key;
    }
    virtual char *value() const;

   private:
    const BackupStoreRow &row_;
    const uint64_t ver_;

    // offset
    uint64_t begin_;
    uint64_t end_;
    uint64_t cur_;

    uint64_t cur_pgn_;
    uint64_t cur_pgo_;
    char *cur_page_;  // content of current page
  };

  friend class Cursor;
};

#endif // BACKUP_STORE_ROW_H_

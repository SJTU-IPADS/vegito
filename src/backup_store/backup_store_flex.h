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

#ifndef BACKUP_STORE_FLEX_H_
#define BACKUP_STORE_FLEX_H_

#include "backup_db.h"
#include "backup_store.h"
#include "custom_config.h"
#include <cstdint>
#include <vector>

class BackupStoreFlex : public BackupStore {
 public:
  BackupStoreFlex(BackupDB::Schema schema, BackupStore *store, uint64_t ver, 
                 const std::vector<std::vector<uint32_t>>& merges);

  ~BackupStoreFlex();

  // for insert
//   virtual void insert(uint64_t off, uint64_t k, char *v, 
//                       uint64_t seq, uint64_t ver);
  
//   virtual void update(uint64_t off, const std::vector<int> &cids, char *v, 
//                       uint64_t seq, uint64_t ver);
                    
//   virtual void update(uint64_t off, int cid, char *v, uint64_t ver);
  
  // get cursor
//   virtual std::unique_ptr<ColCursor> getColCursor(int col_id, uint64_t ver) const {
//     ColCursor *c = new Cursor(*this, col_id, ver);
//     return std::unique_ptr<ColCursor>(c);
//   }

//   virtual uint64_t locateCol(int col_id, int64_t width) const {
//     assert(cols_[col_id].vlen == width);
//     return 0;
//   }

  virtual char *getByOffset(uint64_t offset, int columnID, uint64_t version,
                            uint64_t *walk_cnt = nullptr);

//   virtual void gc(uint64_t version);

  const std::vector<uint64_t> &getKeyCol() const;

  virtual char *col(int col_id, uint64_t *len = nullptr) const {
    return fixCols_[col_id];
  }
  // return pagesize of the col
//   size_t getCol(int col_id, uint64_t start_off, size_t size,
//                 uint64_t lver, std::vector<char *> &pages);
//   size_t getPageSize(int col_id) const;
  char *locateValue(int colid, char *col, size_t offset);
  
 private:
  // physical columns
  struct PColumn {
    uint16_t offset;  // field offset in bytes
    uint16_t width;
    uint16_t primary_col;

    PColumn() { }
    PColumn(uint16_t o)
      :  offset(o) { }
    PColumn(uint16_t o, uint16_t w)
      : offset(o), width(w) { }
  };  

  const int table_id_;
  const uint64_t version_;
  std::vector<BackupDB::Column> cols_;
  std::vector<PColumn> pcols_;

  std::vector<uint64_t> key_col_;
  std::vector<char *> fixCols_;
  uint64_t max_offset_;
};

#endif  // BACKUP_STORE_FLEX_H_


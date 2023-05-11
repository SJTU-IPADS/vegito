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

#include "backup_store_flex.h"
#include "util/util.h"
#include "custom_config.h"

using namespace std;
using namespace nocc::util;

BackupStoreFlex::BackupStoreFlex(BackupDB::Schema schema, BackupStore *store, uint64_t ver, 
                 const std::vector<std::vector<uint32_t>>& merges)
  : BackupStore(schema.max_items),
      table_id_(schema.table_id),
      cols_(schema.cols),
      key_col_(max_items_), 
      fixCols_(schema.cols.size(), nullptr),
      version_(ver)
{
  val_lens_ = store->val_lens_;
  val_type_ = store->val_type_;
  
  max_offset_ = store->getOffsetHeader();
  std::cout << "Table:" << table_id_ << ", max offset:" << max_offset_ << std::endl;

  pcols_.resize(cols_.size());

  for (int i = 0; i < cols_.size(); i++) {
    pcols_[i] = PColumn(0, val_lens_[i]);
  }

  std::vector<bool> flags(cols_.size(), false);

  for (int i = 0; i < merges.size(); ++i) {
    uint64_t off = 0;
    for (int j = 0; j < merges[i].size(); j++) {
      // ensure the merge request is valid
      assert(merges[i][j] < cols_.size());
      assert(!flags[merges[i][j]]);
      flags[merges[i][j]] = true;

      pcols_[merges[i][j]] = PColumn(off);
      off += val_lens_[merges[i][j]];
    }
    
    for (int j = 0; j < merges[i].size(); j++) {
      pcols_[merges[i][j]].width = off;
      pcols_[merges[i][j]].primary_col = merges[i][0];
    }
  }
  
  for (int i = 0; i < cols_.size(); i++) {
    uint16_t wid = pcols_[i].width;
    if (pcols_[i].offset == 0) {
      fixCols_[i] = new char[wid * max_offset_];

      for (int j = 0; j < max_offset_; j++) {
        char* val = store->getByOffset(j, i, ver);
        if (val != nullptr)
          copy_val_(fixCols_[i] + wid * j, val, val_lens_[i]);
      }
    } else {
      uint16_t primary = pcols_[i].primary_col;
      for (int j = 0; j < max_offset_; j++) {
        char* val = store->getByOffset(j, i, ver);
        if (val != nullptr)
          copy_val_(fixCols_[primary] + wid * j + pcols_[i].offset, val, val_lens_[i]);
      }
    }
  }

  // copy key column
  std::vector<uint64_t> kcol = store->getKeyCol();
  for (int i = 0; i < kcol.size(); i++) {
      key_col_[i] = kcol[i];
  }
}

const vector<uint64_t> &BackupStoreFlex::getKeyCol() const {
  return key_col_;
}

char *BackupStoreFlex::getByOffset(uint64_t offset, int col_id,
                                  uint64_t version, uint64_t *walk_cnt) {
  return fixCols_[pcols_[col_id].primary_col] + pcols_[col_id].width * offset + pcols_[col_id].offset;
}

char *BackupStoreFlex::locateValue(int cid, char *col, size_t offset) {
  size_t wid = pcols_[cid].width;
  return (col + wid * offset);
}
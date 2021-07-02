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

#include "backup_index_hash.h"

BackupIndexHash::BackupIndexHash(hash_fn_t hash_fn, size_t max_items)
  : hash_fn_(hash_fn), max_items_(max_items) {
  nodes_ = new BackupNode[max_items_];
}

BackupIndexHash::~BackupIndexHash() {
  delete[] nodes_;
}

BackupNode *BackupIndexHash::Get(uint64_t key) {
  uint64_t offset = hash_fn_(key);
  BackupNode *node = &nodes_[offset];
  // if (node->value_offset == -1) node = NULL;
  return node;
}

BackupNode *BackupIndexHash::GetWithInsert(uint64_t key) {
  uint64_t offset = hash_fn_(key);
  assert(offset < max_items_);
  BackupNode *node = &nodes_[offset];
  return node;
}

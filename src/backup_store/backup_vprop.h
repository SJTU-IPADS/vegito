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

/*
 * This file contains a db class built from backup_store
 */
#ifndef BACKUP_VPROP_H_
#define BACKUP_VPROP_H_

#include <stdint.h>
#include <vector>
#include <unordered_map>

#include "backup_store_col2.h"
#include "backup_db.h"

template<class PropDataType>
class BackupVProp {

 public:

  BackupVProp(BackupStoreCol2* prop_store) 
    : prop_store_(prop_store) { 

  }

  void Insert(uint64_t vid, PropDataType* value, uint64_t ver = 0);

  void Update(uint64_t vid, PropDataType* value, int columnID, uint64_t version);

  PropDataType* Get(uint64_t vid, int columnID, uint64_t version);

  // construct vector for vertices property
  void CopyFrom(std::vector<PropDataType>& data, int columnID, uint64_t version);

  const BackupStoreCol2* get_store() const { return prop_store_; }

  BackupStoreCol2* prop_store_;
};

template<class PropDataType>
void BackupVProp<PropDataType>::Insert(uint64_t vid, PropDataType* value, uint64_t ver) {

}

template<class PropDataType>
void BackupVProp<PropDataType>::Update(uint64_t vid, PropDataType* value, 
                         int columnID, uint64_t version) {

}

template<class PropDataType>
PropDataType* BackupVProp<PropDataType>::Get(uint64_t vid, int columnID, uint64_t version) {

}

// construct vector for vertices property
template<class PropDataType>
void BackupVProp<PropDataType>::CopyFrom(std::vector<PropDataType>& data, int columnID, uint64_t version) {
  
}

#endif


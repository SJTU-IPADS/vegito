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

#include "log_cleaner.h"
#include "framework_cfg.h"

#include <vector>

using namespace std;
using namespace nocc::framework;

namespace nocc{
namespace oltp{

LogCleaner::LogCleaner(bool version) 
  : need_version_(version),
    backup_map_ (config.getNumPrimaries(), nullptr),
    backup_map2_(config.getNumPrimaries(), nullptr) { }  // no bug here

void LogCleaner::add_backup_store(BackupDB *db) {
  int p_id = db->getPID();
  assert(p_id < config.getNumPrimaries());
  // TODO: for single-machine
  // assert(backup_map_[p_id] == nullptr);
  if (backup_map_[p_id]) {
    backup_map2_[p_id] = db;
    return;
  }

  backup_map_[p_id] = db;
}

}  // namespace oltp
}  // namespace nocc

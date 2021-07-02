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

#ifndef BENCH_LOADER_H_
#define BENCH_LOADER_H_

#include "util/fast_random.h"
#include "./utils/thread.h"
#include "memstore/memdb.h"
#include "backup_store/backup_db.h"

namespace nocc {
namespace oltp {

class BenchLoader : public ndb_thread {
 public:
  BenchLoader(uint64_t seed, MemDB *db) ;
  BenchLoader(uint64_t seed, BackupDB *db) ;
  void run();

 protected:
  virtual void load() = 0;
  virtual void loadBackup() { assert(false); }

  const bool isBackup_;
  MemDB * const store_;
  BackupDB * const backup_store_;

  util::fast_random rand_;
  int partition_;
};

}  // namesapce oltp
}  // namespace nocc

#endif

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

#pragma once

#include "ch_mixin.h"
#include "framework/bench_loader.h"

namespace nocc {
namespace oltp {
namespace ch {

/* Loaders */
class ChWarehouseLoader : public BenchLoader, public ChMixin {
 public:
  ChWarehouseLoader(unsigned long seed, int partition, MemDB *store)
      : BenchLoader(seed, store) {
    partition_ = partition;
  }

  ChWarehouseLoader(unsigned long seed, int partition, BackupDB *store)
      : BenchLoader(seed, store) {
    partition_ = partition;    
  }

 protected:
  virtual void load();
  virtual void loadBackup();
};

class ChDistrictLoader : public BenchLoader, public ChMixin {
 public:
  ChDistrictLoader(unsigned long seed, int partition, MemDB *store)
      : BenchLoader(seed, store) {
    partition_ = partition;
  }

  ChDistrictLoader(unsigned long seed, int partition, BackupDB *store)
      : BenchLoader(seed, store) {
    partition_ = partition;    
  }

 protected:
  virtual void load();
  virtual void loadBackup();
};

class ChCustomerLoader : public BenchLoader, public ChMixin {
 public:
  ChCustomerLoader(unsigned long seed, int partition, MemDB *store)
      : BenchLoader(seed, store) {
    partition_ = partition;
  }

  ChCustomerLoader(unsigned long seed, int partition, BackupDB *store)
      : BenchLoader(seed, store) {
    partition_ = partition;    
  }

 protected:
  virtual void load();
  virtual void loadBackup();
};

class ChOrderLoader : public BenchLoader, public ChMixin {
 public:
  ChOrderLoader(unsigned long seed, int partition, MemDB *store)
      : BenchLoader(seed, store) {
    partition_ = partition;
  }

  ChOrderLoader(unsigned long seed, int partition, BackupDB *store)
      : BenchLoader(seed, store) {
    partition_ = partition;    
  }

 protected:
  virtual void load();
  virtual void loadBackup();
};

class ChItemLoader : public BenchLoader, public ChMixin {
 public:
  ChItemLoader(unsigned long seed, int partition, MemDB *store)
      : BenchLoader(seed, store) {
    partition_ = partition;
  }

  ChItemLoader(unsigned long seed, int partition, BackupDB *store)
      : BenchLoader(seed, store) {
    partition_ = partition;    
  }

 protected:
  virtual void load();
  virtual void loadBackup(); 
};

class ChStockLoader : public BenchLoader, public ChMixin {
 public:
  ChStockLoader(unsigned long seed, int partition, MemDB *store)
      : BenchLoader(seed, store) {
    partition_ = partition;
  }
  ChStockLoader(unsigned long seed, int partition, BackupDB *store)
      : BenchLoader(seed, store) {
    partition_ = partition;    
  }

 protected:
  virtual void load();
  virtual void loadBackup(); 

};

class ChSupplierLoader : public BenchLoader, public ChMixin {
 public:
  ChSupplierLoader(unsigned long seed, int partition, BackupDB *store)
      : BenchLoader(seed, store) {
    partition_ = partition;    
  }

 protected:
  virtual void load();
  virtual void loadBackup(); 

};

class ChNationLoader : public BenchLoader, public ChMixin {
 public:
  ChNationLoader(unsigned long seed, int partition, BackupDB *store)
      : BenchLoader(seed, store) {
    partition_ = partition;    
  }

 protected:
  virtual void load();
  virtual void loadBackup(); 


 private:
  struct Nation {
    int id;
    std::string name;
    int regionId;
  };

  static Nation nationNames[];
};

class ChRegionLoader : public BenchLoader, public ChMixin {
 public:
  ChRegionLoader(unsigned long seed, int partition, BackupDB *store)
      : BenchLoader(seed, store) {
    partition_ = partition;    
  }

 protected:
  virtual void load();
  virtual void loadBackup(); 

 private:
  static const char* regionNames[];
};


}  // namespace ch
}  // namespace oltp
}  // namespace nocc

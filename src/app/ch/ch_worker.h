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

#ifndef NOCC_OLTP_CH_WORKER_H_
#define NOCC_OLTP_CH_WORKER_H_

#include "all.h"
#include "ch_schema.h"
#include "ch_mixin.h"
#include "memstore/memdb.h"
#include "backup_store/backup_db.h"

#include "framework/bench_loader.h"
#include "framework/backup_worker.h"
#include "framework/bench_worker.h"

#define STOCK_LEVEL_ORDER_COUNT 10 /* shall be 20 by default */

namespace nocc {
namespace oltp {
namespace ch {
/* Main function */
void ChTest(int argc,char **argv);
int  GetStartWarehouse(int partition);
int  GetEndWarehouse(int partition);
int  NumWarehouses();
int  WarehouseToPartition(int wid);

#define MAX_TXN_WORKERS 24
extern int start_w_id;

struct WorkerBitMap {
  const uint8_t total_num;
  uint32_t bitmap;  // each bit is txn worker
  uint8_t  num;     // number of worker, log(24)
  uint8_t  last_idx;

  WorkerBitMap(int total_num)
    : total_num(total_num), bitmap(0), num(0), last_idx(-1) { 
    assert(total_num <= MAX_TXN_WORKERS);  
  }

  inline void init(int total) { }

  inline void add_worker(int i) {
    assert(i < total_num);
    bitmap |= (1 << i);
    ++num;
  }

  inline void delete_worker(int i) {
    bitmap ^= (1 << i);
    --num;
    if (last_idx == i) {
      last_idx = -1;
    }
  }

  inline int find_worker() {
    assert(num > 0);
    if (num == 1 && last_idx != uint8_t(-1)) return last_idx;
    int i = (last_idx + 1) % total_num;
    while (i != last_idx) {
      if (bitmap & (1 << i)) {
        last_idx = i;
        return i;
      }
      i = (i + 1) % total_num;
    }
    assert(false);
    return -1;
  }

  // switch from a old worker to a new worker
  inline void change(int new_id) {
    if (last_idx == uint8_t(-1)) find_worker();

    assert(last_idx != uint8_t(-1) && num == 1);
    bitmap |= (1 << new_id);
    bitmap ^= (1 << last_idx);
    last_idx = new_id;
  }

  inline void print(int w_idx) {
    printf("[ChWorker] w_id %d -> worker ", w_idx + start_w_id);
    int i = find_worker();
    printf("%d", i);
    for (int next = find_worker(); next != i; next = find_worker()) {
      printf(" | %d", next);
    }
    printf("\n");
  }
};

extern std::vector<WorkerBitMap> wid2tid;  // (w_id - start_w_id) -> ChWorker ids

struct ChReq {
  int32_t w_id;
};

struct ChResp {
  int32_t w_id;
};

/* Tx's implementation */
class ChWorker : public ChMixin, public BenchWorker {

 public:
  ChWorker(unsigned int worker_id,unsigned long seed,
           uint warehouse_id_start,uint warehouse_id_end,
           MemDB *db);

 protected:
  virtual void process_param(const void *padding) override {
    const ChReq *req = (const ChReq *) padding;
    home_wid_ = req->w_id;
  }

  virtual void prepare_param_self(int cor_id) override {
    home_wid_ = PickWarehouseId(rand_gen_[cor_id], start_w_, end_w_);
  }

  virtual void prepare_response(void *padding) override {
    static_assert(sizeof(ChResp) <= TXN_PADDING_SZ);
    ChResp *resp = (ChResp *) padding;
    resp->w_id = home_wid_;
  }

  virtual workload_desc_vec_t get_workload() const ;
  virtual void check_consistency();

 private:
  /* Wrapper for implementation of transaction */
  static txn_result_t TxnNewOrder(BenchWorker *w,yield_func_t &yield) {
    return static_cast<ChWorker *>(w)->txn_new_order(yield);
    // return static_cast<ChWorker *>(w)->txn_micro_new_order(yield);
    // return static_cast<ChWorker *>(w)->txn_micro_index(yield);
  }

  static txn_result_t TxnPayment(BenchWorker *w,yield_func_t &yield) {
    return static_cast<ChWorker *>(w)->txn_payment(yield);
  }

  static txn_result_t TxnDelivery(BenchWorker *w,yield_func_t &yield) {
    return static_cast<ChWorker *>(w)->txn_delivery(yield);
  }

  static txn_result_t TxnStockLevel(BenchWorker *w,yield_func_t &yield) {
    return static_cast<ChWorker *>(w)->txn_stock_level(yield);
  }

  static txn_result_t TxnOrderStatus(BenchWorker *w,yield_func_t &yield) {
    return static_cast<ChWorker *>(w)->txn_order_status(yield);
  }
 
  txn_result_t txn_new_order(yield_func_t &yield);
  txn_result_t txn_payment(yield_func_t &yield);
  txn_result_t txn_delivery(yield_func_t &yield);
  txn_result_t txn_stock_level(yield_func_t &yield);
  txn_result_t txn_order_status(yield_func_t &yield);
  
  txn_result_t txn_micro_new_order(yield_func_t &yield);
  txn_result_t txn_micro_index(yield_func_t &yield);

  const uint start_w_ ;
  const uint end_w_ ;
  int home_wid_;
  uint64_t last_no_o_ids_[240][10];  // TODO: max warehouse of each thread is 240
}; // class ChWorker

#if 0
class ChClient : public BenchClient {
 public:
  ChClient(int num_workers)
    : BenchClient(num_workers), num_wid_(wid2tid.size()), send_wid_(0) { }

 protected:
  virtual int prepare_worker_id() override {
#if 1
    int worker_id = wid2tid[send_wid_ % num_wid_].find_worker();
    return worker_id;
#else

#if 0
    int tids[] = {0, 0, 1, 1, 2, 2, 3, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13,
                14, 15, 16, 17, 18, 19};
#endif
    int tids[] = {  0,  1,  2,  3, 
                    4,  5,  6,  7, 
                    8,  9, /* 10, 11, */
                 //   12, 13, 14, 15, 
                 //  16, 17, 18, 19,
                 //   0,  1,  2,  3, 
                 //   4,  5,  6,  7, 
                 //   8,  9, 10, 11, 
                 //   12, 13, 14, 15, 
                 //   16, 17, 18, 19
                 };
    // volatile int worker_id1 = wid2tid[send_wid_ % num_wid_].find_worker();
    int worker_id = tids[send_wid_ % (sizeof(tids) / sizeof(int))];
    // assert(worker_id1 == worker_id);
    return worker_id;
#endif
  }
  
  virtual void prepare_param(void *padding) override {
    static_assert(sizeof(ChReq) <= TXN_PADDING_SZ);
    ChReq *req = (ChReq *) padding;
    req->w_id = (send_wid_ % num_wid_) + start_w_id;
    ++send_wid_;  // add send_wid_ here, overwrite by `process_response`
  }

  virtual void process_response(const void *padding) override {
    const ChResp *resp = (const ChResp *) padding;
    send_wid_ = resp->w_id - start_w_id;
  }
  
 private:
  const int num_wid_;   
  int send_wid_;  // warehouse id
};
#endif

#define MIN_OL_CNT  5
#define MAX_OL_CNT 15
struct ChNewOrderParam {
  uint32_t w_id;  
  uint32_t d_id;
  uint32_t c_id;
  uint32_t ol_cnt;
  uint32_t o_entry_d;
  uint32_t ol_i_ids[MAX_OL_CNT];
  uint32_t ol_supply_w_ids[MAX_OL_CNT];
  uint32_t ol_quantities[MAX_OL_CNT];

  ChNewOrderParam(fast_random &r, uint32_t w_id) 
    : w_id(w_id), d_id(RandomNumber(r, 1, NumDistrictsPerWarehouse())),
      c_id(GetCustomerId(r)), ol_cnt(RandomNumber(r, MIN_OL_CNT, MAX_OL_CNT)),
      o_entry_d(GetCurrentTimeMillis()) 
  { 
    std::set<uint64_t> stock_set;//remove identity stock ids
    for (int i = 0; i < ol_cnt; ++i) {
      uint32_t i_id = GetItemId(r);
      uint32_t su_w_id;
#if ALL_LOCAL == 0
      if (likely( NumWarehouses() == 1 ||
        RandomNumber(r, 1, 100) > chConfig.getNewOrderRemoteItemPct()))
#else
      if(1)
#endif 
      {
        // local supplier
        su_w_id = w_id;
      } else {
        // remote supplier
        do {
          su_w_id = RandomNumber(r, 1, NumWarehouses());
        } while (su_w_id == w_id);
      }
      uint64_t s_key = makeStockKey(su_w_id, i_id);
      if(stock_set.find(s_key)!=stock_set.end()) {
        --i;
        continue;
      } else {
        stock_set.insert(s_key);
      }
      
      ol_i_ids[i] = i_id;
      ol_supply_w_ids[i] = su_w_id;
      ol_quantities[i] = RandomNumber(r, 1, 10);
    }

  }
};

struct ChPaymentParam {
  uint32_t c_w_id;
  uint32_t c_d_id;
  uint32_t w_id;
  uint32_t d_id;
  float h_amount;

  bool choose_by_name;
  std::string c_last;
  uint32_t c_id;
  uint32_t h_date;
  
  ChPaymentParam(fast_random &r, uint32_t c_w_id)
    : c_w_id(c_w_id), c_d_id(RandomNumber(r, 1, NumDistrictsPerWarehouse())),
      h_date(GetCurrentTimeMicro()) 
  { 
  
#if ALL_LOCAL == 0
    if (likely(NumWarehouses() == 1 || RandomNumber(r, 1, 100) <= 85))
#else
    if (1) 
#endif
    {
      // local
      d_id = c_d_id;
      w_id = c_w_id;
    } else {
      // remote
      d_id = RandomNumber(r, 1, NumDistrictsPerWarehouse());
      do {
        w_id = RandomNumber(r, 1, NumWarehouses());
      } while (w_id == c_w_id);
    }

    h_amount = RandomNumber(r, 100, 500000) / 100.0f;
    choose_by_name = (RandomNumber(r, 1, 100) <= 60);
    if (choose_by_name) {
      // cust by name
      uint8_t lastname_buf[CustomerLastNameMaxSize + 1];
      static_assert(sizeof(lastname_buf) == 16, "xx");
      NDB_MEMSET(lastname_buf, 0, sizeof(lastname_buf));
      GetNonUniformCustomerLastNameRun(lastname_buf, r);
      c_last.assign((const char *) lastname_buf, 16);
    } else {
      // cust by ID
      c_id = GetCustomerId(r);
    }  
  }
};

struct ChDeliveryParam {
  uint32_t w_id;
  uint32_t o_carrier_id;
  
  ChDeliveryParam(fast_random &r, uint32_t w_id) 
    : w_id(w_id), o_carrier_id(RandomNumber(r, 1, NumDistrictsPerWarehouse()))
  { }
};

struct ChStockLevelParam {
  uint32_t w_id;
  uint32_t d_id;
  uint32_t threshold;

  ChStockLevelParam(fast_random &r, uint32_t w_id)
    : w_id(w_id), d_id(RandomNumber(r, 1, NumDistrictsPerWarehouse())),
      threshold(RandomNumber(r, 10, 20))
  { }
};

struct ChOrderStatusParam {
  uint32_t w_id;
  uint32_t d_id;
  bool choose_by_name;
  std::string c_last;
  uint32_t c_id;
    
  ChOrderStatusParam(fast_random &r, uint32_t w_id)
    : w_id(w_id), d_id(RandomNumber(r, 1, NumDistrictsPerWarehouse())),
      choose_by_name((RandomNumber(r, 1, 100) <= 60))  
  { 
    if (choose_by_name) {
      //2.6.1.2 cust by name
      uint8_t lastname_buf[CustomerLastNameMaxSize + 1];
      static_assert(sizeof(lastname_buf) == 16, "xx");
      NDB_MEMSET(lastname_buf, 0, sizeof(lastname_buf));
      GetNonUniformCustomerLastNameRun(lastname_buf, r);
      c_last.assign((const char *) lastname_buf, 16);
    } else {
      //2.6.1.2 cust by ID
      c_id = GetCustomerId(r); 
    }

  
  }
};

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
#endif

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

#include "mindex_main.h"

#include "mindex_config.h"

#include "framework/framework_cfg.h"
#include "framework/view_manager.h"

#include "framework/bench_runner.h"
#include "framework/bench_loader.h"
#include "framework/bench_worker.h"

#include "util/timer.h"

using namespace std;
using namespace nocc::util;
using namespace nocc::framework;

namespace nocc::oltp::mindex {

class MIndexRunner : public Runner {
 public:
  MIndexRunner() { };
  
  virtual vector<BenchLoader *> make_loaders(int partition, MemDB* store = nullptr);
  virtual vector<BenchWorker *> make_workers();
  virtual void init_store(MemDB* &store);
};
  
// utils for generating random #s and strings
inline ALWAYS_INLINE int
CheckBetweenInclusive(int v, int lower, int upper) {
  INVARIANT(v >= lower);
  INVARIANT(v <= upper);
  return v;
}

inline ALWAYS_INLINE int
RandomNumber(util::fast_random &r, int min, int max) {
  return CheckBetweenInclusive(
          (int) (r.next_uniform() * (max - min + 1) + min), min, max);
}

inline ALWAYS_INLINE int64_t
makeOrderLineKey(int32_t w_id, int32_t d_id, int32_t o_id, int32_t number) {
  int32_t upper_id = w_id * 10 + d_id;
  int64_t oid = static_cast<int64_t>(upper_id) * 10000000 + static_cast<int64_t>(o_id);
  int64_t olid = oid * 15 + number;
  int64_t id = static_cast<int64_t>(olid);
  // assert(orderLineKeyToWare(id) == w_id);
  return id;
}

/* Loaders */
class MIndexLoader : public BenchLoader {
 public:
  MIndexLoader(int partition, MemDB *store)
      : BenchLoader(seed_, store) {
    partition_ = partition;
  }

 protected:
  virtual void load();
 
 private:
  MemDB *store_;
  static const uint64_t seed_ = 2343352;
};

const int dist_per_ware = 10;
const int cust_per_dist = 3000;

void MIndexLoader::load() {
  Breakdown_Timer::get_one_second_cycle();

  Breakdown_Timer timer;
  timer.start();
  int w_begin = partition_ * config.getScaleFactorPerPartition() + 1;
  int w_end   = (partition_ + 1) * config.getScaleFactorPerPartition() + 1;
  int cnt = 0;
  for (int w = w_begin; w < w_end; ++w) {
    for (int d = 1; d <= dist_per_ware; ++d) {
      for (int c = 1; c <= cust_per_dist; ++c) {
        // ++cnt;
        int ol_cnt = RandomNumber(rand_, 5, 15);
        for (int l = 1; l <= ol_cnt; ++l) {
          uint64_t olkey = makeOrderLineKey(w, d, c, l);
          uint64_t *olval = new uint64_t(olkey);
          store_->Put(0, olkey, olval);
          ++cnt;
        }
        
      }
    }
  }
  float ms = timer.get_diff_ms();
  printf("  [Micro Index] load %d records using %f ms\n", cnt, ms);

}

void MIndexRunner::init_store(MemDB* &store) {
  assert(store == nullptr);
  store = new MemDB();
  
  store->AddSchema(0, TAB_BTREE, sizeof(uint64_t), sizeof(uint64_t));
}

vector<BenchLoader *>
MIndexRunner::make_loaders(int partition, MemDB* store) {
  /* Current we do not consider parallal loading */
  vector<BenchLoader *> ret;
  ret.push_back(new MIndexLoader(partition, store));

  return ret;
}

class MIndexWorker : public BenchWorker {
 public:
  MIndexWorker(uint32_t worker_id, uint64_t seed, int w_begin, int w_end,
               MemDB *db)
    : BenchWorker(worker_id, seed, db),
    w_begin_(w_begin), w_end_(w_end), store_(*(db->stores_[0])),  
    next_ol_id_(new int[w_end - w_begin][dist_per_ware]) {
     for (int w = w_begin_; w < w_end_; ++w) {
       int w_i = w - w_begin_;
       for (int d = 1; d <= dist_per_ware; ++d) {
         int d_i = d - 1;
         next_ol_id_[w_i][d_i] = cust_per_dist + 1;
       }
     } 
  }

  static txn_result_t Put(BenchWorker *w, yield_func_t & /* yield */) {
    MIndexWorker *worker = reinterpret_cast<MIndexWorker *>(w);
    worker->put(); 
    return txn_result_t(true, 0);
  }

  static txn_result_t Get(BenchWorker *w, yield_func_t & /* yield */) {
    MIndexWorker *worker = reinterpret_cast<MIndexWorker *>(w);
    worker->get(); 
    return txn_result_t(true, 0);
  }

 protected:
  virtual void thread_local_init() {
    cnt_ = 0;
    keys_ = new uint64_t[M];
    int i = 0;
    while (1) {
      int w = RandomNumber(rand_gen_[cor_id_], w_begin_, w_end_ - 1);
      int d = RandomNumber(rand_gen_[cor_id_], 1, dist_per_ware);
      int o = next_ol_id_[w - w_begin_][d - 1]++;
      int num_ol = RandomNumber(rand_gen_[cor_id_], 5, 15); 
      
      for (int ol = 1; ol <= num_ol; ++ol) {
        uint64_t ol_key = makeOrderLineKey(w, d, o, ol);
        keys_[i++] = ol_key;
        if (i >= M) return;
      }
    }
  }

  virtual workload_desc_vec_t get_workload() const {  
    workload_desc_vec_t w;
  
    int pct = miConfig.getTxnWorkloadMix(MIndexConfig::PUT);
    if (pct)
      w.emplace_back("Put", double(pct) / 100.0, Put);
    pct = miConfig.getTxnWorkloadMix(MIndexConfig::GET);
    if (pct)
      w.emplace_back("Get", double(pct) / 100.0, Get);

    return w;
  }

 private:
  void put();
  void get();

  static const int M = 40000000;
  const int w_begin_;
  const int w_end_;
  Memstore &store_;
  
  int (*next_ol_id_) [dist_per_ware];
  uint64_t *keys_;
  uint64_t cnt_;
};

void MIndexWorker::put() {
#if 0
  int w = RandomNumber(rand_gen_[cor_id_], w_begin_, w_end_ - 1);
  int d = RandomNumber(rand_gen_[cor_id_], 1, dist_per_ware);
  int o = next_ol_id_[w - w_begin_][d - 1]++;
  int num_ol = RandomNumber(rand_gen_[cor_id_], 5, 15); 
  for (int ol = 1; ol <= num_ol; ++ol) {
    uint64_t ol_key = makeOrderLineKey(w, d, o, ol);
    uint64_t *ol_val  = new uint64_t(ol_key);
    // store_.Put(ol_key, ol_val); 
  }
#else
  if (cnt_ >= M) {
    while(1) ; 
  }
  uint64_t ol_key = keys_[cnt_++]; 
  uint64_t *ol_val  = new uint64_t(ol_key);
  store_.Put(ol_key, ol_val);
#endif
}

void MIndexWorker::get() {
  int w = RandomNumber(rand_gen_[cor_id_], w_begin_, w_end_ - 1);
  int d = RandomNumber(rand_gen_[cor_id_], 1, dist_per_ware);
  int o = RandomNumber(rand_gen_[cor_id_], 1, next_ol_id_[w - w_begin_][d - 1]);

  uint64_t ol_b = makeOrderLineKey(w, d, o, 1);
  uint64_t ol_e = makeOrderLineKey(w, d, o + 1, 0);

  Memstore::Iterator *iter = store_.GetIterator();
  iter->Seek(ol_b);

  for ( ;iter->Valid(); iter->Next()) {
    uint64_t ol_key = iter->Key();
    if (ol_key >= ol_e) break;
    assert(ol_key == *iter->CurNode()->value);
  }
}

vector<BenchWorker *> MIndexRunner::make_workers() {
  vector<BenchWorker *> ret;

  int mac_id = config.getServerID();
  vector<int> primaries = view.get_primaries(mac_id);
  int num_primaries = primaries.size();
  int num_txn_workers = config.getNumTxnThreads();
  int scale_factor = config.getScaleFactorPerPartition();  // per partition
  int n_ware_per_worker = scale_factor * num_primaries / num_txn_workers;

  assert(num_primaries == 1);  // XXX: Todo

  int start_pid = primaries[0];
  fast_random r(23984543 + start_pid);
  
  for(int i = 0;i < num_txn_workers; ++i) {
    // reponse for [start_ware, end_ware)
    uint32_t w_b = start_pid * scale_factor + i + 1;
    uint32_t w_e = w_b + n_ware_per_worker;
    ret.push_back(new MIndexWorker(i, r.next(), w_b, w_e, stores_[0]));
  }

  return ret;
}
  
void MIndexTest(int argc, char **argv) {
  miConfig.parse_args(argc, argv);
  miConfig.parse_xml(config.getConfigFile());
  miConfig.print_config();

  MIndexRunner().run();
}

}

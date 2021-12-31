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

#ifndef NOCC_OLTP_CH_QUERY_H_
#define NOCC_OLTP_CH_QUERY_H_

#include <unordered_set>

#include "all.h"
#include "ch_schema.h"
#include "ch_mixin.h"
#include "memstore/memdb.h"

#include "framework/bench_query.h"
#include "framework/framework.h"

#define OL_GRAPH 0
#define STAT_JOIN 0

namespace nocc {
namespace oltp {

namespace ch {

typedef const std::vector<uint64_t> &keys_t;
using Keys = const std::vector<uint64_t> &;

inline int64_t key_binary_search(Keys keys, uint64_t k) {
  int mid, header = 0, tailer = keys.size() - 1;

  while (header <= tailer) {
    mid = (header + tailer) / 2;
    uint64_t mid_key = keys[mid];
    if (mid_key == k)
      return mid;
    if (mid_key < k)
      header = mid + 1;
    else tailer = mid - 1;
  }

  return -1;
}

#define DECLARE_QUERY(x) \
  protected: \
    static bool Q##x(QueryWorker *w, yield_func_t &yield) { \
      return static_cast<ChQueryWorker *>(w)->query##x(yield); \
    } \
  private: \
    bool query##x(yield_func_t &yield); \
    void query##x##_init(); \
  protected:

#define DECLARE_MICRO(x, y) \
  protected: \
    static bool Q##x(QueryWorker *w, yield_func_t &yield) { \
      return static_cast<ChQueryWorker *>(w)->micro_##y(yield); \
    } \
  private: \
    bool micro_##y(yield_func_t &yield); \
  protected:

// contexts for worker thhreads
struct BaseCtx {
  BackupDB &db;
  const uint64_t ver;

  // partition informatio, [off_begin, off_end)
  const uint64_t off_begin;  // begin offset
  const uint64_t off_end;    // end offset

  uint64_t cnt;  // Evaluation data
  uint64_t walk_cnt;

  std::vector<float> mem_ms;   // ms of memory access

  inline BaseCtx(BackupDB &d, uint64_t v, uint64_t b, uint64_t e)
    : db(d), ver(v), off_begin(b), off_end(e), cnt(0), walk_cnt(0),
      mem_ms(10, 0.0) { }
};

class ChQueryWorker : public ChMixin, public QueryWorker {
 public:
  // response for [start_w, end_w)
  ChQueryWorker(uint32_t worker_id, uint32_t num_thread, uint32_t seed, 
                uint32_t start_w, uint32_t end_w,
                BackupDB *store);

  virtual QryDescVec get_workload() const;

 protected:
  virtual void thread_local_init();

  inline std::vector<std::vector<uint64_t>> 
  part_workloads(uint64_t num_item) const {
    uint64_t workload_per_thread = (num_item +  num_thread_ - 1) / num_thread_;

    std::vector<std::vector<uint64_t>> res(num_thread_);

    for (int i = 0; i < num_thread_; ++i) {
      uint64_t off_begin = workload_per_thread * i;
      uint64_t off_end = workload_per_thread * (i + 1);
      if (off_end >= num_item)
        off_end = num_item;
      res[i].push_back(off_begin);
      res[i].push_back(off_end);
    }
  
    return res;
  }

  template<class Ctx>
  inline void parallel_process(const std::vector<Ctx> &ctxs, TaskFn query) {
    // start query
    for (int i = 1; i < subs_.size(); ++i) {
      subs_[i]->set_task(query, (void *) &ctxs[i]);
    }
  
    query((void *) &ctxs[0]);
  
    for (int i = 1; i < subs_.size(); ++i) 
      subs_[i]->clear();
  }

  // ch benchmark
  DECLARE_QUERY(01);
  DECLARE_QUERY(02);
  DECLARE_QUERY(03);
  DECLARE_QUERY(04);
  DECLARE_QUERY(05);
  DECLARE_QUERY(06);
  DECLARE_QUERY(07);
  DECLARE_QUERY(08);
  DECLARE_QUERY(09);
  DECLARE_QUERY(10);
  DECLARE_QUERY(11);
  DECLARE_QUERY(12);
  DECLARE_QUERY(13);
  DECLARE_QUERY(14);
  DECLARE_QUERY(15);
  DECLARE_QUERY(16);
  DECLARE_QUERY(17);
  DECLARE_QUERY(18);
  DECLARE_QUERY(19);
  DECLARE_QUERY(20);
  DECLARE_QUERY(21);
  DECLARE_QUERY(22);

  // mirco benchmark
  DECLARE_MICRO(23, col_static);    // static column
  DECLARE_MICRO(24, col_update);    // dynamic column: update w/o insert
  DECLARE_MICRO(25, col_update2);   // dynamic column: update w/o insert
  DECLARE_MICRO(26, freshness);     // freshness
  DECLARE_MICRO(27, index);         // point query on index
  DECLARE_MICRO(28, graph);         // join with graph

 private:
  const uint32_t start_w_;
  const uint32_t end_w_;
  BackupDB *db_;
};


extern const BackupStore *c_tbl;
extern uint64_t c_tbl_sz;
extern const inline_str_fixed<2> *c_states;
extern const inline_str_8<16> *c_lasts;
extern const inline_str_8<20> *c_cities;
extern const inline_str_fixed<16> *c_phones;

extern const BackupStore *no_tbl;

extern const BackupStore *o_tbl;
extern const int8_t *o_ol_cnts;
extern const int32_t *o_c_ids;
extern const uint32_t *o_entry_ds;

extern       BackupStore *ol_tbl;
extern const int8_t *ol_quantities;
extern const int32_t *ol_i_ids;
extern const AMOUNT *ol_amounts;
extern const int32_t *ol_supply_w_ids;

extern const BackupStore *s_tbl;
extern uint64_t s_tbl_sz;

extern const BackupStore *i_tbl;
extern uint64_t i_tbl_sz;
extern const inline_str_8<50> *i_datas;
extern const inline_str_8<24> *i_names;
extern const AMOUNT *i_prices;

extern const BackupStore *r_tbl;
extern const inline_str_8<15> *r_names;

extern const BackupStore *n_tbl;
extern const int8_t *n_regionkeys;
extern const inline_str_8<15> *n_names;

extern const BackupStore *su_tbl;
extern const BackupStore *su_tbl;
extern const int8_t *su_nationkeys;
extern const inline_str_fixed<15> *su_phones;
extern const inline_str_fixed<20> *su_names;
extern const inline_str_8<40> *su_addresses;
extern const inline_str_8<100> *su_comments;


}  // namespace ch
}  // namespace oltp
}  // namespace nocc

#endif  // NOCC_OLTP_CH_QUERY_H_


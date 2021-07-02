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

#include "app/ch/ch_query.h"

using namespace std;
using namespace nocc::oltp::ch;

#define TIMER 0

#include "ch_query_timer.h"

// XXX: 0 for performance, 1 for correctness
#define Q2_STABLE_SORT 0

namespace {

const char *R_NAME_STR = "EUROP";
const char CONST_I_DATA_CHAR_TEMPLATE = 'b';

struct Supplier {
  uint64_t   su_suppkey;
  const char *n_name;
  const char *su_name;
  const char *su_address;
  const char *su_phone;
  const char *su_comment;

  static bool Compare(const Supplier &l, const Supplier &r) {
    return (l.su_suppkey < r.su_suppkey);
  }
};

struct MinQ {
  int16_t m_s_quantity;
  vector<const Supplier *> su_vec;
};

struct MidResult {
  int32_t s_i_id;
  int16_t s_quantity;
  const Supplier *su;

  MidResult(int32_t i, int16_t s, const Supplier *su)
    : s_i_id(i), s_quantity(s), su(su) { }

  static bool Compare(const MidResult &l, const MidResult &r) {
    return (l.s_i_id < r.s_i_id) || (l.s_i_id == r.s_i_id && l.s_quantity >= r.s_quantity);
  }
};

// using M = map<int32_t , MinQ>;  // m_i_id (ordered) -> (m_s_quantity, info)
#if 0
using M = map<int32_t , MinQ>;  // m_i_id (ordered) -> (m_s_quantity, info)
using SubM = map<int32_t , MinQ>;  // m_i_id (ordered) -> (m_s_quantity, info)
using SuMap = unordered_map<uint64_t, Supplier>;  // su_key -> Supplier
#else
using M = map<int32_t , MinQ>;  // m_i_id (ordered) -> (m_s_quantity, info)
using SubM = vector<MidResult>;  // m_i_id (ordered) -> (m_s_quantity, info)
using SuMap = vector<Supplier>;
#endif

struct ResultRow {
  int64_t su_suppkey;
  const char *su_name;
  const char *n_name;
  int64_t i_id;
  const char *i_name;
  const char *su_address;
  const char *su_phone;
  const char *su_comment;

  ResultRow(int64_t su_suppkey, const char *su_name, const char *n_name, 
            int64_t i_id, const char *i_name, const char *su_address, 
            const char *su_phone, const char *su_comment)
    : su_suppkey(su_suppkey), su_name(su_name), n_name(n_name), 
      i_id(i_id), i_name(i_name), su_address(su_address), 
      su_phone(su_phone), su_comment(su_comment) { }

  static bool Compare(const ResultRow &a, const ResultRow &b){
    int res;
    res = strcmp(a.n_name, b.n_name);
    if(res < 0)
      return true;
    else if(res > 0)
      return false;
    else {
      res = strncmp(a.su_name, b.su_name, 20);

      if(res < 0)
        return true;
      else if(res > 0)
        return false;
      else
        return a.i_id < b.i_id;
    }
  }

  static void print_header() {
    printf("%-10s %-20s %-16s %-10s %-24s %-40s %-15s %-16s\n", 
           "su_suppkey", "su_name", "n_name", "i_id", "i_name",
           "su_address", "su_phone", "su_comment");
  }

  void print() const {
    printf("%-10ld %-20.20s %-16s %-10ld %-24s %-40s %-15.15s %-16.16s...\n",
        su_suppkey, su_name, n_name, i_id, i_name,
        su_address, su_phone, su_comment);
  }
};

struct Ctx : public BaseCtx {
  SubM &sub_m;
  const SuMap &su_buf;

  Ctx(BackupDB &d, uint64_t v, uint64_t b, uint64_t e, SubM &m, const SuMap &su) 
    : BaseCtx(d, v, b, e), sub_m(m), su_buf(su) { }
} __attribute__ ((aligned (CACHE_LINE_SZ))) ;

void query(void* arg);

thread_local SuMap su_buf_;
thread_local vector<SubM *> sub_m_vec_;
thread_local vector<ResultRow> result_table;

} // namespace anonymous

namespace nocc {
namespace oltp {
namespace ch {

void ChQueryWorker::query02_init() {
  // printf("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! thread_num %d\n", num_thread_);
  Keys su_keys = su_tbl->getKeyCol();
  su_buf_.reserve(su_keys.size());

  for (int i = 0; i < num_thread_; ++i) {
    SubM *m = new SubM();
    m->reserve(350000); 
    sub_m_vec_.push_back(m); 
  }
  assert(sub_m_vec_.size() != 0);
  result_table.reserve(3000);
}

bool ChQueryWorker::query02(yield_func_t &yield) {
  uint64_t ver = get_read_ver_();

  // Result data
  result_table.clear();
  declare_timer(timer);
  declare_timer(gtimer);

  timer_start(gtimer);

#if 0
  // mirco eval 
  // select count(*) from item where i_data like '%b'
  int i_cnt = 0;
  for (int i_i = 0; i_i < i_tbl_sz; ++i_i) {
    const inline_str_8<50> *i_data = &i_datas[i_i];
    char last = i_data->data()[i_data->size() - 1];

    if(last == CONST_I_DATA_CHAR_TEMPLATE) {
      ++i_cnt;
    }
  }
  printf("item cnt = %d / %lu\n", i_cnt, i_tbl_sz);
  return true;
#endif

  /*
   * Construct "su_buf" by nested loop join
   */
  timer_start(timer);

  Keys r_keys = r_tbl->getKeyCol();
  Keys n_keys = n_tbl->getKeyCol();
  Keys su_keys = su_tbl->getKeyCol();

  su_buf_.clear();

  // Nested-Loop
  for (uint64_t r_i = 0; r_i < r_keys.size(); ++r_i) {
    if (strncmp(r_names[r_i].data(), R_NAME_STR, strlen(R_NAME_STR)) != 0)
      continue;

    uint64_t r_key = r_keys[r_i];
   
    for (uint64_t n_i = 0; n_i < n_keys.size(); ++n_i) {
      if (n_regionkeys[n_i] != r_key) continue;
      uint64_t n_key = n_keys[n_i];
      
      for (uint64_t su_i = 0; su_i < su_keys.size(); ++su_i) {
        if (su_nationkeys[su_i] == n_key) {

          uint64_t su_key = su_keys[su_i];
#if 0
          Supplier &su = su_buf[su_key];

          su.su_suppkey = su_key;
          su.su_name = su_names[su_i].data();
          su.su_address = su_addresses[su_i].data();
          su.su_phone = su_phones[su_i].data();
          su.su_comment = su_comments[su_i].data();
          su.n_name = n_names[n_i].data();
#else
          Supplier su;

          su.su_suppkey = su_key;
          su.su_name = su_names[su_i].data();
          su.su_address = su_addresses[su_i].data();
          su.su_phone = su_phones[su_i].data();
          su.su_comment = su_comments[su_i].data();
          su.n_name = n_names[n_i].data();
          su_buf_.push_back(su);
        }
#endif
      } // end loop supplier
    }  // end loop nation
  } // end loop region

  sort(su_buf_.begin(), su_buf_.end(), Supplier::Compare);
  timer_end_print(timer, "Before parallel");

  /**************** Parallel Part Begin ********************/
  // Calculate workload for each thread
  vector<Ctx> ctxs;
  ctxs.reserve(num_thread_);
  vector<vector<uint64_t>> workloads = part_workloads(s_tbl_sz);

  assert(sub_m_vec_.size() == num_thread_);
  for (int i = 0; i < num_thread_; ++i) {
    const auto &v = workloads[i];
    uint64_t off_begin = v[0],
             off_end   = v[1];
    sub_m_vec_[i]->clear();
    ctxs.emplace_back(*db_, ver, off_begin, off_end, *sub_m_vec_[i], su_buf_);
  }

  timer_start(timer);
  parallel_process(ctxs, query);
  timer_end_print(timer, "Parallel");

  /**************** Parallel Part End *******************/
  timer_start(timer);

  Keys i_keys = i_tbl->getKeyCol(); 

# if 1  // 1 for use vector
  for (int i = 1; i < num_thread_; ++i) {
    sub_m_vec_[0]->insert(sub_m_vec_[0]->end(),
                          sub_m_vec_[i]->begin(), sub_m_vec_[i]->end());
  } 

#if 0
  static thread_local int flag = 0;
  if (flag < 10) {
    printf("vec %d size %lu\n", 0, sub_m_vec_[0]->size()); 
    ++flag;
  }
#endif
  // XXX: for performance, stable_sort is correct but low-efficiency (memory usgae)
#if Q2_STABLE_SORT
  stable_sort(sub_m_vec_[0]->begin(), sub_m_vec_[0]->end(), MidResult::Compare);
#else
  sort(sub_m_vec_[0]->begin(), sub_m_vec_[0]->end(), MidResult::Compare);
#endif

  SubM &sub_m = *sub_m_vec_[0];
  int32_t cur_i_id = sub_m[0].s_i_id;
  int cur_i_i = 0, cur_s_i = 0;
  while (cur_i_i < i_keys.size() && cur_s_i < sub_m.size()) {
    uint64_t s_i_id = sub_m[cur_s_i].s_i_id;
    uint64_t i_key = i_keys[cur_i_i];

    // printf("s_i_id = %lu, i_key = %lu\n", s_i_id, i_key);

    // move item
    if (i_key <  s_i_id) {
      ++cur_i_i;
      continue;
    }

    // move sub_m
    if (i_key > s_i_id) {
      ++cur_s_i;
      continue;
    }

    assert(s_i_id == i_key);
    
    const inline_str_8<50> *i_data = &i_datas[cur_i_i];
    char last = i_data->data()[i_data->size() - 1];

    if(last != CONST_I_DATA_CHAR_TEMPLATE) {
      ++cur_i_i;
      continue;
    }
     
    int16_t cur_s_quantity = sub_m[cur_s_i].s_quantity;
    for ( ; cur_s_i < sub_m.size() && sub_m[cur_s_i].s_i_id == s_i_id; ++cur_s_i) {
      int16_t s_quantity = sub_m[cur_s_i].s_quantity;
      if (s_quantity != cur_s_quantity) break;

      if (s_i_id >= sub_m.size()) {
        printf("s_i_id %d, sub_m.size %d\n", (int) s_i_id, (int) sub_m.size());
        assert(false);
      }

      const Supplier &su = *sub_m[s_i_id].su;
#if 1
      result_table.emplace_back(su.su_suppkey, su.su_name, su.n_name, 
                                s_i_id, i_names[cur_i_i].data(), 
                                su.su_address, su.su_phone, su.su_comment);
#endif
    }
    ++cur_i_i;
  }
  assert(result_table.size() != 0);
  // printf("result size %lu\n", result_table.size());

# else
  M m_table; // s_i_id(m_i_id) -> m_row mapping
  // Collect data from all slave threads
  for (Ctx &ctx : ctxs) {
    auto t_iter = ctx.sub_m.begin();
    db_cnt_ += ctx.sub_m.size();
    for (const MidResult &mr : ctx.sub_m) {
      int32_t s_i_id = mr.s_i_id;
      M::iterator m_iter = m_table.find(s_i_id);
      int16_t s_quantity = mr.s_quantity; 
      
      if (m_iter == m_table.end()) {
        MinQ &min = m_table[s_i_id];
        min.m_s_quantity = s_quantity;
        min.su_vec.push_back(mr.su);
      } else if (s_quantity == m_iter->second.m_s_quantity) {
        MinQ &min = m_table[s_i_id];
        min.su_vec.push_back(mr.su);
      } else if (s_quantity < m_iter->second.m_s_quantity) {
        MinQ &min = m_iter->second;
        min.m_s_quantity = s_quantity;
        min.su_vec.clear();
        min.su_vec.push_back(mr.su);
      }
    }
  }

  printf("map size %lu\n", m_table.size());
  // timer_end_print(timer, "After parallel 1");
  
  // timer_start(timer);

  int i_i = 0;
  // assert(m_table.size() != 0);
  uint64_t max_i_key = i_keys.back();
  int num_key = 0;
  for (pair<const int32_t, MinQ> &pa : m_table) {
    int32_t m_i_id = pa.first;
    assert(m_i_id <= max_i_key);
    while (i_keys[i_i] < m_i_id) ++i_i;
    assert(i_i < i_keys.size() && i_keys[i_i] == m_i_id);
    const inline_str_8<50> *i_data = &i_datas[i_i];
    char last = i_data->data()[i_data->size() - 1];

    if(last != CONST_I_DATA_CHAR_TEMPLATE) continue;

    ++num_key;

    for (auto sup : pa.second.su_vec) {
      const Supplier &su = *sup;
      result_table.emplace_back(su.su_suppkey, su.su_name, su.n_name, 
                                m_i_id, i_names[i_i].data(), 
                                su.su_address, su.su_phone, su.su_comment);
    }
  }
  printf("%d keys, result size %lu\n", num_key, result_table.size());
#endif

  // XXX: for performance, stable_sort is correct but low-efficiency (memory usgae)
#if Q2_STABLE_SORT
  stable_sort(result_table.begin(), result_table.end(), ResultRow::Compare);
#else
  sort(result_table.begin(), result_table.end(), ResultRow::Compare);
#endif

  timer_end_print(timer, "After parallel");
  
  print_timer(ctxs); 
  timer_end_print(gtimer, "Total timer");

  if (!q_verbose_) return true;

  printf("Result of query 2:\n");

  ResultRow::print_header();
  for (const ResultRow &row : result_table)
    row.print();

  printf("Result: %lu tuples\n", result_table.size());
  
  return true;
}

}  // namespace ch
}  // namesapce oltp
}  // namespace nocc

namespace {
inline int64_t binary_search(const SuMap &su_buf, uint64_t su_key) {
  int mid, header = 0, tailer = su_buf.size() - 1;

  while (header <= tailer) {
    mid = (header + tailer) / 2;
    uint64_t mid_key = su_buf[mid].su_suppkey;
    if (mid_key == su_key)
      return mid;
    if (mid_key < su_key)
      header = mid + 1;
    else tailer = mid - 1;
  }

  return -1;
}
 
void query(void *arg) {
  Ctx &ctx = *(Ctx *) arg;
  declare_timer(timer);
  declare_timer(gtimer);
    
  // timer_start(gtimer);

  Keys s_keys = s_tbl->getKeyCol();
  unique_ptr<BackupStore::ColCursor> 
    s_q_cur = s_tbl->getColCursor(S_QUANTITY, ctx.ver);
  assert(s_q_cur.get());
  s_q_cur->seekOffset(ctx.off_begin, ctx.off_end);
  s_tbl->locateCol(S_QUANTITY, sizeof(uint16_t));

  // while (s_q_cur->nextRow()) 
  int s_cnt = 0;
  while (1)
  {
    // timer_start(timer);
    bool has = s_q_cur->nextRow();
    // timer_end(timer, ctx, 0);

    if (!has) break;
    // timer_start(timer);
    ++s_cnt;
    uint64_t i = s_q_cur->cur();
    uint64_t s_key = s_keys[i];
    // timer_end(timer, ctx, 1);

    int32_t s_w_id = stockKeyToWare(s_key);
    int32_t s_i_id = stockKeyToItem(s_key);
    uint64_t su_key = (s_w_id * s_i_id) % 10000 + 1;

    // SuMap::const_iterator su_iter = ctx.su_buf.find(su_key);
    // if (su_iter == ctx.su_buf.end()) continue;
    bool find = false;
    const Supplier *su = nullptr;
#if 0
    for (const Supplier &s : ctx.su_buf) {
      if (su_key == s.su_suppkey) {
        find = true;
        su = &s;
        break;
      }
    }
    if (!find) continue;
#else
    int64_t off = binary_search(ctx.su_buf, su_key);
    if (off == -1) continue;
    su = &ctx.su_buf[off];
#endif

    // timer_start(timer);
    int16_t s_quantity = *(int16_t *) s_q_cur->value();
    // timer_end(timer, ctx, 2);
    
    // timer_start(timer);
#if 0
    SubM::iterator m_iter = ctx.sub_m.find(s_i_id);
    if (m_iter == ctx.sub_m.end()) {
      MinQ &min = ctx.sub_m[s_i_id];
      min.m_s_quantity = s_quantity;
      min.su_vec.push_back(&su_iter->second);
    } else if (s_quantity == m_iter->second.m_s_quantity) {
      MinQ &min = ctx.sub_m[s_i_id];
      min.su_vec.push_back(&su_iter->second);
    } else if (s_quantity < m_iter->second.m_s_quantity) {
      MinQ &min = ctx.sub_m[s_i_id];
      min.m_s_quantity = s_quantity;
      min.su_vec.clear();
      min.su_vec.push_back(&su_iter->second);
    }
#else
    ctx.sub_m.emplace_back(s_i_id, s_quantity, su);
#endif
    // timer_end(timer, ctx, 3);
  }
  // printf("s_cnt = %d\n", s_cnt);
  // timer_end(gtimer, ctx, 4);
}
} // namespace anonymous


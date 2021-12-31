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

#ifndef NOCC_OLTP_CH_MIXIN_H_
#define NOCC_OLTP_CH_MIXIN_H_

#include "ch_config.h"
#include "ch_schema.h"
#include "memstore/memdb.h"
#include "backup_store/backup_db.h"

#include "util/fast_random.h"

#include <string>
#include <ctime>

using namespace nocc::util;
using namespace std;

#define MASK_UPPER ( (0xffffffffLL) << 32)

namespace nocc {
namespace oltp {
namespace ch {

extern uint64_t TimeScale, TimeScale2;

/* TPCC constants */
constexpr inline ALWAYS_INLINE size_t NumItems() {  return 100000; }
constexpr inline ALWAYS_INLINE size_t NumCustomersPerDistrict() { return 3000; }
constexpr inline ALWAYS_INLINE size_t NumDistrictsPerWarehouse() { return 10; }
constexpr inline ALWAYS_INLINE size_t NumSupplier() { return 10000; }
constexpr inline ALWAYS_INLINE size_t NumNation() { return 62; }
constexpr inline ALWAYS_INLINE size_t NumRegion() { return 5; }

inline ALWAYS_INLINE int32_t districtKeyToWare(int64_t d_key) {
  int did = d_key % 10;
  if(did == 0) {
    return (d_key / 10) - 1;
  }
  return d_key / 10;
}

inline ALWAYS_INLINE int32_t customerKeyToWare(int64_t c_key) {
  int32_t upper = (int32_t)((MASK_UPPER & c_key) >> 32);
  int did = (upper % 10);
  if(did == 0)
    return (upper / 10) - 1;
  return upper / 10;
}

inline ALWAYS_INLINE int32_t customerKeyToDistrict(int64_t c_key) {
  int32_t upper = (int32_t)((MASK_UPPER & c_key) >> 32);
  int32_t sid = static_cast<int32_t >((upper - 1) % 10) + 1;
  return sid;
}

inline ALWAYS_INLINE int32_t customerKeyToCustomer(int64_t c_key) {
  int32_t cid = (int32_t)c_key;
  return cid;
}

inline ALWAYS_INLINE int32_t stockKeyToWare(int64_t s_key) {
  int sid = s_key % 100000;
  if (sid == 0)
    return s_key / 100000 - 1;
  return s_key / 100000;
}

inline ALWAYS_INLINE int32_t stockKeyToItem(int64_t s_key) {
  int32_t iid = static_cast<int32_t >((s_key - 1) % 100000) + 1;
  return iid;
}

inline ALWAYS_INLINE int32_t orderKeyToWare(int64_t o_key) {
  return customerKeyToWare(o_key);
}

inline ALWAYS_INLINE int32_t orderKeyToDistrict(int64_t o_key) {
  return customerKeyToDistrict(o_key);
}

inline ALWAYS_INLINE int32_t  orderKeyToOrder(int64_t o_key) {
  int32_t oid = (int32_t)o_key;
  return oid;
}

inline ALWAYS_INLINE int32_t orderLineKeyToWare(int64_t ol_key) {
  int64_t oid = ol_key / 15;
  int32_t upper = oid / 10000000;
  int did = upper % 10;
  if(did == 0)
    return (upper / 10) - 1;
  return upper / 10;

  return customerKeyToWare(ol_key);
}

// return range [1, 15]
inline ALWAYS_INLINE int32_t orderLineKeyToNumber(int64_t ol_key) {
  int32_t number = static_cast<int32_t>((ol_key -1) % 15) + 1;
  return number;
}


inline ALWAYS_INLINE int32_t
newOrderKeyToWare(int64_t no_key) {
  return customerKeyToWare(no_key);
}

inline ALWAYS_INLINE int32_t
newOrderUpper(int64_t no_key) {
  int32_t upper = (int32_t)((MASK_UPPER & no_key) >> 32);
  return upper;
}

inline ALWAYS_INLINE int64_t
makeDistrictKey(int32_t w_id, int32_t d_id) {
  int32_t did = d_id + (w_id * 10);
  int64_t id = static_cast<int64_t>(did);
  // assert(districtKeyToWare(id) == w_id);
  return id;
}

inline ALWAYS_INLINE int64_t
makeCustomerKey(int32_t w_id, int32_t d_id, int32_t c_id) {
  int32_t upper_id = w_id * 10 + d_id;
  int64_t id =  static_cast<int64_t>(upper_id) << 32 | static_cast<int64_t>(c_id);
  // assert(customerKeyToWare(id) == w_id);
  return id;
}

inline ALWAYS_INLINE int64_t
makeHistoryKey(int32_t h_c_id,int32_t h_c_d_id,
               int32_t h_c_w_id, int32_t h_d_id, int32_t h_w_id) {
  int32_t cid = (h_c_w_id * 10 + h_c_d_id) * 3000 + h_c_id;
  int32_t did = h_d_id + (h_w_id * 10);
  int64_t id = static_cast<int64_t>(cid) << 20 | static_cast<int64_t>(did);
  return id;
}

inline ALWAYS_INLINE int64_t
makeOrderKey(int32_t w_id, int32_t d_id, int32_t o_id) {
  int32_t upper_id = w_id * 10 + d_id;
  int64_t id = static_cast<int64_t>(upper_id) << 32 | static_cast<int64_t>(o_id);
  // assert(orderKeyToWare(id) == w_id);
  return id;
}

inline ALWAYS_INLINE int64_t
makeOrderIndex(int32_t w_id, int32_t d_id, int32_t c_id, int32_t o_id) {
  int32_t upper_id = (w_id * 10 + d_id) * 3000 + c_id;
  int64_t id = static_cast<int64_t>(upper_id) << 32 | static_cast<int64_t>(o_id);
  return id;
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

inline ALWAYS_INLINE int64_t orderLineKeyToOrderKey(int64_t ol_key) {
  uint64_t k = uint64_t(ol_key);
  uint64_t upper = (k - 1) / 15;
  uint64_t o_id = (upper - 1) % 10000000 + 1;
  uint64_t upper2 = (upper - 1) / 10000000;
  uint64_t d_id = (upper2 - 1) % 10 + 1;
  uint64_t w_id = (upper2 - 1) / 10;

#if 0  // 1 for debug
  uint64_t ol_n = (k - 1) % 15 + 1;
  assert(makeOrderLineKey(w_id, d_id, o_id, ol_n) == ol_key);
#endif

  return makeOrderKey(w_id, d_id, o_id);
}

inline ALWAYS_INLINE int64_t
makeStockKey(int32_t w_id, int32_t s_id) {
  int32_t sid = s_id + (w_id * 100000);
  int64_t id = static_cast<int64_t>(sid);
  // assert(stockKeyToWare(id) == w_id);
  return id;
}

inline ALWAYS_INLINE int64_t
makeNewOrderKey(int32_t w_id, int32_t d_id, int32_t o_id) {
  int32_t upper_id = w_id * 10 + d_id;
  int64_t id = static_cast<int64_t>
                               (upper_id) << 32 | static_cast<int64_t>(o_id);
  // assert(newOrderKeyToWare(id) == w_id);
  return id;
}

/* functions related to customer index
 * These functions are defined in tpcc_loader.cc
 */
bool compareCustomerIndex(uint64_t key, uint64_t bound);
uint64_t makeCustomerIndex(int32_t w_id, int32_t d_id,
                           string s_last, string s_first);
  
/* Followng pieces of codes mainly comes from Silo */
inline ALWAYS_INLINE uint32_t
GetCurrentTimeMillis() {
  // XXX(stephentu): implement a scalable GetCurrentTimeMillis()
  // for now, we just give each core an increasing number
  // static __thread uint32_t tl_hack = 0;
  // return ++tl_hack;
  time_t t = time(NULL);
  return (uint32_t) t;
}

inline ALWAYS_INLINE uint64_t 
GetCurrentTimeMicro() {
  struct timeval tv;
  gettimeofday(&tv, nullptr);
  return (uint64_t(tv.tv_sec) * 1000000 + tv.tv_usec);
}
  
// utils for generating random #s and strings
inline ALWAYS_INLINE int
CheckBetweenInclusive(int v, int lower, int upper) {
  INVARIANT(v >= lower);
  INVARIANT(v <= upper);
  return v;
}

inline ALWAYS_INLINE int
RandomNumber(fast_random &r, int min, int max) {
  return CheckBetweenInclusive(
          (int) (r.next_uniform() * (max - min + 1) + min), min, max);
}
  
inline ALWAYS_INLINE int
NonUniformRandom(fast_random &r, int A, int C, int min, int max) {
  return (((RandomNumber(r, 0, A) | RandomNumber(r, min, max)) + C)
          % (max - min + 1)) + min;
}

inline ALWAYS_INLINE int
GetItemId(fast_random &r) {
  return CheckBetweenInclusive(
                               chConfig.getUniformItemDist() ?
                               RandomNumber(r, 1, NumItems()) :
                               NonUniformRandom(r, 8191, 7911, 1, NumItems()),
                               1, NumItems());
}

inline ALWAYS_INLINE int
GetCustomerId(fast_random &r) {
  return CheckBetweenInclusive(NonUniformRandom(r, 1023, 259, 1,
                                                NumCustomersPerDistrict()),
                               1, NumCustomersPerDistrict());
}
  
static inline size_t
GetCustomerLastName(uint8_t *buf, fast_random &r, int num) {
  static string NameTokens[10] =
    { string("BAR"),
      string("OUGHT"),
      string("ABLE"),
      string("PRI"),
      string("PRES"),
      string("ESE"),
      string("ANTI"),
      string("CALLY"),
      string("ATION"),
      string("EING"),
    };
  const string &s0 = NameTokens[num / 100];
  const string &s1 = NameTokens[(num / 10) % 10];
  const string &s2 = NameTokens[num % 10];
  uint8_t *const begin = buf;
  const size_t s0_sz = s0.size();
  const size_t s1_sz = s1.size();
  const size_t s2_sz = s2.size();
  NDB_MEMCPY(buf, s0.data(), s0_sz); buf += s0_sz;
  NDB_MEMCPY(buf, s1.data(), s1_sz); buf += s1_sz;
  NDB_MEMCPY(buf, s2.data(), s2_sz); buf += s2_sz;
  return buf - begin;
}

inline ALWAYS_INLINE size_t
GetCustomerLastName(char *buf, fast_random &r, int num) {
  return GetCustomerLastName((uint8_t *) buf, r, num);
}

#define CustomerLastNameMaxSize (5 * 3)
inline string
GetCustomerLastName(fast_random &r, int num) {
  string ret;
  ret.resize(CustomerLastNameMaxSize);
  ret.resize(GetCustomerLastName((uint8_t *) &ret[0], r, num));
  return ret;
}

inline ALWAYS_INLINE string
GetNonUniformCustomerLastNameLoad(fast_random &r) {
  return GetCustomerLastName(r, NonUniformRandom(r, 255, 157, 0, 999));
}

inline ALWAYS_INLINE size_t
GetNonUniformCustomerLastNameRun(uint8_t *buf, fast_random &r) {
  return GetCustomerLastName(buf, r, NonUniformRandom(r, 255, 223, 0, 999));
}

inline ALWAYS_INLINE size_t
GetNonUniformCustomerLastNameRun(char *buf, fast_random &r) {
  return GetNonUniformCustomerLastNameRun((uint8_t *) buf, r);
}

inline ALWAYS_INLINE string
GetNonUniformCustomerLastNameRun(fast_random &r) {
  return GetCustomerLastName(r, NonUniformRandom(r, 255, 223, 0, 999));
}

/* The shared data structure used in all TPCC related classes */
class ChMixin {

 public:
  // pick a number between [start, end)
  inline ALWAYS_INLINE unsigned
  PickWarehouseId(fast_random &r, unsigned start, unsigned end) {
    INVARIANT(start < end);
    const unsigned diff = end - start;
    if (diff == 1)
      return start;
    return (r.next() % diff) + start;
  }


  // following oltpbench, we really generate strings of len - 1...
  inline string
  RandomStr(fast_random &r, uint len) {
    // this is a property of the oltpbench implementation...
    if (!len)
      return "";

    uint i = 0;
    string buf(len - 1, 0);
    while (i < (len - 1)) {
      const char c = (char) r.next_char();
      // XXX(stephentu): oltpbench uses java's Character.isLetter(), which
      // is a less restrictive filter than isalnum()
      if (!isalnum(c))
        continue;
      buf[i++] = c;
    }
    return buf;
  }

  // RandomNStr() actually produces a string of length len
  inline string
  RandomNStr(fast_random &r, uint len) {
    const char base = '0';
    string buf(len, 0);
    for (uint i = 0; i < len; i++)
      buf[i] = (char)(base + (r.next() % 10));
    return buf;
  }

};

class checker {

 public:
  static inline ALWAYS_INLINE void
  SanityCheckCustomer(const customer::value *v) {
    INVARIANT(v->c_credit == "BC" || v->c_credit == "GC");
    INVARIANT(v->c_middle == "OE");
  }

  static inline ALWAYS_INLINE void
  SanityCheckCustomer(const customer::key *k, const customer::value *v) {

    INVARIANT(v->c_credit == "BC" || v->c_credit == "GC");
    INVARIANT(v->c_middle == "OE");
  }

  static inline ALWAYS_INLINE void
  SanityCheckWarehouse(const warehouse::key *k, const warehouse::value *v) {
    INVARIANT(v->w_state.size() == 2);
    INVARIANT(v->w_zip == "123456789");
  }

  static inline ALWAYS_INLINE void
  SanityCheckDistrict (const district::value *v) {
    INVARIANT(v->d_next_o_id >= 3001);
    INVARIANT(v->d_state.size() == 2);
    INVARIANT(v->d_zip == "123456789");
  }

  static inline ALWAYS_INLINE void
  SanityCheckDistrict(const district::key *k, const district::value *v) {
    INVARIANT(v->d_next_o_id >= 3001);
    INVARIANT(v->d_state.size() == 2);
    INVARIANT(v->d_zip == "123456789");
  }

  static inline ALWAYS_INLINE void
  SanityCheckItem(const item::key *k, const item::value *v) {
    INVARIANT(v->i_price >= 1.0 && v->i_price <= 100.0);
  }

  static inline ALWAYS_INLINE void
  SanityCheckStock(const stock::key *k, const stock::value *v) {

  }

  static inline ALWAYS_INLINE void
  SanityCheckNewOrder(const new_order::key *k) {

  }

  static inline ALWAYS_INLINE void
  SanityCheckOOrder(const oorder::value *v) {
    INVARIANT(v->o_c_id >= 1 &&
              static_cast<size_t>(v->o_c_id) <= NumCustomersPerDistrict());
    INVARIANT(v->o_carrier_id >= 0 &&
           static_cast<size_t>(v->o_carrier_id) <= NumDistrictsPerWarehouse());
    INVARIANT(v->o_ol_cnt >= 5 && v->o_ol_cnt <= 15);
  }

  static inline ALWAYS_INLINE void
  SanityCheckOrderLine(const order_line::key *k, const order_line::value *v) {
    INVARIANT(v->ol_i_id >= 1 &&
              static_cast<size_t>(v->ol_i_id) <= NumItems());
  }

  static inline ALWAYS_INLINE void
  SanityCheckSupplier(const supplier::key *k, const supplier::value *v) {

  }

  static inline ALWAYS_INLINE void
  SanityCheckNation(const nation::key *k, const nation::value *v) {

  }

  static inline ALWAYS_INLINE void
  SanityCheckRegion(const region::key *k, const region::value *v) {

  }

};

}  // namespace ch
}  // namespace oltp
}  // namespace nocc
#endif

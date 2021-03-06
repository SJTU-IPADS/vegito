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

#ifndef NOCC_DB_DB_HELPER
#define NOCC_DB_DB_HELPER

#define STATICS 1

// usage: TODO

#if STATICS == 1
// Performance counting stats
// To be more self-contained
inline __attribute__ ((always_inline))
uint64_t db_rdtsc(void)
{
  uint32_t hi, lo;
  __asm volatile("rdtsc" : "=a"(lo), "=d"(hi));
  return ((uint64_t)lo)|(((uint64_t)hi)<<32);
}

#define LAT_VARS(X)  uint64_t _## X ##_lat_; uint64_t _pre_## X ##_lat_; \
  uint64_t _## X ##count_; uint64_t _pre_## X ##count_;
#define INIT_LAT_VARS(X) _## X ##_lat_= 0,_pre_## X ##_lat_ = 0,_## X ##count_ = 0,_pre_## X ##count_ = 0;
#define START(X) auto _## X ##start = db_rdtsc();
#define END(X) { _## X ##_lat_ += db_rdtsc() - _## X ##start;  \
  _## X ##count_    += 1; \
}
#define END_C(C,X) { C-> _## X ##_lat_ += db_rdtsc() - _## X ##start; \
  C-> _## X ## count_ += 1;\
  }

#define REPORT(X) { auto counts = _## X ##count_ - _pre_## X ##count_;    \
  _pre_## X ##count_ = _## X ##count_; \
  counts = counts == 0?1:counts; \
  auto temp = _## X ##_lat_; \
  fprintf(stdout,"%s lat %f\n",#X,(temp - _pre_## X ##_lat_) / (double)counts); \
  _pre_## X ##_lat_ = temp; \
  }

#else
// clear the counting stats to reduce performance impact
#define LAT_VARS(X) ;
#define INIT_LAT_VARS(X) ;
#define START(X) ;
#define END(X) ;
#define END_C(C,X);
#define REPORT(X) ;

#endif

#endif

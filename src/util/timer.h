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

#ifndef NOCC_UTIL_TIMER
#define NOCC_UTIL_TIMER

#include <stdint.h>
#include <vector>


namespace nocc {
namespace util {

class Breakdown_Timer {
  
 public:
  Breakdown_Timer()
    : sum(0), count(0) { 
    buffer.reserve(max_elems_);  
  }

  inline void start() { 
    temp = rdtsc();
  }

  inline float get_diff_ms() const {
    float res = rdtsc() - temp;
    return (res / get_one_second_cycle()) * 1000.0;
  }

  inline void end() {
    uint64_t t = rdtsc(); 
    uint64_t res = (t - temp);
    sum += res; 
    ++count;
    if(buffer.size() >= max_elems_) return;
    buffer.push_back(res);
  }

  void emplace(uint64_t res) {
    sum += res;
    ++count;
    if(buffer.size() >= max_elems_) return;
    buffer.push_back(res);
  }

  inline double report() {
    if(count == 0) return 0.0; // avoids divided by zero
    double ret =  (double) sum / (double) count;
    // clear the info
    sum = 0;
    count = 0;
    // buffer.clear();  // can not used here
    return ret;
  }

  void calculate_detailed() {
    if(buffer.size() == 0) return;

    // first erase some items
    int idx = std::floor(buffer.size() * 0.1 / 100.0);
    buffer.erase(buffer.begin(), buffer.begin() + idx + 1);

    // then sort
    std::sort(buffer.begin(),buffer.end());
    int num = std::floor(buffer.size() * 0.01 / 100.0);
    buffer.erase(buffer.begin() + buffer.size() - num, buffer.begin() + buffer.size());
  }

  inline double report_medium() const {
    if(buffer.size() == 0) return 0;
    // for (int i = 0; i < buffer.size(); ++i)
    //   printf("buffer[%d] = %lf\n", i, (double) buffer[i]);
    return buffer[buffer.size() / 2];
  }

  inline double report_90() const {
    if(buffer.size() == 0) return 0;
    int idx = std::floor( buffer.size() * 90 / 100.0);
    return buffer[idx];
  }

  inline double report_99() const {
    if(buffer.size() == 0) return 0;
    int idx = std::floor(buffer.size() * 99 / 100.0);
    return buffer[idx];
  }

  inline static uint64_t get_one_second_cycle() {
    static uint64_t one_sec_cycle = 0;
    if (one_sec_cycle == 0) {
      uint64_t begin = rdtsc();
      sleep(1);
      one_sec_cycle = rdtsc() - begin;
    }
    return one_sec_cycle;
  }

  inline size_t size() const { return buffer.size(); }

 private:
  static const uint64_t max_elems_ = 1000000;

  uint64_t sum;
  uint64_t count;
  uint64_t temp;
  std::vector<uint64_t> buffer;
};

} // namespace util
}   // namespace nocc
#endif

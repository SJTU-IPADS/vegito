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

#ifndef FAST_RANDOM_H_
#define FAST_RANDOM_H_

#include <cstdint>
#include <string>

namespace nocc {
namespace util {

// not thread-safe
//
// taken from java:
//   http://developer.classpath.org/doc/java/util/Random-source.html
class fast_random {
 public:
  fast_random(unsigned long seed = 0)
    : seed(0) {
    set_seed0(seed);
  }

  inline unsigned long next() {
    return ((unsigned long) next(32) << 32) + next(32);
  }

  inline uint32_t next_u32() {
      return next(32);
  }

    inline uint16_t
      next_u16()
    {
      return next(16);
    }

    /** [0.0, 1.0) */
    inline double
      next_uniform()
    {
      return (((unsigned long) next(26) << 27) + next(27)) / (double) (1L << 53);
    }

    inline char
      next_char()
    {
      return next(8) % 256;
    }

    inline std::string
      next_string(size_t len)
    {
      std::string s(len, 0);
      for (size_t i = 0; i < len; i++)
        s[i] = next_char();
      return s;
    }

    inline unsigned long
      get_seed()
    {
      return seed;
    }

    inline void
      set_seed(unsigned long seed)
    {
      this->seed = seed;
    }

    inline void
      set_seed0(unsigned long seed)
    {
      this->seed = (seed ^ 0x5DEECE66DL) & ((1L << 48) - 1);
    }
  private:

    inline unsigned long
      next(unsigned int bits)
    {
      seed = (seed * 0x5DEECE66DL + 0xBL) & ((1L << 48) - 1);
      return (unsigned long) (seed >> (48 - bits));
    }

    unsigned long seed;
  };


}  // namesapce util
}  // namespace nocc

#endif  // FAST_RANDOM_H_

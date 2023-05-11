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

#if TIMER
#define declare_timer(timer) Breakdown_Timer timer
#define timer_start(timer) {timer.start();}
#define timer_end(timer, ctx, i) { ctx.mem_ms[i] += timer.get_diff_ms(); }
#define timer_end_print(timer, str) { \
  float t = timer.get_diff_ms(); \
  printf("%s: %f ms\n", str, t); \
} while(0)

#define print_timer(ctxs) { \
  const int num_clk = 10; \
  vector<float> sum_ms(num_clk, 0.0f); \
  int num_ms = 0; \
  for (const Ctx &ctx : ctxs) { \
    ++num_ms; \
    for (int i = 0; i < num_clk; ++i) \
      sum_ms[i] += ctx.mem_ms[i]; \
  } \
  for (int i = 0; i < num_clk; ++i) { \
    if (sum_ms[i] == 0.0f) continue; \
      printf("mem ms [%d]: %f\n", i, sum_ms[i] / num_ms); \
  } \
  printf("db_cnt_ = %lu\n", db_cnt_); \
  printf("============= over ===========\n"); \
} while(0)

#define print_each_timer(ctxs) { \
  const int num_clk = 10; \
  for (const Ctx &ctx : ctxs) { \
    for (int i = 0; i < num_clk; ++i) {\
      if (ctx.mem_ms[i] == 0.0f) continue; \
      printf("mem ms [%d]: %f\n", i, ctx.mem_ms[i]); \
    }\
  } \
  printf("db_cnt_ = %lu\n", db_cnt_); \
  printf("============= over ===========\n"); \
} while(0)

#else
#define declare_timer(timer) { }
#define timer_start(timer) { }
#define timer_end(timer, ctx, i) { }
#define timer_end_print(timer, str) { }
#define print_timer(ctxs) { }
#define print_each_timer(ctxs) { }
#endif

#if 0
Breakdown_Timer timer;
#endif

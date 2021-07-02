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

#ifndef _ALL
#define _ALL

/* Coroutine related staff */
/* Using boost coroutine   */
#include<boost/coroutine/all.hpp>

#define MASTER_ROUTINE_ID 0

typedef boost::coroutines::symmetric_coroutine<void>::call_type coroutine_func_t;
typedef boost::coroutines::symmetric_coroutine<void>::yield_type yield_func_t;

#define CACHE_LINE_SZ 64
#define MAX_MSG_SIZE  4096
#define MAX_REMOTE_SET_SIZE (MAX_MSG_SIZE)

#define HUGE_PAGE_SZ (2 * 1024 * 1024)

//**** hardware parameters *******//
#define MAX_SERVERS 16
#define MAX_SERVER_TO_SENT 32


/* some usefull macros  ******************************************************/

#define unlikely(x) __builtin_expect(!!(x), 0)
#define likely(x)   __builtin_expect(!!(x), 1)


#endif

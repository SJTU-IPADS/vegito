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

#ifndef CUSTOM_CONFIG_H_
#define CUSTOM_CONFIG_H_

#define INDEX_BUILD_EVAL 0

#define UPDATE_STAT 0

// Buffer related config
#define STORE_SIZE 4096      // unit: M
#define MAX_INFLIGHT_REPLY (1024)
// #define MAX_INFLIGHT_REQS  (1024 * 12)
#define MAX_INFLIGHT_REQS  (64)

// for micro benchmark usage
//#define STORE_SIZE 8
//#define MAX_INFLIGHT_REQS 16
//#define MAX_INFLIGHT_REPLY 1024

#define ONE_WRITE 1 // use one-sided RDMA write to commit values
#define FASST 0 // merge read & lock req

/* comment *******************************************************************/

//**** log parameters *********
#define RAD_LOG 0 // whether to log timer log information
#define RPC_LOG 0
#define TEST_LOG 0
//***************************//

#define LOGGER_USE_RPC 0
#define USE_BACKUP_STORE 1
#define USE_BACKUP_BENCH_WORKER 1

// *** used for HTAP system
#define QUERY_METHOD 1 // 0 for iterator, 1 for cursor
#define ALL_LOCAL 0
#define DISPLAY_PALMTREE_BREAKDOWN 0 // whether print palmtree information or not
// ************************

// *** used for information on terminal
#define SHOW_QUERY_RESULT 1
// ************************

#define LONG_KEY 1 //whether to use long key, required in TPC-E

/* comment *******************************************************************/

/* TX execution flag  ********************************************************/
#define NO_ABORT 0     // does not retry if the TX is abort, and do the execution
#define NO_TS    0     // In SI, does no manipulate the TS using RDMA
#define OCC_RETRY      // OCC does not issue another round of checks
#define OCC_RO_CHECK 1 // whether do ro validation of OCC

#define PROFILE_RW_SET 0     // whether profile TX's read/write set
#define PROFILE_SERVER_NUM 0 // whether profile number of server accessed per TX

#define CALCULATE_LAT 1
#define LATENCY 1 // calculate the latency as a workload mix
/* comment *******************************************************************/

/* benchmark technique configured ********************************************/
#define ONE_SIDED     0   // using drtm store
#define CACHING       1   // drtm caching
/* comment *******************************************************************/








/* reset some setting according to specific implementations  ****************/
// Warning: Below shall not change!!

#ifndef FARM // start FaRM related setting

#undef  CACHING
#define CACHING 0   // Only one-sided version requires caching
#endif

#ifdef FARM         // also make some checks
#undef  FASST
#define FASST 0     // Only RPC verison can merge read/lock request
#if ONE_SIDED == 0
#error FaRM require one-sided support!
#endif
#endif      // end FaRM related setting

#if ONE_SIDED == 0 // if one-sided operation is not enabled, then no caching
#undef ONE_WRITE
#undef CACHING

#define ONE_WRITE 0
#define CACHING   0

#endif
/* comment *******************************************************************/

#endif  // CUSTOM_CONFIG_H_

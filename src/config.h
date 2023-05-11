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

// compile config parameters

#ifndef NOCC_CONFIG_H_
#define NOCC_CONFIG_H_

#include "all.h"

//**** debug parameters *******
//#define RPC_TIMEOUT_FLAG // whether to check RPC returned
#ifdef RPC_TIMEOUT_FLAG
#define RPC_TIMEOUT_TIME 10000000 // 2 second
#endif

//#define RPC_VERBOSE //whether to record the last RPC's info
//#define RPC_CHECKSUM // whether to do checksum on RPCs

//***************************//

#define USE_UD_MSG 1 // using ud msg


//**** print-parameters *****//
#define LOG_RESULTS 0 // whether log results to a log file
#define LISTENER_PRINT_PERF 1 // whether print results to stdout
/* comment *******************************************************************/

// RDMA related configurations
#define SINGLE_MR 0
/* comment *******************************************************************/


/* Breakdown time statistics *************************************************/
#define POLL_CYCLES 0 // count the pull cycle
/* comment *******************************************************************/

#define HUGE_PAGE 1 // whether to enable huge page

#include "custom_config.h" // contains flags not regarding to the framework




/* Some sanity checks about the setting of flags to be consistent ************/
#if USE_UD_MSG == 1
#undef  MAX_MSG_SIZE
#define MAX_MSG_SIZE 4096 // UD can only support msg size up to 4K
#endif

#endif

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

#ifndef RDMA_MALLOC
#define RDMA_MALLOC

#include <stddef.h>
#include <stdint.h>
// #include "r2/src/allocator_master.hpp"

/*   This file provides interfaces of a malloc for manging registered RDMA regions. 
   It shall be linked to the dedicated ssmalloc library which can be installed 
   by following instructions in ../ralloc/README.md. 
   
   Usage:
     To manage allocation in RDMA registered region, just pass the start pointer and the 
   size to RInit() for initlization. 
     Before Each thread can alloc memory, they shall call RThreadLocalInit() at first. 
     
       Rmalloc and Rfree works as the same as standard malloc and free. The addresses returned 
     is in the registered memory region. 
     
   Limitation:
     We assume there is exactly one RDMA region on one machine.  Which is enough most of the time. 
*/

#if 1
extern "C"  {
  /* Initilize the lib with the dedicated memroy buffer. Can only be called exactly once. 
     @ret
       NULL - An error occured. This is because the memory region size is not large enough.
       A size - The actual size of memory region shall be allocaed .This typicall is less than size for algiment 
       reasons. 
   */
  uint64_t  RInit(char *buffer, uint64_t size);
  /*
    Initilize thread local data structure. 
    Shall be called exactly after RInit and before the first call of Rmalloc or Rfree at this thread. 
   */
  void  RThreadLocalInit(void);
  void *Rmalloc(size_t __size);
  void  Rfree(void *__ptr);
}
#else
  inline void  RInit(char *buf, uint64_t size) {
    r2::AllocatorMaster<0>::init(buf, size);
  }

  inline void  RThreadLocalInit(void) { }
  
  inline void *Rmalloc(size_t __size) {
    return r2::AllocatorMaster<0>::get_thread_allocator()->alloc(__size);
  }

  inline void  Rfree(void *__ptr) {
  
  }
#endif

#endif

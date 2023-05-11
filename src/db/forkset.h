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

#ifndef NOCC_TX_FORK_SET
#define NOCC_TX_FORK_SET

/* This file is used to helpe transactions fork many requests to many servers */
#include "all.h"
#include "memstore/memstore.h"

#include "framework/rpc.h"

namespace nocc {

    using namespace oltp; // framework

    namespace db {

        class ForkSet {
        public:
            ForkSet(Rpc *rpc,int cid = 0) ;
            ~ForkSet() { delete reply_buf_;}
            /* start some initlization work*/
            void  reset();
            char* do_fork(int sizeof_header );
            void  do_fork();

            char* add(int pid,int sizeof_payload);
            void  add(int pid);
            int   fork(int id,int type = 1);
            int   fork(int id,char *val,int size,int type = 1);
            char *get_reply() { return reply_buf_;}
            int   get_server_num() { return server_num_;}

            char  *reply_buf_;
        private:
            Rpc *rpc_handler_;

            char  *msg_buf_start_;
            char  *msg_buf_end_;

            std::set<int> server_set_;
            int server_lists_[MAX_SERVER_TO_SENT];
            int server_num_;
            int cor_id_;
        };
    };
};
#endif

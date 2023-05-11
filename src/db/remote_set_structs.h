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

/* below are internal structures used by the remote-set ****************/
struct RemoteReqObj {
  int tableid;
  uint64_t key;
  int pid;
  int size;
};

struct CommitHeader {
  int32_t  total_size;
  uint64_t commit_seq;
};

union KeyType {
  char long_key[40];
  uint64_t short_key;
};

struct RemoteSetItem {
  uint8_t pid;
  int8_t tableid;
  uint64_t seq;
  char *val;
  uint64_t key;
  MemNode *node;
};

struct RequestHeader {

  uint8_t  cor_id;
  // This payload is used for application sepecific metadata
  //int8_t   tx_id;
  int8_t   num;
  uint64_t padding;
  //char *ptr;

} __attribute__ ((aligned (8)));

struct RemoteSetRequestItem {
  REQ_TYPE type;
  uint8_t  pid;
  int8_t  tableid;
  uint16_t  idx;
  MemNode *node; /* read remote memnode for later locks */
  uint64_t seq;
#if LONG_KEY == 1
  KeyType  key;
#else
  uint64_t key;
#endif
  //      bool     is_write;
} __attribute__ ((aligned (8)));

struct RemoteLockItem {
  uint8_t pid;
  uint64_t seq;
  MemNode *node;
  uint8_t   tableid;
} __attribute__ ((aligned (8)));

struct RemoteWriteItem {
  uint8_t pid;
  MemNode *node;
  uint16_t payload;
} __attribute__ ((aligned (8)));

struct ReplyHeader {
  uint8_t  num_items_;
  uint8_t  partition_id_;
  /* payload shall include this header */
  /* also this payload can be used to carry data*/
  uint32_t payload_;
} __attribute__ ((aligned (8)));

struct RemoteSetReplyItem {
  uint8_t    pid;
  uint64_t   seq;
  MemNode*   node;
  uint16_t   payload;
  int8_t     tableid;
  uint8_t    idx;
} __attribute__ ((aligned (8)));

// single object request buffer
struct RequestItem {
  union {
    uint64_t seq;
    uint64_t key;
  };
  uint8_t  tableid;
  uint8_t  idx;
} __attribute__ ((aligned(8)));

struct RequestItemWrapper {
  uint64_t header;
  struct rpc_header  rpc_padding;
  struct RequestItem req;
  uint64_t tailer;
} __attribute__ ((aligned(8)));

struct ReplyItem {
  uint64_t seq;
  MemNode *node;
  uint8_t  idx;
};

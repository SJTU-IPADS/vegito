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

#ifndef _MAPPED_LOG_H
#define _MAPPED_LOG_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdlib.h>
#include <stdio.h>

// Expose this structure so we can use inline function.
// Call next_log_entry to obtain next avaiable buffer.
typedef struct {
    char *start;
    char *buf; // buf can be changed
    char *end;
    int fd;

    int inc_size;
    int log_size;
} MappedLog;

// Create a new log. inc_size specifies how much space should enlarge_mapped_log
// increase upon each call, it should be a MULTIPLE OF PAGE SIZE (4096).
// Return 0 on success, -1 on error.
int new_mapped_log(const char *path, MappedLog *log, int inc_size);

// Open an existing log
// Return 0 on success, -1 on error.
int open_mapped_log(const char *path, MappedLog *log);

int unmap_log(MappedLog *log);

// Usually you should just call next_log_entry, no need to call this directly.
// Return 0 on success, -1 on error.
int enlarge_mapped_log(MappedLog *log);

// Use this function to obtain next avaiable buffer.
// On error, return NULL;
static inline char *next_log_entry(MappedLog *log, int entry_size) {

    if ((log->buf + entry_size) > log->end) {
        if (enlarge_mapped_log(log) != 0) {
            exit(1);
            return NULL;
        }
    }
    if(log->log_size + entry_size > 1024 * 1024 * 12) {
        // avoids log to be too large
        //return NULL;
    }
    char *start = log->buf;
    log->buf += entry_size;
    log->log_size += entry_size;
    return start;
}

static inline char *read_log_entry(MappedLog *log, int entry_size) {
    if ((log->buf + entry_size) > log->end) {
        return NULL;
    }
    char *start = log->buf;
    log->buf += entry_size;
    return start;
}

#ifdef __cplusplus
}
#endif

#endif

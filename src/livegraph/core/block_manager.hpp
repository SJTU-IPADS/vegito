/* Copyright 2020 Guanyu Feng, Tsinghua University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <atomic>
#include <cstdlib>
#include <mutex>
#include <stdexcept>
#include <vector>

#include <iostream>

#include <tbb/enumerable_thread_specific.h>

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#include "types.hpp"

#ifdef WITH_V6D
#include <memory>
#include "basic/ds/array.h"
#include "client/client.h"
#include "client/ds/object_meta.h"
#include "common/util/uuid.h"
#endif

#define RECORD_MEM_ALLOC 1

namespace livegraph
{
    #define GB (1ul << 30)
    class BlockManager
    {
    public:
        constexpr static uintptr_t NULLPOINTER = 0; // UINTPTR_MAX;

        static uint64_t allocated_mem_size;

        BlockManager(std::string path, size_t _capacity = 10 * GB)
            : capacity(_capacity),
              mutex(),
              free_blocks(std::vector<std::vector<uintptr_t>>(LARGE_BLOCK_THRESHOLD, std::vector<uintptr_t>())),
              large_free_blocks(MAX_ORDER, std::vector<uintptr_t>())
        {
#ifndef WITH_V6D
            if (path.empty())
            {
                fd = EMPTY_FD;
                data =
                    mmap(nullptr, capacity, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
                if (data == MAP_FAILED)
                    throw std::runtime_error("mmap block error.");
            }
            else
            {
                fd = open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0640);
                if (fd == EMPTY_FD)
                    throw std::runtime_error("open block file error.");
                if (ftruncate(fd, FILE_TRUNC_SIZE) != 0)
                    throw std::runtime_error("ftruncate block file error.");
                data = mmap(nullptr, capacity, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
                if (data == MAP_FAILED)
                    throw std::runtime_error("mmap block error.");
            }

            if (madvise(data, capacity, MADV_RANDOM) != 0)
                throw std::runtime_error("madvise block error.");
#else
            {
                using namespace vineyard;

                fd = EMPTY_FD;

                printf("[BM] Prepare apply %.2lf GB on vineyard\n",
                        capacity / 1024.0 / 1024.0 / 1024.0);

                std::string ipc_socket = "/opt/ssj/test.sock";

                VINEYARD_CHECK_OK(client.Connect(ipc_socket));
                std::unique_ptr<BlobWriter> blob_writer;
                std::shared_ptr<Blob> blob;
                VINEYARD_CHECK_OK(client.CreateBlob(capacity, blob_writer));
                VINEYARD_CHECK_OK(client.GetBlob(blob_writer->id(), true, blob));
                data = (void *) blob_writer->data();

                oid = blob_writer->id();
            }
#endif
            file_size = FILE_TRUNC_SIZE;
            used_size = 0;

            null_holder = alloc(LARGE_BLOCK_THRESHOLD);
        }

        ~BlockManager()
        {
            free(null_holder, LARGE_BLOCK_THRESHOLD);
            msync(data, capacity, MS_SYNC);
            munmap(data, capacity);
            if (fd != EMPTY_FD)
                close(fd);
#ifdef WITH_V6D
            client.Disconnect();
#endif
        }

        void print_free_blocks_info() {
            std::cout << "Free blocks info: " << std::endl;
            for (int i = 0; i < LARGE_BLOCK_THRESHOLD; i++) {
                std::cout << "  number of free blocks whose order = " << i << " : " << free_blocks.local()[i].size() << std::endl;
            }
            for (int i = LARGE_BLOCK_THRESHOLD; i < LARGE_BLOCK_THRESHOLD + 20; i++) {
                std::cout << "  number of free blocks whose order = " << i << " : " << large_free_blocks[i].size() << std::endl;
            }
        }

        size_t getUsedMemory() {
            size_t ret = used_size;
            for (int i = 0; i < LARGE_BLOCK_THRESHOLD; i++) {
                ret -= free_blocks.local()[i].size() * (1ul << i);
            }
            for (int i = LARGE_BLOCK_THRESHOLD; i < LARGE_BLOCK_THRESHOLD + 20; i++) {
                ret -= large_free_blocks[i].size() * (1ul << i);
            }
            return ret;
        }

        uintptr_t alloc(order_t order)
        {
            uintptr_t pointer = NULLPOINTER;
            if (order < LARGE_BLOCK_THRESHOLD)
            {
                pointer = pop(free_blocks.local(), order);
            }
            else
            {
                std::lock_guard<std::mutex> lock(mutex);
                pointer = pop(large_free_blocks, order);
            }

            if (pointer == NULLPOINTER)
            {
                size_t block_size = 1ul << order;
                pointer = used_size.fetch_add(block_size);

                if (pointer + block_size >= file_size)
                {
                    auto new_file_size = ((pointer + block_size) / FILE_TRUNC_SIZE + 1) * FILE_TRUNC_SIZE;
                    std::lock_guard<std::mutex> lock(mutex);
                    if (new_file_size >= file_size)
                    {
                        if (fd != EMPTY_FD)
                        {
                            if (ftruncate(fd, new_file_size) != 0)
                                throw std::runtime_error("ftruncate block file error.");
                        }
                        file_size = new_file_size;
                    }
                }
            }

            return pointer;
        }

        void free(uintptr_t block, order_t order)
        {
            if (order < LARGE_BLOCK_THRESHOLD)
            {
                push(free_blocks.local(), order, block);
            }
            else
            {
                std::lock_guard<std::mutex> lock(mutex);
                push(large_free_blocks, order, block);
            }
        }

        template <typename T> inline T *convert(uintptr_t block)
        {
            if (__builtin_expect((block == NULLPOINTER), 0))
                return nullptr;
            return reinterpret_cast<T *>(reinterpret_cast<char *>(data) + block);
        }

        inline uintptr_t revert(uintptr_t block)
        {
            return block - (uintptr_t)data;
        }

#ifdef WITH_V6D
        inline vineyard::ObjectID get_block_oid() const { return oid; }
#endif

    private:
        const size_t capacity;
        int fd;
        void *data;
#ifdef WITH_V6D
        vineyard::Client client;
        vineyard::ObjectID oid;
#endif
        std::mutex mutex;
        tbb::enumerable_thread_specific<std::vector<std::vector<uintptr_t>>> free_blocks;
        std::vector<std::vector<uintptr_t>> large_free_blocks;
        std::atomic<size_t> used_size, file_size;
        uintptr_t null_holder;

        uintptr_t pop(std::vector<std::vector<uintptr_t>> &free_block, order_t order)
        {
            uintptr_t pointer = NULLPOINTER;
            if (free_block[order].size())
            {
                pointer = free_block[order].back();
                free_block[order].pop_back();
            }
            return pointer;
        }

        void push(std::vector<std::vector<uintptr_t>> &free_block, order_t order, uintptr_t pointer)
        {
            free_block[order].push_back(pointer);
        }

        constexpr static int EMPTY_FD = -1;
        constexpr static order_t MAX_ORDER = 64;
        constexpr static order_t LARGE_BLOCK_THRESHOLD = 20;
        constexpr static size_t FILE_TRUNC_SIZE = 1ul << 30; // 1GB
    };

    class BlockManagerLibc
    {
    public:
        constexpr static uintptr_t NULLPOINTER = UINTPTR_MAX;

        uintptr_t alloc(order_t order)
        {
            auto p = aligned_alloc(1ul << order, 1ul << order);
            if (!p)
                throw std::runtime_error("Failed to alloc block");
            return reinterpret_cast<std::uintptr_t>(p);
        }

        void free(uintptr_t block, order_t order) { ::free(reinterpret_cast<void *>(block)); }

        template <typename T> T *convert(uintptr_t block)
        {
            if (block == NULLPOINTER)
                return nullptr;
            return reinterpret_cast<T *>(block);
        }
    };
} // namespace livegraph

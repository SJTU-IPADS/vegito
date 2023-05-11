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

#include <cstdlib>
#include <limits>
#include <new>

#include <sys/mman.h>

#ifdef WITH_V6D
#include <memory>
#include "basic/ds/array.h"
#include "client/client.h"
#include "client/ds/object_meta.h"
#include "common/util/uuid.h"
#endif

namespace livegraph
{

    template <typename T> struct SparseArrayAllocator
    {
        using value_type = T;

#ifndef WITH_V6D
        SparseArrayAllocator() = default;
        template <class U> constexpr SparseArrayAllocator(const SparseArrayAllocator<U> &) noexcept {}
#else
        SparseArrayAllocator()
            : last_oid(vineyard::ObjectID(-1)), client(new vineyard::Client),
              copied(false) {
            std::string ipc_socket = "/opt/ssj/test.sock";

            VINEYARD_CHECK_OK(client->Connect(ipc_socket));
        }

        template <class U>
        SparseArrayAllocator(const SparseArrayAllocator<U> &that)
            : last_oid(that.last_oid), client(that.client), copied(true)
        {
            assert(client);
        }

        ~SparseArrayAllocator() {
            if (!copied) {
                client->Disconnect();
                delete client;
                client = nullptr;
            }
        }

        T *allocate_v6d(size_t n)
        {
            size_t size = n * sizeof(T);
            if (n > std::numeric_limits<std::size_t>::max() / sizeof(T))
                throw std::bad_alloc();

            printf("[Allocator] Prepare apply %.2lf GB on vineyard\n",
                size / 1024.0 / 1024.0 / 1024.0);

            std::unique_ptr<vineyard::BlobWriter> blob_writer;
            std::shared_ptr<vineyard::Blob> blob;
            VINEYARD_CHECK_OK(client->CreateBlob(size, blob_writer));
            VINEYARD_CHECK_OK(client->GetBlob(blob_writer->id(), true, blob));
            auto data = (void *) blob_writer->data();
            last_oid = blob_writer->id();
            return static_cast<T *>(data);
        }

        void deallocate_v6d(vineyard::ObjectID oid)
        {
            VINEYARD_CHECK_OK(client->DelData(oid));
        }
#endif

        T *allocate(size_t n)
        {
            size_t size = n * sizeof(T);
            if (n > std::numeric_limits<std::size_t>::max() / sizeof(T))
                throw std::bad_alloc();
            auto data = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
            if (data == MAP_FAILED)
                throw std::bad_alloc();
            return static_cast<T *>(data);
        }

        void deallocate(T *data, size_t n) noexcept
        {
            size_t size = n * sizeof(T);
            munmap(data, size);
        }

        template <class U> bool operator==(const SparseArrayAllocator<U> &) { return true; }
        template <class U> bool operator!=(const SparseArrayAllocator<U> &) { return false; }

#ifdef WITH_V6D
        vineyard::ObjectID get_last_oid() const {
            assert(last_oid != vineyard::ObjectID(-1));
            return last_oid;
        }

      private:
        vineyard::Client * client;
        vineyard::ObjectID last_oid;
        const bool copied;

        template<typename> friend struct SparseArrayAllocator;
#endif
    };

} // namespace livegraph

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

#include <vector>
#include "types.hpp"
#include "allocator.hpp"
#include "blocks.hpp"

namespace livegraph
{
    class CSREdgeIterator;
    class CSRTransaction;

    class CSRGraph
    {
    public:
        CSRGraph() {}

        CSRGraph(const CSRGraph &) = delete;

        CSRGraph(CSRGraph &&) = delete;

        ~CSRGraph()
        {
        }

        vertex_t get_max_vertex_id() const {
            vertex_t ret = 0;
            for (int i = 0; i < vertices.size(); i++)  
                ret = vertices[i].size() > ret ? vertices[i].size() : ret;
            return ret; 
        }

        CSRTransaction begin_read_only_transaction();

        /**
         * Analytics interface
         */
        template<class T>
        T* alloc_vertex_array(T init) {
            auto vertex_data_allocater =
                std::allocator_traits<decltype(array_allocator)>::rebind_alloc<T>(array_allocator);
            T* vertex_data = vertex_data_allocater.allocate(get_max_vertex_id());
            assert(vertex_data);
            for (vertex_t v_i = 0; v_i < get_max_vertex_id(); v_i++) {
                vertex_data[v_i] = init;
            }
            return vertex_data;
        }

        template<typename T>
        T * dealloc_vertex_array(T * vertex_data) {
            auto vertex_data_allocater =
                std::allocator_traits<decltype(array_allocator)>::rebind_alloc<T>(array_allocator);
            vertex_data_allocater.deallocate(vertex_data, get_max_vertex_id());
        }
        
        std::vector<std::vector<off_t>> vertices;
        std::vector<std::vector<EdgeEntry>> edges;

    private:
        constexpr static vertex_t VERTEX_TOMBSTONE = UINT64_MAX;
        SparseArrayAllocator<void> array_allocator;
        friend class CSREdgeIterator;
        friend class CSRTransaction;
    };
} // namespace livegraph
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
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_set>
#include <math.h>
#include <stdlib.h>
#include <time.h>

#include <tbb/concurrent_queue.h>
#include <tbb/enumerable_thread_specific.h>

#include "blocks.hpp"
#include "bitmap.hpp"
#include "allocator.hpp"
#include "block_manager.hpp"
#include "commit_manager.hpp"
#include "futex.hpp"

#include "graph/ddl.h"

#ifdef WITH_V6D
#include "common/util/uuid.h"
#include "fragment/shared_storage.h"
#endif

namespace nocc::oltp::wiki
{
  class EpochTimer;
}


namespace livegraph
{
    class SegEdgeIterator;
    class EpochEdgeIterator;
    class SegTransaction;
    class EpochGraphWriter;
    class EpochGraphReader;

    class SegGraph
    {
    public:
        SegGraph(nocc::graph::RGMapping* rg_map = nullptr,
              std::string block_path = "",
              std::string wal_path = "",
              size_t _max_block_size = 1ul << 40,
              vertex_t _max_vertex_id = 1ul << 30)
            : mutex(),
              epoch_id(0),
              transaction_id(0),
              vertex_id(0),
              read_epoch_table(NO_TRANSACTION),
              recycled_vertex_ids(),
              max_vertex_id(_max_vertex_id),

              // segment
              seg_id(0),
              max_seg_id(_max_vertex_id / VERTEX_PER_SEG),

              array_allocator(),
              block_manager(block_path, _max_block_size),
              commit_manager(wal_path, epoch_id),

              rg_map(rg_map)
        {
            auto futex_allocater =
                std::allocator_traits<decltype(array_allocator)>::rebind_alloc<Futex>(array_allocator);
            vertex_futexes = futex_allocater.allocate(max_vertex_id);

            auto shared_mutex_allocater =
                std::allocator_traits<decltype(array_allocator)>::rebind_alloc<std::shared_timed_mutex*>(array_allocator);
            seg_mutexes = shared_mutex_allocater.allocate(max_seg_id);

            auto pointer_allocater =
                std::allocator_traits<decltype(array_allocator)>::rebind_alloc<uintptr_t>(array_allocator);
#ifndef WITH_V6D
            vertex_ptrs = pointer_allocater.allocate(max_vertex_id);
            edge_label_ptrs = pointer_allocater.allocate(max_seg_id);
#else
            vertex_ptrs = pointer_allocater.allocate_v6d(max_vertex_id);
            vertex_ptrs_oid = pointer_allocater.get_last_oid();

            edge_label_ptrs = pointer_allocater.allocate_v6d(max_seg_id);
            edge_label_ptrs_oid = pointer_allocater.get_last_oid();

            gart::EdgeLabel2PtrMeta meta(edge_label_ptrs_oid, max_seg_id);
            blob_schema.set_block_oid(block_manager.get_block_oid());
            blob_schema.set_elabel2segs(meta);
#endif

            // tricky method: avoid corner case in segment lock
            seg_mutexes[0] = new std::shared_timed_mutex();
            edge_label_ptrs[0] = block_manager.NULLPOINTER;
            srand(time(0));
        }

        SegGraph(const SegGraph &) = delete;

        SegGraph(SegGraph &&) = delete;

        ~SegGraph() noexcept
        {
            auto futex_allocater =
                std::allocator_traits<decltype(array_allocator)>::rebind_alloc<Futex>(array_allocator);
            futex_allocater.deallocate(vertex_futexes, max_vertex_id);

            auto shared_mutex_allocater =
                std::allocator_traits<decltype(array_allocator)>::rebind_alloc<std::shared_timed_mutex*>(array_allocator);
            shared_mutex_allocater.deallocate(seg_mutexes, max_seg_id);

            auto pointer_allocater =
                std::allocator_traits<decltype(array_allocator)>::rebind_alloc<uintptr_t>(array_allocator);

#ifndef WITH_V6D
            pointer_allocater.deallocate(vertex_ptrs, max_vertex_id);
            pointer_allocater.deallocate(edge_label_ptrs, max_seg_id);
#else
            pointer_allocater.deallocate_v6d(vertex_ptrs_oid);
            pointer_allocater.deallocate_v6d(edge_label_ptrs_oid);
#endif
        }

        vertex_t get_max_vertex_id() const { return vertex_id; }

        vertex_t get_seg_start_vid(segid_t seg_id) const { return seg_id * VERTEX_PER_SEG; }

        vertex_t get_seg_end_vid(segid_t seg_id) const {
            return std::min(vertex_id.load(), (seg_id + 1) * VERTEX_PER_SEG);
        }

        segid_t get_max_seg_id() const { return seg_id; }

        segid_t get_vertex_seg_id(vertex_t _vertex_id) const { return _vertex_id / VERTEX_PER_SEG; }

        uint32_t get_vertex_seg_idx(vertex_t _vertex_id) const { return _vertex_id % VERTEX_PER_SEG; }

        BlockManager& get_block_manager() { return block_manager; }

        void recycle_segments(timestamp_t epoch_id);

        SegTransaction begin_transaction();
        SegTransaction begin_read_only_transaction();
        SegTransaction begin_batch_loader();

#ifdef ENABLE_EPOCH
        EpochGraphReader create_graph_reader(timestamp_t read_epoch);
        EpochGraphWriter create_graph_writer(timestamp_t write_epoch);
#else
        SegTransaction create_graph_reader(timestamp_t read_epoch);
        SegTransaction create_graph_writer(timestamp_t write_epoch);
#endif

        size_t get_edge_prop_size(label_t label) {
            if (!rg_map) return 0;
            return rg_map->get_edge_meta(static_cast<int>(label)).edge_prop_size;
        }

        /**
         * Analytics interface
         */
        template<class T>
        T* alloc_vertex_array(T init, vertex_t sz = -1) {
            if(sz == -1) sz = get_max_vertex_id();
            auto vertex_data_allocater =
                std::allocator_traits<decltype(array_allocator)>::rebind_alloc<T>(array_allocator);
            T* vertex_data = vertex_data_allocater.allocate(sz);
            assert(vertex_data);
            for (vertex_t v_i = 0; v_i < sz; v_i++) {
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

#ifdef WITH_V6D
        gart::BlobSchema &get_blob_schema() {
            return blob_schema;
        }
#endif

    private:
        using cacheline_padding_t = char[64];

        cacheline_padding_t padding0;
        std::mutex mutex;
        cacheline_padding_t padding1;
        std::atomic<timestamp_t> epoch_id;
        cacheline_padding_t padding2;
        std::atomic<timestamp_t> transaction_id;
        cacheline_padding_t padding3;
        std::atomic<vertex_t> vertex_id;
        cacheline_padding_t padding4;
        std::atomic<segid_t> seg_id;
        cacheline_padding_t padding5;

        tbb::enumerable_thread_specific<timestamp_t> read_epoch_table;
        tbb::enumerable_thread_specific<std::vector<std::tuple<uintptr_t, order_t, timestamp_t>>> segments_to_recycle;
        tbb::concurrent_queue<vertex_t> recycled_vertex_ids;

        const vertex_t max_vertex_id;
        const segid_t max_seg_id;

        SparseArrayAllocator<void> array_allocator;
        BlockManager block_manager;
        CommitManager commit_manager;

        Futex *vertex_futexes;
        std::shared_timed_mutex **seg_mutexes;
        uintptr_t *vertex_ptrs;
        uintptr_t *edge_label_ptrs;

#ifdef WITH_V6D
        vineyard::ObjectID vertex_ptrs_oid;
        vineyard::ObjectID edge_label_ptrs_oid;
        gart::BlobSchema blob_schema;
#endif

        nocc::graph::RGMapping* rg_map;

        constexpr static size_t COMPACTION_CYCLE = 1ul << 20;
        constexpr static size_t RECYCLE_FREQ = 1ul << 20;
        constexpr static size_t LAG_EPOCH_NUMBER = 2;

        constexpr static timestamp_t ROLLBACK_TOMBSTONE = INT64_MAX;
        constexpr static timestamp_t NO_TRANSACTION = -1;
        constexpr static timestamp_t RO_TRANSACTION = ROLLBACK_TOMBSTONE - 1;
        constexpr static vertex_t VERTEX_TOMBSTONE = UINT64_MAX;
        constexpr static auto TIMEOUT = std::chrono::milliseconds(1);
        constexpr static size_t COMPACT_EDGE_BLOCK_THRESHOLD = 5; // at least compact 20% edges
        constexpr static label_t MAX_LABEL = UINT16_MAX;

        constexpr static size_t COPY_THRESHOLD_ORDER = 3;

        constexpr static order_t INIT_SEGMENT_ORDER = 20;

        friend class SegEdgeIterator;
        friend class EpochEdgeIterator;
        friend class SegTransaction;
        friend class EpochGraphWriter;
        friend class EpochGraphReader;
        friend class nocc::oltp::wiki::EpochTimer;
    };
} // namespace livegraph

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

#include <deque>
#include <map>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <vector>
#include <functional>

#include "atomic.hpp"
#include "bitmap.hpp"
#include "blocks.hpp"
#include "edge_iterator.hpp"
#include "segment_graph.hpp"
#include "utils.hpp"

namespace livegraph
{
    class SegTransaction
    {
        enum class OPType
        {
            NewVertex,
            PutVertex,
            DelVertex,
            PutEdge,
            DelEdge,
        };

        enum class ThreadStatus {
            WORKING,
            STEALING
        };

    public:
        class RollbackExcept : public std::runtime_error
        {
        public:
            RollbackExcept(const std::string &what_arg) : std::runtime_error(what_arg) {}
            RollbackExcept(const char *what_arg) : std::runtime_error(what_arg) {}
        };

        SegTransaction(
            SegGraph &_graph, timestamp_t _local_txn_id, timestamp_t _read_epoch_id, bool _batch_update, bool _trace_cache)
            : graph(_graph),
              local_txn_id(_local_txn_id),
              read_epoch_id(_read_epoch_id),
              batch_update(_batch_update),
              trace_cache(_trace_cache),
              write_epoch_id(batch_update ? read_epoch_id : -local_txn_id),
              valid(true),
              wal(),
              vertex_ptr_cache(),
              segment_ptr_cache(),
              block_cache(), // added blocks
              segment_cache(), // added segments
              segment_to_be_freed_cache(),
              region_ptr_cache(),
              edge_block_num_entries_data_length_cache(),
              new_vertex_cache(),
              recycled_vertex_cache(),
              acquired_locks(),
              acquired_seg_read_locks(),
              acquired_seg_write_locks(),
              timestamps_to_update()
        {
            wal_append((uint64_t)0); // number of operations
            wal_append(read_epoch_id);
            wal_append(local_txn_id);
        }

        SegTransaction(const SegTransaction &) = delete;

        SegTransaction(SegTransaction &&txn)
            : graph(txn.graph),
              local_txn_id(std::move(txn.local_txn_id)),
              read_epoch_id(std::move(txn.read_epoch_id)),
              batch_update(std::move(txn.batch_update)),
              trace_cache(std::move(txn.trace_cache)),
              write_epoch_id(std::move(txn.write_epoch_id)),
              valid(std::move(txn.valid)),
              wal(std::move(txn.wal)),
              vertex_ptr_cache(std::move(txn.vertex_ptr_cache)),
              segment_ptr_cache(std::move(txn.segment_ptr_cache)),
              block_cache(std::move(txn.block_cache)),
              segment_cache(std::move(txn.segment_cache)),
              segment_to_be_freed_cache(std::move(txn.segment_to_be_freed_cache)),
              region_ptr_cache(std::move(txn.region_ptr_cache)),
              edge_block_num_entries_data_length_cache(std::move(txn.edge_block_num_entries_data_length_cache)),
              new_vertex_cache(std::move(txn.new_vertex_cache)),
              recycled_vertex_cache(std::move(txn.recycled_vertex_cache)),
              acquired_locks(std::move(txn.acquired_locks)),
              acquired_seg_read_locks(std::move(txn.acquired_seg_read_locks)),
              acquired_seg_write_locks(std::move(txn.acquired_seg_write_locks)),
              timestamps_to_update(std::move(txn.timestamps_to_update))
        {
            txn.valid = false;
        }

        timestamp_t get_read_epoch_id() const { return read_epoch_id; }

        vertex_t new_vertex(bool use_recycled_vertex = false);
        void put_vertex(vertex_t vertex_id, std::string_view data);
        bool del_vertex(vertex_t vertex_id, bool recycle = false);

        void put_edge(vertex_t src, label_t label, dir_t dir, vertex_t dst, 
                      std::string_view edge_data = "", bool force_insert = true);
        
        void put_edge(vertex_t src, label_t label, vertex_t dst, 
                      std::string_view edge_data = "", bool force_insert = true) {
          put_edge(src, label, EOUT, dst, edge_data, force_insert);
        }

        bool del_edge(vertex_t src, label_t label, dir_t dir, vertex_t dst);

        bool del_edge(vertex_t src, label_t label, vertex_t dst) {
            del_edge(src, label, EOUT, dst);
        }

        uintptr_t locate_segment(segid_t segid, label_t label, dir_t dir = EOUT);

        std::string_view get_vertex(vertex_t vertex_id);
        std::string_view get_edge(vertex_t src, label_t label, dir_t dir, vertex_t dst);

        std::string_view get_edge(vertex_t src, label_t label, vertex_t dst) {
            get_edge(src, label, EOUT, dst);
        }

        SegEdgeIterator get_edges_in_seg(uintptr_t segment, vertex_t src, bool reverse = false);
        SegEdgeIterator get_edges(vertex_t src, label_t label, dir_t dir, 
                                  bool reverse = false);

        SegEdgeIterator get_edges(vertex_t src, label_t label, bool reverse = false) {
            get_edges(src, label, EOUT, reverse);
        }

        // segment compact
        bool merge_segment(SegmentHeader* old_seg, SegmentHeader* new_seg, vertex_t seg_idx, EdgeEntry& entry, uintptr_t* pointer, EdgeBlockHeader** edge_block);
        void merge_segments(label_t label, dir_t dir = EOUT);

        timestamp_t commit(bool wait_visable = true, dir_t dir = EOUT);
        void abort();

        ~SegTransaction()
        {
            if (valid)
                abort();
        }

    private:
        SegGraph &graph;
        const timestamp_t local_txn_id;
        const timestamp_t read_epoch_id;
        const bool batch_update;
        const bool trace_cache;
        const timestamp_t write_epoch_id;
        bool valid;
        std::string wal;

        std::unordered_map<vertex_t, uintptr_t> vertex_ptr_cache;
        std::map<std::pair<segid_t, label_t>, uintptr_t> segment_ptr_cache;
        std::vector<std::pair<uintptr_t, order_t>> block_cache;

        std::vector<std::pair<uintptr_t, order_t>> segment_cache;
        std::vector<std::pair<uintptr_t, order_t>> segment_to_be_freed_cache;
        std::unordered_map<uintptr_t *, uintptr_t> region_ptr_cache;
        
        std::unordered_map<EdgeBlockHeader *, std::pair<size_t, size_t>> edge_block_num_entries_data_length_cache;
        std::vector<vertex_t> new_vertex_cache;
        std::deque<vertex_t> recycled_vertex_cache;

        std::unordered_set<vertex_t> acquired_locks;
        std::unordered_set<segid_t> acquired_seg_read_locks;
        std::unordered_set<segid_t> acquired_seg_write_locks;
        std::vector<std::pair<timestamp_t *, timestamp_t>> timestamps_to_update;

        template <typename T, typename = std::enable_if_t<std::is_trivial_v<T>>> inline void wal_append(T data)
        {
            wal.append(reinterpret_cast<char *>(&data), sizeof(T));
        }

        inline void wal_append(std::string_view data)
        {
            wal_append(data.size());
            wal.append(data);
        }

        inline uint64_t &wal_num_ops() { return *reinterpret_cast<uint64_t *>(wal.data()); }

        void check_writable()
        {
            if (!batch_update && !trace_cache)
                throw std::invalid_argument("The transaction is read-only without cache.");
        }

        void check_valid()
        {
            if (!valid)
                throw std::invalid_argument("The transaction is committed or aborted.");
        }

        void check_vertex_id(vertex_t vertex_id)
        {
            if (vertex_id >= graph.vertex_id.load(std::memory_order_relaxed))
                throw std::invalid_argument("The vertex id is invalid.");
        }

        bool ensure_segment_read_lock(segid_t seg_id, bool write) {
            if (write)
                return false;
            auto iter = acquired_seg_read_locks.find(seg_id);
            if (iter != acquired_seg_read_locks.end())
                return true;
            if (!graph.seg_mutexes[seg_id]->try_lock_shared_for(SegGraph::TIMEOUT))
                throw RollbackExcept("Deadlock on Segment: " + std::to_string(seg_id) + ".");
            acquired_seg_read_locks.emplace_hint(iter, seg_id);
            return false;
        }

        void ensure_segment_write_lock(segid_t seg_id) {
            if (acquired_seg_read_locks.find(seg_id) != acquired_seg_read_locks.end()) {
                graph.seg_mutexes[seg_id]->unlock_shared();
                acquired_seg_read_locks.erase(seg_id);
            }

            auto iter = acquired_seg_write_locks.find(seg_id);
            if (iter != acquired_seg_write_locks.end())
                return;
            if (!graph.seg_mutexes[seg_id]->try_lock_for(SegGraph::TIMEOUT))
                throw RollbackExcept("Deadlock on Segment: " + std::to_string(seg_id) + ".");
            acquired_seg_write_locks.emplace_hint(iter, seg_id);
        }

        bool has_write_lock(segid_t seg_id) {
            return (acquired_seg_write_locks.find(seg_id) != acquired_seg_write_locks.end());
        }

        bool ensure_vertex_lock(vertex_t vertex_id)
        {
            auto iter = acquired_locks.find(vertex_id);
            if (iter != acquired_locks.end())
                return true;
            if (!graph.vertex_futexes[vertex_id].try_lock_for(SegGraph::TIMEOUT))
                throw RollbackExcept("Deadlock on Vertex: " + std::to_string(vertex_id) + ".");
            acquired_locks.emplace_hint(iter, vertex_id);
            return false;
        }

        void recover_lock_status(segid_t seg_id, vertex_t vid, bool recover_read, bool recover_write, bool recover_vertex)
        {
            if (recover_read) {
                graph.seg_mutexes[seg_id]->unlock();
                graph.seg_mutexes[seg_id]->lock_shared();
                acquired_seg_write_locks.erase(seg_id);
                acquired_seg_read_locks.emplace(seg_id);
            } else if (recover_write) {
                // do nothing
            } else {
                graph.seg_mutexes[seg_id]->unlock();
                acquired_seg_write_locks.erase(seg_id);
            }

            if (!recover_vertex) {
                graph.vertex_futexes[vid].unlock();
                acquired_locks.erase(vid);
            }
            
        }

        void ensure_no_confict(vertex_t vertex_id)
        {
            auto header = graph.block_manager.convert<VertexBlockHeader>(graph.vertex_ptrs[vertex_id]);
            if (header && cmp_timestamp(header->get_creation_time_pointer(), read_epoch_id, local_txn_id) > 0)
                throw RollbackExcept("Write-write confict on: " + std::to_string(vertex_id) + ".");
        }

        void clean()
        {
            for (const auto &vertex_id : acquired_locks)
            {
                graph.vertex_futexes[vertex_id].unlock();
            }

            for (const auto &segid : acquired_seg_read_locks)
            {
                graph.seg_mutexes[segid]->unlock_shared();
            }

            for (const auto &segid : acquired_seg_write_locks)
            {
                graph.seg_mutexes[segid]->unlock();
            }
            valid = false;
            graph.read_epoch_table.local() = SegGraph::NO_TRANSACTION;
        }

        std::pair<size_t, size_t> get_num_entries_data_length_cache(EdgeBlockHeader *edge_block) const
        {
            if (batch_update || !trace_cache)
                return edge_block->get_num_entries_data_length_atomic();
            auto iter = edge_block_num_entries_data_length_cache.find(edge_block);
            if (iter == edge_block_num_entries_data_length_cache.end())
                return edge_block->get_num_entries_data_length_atomic();
            else
                return iter->second;
        }

        void set_num_entries_data_length_cache(EdgeBlockHeader *edge_block, size_t num_entries, size_t data_length)
        {
            if (batch_update)
                edge_block->set_num_entries_data_length_atomic(num_entries, data_length);
            else
                edge_block_num_entries_data_length_cache[edge_block] = {num_entries, data_length};
        }

        std::pair<EdgeEntry *, char *>
        locate_edge_in_block(vertex_t dst, EdgeBlockHeader *edge_block, size_t num_entries, size_t data_length);
        uintptr_t locate_block_in_segment(uintptr_t ptr, vertex_t idx);

        void ensure_no_confict(vertex_t src, label_t label, dir_t dir);

        void update_edge_label_block(vertex_t src, label_t label, dir_t dir,
                                     uintptr_t edge_block_pointer);
    };
} // namespace livegraph

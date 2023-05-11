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
    class EpochGraphWriter
    {

    public:

        EpochGraphWriter(
            SegGraph &_graph, timestamp_t _write_epoch_id)
            : graph(_graph),
              read_epoch_id(_write_epoch_id),
              write_epoch_id(_write_epoch_id) {}

        EpochGraphWriter(const EpochGraphWriter &) = delete;

        EpochGraphWriter(EpochGraphWriter &&txn)
            : graph(txn.graph),
              read_epoch_id(std::move(txn.read_epoch_id)),
              write_epoch_id(std::move(txn.write_epoch_id))
        {}

        timestamp_t get_write_epoch_id() const { return write_epoch_id; }

        VegitoSegmentHeader* locate_segment(segid_t segid, label_t label, dir_t dir = EOUT);
        uintptr_t locate_segment_ptr(segid_t seg_id, label_t label, dir_t dir = EOUT);
        vertex_t new_vertex(bool use_recycled_vertex = false);
        void put_vertex(vertex_t vertex_id, std::string_view data);
        void put_edge(vertex_t src, label_t label, vertex_t dst, 
                      std::string_view edge_data = "") {
            put_edge(src, label, EOUT, dst, edge_data);
        }
        void put_edge(vertex_t src, label_t label, dir_t dir, vertex_t dst,
                      std::string_view edge_data = "");

        // segment compact
        void merge_segments(label_t label, dir_t dir = EOUT);

        ~EpochGraphWriter(){}

        void lock_vertex(vertex_t vertex_id) {
            graph.vertex_futexes[vertex_id].lock();
        }

        void unlock_vertex(vertex_t vertex_id) {
            graph.vertex_futexes[vertex_id].unlock();
        }

        const order_t DEFAULT_INIT_ORDER = 1;

    private:
        SegGraph &graph;
        const timestamp_t read_epoch_id;
        const timestamp_t write_epoch_id;

        void check_vertex_id(vertex_t vertex_id)
        {
            if (vertex_id >= graph.vertex_id.load(std::memory_order_relaxed))
                throw std::invalid_argument("The vertex id is invalid.");
        }

        void update_edge_label_block(vertex_t src, label_t label, dir_t dir, uintptr_t edge_block_pointer);

        void merge_segment(VegitoSegmentHeader* old_seg, 
                            VegitoSegmentHeader* new_seg, 
                            vertex_t segidx,
                            uintptr_t* pointer,
                            VegitoEdgeBlockHeader** edge_block, 
                            size_t edge_prop_size);
    };
} // namespace livegraph

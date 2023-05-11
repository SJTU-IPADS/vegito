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
    class EpochGraphReader
    {

    public:
        EpochGraphReader(
            SegGraph &_graph, timestamp_t _read_epoch_id)
            : graph(_graph),
              read_epoch_id(_read_epoch_id),
              segment_cache_meta(0, 0),
              segment_cache_ptr(nullptr, nullptr)
        {
        }

        EpochGraphReader(const EpochGraphReader &) = delete;

        EpochGraphReader(EpochGraphReader &&txn)
            : graph(txn.graph),
              read_epoch_id(std::move(txn.read_epoch_id)),
              segment_cache_meta(std::move(txn.segment_cache_meta)),
              segment_cache_ptr(std::move(txn.segment_cache_ptr))
        {
        }

        timestamp_t get_read_epoch_id() const { return read_epoch_id; }

        VegitoSegmentHeader* locate_segment(segid_t segid, label_t label, dir_t dir = EOUT);
        EpochEdgeIterator get_edges_in_seg(VegitoSegmentHeader* segment, vertex_t src, size_t edge_prop_size);
        EpochEdgeIterator get_edges(vertex_t src, label_t label, dir_t dir = EOUT);

        ~EpochGraphReader() {}

    private:
        SegGraph &graph;
        const timestamp_t read_epoch_id;

        std::pair<segid_t, label_t> segment_cache_meta;
        std::pair<VegitoSegmentHeader*, VegitoSegmentHeader*> segment_cache_ptr;

        void check_vertex_id(vertex_t vertex_id)
        {
            if (vertex_id >= graph.vertex_id.load(std::memory_order_relaxed))
                throw std::invalid_argument("The vertex id is invalid.");
        }

        std::pair<size_t, size_t> get_num_entries_data_length_cache(EdgeBlockHeader *edge_block) const
        {
            return edge_block->get_num_entries_data_length_atomic();
        }
    };
} // namespace livegraph

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

#include "core/epoch_graph_reader.hpp"
#include "core/edge_iterator.hpp"
#include "core/segment_graph.hpp"
#include "app/ldbc/ldbc_schema.h"

using namespace livegraph;

VegitoSegmentHeader* EpochGraphReader::locate_segment(segid_t seg_id, label_t label, dir_t dir)
{
    auto pointer = graph.edge_label_ptrs[seg_id];
    if (pointer == graph.block_manager.NULLPOINTER)
        return nullptr;
    // get edge_label_block
    auto edge_label_block = graph.block_manager.convert<EdgeLabelBlockHeader>(pointer);
    for (size_t i = 0; i < edge_label_block->get_num_entries(); i++)
    {
        auto label_entry = edge_label_block->get_entries()[i];
        if (label_entry.get_label() == label)
        {
            // update cache
            segment_cache_meta = std::make_pair(seg_id, label);
            segment_cache_ptr = std::make_pair(
                graph.block_manager.convert<VegitoSegmentHeader>(label_entry.get_pointer(EOUT)), 
                graph.block_manager.convert<VegitoSegmentHeader>(label_entry.get_pointer(EIN)));

            // return result
            return graph.block_manager.convert<VegitoSegmentHeader>(label_entry.get_pointer(dir));
        }
    }
    return nullptr;
}

EpochEdgeIterator EpochGraphReader::get_edges_in_seg(VegitoSegmentHeader* segment,
                                                    vertex_t src, size_t edge_prop_size)
{
    if (src >= graph.vertex_id.load(std::memory_order_relaxed) || !segment)
        return EpochEdgeIterator(nullptr, nullptr, nullptr, 0, 0, read_epoch_id);

    uint32_t segidx = graph.get_vertex_seg_idx(src);
    uintptr_t edge_block_pointer = segment->get_region_ptr(segidx);
    auto edge_block = (VegitoEdgeBlockHeader*)(edge_block_pointer);
    uintptr_t epoch_table_pointer = segment->get_epoch_table(segidx);
    auto epoch_table = graph.block_manager.convert<EpochBlockHeader>(epoch_table_pointer);

    if (!edge_block || !epoch_table)
        return EpochEdgeIterator(nullptr, nullptr, nullptr, 0, 0, read_epoch_id);
        
    // NOTICE: get the value of num_entries before get the latest epoch table!
    size_t num_entries = edge_block->get_num_entries();

    return EpochEdgeIterator(segment, edge_block, epoch_table, num_entries, edge_prop_size, read_epoch_id);
}

EpochEdgeIterator EpochGraphReader::get_edges(vertex_t src, label_t label, dir_t dir)
{
    if (src >= graph.vertex_id.load(std::memory_order_relaxed))
        return EpochEdgeIterator(nullptr, nullptr, nullptr, 0, 0, read_epoch_id);

    segid_t segid = graph.get_vertex_seg_id(src);

    VegitoSegmentHeader* segment;

    // cache recently-visited segment
    if(segid == segment_cache_meta.first && label == segment_cache_meta.second) {
        if(dir == EOUT) {
            segment = segment_cache_ptr.first;
        } else {
            segment = segment_cache_ptr.second;
        }
    }
    else
    {
        segment = locate_segment(segid, label, dir);
    }

    size_t edge_prop_size = graph.get_edge_prop_size(label);

    return get_edges_in_seg(segment, src, edge_prop_size);
}

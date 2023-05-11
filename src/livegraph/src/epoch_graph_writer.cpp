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

#include "core/epoch_graph_writer.hpp"
#include "core/edge_iterator.hpp"
#include "core/segment_graph.hpp"

using namespace livegraph;

vertex_t EpochGraphWriter::new_vertex(bool use_recycled_vertex)
{
    vertex_t vertex_id = graph.vertex_id.fetch_add(1, std::memory_order_relaxed);
    graph.vertex_futexes[vertex_id].clear();
    graph.vertex_ptrs[vertex_id] = graph.block_manager.NULLPOINTER;

    // create new segment
    if(graph.get_vertex_seg_id(vertex_id) >= graph.get_max_seg_id()) {
        segid_t seg_id = graph.seg_id.fetch_add(1, std::memory_order_relaxed);
        graph.seg_mutexes[seg_id+1] = new std::shared_timed_mutex();
        graph.edge_label_ptrs[seg_id+1] = graph.block_manager.NULLPOINTER;
    }

    return vertex_id;
}

void EpochGraphWriter::put_vertex(vertex_t vertex_id, std::string_view data)
{
    check_vertex_id(vertex_id);

    // lock vertex
    lock_vertex(vertex_id);

    uintptr_t prev_pointer = graph.vertex_ptrs[vertex_id];

    auto size = sizeof(VertexBlockHeader) + data.size();
    auto order = size_to_order(size);
    auto pointer = graph.block_manager.alloc(order);

    auto vertex_block = graph.block_manager.convert<VertexBlockHeader>(pointer);
    vertex_block->fill(order, vertex_id, write_epoch_id, prev_pointer, data.data(), data.size());

    // update vertex pointer
    graph.vertex_ptrs[vertex_id] = pointer;

    // unlock vertex
    unlock_vertex(vertex_id);
}

VegitoSegmentHeader* EpochGraphWriter::locate_segment(segid_t seg_id, label_t label, dir_t dir)
{
    auto pointer = graph.edge_label_ptrs[seg_id];
    if (pointer == graph.block_manager.NULLPOINTER)
        return nullptr;
    // get edge_label_block
    auto edge_label_block = graph.block_manager.convert<EdgeLabelBlockHeader>(pointer);
    for (size_t i = 0; i < edge_label_block->get_num_entries(); i++)
    {
        auto& label_entry = edge_label_block->get_entries()[i];
        if (label_entry.get_label() == label)
        {
            return graph.block_manager.convert<VegitoSegmentHeader>(label_entry.get_pointer(dir));
        }
    }
    return nullptr;
}

void EpochGraphWriter::update_edge_label_block(segid_t segid, label_t label, dir_t dir, uintptr_t segment_pointer)
{
    auto pointer = graph.edge_label_ptrs[segid];
    auto edge_label_block = graph.block_manager.convert<EdgeLabelBlockHeader>(pointer);
    if (edge_label_block)
    {
        for (size_t i = 0; i < edge_label_block->get_num_entries(); i++)
        {
            auto &label_entry = edge_label_block->get_entries()[i];
            if (label_entry.get_label() == label)
            {
                label_entry.set_pointer(segment_pointer, dir);
                return;
            }
        }
    }

    EdgeLabelEntry label_entry;
    label_entry.set_label(label);
    label_entry.set_pointer(segment_pointer, dir);

    if (!edge_label_block || !edge_label_block->append(label_entry))
    {
        auto num_entries = edge_label_block ? edge_label_block->get_num_entries() : 0;
        auto size = sizeof(EdgeLabelBlockHeader) + (1 + num_entries) * sizeof(EdgeLabelEntry);
        auto order = size_to_order(size);

        auto new_pointer = graph.block_manager.alloc(order);

        auto new_edge_label_block = graph.block_manager.convert<EdgeLabelBlockHeader>(new_pointer);
        new_edge_label_block->fill(order, segid, write_epoch_id, pointer);

        for (size_t i = 0; i < num_entries; i++)
        {
            auto old_label_entry = edge_label_block->get_entries()[i];
            new_edge_label_block->append(old_label_entry);
        }

        new_edge_label_block->append(label_entry);

        graph.edge_label_ptrs[segid] = new_pointer;
    }
}

void EpochGraphWriter::put_edge(vertex_t src, label_t label, dir_t dir, vertex_t dst, 
                                std::string_view edge_data)
{
    check_vertex_id(src);
    // we don't need to check dst id
    // check_vertex_id(dst);

    segid_t segid = graph.get_vertex_seg_id(src);
    uint32_t segidx = graph.get_vertex_seg_idx(src);

    auto edge_prop_value = edge_data.data();
    size_t edge_prop_size = edge_data.size();

    VegitoSegmentHeader *segment, *test_segment;

    graph.vertex_futexes[src].lock();

start:
    // 同样要获取segment的lock，因为此时依然有读事务在做AP任务
    graph.seg_mutexes[segid]->lock_shared();
    // 就算是batch_add的话也可以有cache？每次都要locate的overhead太大了
    segment = locate_segment(segid, label, dir);
    test_segment = segment;

    // init segment edge block
    if(!segment) {
        // acquire exclusive write lock
        graph.seg_mutexes[segid]->unlock_shared();
        graph.seg_mutexes[segid]->lock();

        segment = locate_segment(segid, label, dir);

        // the first writer to change segment location
        if(segment == test_segment) {
            // allocate a new segment
            auto order = SegGraph::INIT_SEGMENT_ORDER;
            while((1 << order) < sizeof(VegitoSegmentHeader) * 10)
                order++;

            auto new_seg_pointer = graph.block_manager.alloc(order);
            auto new_segment = graph.block_manager.convert<VegitoSegmentHeader>(new_seg_pointer);
            new_segment->fill(order, segid);

            update_edge_label_block(segid, label, dir, new_seg_pointer);
            graph.seg_mutexes[segid]->unlock();
            graph.seg_mutexes[segid]->lock_shared();

            segment = new_segment;
            test_segment = segment;
        } else {  // other writer has updated the segment
            // release lock and restart
            graph.seg_mutexes[segid]->unlock();
            goto start;
        }
    }

    uintptr_t edge_block_pointer = segment->get_region_ptr(segidx);
    VegitoEdgeBlockHeader* edge_block = (VegitoEdgeBlockHeader*)(edge_block_pointer);

    VegitoEdgeEntry entry;
    entry.set_dst(dst);

    if (!edge_block || !edge_block->has_space())
    {
        size_t size;
        order_t order;

        // calculate the size for new edge block
        if(edge_block)
        {
            order = edge_block->get_order() + 1;
        }
        else
        {
            // default init value
            order = DEFAULT_INIT_ORDER;
        }

        auto new_edge_block_pointer = segment->alloc(order, edge_prop_size);

        // segment has no space for the new edge block
        if(!new_edge_block_pointer) {
            graph.seg_mutexes[segid]->unlock_shared();
            graph.seg_mutexes[segid]->lock();

            segment = locate_segment(segid, label, dir);

            // the first writer to change segment location
            if (test_segment == segment) {
                // allocate a new segment
                order_t new_order = segment->get_order() + 1;
                auto new_seg_pointer = graph.block_manager.alloc(new_order);
                auto new_segment = graph.block_manager.convert<VegitoSegmentHeader>(new_seg_pointer);
                new_segment->fill(new_order, segid);

                // copy&merge data of old segment into new segment
                merge_segment(segment, new_segment, segidx, &edge_block_pointer, &edge_block, edge_prop_size);

                graph.segments_to_recycle.local().push_back(std::make_tuple(graph.block_manager.revert((uintptr_t)segment), segment->get_order(), write_epoch_id));
                update_edge_label_block(segid, label, dir, new_seg_pointer);
                graph.seg_mutexes[segid]->unlock();
                graph.seg_mutexes[segid]->lock_shared();

                segment = new_segment;
            } else {
                // release lock and restart
                graph.seg_mutexes[segid]->unlock();
                goto start;
            }
        }
        else
        {
            auto new_edge_block = (VegitoEdgeBlockHeader*)new_edge_block_pointer;
            if (order >= SegGraph::COPY_THRESHOLD_ORDER) {
                size_t prev_num_entries = 0;
                if(edge_block)
                    prev_num_entries = edge_block->get_prev_num_entries() + edge_block->get_num_entries();
                new_edge_block->fill(order, edge_block, prev_num_entries);
            } else {
                new_edge_block->fill(order, nullptr, 0);

                if (edge_block) {
                    auto entries = edge_block->get_entries();
                    auto num_entries = edge_block->get_num_entries();
                    for (size_t i = 0; i < num_entries; i++)
                    {
                        entries--;
                        auto edge = new_edge_block->append(*entries); // direct update size

                        if (edge_prop_size > 0) {
                            auto old_edge_prop_offset = segment->get_allocated_edge_num((uintptr_t)edge_block) + i;
                            auto new_edge_prop_offset = segment->get_allocated_edge_num((uintptr_t)new_edge_block) + new_edge_block->get_num_entries() - 1;
                            const void* edge_prop_value = segment->get_property(old_edge_prop_offset, edge_prop_size);
                            segment->append_property(new_edge_prop_offset, edge_prop_value, edge_prop_size);
                        }
                    }
                }
            }

            // update region pointer
            segment->set_region_ptr(segidx, new_edge_block_pointer);

            edge_block_pointer = new_edge_block_pointer;
            edge_block = new_edge_block;
        }
    }

    uintptr_t epoch_table_pointer = segment->get_epoch_table(segidx);
    EpochBlockHeader* epoch_table = graph.block_manager.convert<EpochBlockHeader>(epoch_table_pointer);

    if(!epoch_table) {
        size_t size = sizeof(EpochBlockHeader) + sizeof(VegitoEpochEntry);
        order_t order = size_to_order(size);
        auto new_epoch_table_pointer = graph.block_manager.alloc(order);
        auto new_epoch_table = graph.block_manager.convert<EpochBlockHeader>(new_epoch_table_pointer);

        // append the initial epoch entry
        VegitoEpochEntry epoch_entry;
        epoch_entry.set_offset(0);
        epoch_entry.set_epoch(write_epoch_id);

        new_epoch_table->fill(order, 0, write_epoch_id);
        new_epoch_table->append(epoch_entry);

        // update epoch table
        segment->set_epoch_table(segidx, new_epoch_table_pointer);

        epoch_table_pointer = new_epoch_table_pointer;
        epoch_table = new_epoch_table;
    }

    auto latest_epoch = epoch_table->get_latest_epoch();

    if(write_epoch_id > latest_epoch) {
        if(!epoch_table->has_space()) {
            // create new epoch table
            order_t order = epoch_table->get_order() + 1;
            auto new_epoch_table_pointer = graph.block_manager.alloc(order);
            auto new_epoch_table = graph.block_manager.convert<EpochBlockHeader>(new_epoch_table_pointer);
            new_epoch_table->fill(order, epoch_table_pointer, latest_epoch);

            // copy all old entries
            auto entries = epoch_table->get_entries();
            auto num_entries = epoch_table->get_num_entries();

            for (size_t i = 0; i < num_entries; i++) {
                entries--;
                new_epoch_table->append(*entries);
            }

            // update epoch table
            segment->set_epoch_table(segidx, new_epoch_table_pointer);

            epoch_table_pointer = new_epoch_table_pointer;
            epoch_table = new_epoch_table;
        }

        VegitoEpochEntry epoch_entry;
        epoch_entry.set_offset(edge_block->get_prev_num_entries() +
                               edge_block->get_num_entries());
        epoch_entry.set_epoch(write_epoch_id);
        // std::cout << "Write new epoch" << std::endl;

        epoch_table->set_latest_epoch(write_epoch_id);
        epoch_table->append(epoch_entry);
    }

    auto allocated_edge_num = segment->get_allocated_edge_num((uintptr_t)edge_block) + edge_block->get_num_entries();

    // insert edge
    auto edge = edge_block->append(entry);

    // insert edge property
    if(edge_prop_size > 0) {
        segment->append_property(allocated_edge_num, edge_prop_value, edge_prop_size);
    }
    graph.seg_mutexes[segid]->unlock_shared();
    graph.vertex_futexes[src].unlock();
}

// TODO:
void EpochGraphWriter::merge_segment(VegitoSegmentHeader* old_seg, 
                                   VegitoSegmentHeader* new_seg, 
                                   vertex_t segidx,
                                   uintptr_t* pointer,
                                   VegitoEdgeBlockHeader** edge_block, 
                                   size_t edge_prop_size) {
    // merge old edge block + compact
    for(int i = 0; i < VERTEX_PER_SEG; i++)
    {
        size_t new_num_entries = 0;
        uintptr_t region_ptr = old_seg->get_region_ptr(i);
        auto merged_block = (VegitoEdgeBlockHeader*)(region_ptr);
        std::vector<VegitoEdgeBlockHeader*> merged_edge_blocks;

        // 1. calculate the required order
        while (merged_block)
        {
            merged_edge_blocks.emplace_back(merged_block);
            size_t num_merged_entries = merged_block->get_num_entries();

            new_num_entries += num_merged_entries;
            merged_block = merged_block->get_prev_pointer();
        }

        if(new_num_entries == 0 && i != segidx)
            continue;

        auto merged_size = (pointer != nullptr && i == segidx) ? (new_num_entries + 1) : new_num_entries;
        auto merged_order = size_to_order(merged_size);

        auto new_edge_block_pointer = new_seg->alloc(merged_order, edge_prop_size);
        new_seg->set_region_ptr(i, new_edge_block_pointer);
        new_seg->set_epoch_table(i, old_seg->get_epoch_table(i));
        auto new_edge_block = (VegitoEdgeBlockHeader*)(new_edge_block_pointer);
        new_edge_block->fill(merged_order, nullptr, 0);

        // 2. merge + compact
        for(int j = merged_edge_blocks.size() - 1; j >= 0; j--)
        {   
            auto merged_block = merged_edge_blocks[j];
            size_t num_merged_entries = merged_block->get_num_entries();
            auto merged_entries = merged_block->get_entries();

            for(size_t k = 0; k < num_merged_entries; k++)
            {
                merged_entries--;
                auto merged_edge = new_edge_block->append(*merged_entries); // direct update size
                // insert edge property
                if(edge_prop_size > 0) {
                    auto old_edge_prop_offset = old_seg->get_allocated_edge_num((uintptr_t)merged_block) + k;
                    auto new_edge_prop_offset = new_seg->get_allocated_edge_num((uintptr_t)new_edge_block) + new_edge_block->get_num_entries()-1;
                    const void* edge_prop_value = old_seg->get_property(old_edge_prop_offset, edge_prop_size);
                    new_seg->append_property(new_edge_prop_offset, edge_prop_value, edge_prop_size);
                }
            }
        }
        
        if(pointer != nullptr && i == segidx) {
            *pointer = new_edge_block_pointer;
            *edge_block = new_edge_block;
        }
    }
}

void EpochGraphWriter::merge_segments(label_t label, dir_t dir) {
    size_t edge_prop_size = graph.get_edge_prop_size(label);
    for(segid_t segid = 0; segid < graph.get_max_seg_id(); segid++) {
        auto segment = locate_segment(segid, label, dir);
        if(segment) {
            auto new_seg_pointer = graph.block_manager.alloc(segment->get_order());
            auto new_segment = graph.block_manager.convert<VegitoSegmentHeader>(new_seg_pointer);
            new_segment->fill(segment->get_order(), segid);

            merge_segment(segment, new_segment, -1, nullptr, nullptr, edge_prop_size);

            // TODO: when to free the old segment?
            //graph.block_manager.free(segment, segment->get_order());
            update_edge_label_block(segid, label, dir, new_seg_pointer);
        }
    }
}

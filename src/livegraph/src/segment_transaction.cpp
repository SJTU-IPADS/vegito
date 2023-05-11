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

#include "core/segment_transaction.hpp"
#include "core/edge_iterator.hpp"
#include "core/segment_graph.hpp"

using namespace livegraph;

vertex_t SegTransaction::new_vertex(bool use_recycled_vertex)
{
    check_valid();
    check_writable();

    vertex_t vertex_id;
    if (!batch_update && recycled_vertex_cache.size())
    {
        vertex_id = recycled_vertex_cache.front();
        recycled_vertex_cache.pop_front();
    }
    else if (!use_recycled_vertex || (!graph.recycled_vertex_ids.try_pop(vertex_id)))
    {
        vertex_id = graph.vertex_id.fetch_add(1, std::memory_order_relaxed);
    }
    graph.vertex_futexes[vertex_id].clear();
    graph.vertex_ptrs[vertex_id] = graph.block_manager.NULLPOINTER;

    segid_t seg_id;
    if(graph.get_vertex_seg_id(vertex_id) >= graph.get_max_seg_id()) {
        seg_id = graph.seg_id.fetch_add(1, std::memory_order_relaxed);
        graph.seg_mutexes[seg_id+1] = new std::shared_timed_mutex();
        graph.edge_label_ptrs[seg_id+1] = graph.block_manager.NULLPOINTER;
    }

    if (!batch_update)
    {
        new_vertex_cache.emplace_back(vertex_id);
        ++wal_num_ops();
        wal_append(OPType::NewVertex);
        wal_append(vertex_id);
    }

    return vertex_id;
}

void SegTransaction::put_vertex(vertex_t vertex_id, std::string_view data)
{
    check_valid();
    check_writable();
    check_vertex_id(vertex_id);

    uintptr_t prev_pointer;
    if (batch_update)
    {
        graph.vertex_futexes[vertex_id].lock();
        prev_pointer = graph.vertex_ptrs[vertex_id];
    }
    else
    {
        ensure_vertex_lock(vertex_id);
        auto cache_iter = vertex_ptr_cache.find(vertex_id);
        if (cache_iter != vertex_ptr_cache.end())
            prev_pointer = cache_iter->second;
        else
        {
            ensure_no_confict(vertex_id);
            prev_pointer = graph.vertex_ptrs[vertex_id];
        }
    }

    auto size = sizeof(VertexBlockHeader) + data.size();
    auto order = size_to_order(size);
    auto pointer = graph.block_manager.alloc(order);

    auto vertex_block = graph.block_manager.convert<VertexBlockHeader>(pointer);
    vertex_block->fill(order, vertex_id, write_epoch_id, prev_pointer, data.data(), data.size());

    if (batch_update)
    {
        graph.vertex_ptrs[vertex_id] = pointer;
        graph.vertex_futexes[vertex_id].unlock();
    }
    else
    {
        block_cache.emplace_back(pointer, order);
        timestamps_to_update.emplace_back(vertex_block->get_creation_time_pointer(), SegGraph::ROLLBACK_TOMBSTONE);
        vertex_ptr_cache[vertex_id] = pointer;

        ++wal_num_ops();
        wal_append(OPType::PutVertex);
        wal_append(vertex_id);
        wal_append(data);
    }
}

bool SegTransaction::del_vertex(vertex_t vertex_id, bool recycle)
{
    check_valid();
    check_writable();
    check_vertex_id(vertex_id);

    uintptr_t prev_pointer;
    if (batch_update)
    {
        graph.vertex_futexes[vertex_id].lock();
        prev_pointer = graph.vertex_ptrs[vertex_id];
    }
    else
    {
        ensure_vertex_lock(vertex_id);
        auto cache_iter = vertex_ptr_cache.find(vertex_id);
        if (cache_iter != vertex_ptr_cache.end())
            prev_pointer = cache_iter->second;
        else
        {
            ensure_no_confict(vertex_id);
            prev_pointer = graph.vertex_ptrs[vertex_id];
        }
    }

    bool ret = false;
    auto prev_vertex_block = graph.block_manager.convert<VertexBlockHeader>(prev_pointer);
    if (prev_vertex_block && prev_vertex_block->get_length() != prev_vertex_block->TOMBSTONE)
    {
        ret = true;
        auto size = sizeof(VertexBlockHeader);
        auto order = size_to_order(size);
        auto pointer = graph.block_manager.alloc(order);

        auto vertex_block = graph.block_manager.convert<VertexBlockHeader>(pointer);
        vertex_block->fill(order, vertex_id, write_epoch_id, prev_pointer, nullptr, vertex_block->TOMBSTONE);

        if (!batch_update)
        {
            block_cache.emplace_back(pointer, order);
            timestamps_to_update.emplace_back(vertex_block->get_creation_time_pointer(), SegGraph::ROLLBACK_TOMBSTONE);
            vertex_ptr_cache[vertex_id] = pointer;
        }
        /// debug: else needed here?
        else 
        {
            graph.vertex_ptrs[vertex_id] = pointer;
        }
    }

    if (batch_update)
    {
        if (recycle)
            graph.recycled_vertex_ids.push(vertex_id);
        graph.vertex_futexes[vertex_id].unlock();
    }
    else
    {
        ++wal_num_ops();
        wal_append(OPType::DelVertex);
        wal_append(vertex_id);
        wal_append(recycle);

        if (recycle)
            recycled_vertex_cache.emplace_back(vertex_id);
    }

    return ret;
}

std::string_view SegTransaction::get_vertex(vertex_t vertex_id)
{
    check_valid();

    if (vertex_id >= graph.vertex_id.load(std::memory_order_relaxed))
        return std::string_view();

    uintptr_t pointer;
    if (batch_update || !trace_cache)
    {
        pointer = graph.vertex_ptrs[vertex_id];
    }
    else
    {
        auto cache_iter = vertex_ptr_cache.find(vertex_id);
        if (cache_iter != vertex_ptr_cache.end())
            pointer = cache_iter->second;
        else
            pointer = graph.vertex_ptrs[vertex_id];
    }

    auto vertex_block = graph.block_manager.convert<VertexBlockHeader>(pointer);
    while (vertex_block)
    {
        if (cmp_timestamp(vertex_block->get_creation_time_pointer(), read_epoch_id, local_txn_id) <= 0)
            break;
        pointer = vertex_block->get_prev_pointer();
        vertex_block = graph.block_manager.convert<VertexBlockHeader>(pointer);
    }

    // if (!(batch_update || !trace_cache))
    //{
    //    vertex_ptr_cache[vertex_id] = pointer;
    //}

    if (!vertex_block || vertex_block->get_length() == vertex_block->TOMBSTONE)
        return std::string_view();

    return std::string_view(vertex_block->get_data(), vertex_block->get_length());
}

std::pair<EdgeEntry *, char *> 
SegTransaction::locate_edge_in_block(vertex_t dst, EdgeBlockHeader *edge_block, size_t num_entries, size_t data_length)
{
    if (!edge_block)
        return {nullptr, nullptr};

    auto bloom_filter = edge_block->get_bloom_filter();
    if (bloom_filter.valid() && !bloom_filter.find(dst))
        return {nullptr, nullptr};

    auto entries = edge_block->get_entries() - num_entries;
    auto data = edge_block->get_data() + data_length;
    for (size_t i = 0; i < num_entries; i++)
    {
        data -= entries->get_length();
        if (entries->get_dst() == dst &&
            cmp_timestamp(entries->get_creation_time_pointer(), read_epoch_id, local_txn_id) <= 0 &&
            cmp_timestamp(entries->get_deletion_time_pointer(), read_epoch_id, local_txn_id) > 0)
        {
            return {entries, data};
        }
        entries++;
    }

    return {nullptr, nullptr};
}

uintptr_t 
SegTransaction::locate_segment(segid_t seg_id, label_t label, dir_t dir)
{
    auto pointer = graph.edge_label_ptrs[seg_id];
    if (pointer == graph.block_manager.NULLPOINTER)
        return pointer;
    // get edge_label_block
    auto edge_label_block = graph.block_manager.convert<EdgeLabelBlockHeader>(pointer);
    for (size_t i = 0; i < edge_label_block->get_num_entries(); i++)
    {
        auto label_entry = edge_label_block->get_entries()[i];
        if (label_entry.get_label() == label)
        {
            return label_entry.get_pointer(dir);
        }
    }
    return graph.block_manager.NULLPOINTER;
}

uintptr_t SegTransaction::locate_block_in_segment(uintptr_t ptr, vertex_t idx)
{
    if(ptr == graph.block_manager.NULLPOINTER) 
        return ptr;

    auto segment_ptr = graph.block_manager.convert<SegmentHeader>(ptr);
    if(!segment_ptr)
        return graph.block_manager.NULLPOINTER;

    uintptr_t region_ptr = segment_ptr->get_region_ptr(idx);

    while (region_ptr != graph.block_manager.NULLPOINTER)
    {
        auto edge_block = (EdgeBlockHeader*)(region_ptr);
        if (cmp_timestamp(edge_block->get_creation_time_pointer(), read_epoch_id, local_txn_id) <= 0)
            break;
        region_ptr = edge_block->get_prev_pointer();
    }
    return region_ptr;
}

void SegTransaction::ensure_no_confict(vertex_t src, label_t label, dir_t dir)
{
    segid_t seg_id = graph.get_vertex_seg_id(src);
    uint32_t seg_idx = graph.get_vertex_seg_idx(src);
    auto pointer = locate_segment(seg_id, label, dir);
    if (pointer != graph.block_manager.NULLPOINTER)
    {
        auto edge_block = locate_block_in_segment(pointer, seg_idx);
        auto header = (EdgeBlockHeader*)(edge_block);
        if (header && cmp_timestamp(header->get_committed_time_pointer(), read_epoch_id, local_txn_id) > 0)
            throw RollbackExcept("Write-write confict on: " + std::to_string(src) + ": " +
                                    std::to_string(label) + ".");
    }
    return;
}

void SegTransaction::update_edge_label_block(segid_t segid, label_t label, dir_t dir,
                                             uintptr_t segment_pointer)
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

        if (!batch_update)
        {
            block_cache.emplace_back(new_pointer, order);
            timestamps_to_update.emplace_back(new_edge_label_block->get_creation_time_pointer(),
                                              SegGraph::ROLLBACK_TOMBSTONE);
        }

        for (size_t i = 0; i < num_entries; i++)
        {
            auto old_label_entry = edge_label_block->get_entries()[i];
            new_edge_label_block->append(old_label_entry);
        }

        new_edge_label_block->append(label_entry);

        graph.edge_label_ptrs[segid] = new_pointer;
    }
}

void SegTransaction::put_edge(vertex_t src, label_t label, dir_t dir, vertex_t dst, 
                              std::string_view edge_data, bool force_insert)
{
    check_valid();
    check_writable();
    check_vertex_id(src);
    // check_vertex_id(dst);

    segid_t segid = graph.get_vertex_seg_id(src);
    uint32_t segidx = graph.get_vertex_seg_idx(src);

    uintptr_t seg_pointer, test_seg_pointer;
    uintptr_t pointer;

    bool recover_read, recover_write, recover_vertex;
    graph.vertex_futexes[src].lock();

    start:
    if (batch_update)
    {
        // 同样要获取segment的lock，因为此时依然有读事务在做AP任务
        graph.seg_mutexes[segid]->lock_shared();
        // 就算是batch_add的话也可以有cache？每次都要locate的overhead太大了
        seg_pointer = locate_segment(segid, label, dir);
        test_seg_pointer = seg_pointer;
        pointer = locate_block_in_segment(seg_pointer, segidx);
    }
    else
    {
        recover_write = has_write_lock(segid);
        recover_read = ensure_segment_read_lock(segid, recover_write);
        recover_vertex = ensure_vertex_lock(src);
        auto cache_iter = segment_ptr_cache.find(std::make_pair(segid, label));
        if (cache_iter != segment_ptr_cache.end())
        {
            seg_pointer = cache_iter->second;
            test_seg_pointer = locate_segment(segid, label, dir);
        }
        else
        {
            ensure_no_confict(src, label, dir);
            seg_pointer = locate_segment(segid, label, dir);
            test_seg_pointer = seg_pointer;
            segment_ptr_cache.emplace_hint(cache_iter, std::make_pair(segid, label), seg_pointer);
        }
        pointer = locate_block_in_segment(seg_pointer, segidx);
    }

    EdgeEntry entry;
    entry.set_length(edge_data.size());
    entry.set_dst(dst);
    entry.set_creation_time(write_epoch_id);
    entry.set_deletion_time(SegGraph::ROLLBACK_TOMBSTONE);

    auto segment = graph.block_manager.convert<SegmentHeader>(seg_pointer);
    EdgeBlockHeader* edge_block = (EdgeBlockHeader*)(pointer);

    auto [num_entries, data_length] =
        edge_block ? get_num_entries_data_length_cache(edge_block) : std::pair<size_t, size_t>{0, 0};

    // init segment edge block
    if(!segment) {
        if (batch_update)
        {
            graph.seg_mutexes[segid]->unlock_shared();
            graph.seg_mutexes[segid]->lock();
        }
        else
        {
            ensure_segment_write_lock(segid);
        }

        seg_pointer = locate_segment(segid, label, dir);

        if(seg_pointer == test_seg_pointer) {
            auto order = SegGraph::INIT_SEGMENT_ORDER;
            while((1ul << order) < sizeof(SegmentHeader) * 10)
                order++;
            auto new_seg_pointer = graph.block_manager.alloc(order);
            auto new_segment = graph.block_manager.convert<SegmentHeader>(new_seg_pointer);
            new_segment->fill(order, segid);

            if (!batch_update)
            {
                segment_cache.emplace_back(new_seg_pointer, order);
                segment_ptr_cache[std::make_pair(segid, label)] = new_seg_pointer;
            }
            else
            {
                update_edge_label_block(segid, label, dir, new_seg_pointer);
                graph.seg_mutexes[segid]->unlock();
                graph.seg_mutexes[segid]->lock_shared();
            }

            segment = new_segment;
        } else {
            if (batch_update)
            {
                graph.seg_mutexes[segid]->unlock();
            }
            else
            {
                recover_lock_status(segid, src, recover_read, recover_write, recover_vertex);
            }
            goto start;
        }
    }

    if (!edge_block || !edge_block->has_space(entry, num_entries, data_length))
    {
        size_t size;
        order_t order;
        if(edge_block)
        {
            order = edge_block->get_order() + 1;
        }
        else
        {
            size = sizeof(EdgeBlockHeader) + sizeof(EdgeEntry) + entry.get_length();
            order = size_to_order(size);
        }

        auto new_edge_block_pointer = segment->alloc(order);
        if(!new_edge_block_pointer) {
            if (batch_update)
            {
                graph.seg_mutexes[segid]->unlock_shared();
                graph.seg_mutexes[segid]->lock();
            }
            else
            {
                ensure_segment_write_lock(segid);
            }

            auto cur_seg_pointer = locate_segment(segid, label, dir);

            if (test_seg_pointer == cur_seg_pointer) {
                order_t new_order = segment->get_order() + 1;
                auto new_seg_pointer = graph.block_manager.alloc(new_order);
                auto new_segment = graph.block_manager.convert<SegmentHeader>(new_seg_pointer);
                new_segment->fill(new_order, segid);

                merge_segment(segment, new_segment, segidx, entry, &pointer, &edge_block);
                
                if (!batch_update)
                {
                    segment_to_be_freed_cache.emplace_back(seg_pointer, segment->get_order());
                    segment_cache.emplace_back(new_seg_pointer, new_order);
                    segment_ptr_cache[std::make_pair(segid, label)] = new_seg_pointer;
                }
                else
                {
                    graph.block_manager.free(seg_pointer, segment->get_order());
                    update_edge_label_block(segid, label, dir, new_seg_pointer);
                    graph.seg_mutexes[segid]->unlock();
                    graph.seg_mutexes[segid]->lock_shared();
                }

                segment = new_segment;
            } else {
                if (batch_update)
                {
                    graph.seg_mutexes[segid]->unlock();
                }
                else
                {
                    recover_lock_status(segid, src, recover_read, recover_write, recover_vertex);
                }
                goto start;
            }
        }
        else
        {   
            auto new_edge_block = (EdgeBlockHeader*)new_edge_block_pointer;
            new_edge_block->fill(order, src, write_epoch_id, graph.block_manager.NULLPOINTER, write_epoch_id);

            if (edge_block) {
                auto entries = edge_block->get_entries();
                auto data = edge_block->get_data();

                auto bloom_filter = new_edge_block->get_bloom_filter();
                for (size_t i = 0; i < num_entries; i++)
                {
                    entries--;
                    auto edge = new_edge_block->append(*entries, data, bloom_filter); // direct update size
                    if (!batch_update && edge->get_creation_time() == -local_txn_id)
                        timestamps_to_update.emplace_back(edge->get_creation_time_pointer(), SegGraph::ROLLBACK_TOMBSTONE);
                    data += entries->get_length();
                }
            }
            

            if (!batch_update)
            {
                timestamps_to_update.emplace_back(new_edge_block->get_creation_time_pointer(), SegGraph::ROLLBACK_TOMBSTONE);
                region_ptr_cache[segment->get_region_ptr_pointer(segidx)] = new_edge_block_pointer;
            }
            else
            {
                segment->set_region_ptr(segidx, new_edge_block_pointer);
            }

            pointer = new_edge_block_pointer;
            edge_block = new_edge_block;
        }
    }
    
    if (!force_insert)
    {
        // delete previous edge
        auto prev_edge_block = edge_block;
        while (prev_edge_block) {
            auto [num_entries, data_length] = get_num_entries_data_length_cache(prev_edge_block);
            if (num_entries > 0) {
                auto prev_edge = locate_edge_in_block(dst, prev_edge_block, num_entries, data_length);
                if (prev_edge.first)
                {
                    prev_edge.first->set_deletion_time(write_epoch_id);
                    if (!batch_update)
                        timestamps_to_update.emplace_back(prev_edge.first->get_deletion_time_pointer(),
                                                        SegGraph::ROLLBACK_TOMBSTONE);
                    break;
                }
            }
            prev_edge_block = (EdgeBlockHeader*)(prev_edge_block->get_prev_pointer());
        }
    }

    // insert edge
    std::tie(num_entries, data_length) = get_num_entries_data_length_cache(edge_block);
    auto edge = edge_block->append_without_update_size(entry, edge_data.data(), num_entries, data_length);
    set_num_entries_data_length_cache(edge_block, num_entries + 1, data_length + entry.get_length());
    if (!batch_update)
        timestamps_to_update.emplace_back(edge->get_creation_time_pointer(), SegGraph::ROLLBACK_TOMBSTONE);

    if (batch_update)
    {
        graph.seg_mutexes[segid]->unlock_shared();
        graph.vertex_futexes[src].unlock();
    }
    else
    {
        ++wal_num_ops();
        wal_append(OPType::PutEdge);
        wal_append(src);
        wal_append(label);
        wal_append(dst);
        wal_append(force_insert);
        wal_append(edge_data);
    }
}

bool SegTransaction::del_edge(vertex_t src, label_t label, dir_t dir, vertex_t dst)
{
    check_valid();
    check_writable();
    check_vertex_id(src);
    check_vertex_id(dst);

    segid_t segid = graph.get_vertex_seg_id(src);
    uint32_t segidx = graph.get_vertex_seg_idx(src);

    uintptr_t seg_pointer;
    uintptr_t pointer;

    bool ret = false;

    if (batch_update)
    {
        graph.vertex_futexes[src].lock();
        seg_pointer = locate_segment(segid, label, dir);
        pointer = locate_block_in_segment(seg_pointer, segidx);
    }
    else
    {
        ensure_vertex_lock(src);
        auto cache_iter = segment_ptr_cache.find(std::make_pair(segid, label));
        if (cache_iter != segment_ptr_cache.end())
        {
            seg_pointer = cache_iter->second;
        }
        else
        {
            ensure_no_confict(src, label, dir);
            seg_pointer = locate_segment(segid, label, dir);
            segment_ptr_cache.emplace_hint(cache_iter, std::make_pair(segid, label), seg_pointer);
        }
        pointer = locate_block_in_segment(seg_pointer, segidx);
    }

    auto segment = graph.block_manager.convert<SegmentHeader>(seg_pointer);
    auto edge_block = (EdgeBlockHeader*)(pointer);

    if (!segment || !edge_block)
        return false;

    // delete edge
    while(edge_block) {
        auto [num_entries, data_length] = get_num_entries_data_length_cache(edge_block);
        auto edge = locate_edge_in_block(dst, edge_block, num_entries, data_length);
        if (edge.first)
        {
            ret = true;
            edge.first->set_deletion_time(write_epoch_id);
            if (!batch_update)
                timestamps_to_update.emplace_back(edge.first->get_deletion_time_pointer(),
                                                SegGraph::ROLLBACK_TOMBSTONE);
            break;
        }
        edge_block = (EdgeBlockHeader*)(edge_block->get_prev_pointer());
    }

    if (batch_update)
    {
        graph.vertex_futexes[src].unlock();
    }
    else
    {        
        ++wal_num_ops();
        wal_append(OPType::DelEdge);
        wal_append(src);
        wal_append(label);
        wal_append(dst);
    }

    return ret;
}

std::string_view 
SegTransaction::get_edge(vertex_t src, label_t label, dir_t dir, vertex_t dst)
{
    check_valid();

    if (src >= graph.vertex_id.load(std::memory_order_relaxed))
        return std::string_view();

    segid_t segid = graph.get_vertex_seg_id(src);
    uint32_t segidx = graph.get_vertex_seg_idx(src);

    uintptr_t seg_pointer;
    uintptr_t pointer;
    if (batch_update || !trace_cache)
    {
        seg_pointer = locate_segment(segid, label, dir);
        pointer = locate_block_in_segment(seg_pointer, segidx);
    }
    else
    {
        auto cache_iter = segment_ptr_cache.find(std::make_pair(segid, label));
        if (cache_iter != segment_ptr_cache.end())
        {
            seg_pointer = cache_iter->second;
        }
        else
        {
            seg_pointer = locate_segment(segid, label, dir);
            segment_ptr_cache.emplace_hint(cache_iter, std::make_pair(segid, label), seg_pointer);
        }
        pointer = locate_block_in_segment(seg_pointer, segidx);
    }

    auto edge_block = (EdgeBlockHeader*)(pointer);

    // find edge
    while(edge_block) {
        auto [num_entries, data_length] = get_num_entries_data_length_cache(edge_block);
        auto edge = locate_edge_in_block(dst, edge_block, num_entries, data_length);
        if (edge.first)
        {
            return std::string_view(edge.second, edge.first->get_length());
        }
        edge_block = (EdgeBlockHeader*)(edge_block->get_prev_pointer());
    }

    return std::string_view();
}

void SegTransaction::abort()
{
    check_valid();

    for (const auto &p : timestamps_to_update)
    {
        *p.first = p.second;
    }

    for (const auto &vid : new_vertex_cache)
    {
        graph.recycled_vertex_ids.push(vid);
    }

    for (const auto &p : block_cache)
    {
        graph.block_manager.free(p.first, p.second);
    }

    for (const auto &p : segment_cache)
    {
        graph.block_manager.free(p.first, p.second);
    }

    clean();
}

SegEdgeIterator SegTransaction::get_edges_in_seg(uintptr_t segment, 
                                                 vertex_t src, 
                                                 bool reverse) {
    check_valid();
    std::vector<EdgeBlockHeader*> blocks;
    std::vector<size_t> nums;
    std::vector<size_t> lengths;

    if (src >= graph.vertex_id.load(std::memory_order_relaxed))
        return SegEdgeIterator(nullptr, nullptr, nullptr, 0, 0, read_epoch_id, local_txn_id, reverse);

    uint32_t segidx = graph.get_vertex_seg_idx(src);
    uintptr_t pointer;
    pointer = locate_block_in_segment(segment, segidx);

    auto edge_block = (EdgeBlockHeader*)(pointer);

    if (!edge_block)
        return SegEdgeIterator(nullptr, nullptr, nullptr, 0, 0, read_epoch_id, local_txn_id, reverse);

    auto [num_entries, data_length] = get_num_entries_data_length_cache(edge_block);

    return SegEdgeIterator(edge_block, edge_block->get_entries(), edge_block->get_data(), num_entries, data_length, read_epoch_id,
                        local_txn_id, reverse);
}

SegEdgeIterator SegTransaction::get_edges(vertex_t src, label_t label, 
                                          dir_t dir, bool reverse)
{
    check_valid();
    std::vector<EdgeBlockHeader*> blocks;
    std::vector<size_t> nums;
    std::vector<size_t> lengths;

    if (src >= graph.vertex_id.load(std::memory_order_relaxed))
        return SegEdgeIterator(nullptr, nullptr, nullptr, 0, 0, read_epoch_id, local_txn_id, reverse);

    segid_t segid = graph.get_vertex_seg_id(src);
    uint32_t segidx = graph.get_vertex_seg_idx(src);

    uintptr_t seg_pointer;
    uintptr_t pointer;
    if (batch_update || !trace_cache)
    {
        seg_pointer = locate_segment(segid, label, dir);
        pointer = locate_block_in_segment(seg_pointer, segidx);
    }
    else
    {
        auto cache_iter = segment_ptr_cache.find(std::make_pair(segid, label));
        if (cache_iter != segment_ptr_cache.end())
        {
            seg_pointer = cache_iter->second;
        }
        else
        {
            seg_pointer = locate_segment(segid, label, dir);
            segment_ptr_cache.emplace_hint(cache_iter, std::make_pair(segid, label), seg_pointer);
        }
        pointer = locate_block_in_segment(seg_pointer, segidx);
    }

    auto edge_block = (EdgeBlockHeader*)(pointer);

    auto [num_entries, data_length] = get_num_entries_data_length_cache(edge_block);

    return SegEdgeIterator(edge_block, edge_block->get_entries(), edge_block->get_data(), num_entries, data_length, read_epoch_id,
                        local_txn_id, reverse);
}

timestamp_t SegTransaction::commit(bool wait_visable, dir_t dir)
{
    check_valid();
    check_writable();

    if (batch_update)
        return read_epoch_id;

    auto [commit_epoch_id, num_unfinished] = graph.commit_manager.register_commit(wal);

    for (const auto &p : vertex_ptr_cache)
    {
        auto vertex_id = p.first;
        auto pointer = p.second;
        if (graph.vertex_ptrs[vertex_id] != pointer)
            graph.vertex_ptrs[vertex_id] = pointer;
    }

    for (const auto &vid : recycled_vertex_cache)
    {
        graph.recycled_vertex_ids.push(vid);
    }

    for (const auto &p : edge_block_num_entries_data_length_cache)
    {
        p.first->set_num_entries_data_length_atomic(p.second.first, p.second.second);
        timestamps_to_update.emplace_back(p.first->get_committed_time_pointer(), p.first->get_committed_time());
        p.first->set_committed_time(write_epoch_id);
    }

    for (const auto &p : segment_ptr_cache)
    {
        auto prev_pointer = locate_segment(p.first.first, p.first.second, dir);
        if (p.second != prev_pointer)
        {
            update_edge_label_block(p.first.first, p.first.second, dir, p.second);
        }
    }

    for (const auto &p : region_ptr_cache)
    {
        *p.first = p.second;
    }

    for (const auto &p : segment_to_be_freed_cache)
    {
        graph.block_manager.free(p.first, p.second);
    }

    for (const auto &p : timestamps_to_update)
    {
        *p.first = commit_epoch_id;
    }

    clean();

    graph.commit_manager.finish_commit(commit_epoch_id, num_unfinished, wait_visable);

    return commit_epoch_id;
}

bool SegTransaction::merge_segment(SegmentHeader* old_seg, 
                                   SegmentHeader* new_seg, 
                                   vertex_t segidx,
                                   EdgeEntry& entry,
                                   uintptr_t* pointer,
                                   EdgeBlockHeader** edge_block) {
    // merge old edge block + compact
    for(int i = 0; i < VERTEX_PER_SEG; i++)
    {
        size_t new_num_entries = 0;
        size_t new_data_length = 0;
        uintptr_t region_ptr = old_seg->get_region_ptr(i);
        std::vector<uintptr_t> merged_edge_blocks;
        timestamp_t new_creation_time = write_epoch_id;

        // 1. calculate the required order
        while (region_ptr)
        {
            merged_edge_blocks.emplace_back(region_ptr);
            auto merged_block = (EdgeBlockHeader*)(region_ptr);
            new_creation_time = merged_block->get_creation_time();
            size_t num_merged_entries = merged_block->get_num_entries();
            auto merged_entries = merged_block->get_entries();

            for(size_t j = 0; j < num_merged_entries; j++)
            {
                merged_entries--;
                //if (cmp_timestamp(merged_entries->get_deletion_time_pointer(), read_epoch_id) > 0)
                {
                    new_num_entries++;
                    new_data_length += merged_entries->get_length();
                }
            }
            region_ptr = merged_block->get_prev_pointer();
        }

        if(new_num_entries == 0 && i != segidx)
            continue;

        auto merged_size = (pointer != nullptr && i == segidx) ? sizeof(EdgeBlockHeader) + (new_num_entries + 1) * sizeof(EdgeEntry) + new_data_length + entry.get_length() :
                sizeof(EdgeBlockHeader) + new_num_entries * sizeof(EdgeEntry) + new_data_length;
        auto merged_order = size_to_order(merged_size);

        if (merged_order > EdgeBlockHeader::BLOOM_FILTER_PORTION &&
            merged_size + (1ul << (merged_order - EdgeBlockHeader::BLOOM_FILTER_PORTION)) >= (1ul << EdgeBlockHeader::BLOOM_FILTER_THRESHOLD))
        {
            merged_size += 1ul << (merged_order - EdgeBlockHeader::BLOOM_FILTER_PORTION);
        }
        merged_order = size_to_order(merged_size);

        auto new_edge_block_pointer = new_seg->alloc(merged_order);
        if (!new_edge_block_pointer) return false;
        new_seg->set_region_ptr(i, new_edge_block_pointer);
        auto new_edge_block = (EdgeBlockHeader*)(new_edge_block_pointer);
        new_edge_block->fill(merged_order, VERTEX_PER_SEG * old_seg->get_segment_id() + i, new_creation_time, graph.block_manager.NULLPOINTER, write_epoch_id);
        auto new_bloom_filter = new_edge_block->get_bloom_filter();

        //set_num_entries_data_length_cache(new_edge_block, new_num_entries, new_data_length);

        // 2. merge + compact
        for(int j = merged_edge_blocks.size() - 1; j >= 0; j--)
        {   
            uintptr_t merged_block_ptr = merged_edge_blocks[j];
            auto merged_block = (EdgeBlockHeader*)(merged_block_ptr);
            size_t num_merged_entries = merged_block->get_num_entries();
            auto merged_entries = merged_block->get_entries();
            auto merged_data = merged_block->get_data();

            for(size_t k = 0; k < num_merged_entries; k++)
            {
                merged_entries--;
                //if (cmp_timestamp(merged_entries->get_deletion_time_pointer(), read_epoch_id) > 0)
                {
                    auto merged_edge = new_edge_block->append(*merged_entries, merged_data, new_bloom_filter); // direct update size
                    if (!batch_update && merged_edge->get_creation_time() == -local_txn_id)
                        timestamps_to_update.emplace_back(merged_edge->get_creation_time_pointer(), SegGraph::ROLLBACK_TOMBSTONE);
                }
                merged_data += merged_entries->get_length();
            }
        }
        
        if(pointer != nullptr && i == segidx) {
            *pointer = new_edge_block_pointer;
            *edge_block = new_edge_block;
        }
    }
    return true;
}

void SegTransaction::merge_segments(label_t label, dir_t dir) {
    for(segid_t segid = 0; segid < graph.get_max_seg_id(); segid++) {
        uintptr_t seg_pointer = locate_segment(segid, label, dir);
        auto segment = graph.block_manager.convert<SegmentHeader>(seg_pointer);
        if(segment) {
            auto new_seg_pointer = graph.block_manager.alloc(segment->get_order());
            auto new_segment = graph.block_manager.convert<SegmentHeader>(new_seg_pointer);
            new_segment->fill(segment->get_order(), segid);

            EdgeEntry entry;
            if (!merge_segment(segment, new_segment, -1, entry, nullptr, nullptr)) {
                graph.block_manager.free(new_seg_pointer, new_segment->get_order());
                new_seg_pointer = graph.block_manager.alloc(segment->get_order() + 1);
                new_segment = graph.block_manager.convert<SegmentHeader>(new_seg_pointer);
                new_segment->fill(segment->get_order() + 1, segid);
                merge_segment(segment, new_segment, -1, entry, nullptr, nullptr);
            }

            graph.block_manager.free(seg_pointer, segment->get_order());
            update_edge_label_block(segid, label, dir, new_seg_pointer);
        }
    }
}

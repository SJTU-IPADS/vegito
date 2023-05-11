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

#include <cassert>
#include <cstddef>

#include "bloom_filter.hpp"
#include "types.hpp"
#include "utils.hpp"

namespace livegraph
{
    // direction of the edge
    enum dir_t : uint8_t { EOUT, EIN, EOUTIN };

    class BlockHeader
    {
    public:
        enum class Type : uint8_t
        {
            FREE,
            VERTEX,
            EDGE,
            SEGMENT,
            EDGE_LABEL,
            SPECIAL
        };

        order_t get_order() const { return order; }

        void set_order(order_t order) { this->order = order; }

        size_t get_block_size() const { return 1ul << order; }

        size_t get_block_size(order_t _order) const { return 1ul << _order; }

        Type get_type() const { return type; }

        void set_type(Type type) { this->type = type; }

        void fill(order_t order, Type type)
        {
            set_order(order);
            set_type(type);
        }

    private:
        order_t order;
        Type type;
    };

    class SegmentHeader : public BlockHeader {
    public:
        vertex_t get_segment_id() const { return ((segid_t)segid_high << 32) + (vertex_t)segid_low; }

        void set_segment_id(segid_t segid)
        {
            assert(segid <= ((vertex_t)UINT16_MAX << 32) + UINT32_MAX);
            segid_high = (segid >> 32) & UINT16_MAX;
            segid_low = segid & UINT32_MAX;
        }

        uintptr_t get_region_ptr(uint32_t idx) const { return region_ptrs[idx]; }

        uintptr_t *get_region_ptr_pointer(uint32_t idx) { return &(region_ptrs[idx]); }

        void set_region_ptr(uint32_t idx, uintptr_t ptr) { region_ptrs[idx] = ptr; }

        void fill(order_t order, segid_t segid)
        {
            BlockHeader::fill(order, Type::SEGMENT);
            set_segment_id(segid);
            used_size = 0;

            for(int i = 0; i < VERTEX_PER_SEG; i++) {
                region_ptrs[i] = 0;
            }
        }

        uintptr_t alloc(order_t added_order)
        {
            size_t added_block_size = get_block_size(added_order);
            size_t ret = __sync_fetch_and_add(&used_size, added_block_size);

            if(sizeof(*this) + ret + added_block_size > get_block_size())
                return 0;
            else
                return (uintptr_t)this + sizeof(*this) + ret;
        }
        
        size_t *get_used_size_pointer() { return &used_size; }


    private:
        uint16_t segid_high;
        uint32_t segid_low;
        size_t used_size;
        uintptr_t region_ptrs[VERTEX_PER_SEG];
    };

    class N2OBlockHeader : public BlockHeader
    {
    public:
        vertex_t get_vertex_id() const { return ((vertex_t)vid_high << 32) + (vertex_t)vid_low; }

        void set_vertex_id(vertex_t vid)
        {
            assert(vid <= ((vertex_t)UINT16_MAX << 32) + UINT32_MAX);
            vid_high = (vid >> 32) & UINT16_MAX;
            vid_low = vid & UINT32_MAX;
        }

        timestamp_t get_creation_time() const { return creation_time; }

        timestamp_t *get_creation_time_pointer() { return &creation_time; }

        void set_creation_time(timestamp_t creation_time) { this->creation_time = creation_time; }

        uintptr_t get_prev_pointer() const { return prev_pointer; }

        void set_prev_pointer(uintptr_t prev_pointer) { this->prev_pointer = prev_pointer; }

        void fill(order_t order, Type type, vertex_t vid, timestamp_t creation_time, uintptr_t prev_pointer)
        {
            BlockHeader::fill(order, type);
            set_vertex_id(vid);
            set_creation_time(creation_time);
            set_prev_pointer(prev_pointer);
        }

    private:
        uint16_t vid_high;
        uint32_t vid_low;
        timestamp_t creation_time;
        uintptr_t prev_pointer;
    };

    class VertexBlockHeader : public N2OBlockHeader
    {
    public:
        size_t get_length() const { return length; }

        void set_length(size_t length) { this->length = length; }

        const char *get_data() const { return data; }

        char *get_data() { return data; }

        void clear() { set_length(0); }

        bool set_data(const char *data, size_t length)
        {
            if (sizeof(*this) + length > get_block_size())
                return false;
            for (size_t i = 0; i < length; i++)
                get_data()[i] = data[i];
            set_length(length);
            return true;
        }

        constexpr static size_t TOMBSTONE = UINT64_MAX;

        void fill(order_t order,
                  vertex_t vid,
                  timestamp_t creation_time,
                  uintptr_t prev_pointer,
                  const char *data,
                  size_t length)
        {
            N2OBlockHeader::fill(order, Type::VERTEX, vid, creation_time, prev_pointer);
            if (length == TOMBSTONE)
                set_length(TOMBSTONE);
            else
                set_data(data, length);
        }

    private:
        size_t length;
        char data[0];
    };

    class EdgeLabelEntry
    {
    public:
        EdgeLabelEntry() {
            pointers[EOUT] = 0;
            pointers[EIN] = 0;
        }

        label_t get_label() const { return label; }

        void set_label(label_t label) { this->label = label; }

        uintptr_t get_pointer(dir_t dir = EOUT) const { 
          return pointers[dir]; 
        }

        void set_pointer(uintptr_t pointer, dir_t dir = EOUT) { 
          this->pointers[dir] = pointer; 
        }

    private:
        label_t label;
        uintptr_t pointers[2];
    };

    class EdgeLabelBlockHeader : public N2OBlockHeader
    {
    public:
        size_t get_num_entries() const { return num_entries; }

        void set_num_entries(size_t num_entries) { this->num_entries = num_entries; }

        const EdgeLabelEntry *get_entries() const { return entries; }

        EdgeLabelEntry *get_entries() { return entries; }

        void clear() { set_num_entries(0); }
        bool append(EdgeLabelEntry entry)
        {
            auto num = get_num_entries();
            if (sizeof(*this) + (num + 1) * sizeof(entry) > get_block_size())
                return false;
            get_entries()[num] = entry;
            compiler_fence();
            set_num_entries(num + 1);
            return true;
        }

        void fill(order_t order, uint64_t segid, timestamp_t creation_time, uintptr_t prev_pointer)
        {
            N2OBlockHeader::fill(order, Type::EDGE_LABEL, segid, creation_time, prev_pointer);
            clear();
        }

    private:
        size_t num_entries;
        EdgeLabelEntry entries[0];
    };

    class EdgeEntry
    {
    public:
        vertex_t get_dst() const { return ((vertex_t)dst_high << 32) + (vertex_t)dst_low; }

        void set_dst(vertex_t dst)
        {
            assert(dst <= ((vertex_t)UINT16_MAX << 32) + UINT32_MAX);
            dst_high = (dst >> 32) & UINT16_MAX;
            dst_low = dst & UINT32_MAX;
        }

        timestamp_t get_creation_time() const { return creation_time; }

        timestamp_t *get_creation_time_pointer() { return &creation_time; }

        void set_creation_time(timestamp_t creation_time) { this->creation_time = creation_time; }

        timestamp_t get_deletion_time() const { return deletion_time; }

        timestamp_t *get_deletion_time_pointer() { return &deletion_time; }

        void set_deletion_time(timestamp_t deletion_time) { this->deletion_time = deletion_time; }

        uint16_t get_length() const { return length; }

        void set_length(uint16_t length) { this->length = length; }

    private:
        uint16_t length;
        uint16_t dst_high;
        uint32_t dst_low;
        timestamp_t creation_time;
        timestamp_t deletion_time;
    };

    class VegitoEdgeEntry
    {
    public:
        vertex_t get_dst() const { return this->dst; }

        void set_dst(vertex_t dst) { this->dst = dst; }

    private:
        vertex_t dst;
    };

    class VegitoEpochEntry
    {
    public:
        timestamp_t get_epoch() const { return this->epoch; }

        void set_epoch(timestamp_t epoch) { this->epoch = epoch; }

        size_t get_offset() const { return this->offset; }

        void set_offset(size_t offset) { this->offset = offset; }

    private:
        timestamp_t epoch;
        size_t offset;
    };

    class EdgeBlockHeader : public N2OBlockHeader
    {
    public:
        timestamp_t get_committed_time() const { return committed_time; }

        timestamp_t *get_committed_time_pointer() { return &committed_time; }

        void set_committed_time(timestamp_t committed_time) { this->committed_time = committed_time; }

        size_t get_data_length() const { return tail.data.data_length; }

        void set_data_length(size_t data_length) { this->tail.data.data_length = data_length; }

        const char *get_data() const { return data; }

        char *get_data() { return data; }

        size_t get_num_entries() const { return tail.data.num_entries; }

        void set_num_entries(size_t num_entries) { this->tail.data.num_entries = num_entries; }

        const EdgeEntry *get_entries() const
        {
            size_t block_size = get_block_size();
            if (get_order() >= BLOOM_FILTER_THRESHOLD)
                block_size -= block_size >> BLOOM_FILTER_PORTION;
            return (EdgeEntry *)((uint8_t *)this + block_size);
        }

        EdgeEntry *get_entries()
        {
            size_t block_size = get_block_size();
            if (get_order() >= BLOOM_FILTER_THRESHOLD)
                block_size -= block_size >> BLOOM_FILTER_PORTION;
            return (EdgeEntry *)((uint8_t *)this + block_size);
        }

        const BloomFilter get_bloom_filter() const
        {
            if (get_order() < BLOOM_FILTER_THRESHOLD)
                return BloomFilter();
            size_t block_size = get_block_size();
            size_t bloom_filter_size = block_size >> BLOOM_FILTER_PORTION;
            return BloomFilter(bloom_filter_size, ((uint8_t *)this) + block_size - bloom_filter_size);
        }

        BloomFilter get_bloom_filter()
        {
            if (get_order() < BLOOM_FILTER_THRESHOLD)
                return BloomFilter();
            size_t block_size = get_block_size();
            size_t bloom_filter_size = block_size >> BLOOM_FILTER_PORTION;
            return BloomFilter(get_order() - BLOOM_FILTER_PORTION, ((uint8_t *)this) + block_size - bloom_filter_size);
        }

        void clear()
        {
            set_num_entries(0);
            set_data_length(0);
            auto filter = get_bloom_filter();
            if (filter.valid())
                filter.clear();
        }

        bool has_space(EdgeEntry entry, size_t num_entries, size_t data_length) const
        {
            size_t block_size = get_block_size();
            size_t bloom_filter_size;
            if (get_order() < BLOOM_FILTER_THRESHOLD)
                bloom_filter_size = 0;
            else
                bloom_filter_size = block_size >> BLOOM_FILTER_PORTION;
            if (sizeof(*this) + (num_entries + 1) * sizeof(entry) + data_length + entry.get_length() +
                    bloom_filter_size >
                get_block_size())
                return false;
            else
                return true;
        }

        EdgeEntry *append(EdgeEntry entry, const char *data, BloomFilter &filter)
        {
            auto num = get_num_entries();
            auto length = get_data_length();
            if (!has_space(entry, num, length))
                return nullptr;
            *(get_entries() - num - 1) = entry;
            for (size_t i = 0; i < entry.get_length(); i++)
                (get_data() + length)[i] = data[i];
            compiler_fence();
            set_num_entries(num + 1);
            set_data_length(length + entry.get_length());
            if (filter.valid())
                filter.insert(entry.get_dst());
            return get_entries() - num - 1;
        }

        EdgeEntry *append(EdgeEntry entry, const char *data)
        {
            auto filter = get_bloom_filter();
            return append(entry, data, filter);
        }

        EdgeEntry *append_without_update_size(EdgeEntry entry, const char *data, size_t num, size_t length)
        {
            auto filter = get_bloom_filter();
            if (!has_space(entry, num, length))
                return nullptr;
            *(get_entries() - num - 1) = entry;
            for (size_t i = 0; i < entry.get_length(); i++)
                (get_data() + length)[i] = data[i];
            if (filter.valid())
                filter.insert(entry.get_dst());
            return get_entries() - num - 1;
        }

        void set_num_entries_data_length_atomic(size_t num_entries, size_t data_length)
        {
            Int128Union new_val;
            new_val.data.num_entries = num_entries;
            new_val.data.data_length = data_length;
            // _mm_store_si128(&tail.m128i, new_val.m128i);
            // should be inlined as "lock cmpxchg16b"
            while (!__sync_bool_compare_and_swap(&tail.int128, tail.int128, new_val.int128))
                _mm_pause();
        }

        std::pair<size_t, size_t> get_num_entries_data_length_atomic()
        {
            // Int128Union new_val, cur_val;
            // should be inlined as "lock cmpxchg16b"
            // cur_val.int128 = __sync_val_compare_and_swap(&tail.int128, new_val.int128, tail.int128);
            Int128Union cur_val;
            cur_val.m128i = _mm_load_si128(&tail.m128i);
            return std::make_pair(cur_val.data.num_entries, cur_val.data.data_length);
        }

        void
        fill(order_t order, vertex_t vid, timestamp_t creation_time, uintptr_t prev_pointer, timestamp_t committed_time)
        {
            N2OBlockHeader::fill(order, Type::EDGE, vid, creation_time, prev_pointer);
            set_committed_time(committed_time);
            clear();
        }

        constexpr static order_t BLOOM_FILTER_THRESHOLD = 40;
        constexpr static order_t BLOOM_FILTER_PORTION = 4;

    private:
        timestamp_t committed_time;
        union alignas(16) Int128Union {
            struct
            {
                size_t num_entries;
                size_t data_length;
            } data;
            __int128 int128;
            __m128i m128i;
        } tail;
        char data[0];
    };

    class VegitoEdgeBlockHeader : public BlockHeader
    {
    public:
        size_t get_num_entries() const { return this->num_entries; }

        void set_num_entries(size_t num_entries) { this->num_entries = num_entries; }

        size_t get_prev_num_entries() const { return this->prev_num_entries; }

        void set_prev_num_entries(size_t prev_num_entries) { this->prev_num_entries = prev_num_entries; }

        VegitoEdgeBlockHeader* get_prev_pointer() const { return prev_pointer; }

        void set_prev_pointer(VegitoEdgeBlockHeader* prev_pointer) { 
            this->prev_pointer = prev_pointer; 
        }

        size_t get_vegito_block_size() const {
            return sizeof(*this) + get_block_size() * sizeof(VegitoEdgeEntry);
        }

        const VegitoEdgeEntry *get_entries() const
        {
            size_t block_size = get_vegito_block_size();
            return (VegitoEdgeEntry *)((uint8_t *)this + block_size);
        }

        VegitoEdgeEntry *get_entries()
        {
            size_t block_size = get_vegito_block_size();
            return (VegitoEdgeEntry *)((uint8_t *)this + block_size);
        }

        bool has_space() const
        {
            if (sizeof(*this) + (num_entries + 1) * sizeof(VegitoEdgeEntry) > get_vegito_block_size())
                return false;
            else
                return true;
        }

        VegitoEdgeEntry *append(VegitoEdgeEntry entry)
        {
            auto num = get_num_entries();
            if (!has_space())
                return nullptr;
            *(get_entries() - num - 1) = entry;

            compiler_fence();
            set_num_entries(num + 1);

            return get_entries() - num - 1;
        }

        VegitoEdgeEntry *append_without_update_size(VegitoEdgeEntry entry)
        {
            auto num = get_num_entries();
            if (!has_space())
                return nullptr;
            *(get_entries() - num - 1) = entry;

            return get_entries() - num - 1;
        }

        void
        fill(order_t order, VegitoEdgeBlockHeader* prev_pointer, size_t prev_num_entries)
        {
            BlockHeader::fill(order, Type::EDGE);
            set_prev_pointer(prev_pointer);
            set_prev_num_entries(prev_num_entries);
            set_num_entries(0);
        }

    private:
        uint32_t num_entries;
        uint32_t prev_num_entries;
        VegitoEdgeBlockHeader* prev_pointer;
    };

    class EpochBlockHeader : public BlockHeader
    {
    public:
        size_t get_num_entries() const { return num_entries; }

        void set_num_entries(size_t num_entries) { this->num_entries = num_entries; }

        timestamp_t get_latest_epoch() const { return latest_epoch; }

        void set_latest_epoch(timestamp_t latest_epoch) { this->latest_epoch = latest_epoch; }

        uintptr_t get_prev_pointer() const { return prev_pointer; }

        void set_prev_pointer(uintptr_t prev_pointer) { this->prev_pointer = prev_pointer; }

        const VegitoEpochEntry *get_entries() const
        {
            size_t block_size = get_block_size();
            return (VegitoEpochEntry *)((uint8_t *)this + block_size);
        }

        VegitoEpochEntry *get_entries()
        {
            size_t block_size = get_block_size();
            return (VegitoEpochEntry *)((uint8_t *)this + block_size);
        }

        bool has_space() const
        {
            size_t block_size = get_block_size();

            if ((sizeof(*this) + (num_entries + 1) * sizeof(VegitoEpochEntry)) > get_block_size())
                return false;
            else
                return true;
        }

        VegitoEpochEntry *append(VegitoEpochEntry entry)
        {
            auto num = get_num_entries();
            if (!has_space())
                return nullptr;
            *(get_entries() - num - 1) = entry;

            compiler_fence();

            set_num_entries(num + 1);

            return get_entries() - num - 1;
        }

        void fill(order_t order, uintptr_t prev_pointer, timestamp_t init_epoch)
        {
            BlockHeader::fill(order, Type::EDGE);
            set_prev_pointer(prev_pointer);
            set_latest_epoch(init_epoch);
            set_num_entries(0);
        }

    private:
        size_t num_entries;
        timestamp_t latest_epoch;
        uintptr_t prev_pointer;
    };

    class VegitoSegmentHeader : public BlockHeader {
    public:
        vertex_t get_segment_id() const { return ((segid_t)segid_high << 32) + (vertex_t)segid_low; }

        void set_segment_id(segid_t segid)
        {
            assert(segid <= ((vertex_t)UINT16_MAX << 32) + UINT32_MAX);
            segid_high = (segid >> 32) & UINT16_MAX;
            segid_low = segid & UINT32_MAX;
        }

        uintptr_t get_region_ptr(uint32_t idx) const { return region_ptrs[idx]; }

        uintptr_t *get_region_ptr_pointer(uint32_t idx) { return &(region_ptrs[idx]); }

        void set_region_ptr(uint32_t idx, uintptr_t ptr) { region_ptrs[idx] = ptr; }

        void fill(order_t order, segid_t segid)
        {
            BlockHeader::fill(order, Type::SEGMENT);
            set_segment_id(segid);
            allocated_edge_num = 0;

            for(int i = 0; i < VERTEX_PER_SEG; i++) {
                region_ptrs[i] = 0;
            }

            for(int i = 0; i < VERTEX_PER_SEG; i++) {
                epoch_tables[i] = 0;
            }
        }

        uintptr_t alloc(order_t edge_num_order, size_t edge_prop_size)
        {
            size_t edge_num = get_block_size(edge_num_order);
            size_t require_edge_num = sizeof(VegitoEdgeBlockHeader)/sizeof(VegitoEdgeEntry) + edge_num;
            size_t ret = __sync_fetch_and_add(&allocated_edge_num, require_edge_num);

            if(sizeof(*this) + (ret + require_edge_num) * (sizeof(VegitoEdgeEntry) + edge_prop_size) > get_block_size())
                return 0;
            else
                return (uintptr_t)this + sizeof(*this) + ret * sizeof(VegitoEdgeEntry);
        }

        size_t get_allocated_edge_num(uintptr_t edge_block) {
            return (edge_block - ((uintptr_t)this + sizeof(*this))) / sizeof(VegitoEdgeEntry);
        }

        const void* get_property(size_t offset, size_t edge_prop_size) {
            auto block_size = get_block_size();
            void* data = (void*)((uintptr_t)this + block_size - (offset+1) * edge_prop_size);
            return data;
        }

        void append_property(size_t offset, const void* edge_prop_value, size_t edge_prop_size) {
            auto block_size = get_block_size();
            void* data = (void*)((uintptr_t)this + block_size - (offset+1) * edge_prop_size);
            memcpy(data, edge_prop_value, edge_prop_size);
        }
        
        uintptr_t get_epoch_table(uint32_t idx) const { return epoch_tables[idx]; }

        void set_epoch_table(uint32_t idx, uintptr_t epoch_table) { epoch_tables[idx] = epoch_table; }

    private:
        uint16_t segid_high;
        uint32_t segid_low;
        size_t allocated_edge_num;
        uintptr_t region_ptrs[VERTEX_PER_SEG];
        uintptr_t epoch_tables[VERTEX_PER_SEG];
    };

    static_assert(sizeof(BlockHeader) == 2);
    // static_assert(sizeof(SegmentHeader) == 16);
    static_assert(sizeof(N2OBlockHeader) == 24);
    static_assert(sizeof(VertexBlockHeader) == 32);
    static_assert(sizeof(EdgeLabelEntry) == 24);
    static_assert(sizeof(EdgeLabelBlockHeader) == 32);
    static_assert(sizeof(EdgeEntry) == 24);
    static_assert(sizeof(EdgeBlockHeader) == 48);
    static_assert(sizeof(VegitoEdgeBlockHeader) == 24);
} // namespace livegraph

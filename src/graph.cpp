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

#include "core/graph.hpp"
#include "core/transaction.hpp"

using namespace livegraph;

Transaction Graph::begin_transaction()
{
    auto local_txn_id = transaction_id.fetch_add(1, std::memory_order_relaxed) + 1; // txn_id begin from 1
    auto read_epoch_id = epoch_id.load(std::memory_order_acquire);
    read_epoch_table.local() = read_epoch_id;
    if (local_txn_id % COMPACTION_CYCLE == 0)
        compact(local_txn_id);
    return Transaction(*this, local_txn_id, read_epoch_id, false, true);
}

Transaction Graph::begin_read_only_transaction()
{
    auto read_epoch_id = epoch_id.load(std::memory_order_acquire);
    read_epoch_table.local() = read_epoch_id;
    return Transaction(*this, RO_TRANSACTION, read_epoch_id, false, false);
}

Transaction Graph::begin_batch_loader()
{
    auto read_epoch_id = epoch_id.load(std::memory_order_acquire);
    read_epoch_table.local() = read_epoch_id;
    return Transaction(*this, RO_TRANSACTION, read_epoch_id, true, false);
}

timestamp_t Graph::compact(timestamp_t read_epoch_id)
{
    if (read_epoch_id == NO_TRANSACTION)
        read_epoch_id = epoch_id.load();
    for (auto id : read_epoch_table)
    {
        if (id != NO_TRANSACTION && id < read_epoch_id)
            read_epoch_id = id;
    }

    size_t recycled_block_size = 0;
    std::unordered_set<vertex_t> new_compact_table;

    for (vertex_t vid : compact_table.local())
    {
        if (!vertex_futexes[vid].try_lock_for(TIMEOUT))
        {
            new_compact_table.emplace(vid);
            continue;
        }

        bool need_future_compact = false;

        auto compact_n2o_blocks = [&](uintptr_t pointer) {
            auto block = block_manager.convert<N2OBlockHeader>(pointer);
            while (block)
            {
                // The first block before the minimal epoch
                // Blocks before that are garbage
                if (cmp_timestamp(block->get_creation_time_pointer(), read_epoch_id) < 0)
                {
                    std::vector<std::pair<uintptr_t, order_t>> pointers_to_recycle;

                    auto garbage_pointer = block->get_prev_pointer();
                    auto garbage_block = block_manager.convert<N2OBlockHeader>(garbage_pointer);
                    while (garbage_block)
                    {
                        pointers_to_recycle.emplace_back(garbage_pointer, garbage_block->get_order());
                        garbage_pointer = garbage_block->get_prev_pointer();
                        garbage_block = block_manager.convert<N2OBlockHeader>(garbage_pointer);
                    }

                    block->set_prev_pointer(block_manager.NULLPOINTER);
                    for (auto [pointer, order] : pointers_to_recycle)
                    {
                        recycled_block_size += 1ul << order;
                        block_manager.free(pointer, order);
                    }

                    break;
                }

                pointer = block->get_prev_pointer();
                block = block_manager.convert<VertexBlockHeader>(pointer);
                // The next block is garbage with larger epoch
                if (block)
                    need_future_compact = true;
            }
        };

        // Compact VertexBlock
        compact_n2o_blocks(vertex_ptrs[vid]);

        // Compact EdgeLabelBlock
        compact_n2o_blocks(edge_label_ptrs[vid]);

        // Compact EdgeBlock
        {
            auto edge_label_pointer = edge_label_ptrs[vid];
            auto edge_label_block = block_manager.convert<EdgeLabelBlockHeader>(edge_label_pointer);
            if (edge_label_block)
            {
                for (size_t i = 0; i < edge_label_block->get_num_entries(); i++)
                {
                    auto &label_entry = edge_label_block->get_entries()[i];
                    auto pointer = label_entry.get_pointer();
                    auto edge_block = block_manager.convert<EdgeBlockHeader>(pointer);
                    if (!edge_block)
                        continue;
                    compact_n2o_blocks(pointer);

                    size_t new_num_entries = 0;
                    size_t new_data_length = 0;

                    // Scan deleted edges
                    auto entries = edge_block->get_entries();
                    auto data = edge_block->get_data();
                    auto num_entries = edge_block->get_num_entries();
                    for (size_t i = 0; i < num_entries; i++)
                    {
                        entries--;
                        if (cmp_timestamp(entries->get_deletion_time_pointer(), read_epoch_id) > 0)
                        {
                            new_num_entries++;
                            new_data_length += entries->get_length();
                        }
                    }
                    entries = edge_block->get_entries(); // Reset cursor

                    if (new_num_entries == num_entries)
                        continue;

                    // Copy a new edge block
                    need_future_compact = true;

                    auto size = sizeof(EdgeBlockHeader) + new_num_entries * sizeof(EdgeEntry) + new_data_length;
                    auto order = size_to_order(size);

                    if (order > edge_block->BLOOM_FILTER_PORTION &&
                        size + (1ul << (order - edge_block->BLOOM_FILTER_PORTION)) >=
                            (1ul << edge_block->BLOOM_FILTER_THRESHOLD))
                    {
                        size += 1ul << (order - edge_block->BLOOM_FILTER_PORTION);
                    }
                    order = size_to_order(size);

                    auto new_pointer = block_manager.alloc(order);

                    auto new_edge_block = block_manager.convert<EdgeBlockHeader>(new_pointer);
                    new_edge_block->fill(order, vid, read_epoch_id, pointer, edge_block->get_committed_time());

                    auto bloom_filter = new_edge_block->get_bloom_filter();
                    for (size_t i = 0; i < num_entries; i++)
                    {
                        entries--;
                        if (cmp_timestamp(entries->get_deletion_time_pointer(), read_epoch_id) > 0)
                            new_edge_block->append(*entries, data, bloom_filter);
                        data += entries->get_length();
                    }

                    label_entry.set_pointer(new_pointer);

                    // printf("Compact %lu edges, %lu data\n",
                    // num_entries-new_num_entries,
                    // edge_block->get_data_length()-new_data_length);
                }
            }
        }

        if (need_future_compact)
            new_compact_table.emplace(vid);
        vertex_futexes[vid].unlock();
    }

    compact_table.local().swap(new_compact_table);

    // printf("Compact %lu bytes blocks\n", recycled_block_size);

    return read_epoch_id;
}

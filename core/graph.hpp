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
#include <unordered_set>

#include <tbb/concurrent_queue.h>
#include <tbb/enumerable_thread_specific.h>

#include "allocator.hpp"
#include "block_manager.hpp"
#include "commit_manager.hpp"
#include "futex.hpp"

namespace livegraph
{
    class EdgeIterator;
    class Transaction;

    class Graph
    {
    public:
        Graph(std::string block_path = "",
              std::string wal_path = "",
              size_t _max_block_size = 1ul << 40,
              vertex_t _max_vertex_id = 1ul << 40)
            : mutex(),
              epoch_id(0),
              transaction_id(0),
              vertex_id(0),
              read_epoch_table(NO_TRANSACTION),
              compact_table(),
              recycled_vertex_ids(),
              max_vertex_id(_max_vertex_id),
              array_allocator(),
              block_manager(block_path, _max_block_size),
              commit_manager(wal_path, epoch_id)
        {
            auto futex_allocater =
                std::allocator_traits<decltype(array_allocator)>::rebind_alloc<Futex>(array_allocator);
            vertex_futexes = futex_allocater.allocate(max_vertex_id);

            auto pointer_allocater =
                std::allocator_traits<decltype(array_allocator)>::rebind_alloc<uintptr_t>(array_allocator);
            vertex_ptrs = pointer_allocater.allocate(max_vertex_id);
            edge_label_ptrs = pointer_allocater.allocate(max_vertex_id);
        }

        Graph(const Graph &) = delete;

        Graph(Graph &&) = delete;

        ~Graph() noexcept
        {
            auto futex_allocater =
                std::allocator_traits<decltype(array_allocator)>::rebind_alloc<Futex>(array_allocator);
            futex_allocater.deallocate(vertex_futexes, max_vertex_id);

            auto pointer_allocater =
                std::allocator_traits<decltype(array_allocator)>::rebind_alloc<uintptr_t>(array_allocator);
            pointer_allocater.deallocate(vertex_ptrs, max_vertex_id);
            pointer_allocater.deallocate(edge_label_ptrs, max_vertex_id);
        }

        vertex_t get_max_vertex_id() const { return vertex_id; }

        timestamp_t compact(timestamp_t read_epoch_id = NO_TRANSACTION);

        Transaction begin_transaction();
        Transaction begin_read_only_transaction();
        Transaction begin_batch_loader();

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

        tbb::enumerable_thread_specific<timestamp_t> read_epoch_table;
        tbb::enumerable_thread_specific<std::unordered_set<vertex_t>> compact_table;

        tbb::concurrent_queue<vertex_t> recycled_vertex_ids;

        const vertex_t max_vertex_id;

        SparseArrayAllocator<void> array_allocator;
        BlockManager block_manager;
        CommitManager commit_manager;

        Futex *vertex_futexes;
        uintptr_t *vertex_ptrs;
        uintptr_t *edge_label_ptrs;

        constexpr static size_t COMPACTION_CYCLE = 1ul << 20;
        constexpr static timestamp_t ROLLBACK_TOMBSTONE = INT64_MAX;
        constexpr static timestamp_t NO_TRANSACTION = -1;
        constexpr static timestamp_t RO_TRANSACTION = ROLLBACK_TOMBSTONE - 1;
        constexpr static vertex_t VERTEX_TOMBSTONE = UINT64_MAX;
        constexpr static auto TIMEOUT = std::chrono::milliseconds(1);
        constexpr static size_t COMPACT_EDGE_BLOCK_THRESHOLD = 5; // at least compact 20% edges

        friend class EdgeIterator;
        friend class Transaction;
    };
} // namespace livegraph

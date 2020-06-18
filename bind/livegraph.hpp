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

#include <cstddef>
#include <cstdint>
#include <memory>

namespace livegraph
{
    class Graph;
    class Transaction;
    class EdgeIterator;
} // namespace livegraph

namespace lg
{
    using label_t = uint16_t;
    using vertex_t = uint64_t;
    using order_t = uint8_t;
    using timestamp_t = int64_t;

    class EdgeIterator;
    class Transaction;

    class Graph
    {
    public:
        Graph(std::string block_path = "",
              std::string wal_path = "",
              size_t max_block_size = 1ul << 40,
              vertex_t max_vertex_id = 1ul << 40);
        ~Graph();

        vertex_t get_max_vertex_id() const;

        timestamp_t compact(timestamp_t read_epoch_id = NO_TRANSACTION);

        Transaction begin_transaction();
        Transaction begin_read_only_transaction();
        Transaction begin_batch_loader();

    private:
        const std::unique_ptr<livegraph::Graph> graph;
        constexpr static timestamp_t NO_TRANSACTION = -1;
    };

    class Transaction
    {
    public:
        class RollbackExcept : public std::runtime_error
        {
        public:
            RollbackExcept(const std::string &what_arg) : std::runtime_error(what_arg) {}
            RollbackExcept(const char *what_arg) : std::runtime_error(what_arg) {}
        };

        Transaction(std::unique_ptr<livegraph::Transaction> _txn);
        ~Transaction();

        timestamp_t get_read_epoch_id() const;

        vertex_t new_vertex(bool use_recycled_vertex = false);
        void put_vertex(vertex_t vertex_id, std::string_view data);
        bool del_vertex(vertex_t vertex_id, bool recycle = false);

        void put_edge(vertex_t src, label_t label, vertex_t dst, std::string_view edge_data, bool force_insert = false);
        bool del_edge(vertex_t src, label_t label, vertex_t dst);

        std::string_view get_vertex(vertex_t vertex_id);
        std::string_view get_edge(vertex_t src, label_t label, vertex_t dst);
        EdgeIterator get_edges(vertex_t src, label_t label, bool reverse = false);

        timestamp_t commit(bool wait_visable = true);
        void abort();

    private:
        const std::unique_ptr<livegraph::Transaction> txn;
    };

    class EdgeIterator
    {
    public:
        EdgeIterator(std::unique_ptr<livegraph::EdgeIterator> _iter);
        ~EdgeIterator();

        bool valid() const;
        void next();
        vertex_t dst_id() const;
        std::string_view edge_data() const;

    private:
        const std::unique_ptr<livegraph::EdgeIterator> iter;
    };

} // namespace lg

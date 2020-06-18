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

#include "livegraph.hpp"
#include "core/livegraph.hpp"

using namespace lg;
namespace impl = livegraph;

Graph::Graph(std::string block_path, std::string wal_path, size_t max_block_size, vertex_t max_vertex_id)
    : graph(std::make_unique<impl::Graph>(block_path, wal_path, max_block_size, max_vertex_id))
{
}

Graph::~Graph() = default;

vertex_t Graph::get_max_vertex_id() const { return graph->get_max_vertex_id(); }

timestamp_t Graph::compact(timestamp_t read_epoch_id) { return graph->compact(read_epoch_id); }

Transaction Graph::begin_transaction() { return std::make_unique<impl::Transaction>(graph->begin_transaction()); }

Transaction Graph::begin_read_only_transaction()
{
    return std::make_unique<impl::Transaction>(graph->begin_read_only_transaction());
}

Transaction Graph::begin_batch_loader() { return std::make_unique<impl::Transaction>(graph->begin_batch_loader()); }

Transaction::Transaction(std::unique_ptr<livegraph::Transaction> _txn) : txn(std::move(_txn)) {}

Transaction::~Transaction() = default;

timestamp_t Transaction::get_read_epoch_id() const { return txn->get_read_epoch_id(); }

vertex_t Transaction::new_vertex(bool use_recycled_vertex) { return txn->new_vertex(use_recycled_vertex); }

void Transaction::put_vertex(vertex_t vertex_id, std::string_view data)
{
    try
    {
        txn->put_vertex(vertex_id, data);
    }
    catch (impl::Transaction::RollbackExcept e)
    {
        throw RollbackExcept(e.what());
    }
}

bool Transaction::del_vertex(vertex_t vertex_id, bool recycle)
{
    try
    {
        return txn->del_vertex(vertex_id, recycle);
    }
    catch (impl::Transaction::RollbackExcept e)
    {
        throw RollbackExcept(e.what());
    }
}

void Transaction::put_edge(vertex_t src, label_t label, vertex_t dst, std::string_view edge_data, bool force_insert)
{
    try
    {
        txn->put_edge(src, label, dst, edge_data, force_insert);
    }
    catch (impl::Transaction::RollbackExcept e)
    {
        throw RollbackExcept(e.what());
    }
}

bool Transaction::del_edge(vertex_t src, label_t label, vertex_t dst)
{
    try
    {
        return txn->del_edge(src, label, dst);
    }
    catch (impl::Transaction::RollbackExcept e)
    {
        throw RollbackExcept(e.what());
    }
}

std::string_view Transaction::get_vertex(vertex_t vertex_id) { return txn->get_vertex(vertex_id); }

std::string_view Transaction::get_edge(vertex_t src, label_t label, vertex_t dst)
{
    return txn->get_edge(src, label, dst);
}

EdgeIterator Transaction::get_edges(vertex_t src, label_t label, bool reverse)
{
    return std::make_unique<impl::EdgeIterator>(txn->get_edges(src, label, reverse));
}

timestamp_t Transaction::commit(bool wait_visable) { return txn->commit(wait_visable); }

void Transaction::abort() { txn->abort(); }

EdgeIterator::EdgeIterator(std::unique_ptr<livegraph::EdgeIterator> _iter) : iter(std::move(_iter)) {}

EdgeIterator::~EdgeIterator() = default;

bool EdgeIterator::valid() const { return iter->valid(); }

void EdgeIterator::next() { iter->next(); }

vertex_t EdgeIterator::dst_id() const { return iter->dst_id(); }

std::string_view EdgeIterator::edge_data() const { return iter->edge_data(); }

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

#include <doctest/doctest.h>

#include <cstdio>
#include <map>
#include <mutex>
#include <random>
#include <string>

#include <omp.h>

#include "bind/livegraph.hpp"
#include "core/livegraph.hpp"

TYPE_TO_STRING(livegraph::Graph);
TYPE_TO_STRING(lg::Graph);

TEST_CASE_TEMPLATE("testing the Graph", GraphType, livegraph::Graph, lg::Graph)
{
    using namespace livegraph;
    const vertex_t max_vertices = 256;
    const label_t max_label = 16;
    const label_t max_init_label = 4;
    const size_t num_edges_per_vertex = max_vertices;
    const size_t max_data_length = 64;
    const size_t num_transactions = 1024;
    const size_t ops_per_txn = 4;
    std::vector<std::map<timestamp_t, std::string>> vertices(max_vertices);
    std::vector<std::map<std::tuple<label_t, vertex_t, timestamp_t>, std::string>> edges(max_vertices);
    std::vector<std::mutex> mutexs(max_vertices);

    {
        GraphType graph("./block.mmap");

        #pragma omp parallel
        {
            std::mt19937 rand(omp_get_thread_num());
            auto gendata = [&]() {
                auto length = rand() % max_data_length + 1;
                std::string data;
                for (size_t i = 0; i < length; i++)
                {
                    data += 'a' + rand() % 26;
                }
                return data;
            };

            auto check_vertex = [&](auto &txn, vertex_t src) {
                auto read_epoch_id = txn.get_read_epoch_id();
                std::lock_guard<std::mutex> lock(mutexs[src]);
                {
                    auto iter = --vertices[src].upper_bound(read_epoch_id);
                    CHECK(iter->first <= read_epoch_id);
                    CHECK(iter->second == txn.get_vertex(src));
                }
                for (label_t label = 0; label < max_label; label++)
                {
                    auto edge_iter = txn.get_edges(src, label);
                    size_t num_edges = 0, ref_num_edges = 0;
                    while (edge_iter.valid())
                    {
                        num_edges++;
                        auto iter = --edges[src].upper_bound(std::make_tuple(label, edge_iter.dst_id(), read_epoch_id));
                        auto [ref_label, ref_dst, ref_timestamp] = iter->first;
                        CHECK(ref_label == label);
                        CHECK(ref_dst == edge_iter.dst_id());
                        CHECK(ref_timestamp <= read_epoch_id);
                        CHECK(iter->second == edge_iter.edge_data());
                        edge_iter.next();
                    }

                    auto iter = edges[src].lower_bound(std::make_tuple(label, 0, 0));
                    while (iter != edges[src].end())
                    {
                        auto [ref_label, ref_dst, ref_timestamp] = iter->first;
                        if (ref_label != label)
                            break;
                        auto next_iter = iter;
                        ++next_iter;
                        auto [next_label, next_dst, next_timestamp] = next_iter->first;
                        if (next_iter == edges[src].end() || next_label != label || next_dst != ref_dst ||
                            next_timestamp > read_epoch_id)
                        {
                            if (ref_timestamp <= read_epoch_id && !iter->second.empty())
                                ++ref_num_edges;
                        }
                        ++iter;
                    }

                    CHECK(num_edges == ref_num_edges);
                }
            };
            auto check_graph = [&](auto &txn) {
                for (size_t src = 0; src < max_vertices; src++)
                {
                    check_vertex(txn, src);
                }
            };

            {
                auto txn = graph.begin_batch_loader();
                #pragma omp for
                for (size_t i = 0; i < max_vertices; i++)
                {
                    auto src = txn.new_vertex();
                    auto data = gendata();
                    txn.put_vertex(src, data);
                    vertices[src][0] = data;
                }
                #pragma omp for
                for (size_t i = 0; i < max_vertices; i++)
                {
                    auto src = i;
                    for (size_t j = 0; j < num_edges_per_vertex; j++)
                    {
                        auto label = rand() % max_init_label;
                        auto dst = rand() % max_vertices;
                        auto data = gendata();
                        txn.put_edge(src, label, dst, data);
                        edges[src][std::make_tuple(label, dst, 0)] = data;
                    }
                }
                #pragma omp for
                for (size_t i = 0; i < max_vertices; i++)
                {
                    check_vertex(txn, i);
                }
            }

            for (size_t i = 0; i < num_transactions; i++)
            {
                auto type = rand() % 1000;
                if (type < 1)
                {
                    auto txn = graph.begin_read_only_transaction();
                    check_graph(txn);
                    MESSAGE("Successfully checked the graph at " << txn.get_read_epoch_id());
                }
                else if (type < 2)
                {
                    auto read_epoch_id = graph.compact();
                    MESSAGE("Successfully compacted the graph at " << read_epoch_id);
                }
                else if (type < 100)
                {
                    auto txn = graph.begin_read_only_transaction();
                    auto src = rand() % max_vertices;
                    check_vertex(txn, src);
                }
                else
                {
                    while (true)
                    {
                        std::map<vertex_t, std::lock_guard<std::mutex>> lock;
                        std::map<vertex_t, std::string> local_vertices;
                        std::map<std::tuple<vertex_t, label_t, vertex_t>, std::string> local_edges;
                        auto txn = graph.begin_transaction();
                        try
                        {
                            for (size_t j = 0; j < ops_per_txn; j++)
                            {
                                auto src = rand() % max_vertices;
                                auto type = rand() % 100;
                                if (type < 1)
                                {
                                    txn.abort();
                                    throw typename decltype(txn)::RollbackExcept("Abort.");
                                }
                                if (type < 10)
                                {
                                    txn.del_vertex(src);
                                    CHECK(txn.get_vertex(src) == "");
                                    local_vertices[src] = "";
                                }
                                else if (type < 20)
                                {
                                    auto label = (type < 30) ? (rand() % max_label) : (rand() % max_init_label);
                                    auto dst = rand() % max_vertices;
                                    txn.del_edge(src, label, dst);
                                    CHECK(txn.get_edge(src, label, dst) == "");
                                    local_edges[std::make_tuple(src, label, dst)] = "";
                                }
                                else if (type < 60)
                                {
                                    auto data = gendata();
                                    txn.put_vertex(src, data);
                                    CHECK(txn.get_vertex(src) == data);
                                    local_vertices[src] = data;
                                }
                                else
                                {
                                    auto label = (type < 70) ? (rand() % max_label) : (rand() % max_init_label);
                                    auto dst = rand() % max_vertices;
                                    auto data = gendata();
                                    txn.put_edge(src, label, dst, data);
                                    CHECK(txn.get_edge(src, label, dst) == data);
                                    local_edges[std::make_tuple(src, label, dst)] = data;
                                }
                                if (lock.find(src) == lock.end())
                                    lock.emplace(src, mutexs[src]);
                            }

                            for (auto [src, data] : local_vertices)
                            {
                                CHECK(txn.get_vertex(src) == data);
                            }
                            for (auto [edge, data] : local_edges)
                            {
                                auto [src, label, dst] = edge;
                                CHECK(txn.get_edge(src, label, dst) == data);
                            }

                            auto write_epoch_id = txn.commit();

                            auto read_txn = graph.begin_read_only_transaction();
                            for (auto [src, data] : local_vertices)
                            {
                                vertices[src][write_epoch_id] = data;
                                CHECK(read_txn.get_vertex(src) == data);
                            }
                            for (auto [edge, data] : local_edges)
                            {
                                auto [src, label, dst] = edge;
                                edges[src][std::make_tuple(label, dst, write_epoch_id)] = data;
                                if (read_txn.get_edge(src, label, dst) != data)
                                {
                                    read_txn.get_edge(src, label, dst);
                                }
                                CHECK(read_txn.get_edge(src, label, dst) == data);
                            }
                        }
                        catch (typename decltype(txn)::RollbackExcept &e)
                        {
                            continue;
                        }
                        break;
                    }
                }
            }
        }
    }

    CHECK(std::remove("./block.mmap") == 0);
}

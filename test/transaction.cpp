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

#include <string>
#include <vector>

#include "core/livegraph.hpp"

using namespace livegraph;

TEST_CASE("testing the Transaction: new_vertex")
{
    Graph graph;

    {
        auto txn = graph.begin_transaction();
        CHECK(txn.new_vertex() == 0);
        CHECK(txn.new_vertex() == 1);
        CHECK(txn.new_vertex() == 2);
    }
    {
        auto txn = graph.begin_transaction();
        CHECK(txn.new_vertex() == 3);
        CHECK(txn.new_vertex() == 4);
        CHECK(txn.new_vertex() == 5);
    }
    {
        auto txn = graph.begin_transaction();
        CHECK(txn.new_vertex(true) == 0);
        CHECK(txn.new_vertex(true) == 1);
        CHECK(txn.new_vertex(true) == 2);
        txn.abort();
    }
    {
        auto txn = graph.begin_transaction();
        CHECK(txn.new_vertex(true) == 3);
        CHECK(txn.new_vertex(true) == 4);
        CHECK(txn.new_vertex(true) == 5);
        txn.commit();
    }
    {
        auto txn = graph.begin_transaction();
        CHECK(txn.new_vertex(true) == 0);
        CHECK(txn.new_vertex(true) == 1);
        CHECK(txn.new_vertex(true) == 2);
        CHECK(txn.new_vertex(true) == 6);
        CHECK(txn.new_vertex(true) == 7);
        CHECK(txn.new_vertex(true) == 8);
    }
    {
        auto txn1 = graph.begin_transaction();
        auto txn2 = graph.begin_transaction();
        CHECK(txn1.new_vertex(true) == 0);
        CHECK(txn2.new_vertex(true) == 1);
        CHECK(txn1.new_vertex(true) == 2);
        CHECK(txn2.new_vertex(true) == 6);
        CHECK(txn1.new_vertex(true) == 7);
        CHECK(txn2.new_vertex(true) == 8);
        txn1.commit();
    }
    {
        auto txn = graph.begin_batch_loader();
        CHECK(txn.new_vertex(true) == 1);
        CHECK(txn.new_vertex(true) == 6);
        CHECK(txn.new_vertex(true) == 8);
        CHECK(txn.new_vertex(true) == 9);
        CHECK(txn.new_vertex() == 10);
    }
    {
        auto txn = graph.begin_read_only_transaction();
        CHECK_THROWS_AS(txn.new_vertex(), std::invalid_argument);
    }
    {
        auto txn = graph.begin_transaction();
        CHECK(txn.new_vertex(true) == 11);
    }
}

TEST_CASE("testing the Transaction: put/get/del_vertex")
{
    Graph graph;

    {
        auto txn = graph.begin_transaction();
        CHECK(txn.new_vertex() == 0);
        CHECK(txn.new_vertex() == 1);
        CHECK(txn.new_vertex() == 2);
        txn.commit();
    }
    {
        auto txn = graph.begin_transaction();
        CHECK(txn.get_vertex(0) == "");
        CHECK(txn.get_vertex(1) == "");
        CHECK(txn.get_vertex(2) == "");
    }
    {
        auto txn1 = graph.begin_transaction();
        txn1.put_vertex(0, "aaaa");
        txn1.put_vertex(1, "bbbb");
        txn1.put_vertex(2, "cccc");
        CHECK(txn1.get_vertex(0) == "aaaa");
        CHECK(txn1.get_vertex(1) == "bbbb");
        CHECK(txn1.get_vertex(2) == "cccc");
        auto txn2 = graph.begin_transaction();
        CHECK(txn2.get_vertex(0) == "");
        CHECK(txn2.get_vertex(1) == "");
        CHECK(txn2.get_vertex(2) == "");
    }
    {
        auto txn = graph.begin_transaction();
        CHECK(txn.get_vertex(0) == "");
        CHECK(txn.get_vertex(1) == "");
        CHECK(txn.get_vertex(2) == "");
    }
    {
        auto txn = graph.begin_transaction();
        txn.put_vertex(0, "aaaa");
        txn.put_vertex(1, "bbbb");
        txn.put_vertex(2, "cccc");
        CHECK(txn.get_vertex(0) == "aaaa");
        CHECK(txn.get_vertex(1) == "bbbb");
        CHECK(txn.get_vertex(2) == "cccc");
        auto txn2 = graph.begin_transaction();
        CHECK(txn2.get_vertex(0) == "");
        CHECK(txn2.get_vertex(1) == "");
        CHECK(txn2.get_vertex(2) == "");
        txn.commit();
    }
    {
        auto txn = graph.begin_transaction();
        CHECK(txn.get_vertex(0) == "aaaa");
        CHECK(txn.get_vertex(1) == "bbbb");
        CHECK(txn.get_vertex(2) == "cccc");
    }

    {
        auto txn_b = graph.begin_batch_loader();
        txn_b.put_vertex(0, "AAAA");
        txn_b.put_vertex(1, "BBBB");
        txn_b.put_vertex(2, "CCCC");
    }
    {
        auto txn = graph.begin_transaction();
        CHECK(txn.get_vertex(0) == "AAAA");
        CHECK(txn.get_vertex(1) == "BBBB");
        CHECK(txn.get_vertex(2) == "CCCC");
    }

    {
        auto txn = graph.begin_transaction();
        CHECK(txn.del_vertex(0));
        CHECK(txn.get_vertex(0) == "");
        CHECK(!txn.del_vertex(0));
        txn.put_vertex(0, "aaa");
        CHECK(txn.get_vertex(0) == "aaa");
        CHECK(txn.del_vertex(1));
        CHECK(txn.get_vertex(1) == "");
        CHECK(txn.del_vertex(2));
        CHECK(txn.get_vertex(2) == "");
        txn.put_vertex(0, "aaaa");
        txn.put_vertex(1, "bbbb");
        txn.put_vertex(2, "cccc");
        CHECK(txn.get_vertex(0) == "aaaa");
        CHECK(txn.get_vertex(1) == "bbbb");
        CHECK(txn.get_vertex(2) == "cccc");
        auto txn2 = graph.begin_transaction();
        CHECK(txn2.get_vertex(0) == "AAAA");
        CHECK(txn2.get_vertex(1) == "BBBB");
        CHECK(txn2.get_vertex(2) == "CCCC");
        txn.commit();
        CHECK(txn2.get_vertex(0) == "AAAA");
        CHECK(txn2.get_vertex(1) == "BBBB");
        CHECK(txn2.get_vertex(2) == "CCCC");
    }
    {
        auto txn = graph.begin_transaction();
        CHECK(txn.get_vertex(0) == "aaaa");
        CHECK(txn.get_vertex(1) == "bbbb");
        CHECK(txn.get_vertex(2) == "cccc");
    }

    {
        auto txn1 = graph.begin_transaction();
        CHECK(txn1.del_vertex(0, true));
        auto txn2 = graph.begin_transaction();
        CHECK(txn2.new_vertex(true) == 3);
        txn1.commit();
        CHECK(txn2.new_vertex(true) == 0);
        CHECK(!txn2.del_vertex(3));
        CHECK(txn2.new_vertex(true) == 4);
        CHECK(!txn2.del_vertex(3, true));
        txn2.commit();
        auto txn3 = graph.begin_transaction();
        CHECK(txn3.new_vertex(true) == 3);
        CHECK(!txn3.del_vertex(3, true));
        CHECK(txn3.new_vertex(true) == 3);
        txn3.abort();
        auto txn4 = graph.begin_transaction();
        CHECK(txn4.new_vertex(true) == 3);
    }

    { // Deadlock
        auto txn1 = graph.begin_transaction();
        auto txn2 = graph.begin_transaction();
        txn1.put_vertex(0, "AAAA");
        CHECK_THROWS_AS(txn2.put_vertex(0, "aaaa"), Transaction::RollbackExcept);
        txn2.del_vertex(1);
        CHECK_THROWS_AS(txn1.del_vertex(1), Transaction::RollbackExcept);
    }

    { // Write-write conflict
        auto txn1 = graph.begin_transaction();
        auto txn2 = graph.begin_transaction();
        txn1.put_vertex(0, "AAAA");
        txn1.del_vertex(1);
        txn1.commit();
        CHECK_THROWS_AS(txn2.put_vertex(0, "aaaa"), Transaction::RollbackExcept);
        CHECK_THROWS_AS(txn2.del_vertex(1), Transaction::RollbackExcept);
    }
    {
        auto txn = graph.begin_read_only_transaction();
        CHECK_THROWS_AS(txn.put_vertex(0, "aaaa"), std::invalid_argument);
        CHECK_THROWS_AS(txn.del_vertex(0, "aaaa"), std::invalid_argument);
        CHECK(txn.get_vertex(0) == "AAAA");
    }
    {
        auto txn = graph.begin_transaction();
        CHECK_THROWS_AS(txn.put_vertex(10, ""), std::invalid_argument);
        CHECK_THROWS_AS(txn.del_vertex(10), std::invalid_argument);
        txn.abort();
        CHECK_THROWS_AS(txn.put_vertex(0, ""), std::invalid_argument);
        CHECK_THROWS_AS(txn.get_vertex(0), std::invalid_argument);
        CHECK_THROWS_AS(txn.del_vertex(0), std::invalid_argument);
    }
}

TEST_CASE("testing the Transaction: put/get/del_edge")
{
    Graph graph;

    const vertex_t vertices = 128, src_vertices = 4;
    const label_t labels = 16;

    auto loop = [&](auto f) {
        for (label_t label = 0; label < labels; label++)
            for (vertex_t i = 0; i < src_vertices; i++)
                for (vertex_t j = 0; j < vertices; j++)
                    f(i, label, j);
    };

    {
        auto txn = graph.begin_transaction();
        for (vertex_t i = 0; i < vertices; i++)
            CHECK(txn.new_vertex() == i);
        txn.commit();
    }
    {
        auto txn1 = graph.begin_transaction();
        loop([&](vertex_t i, label_t label, vertex_t j) {
            std::string data = std::to_string(i) + "-" + std::to_string(label) + "->" + std::to_string(j);
            txn1.put_edge(i, label, j, data);
        });
        loop([&](vertex_t i, label_t label, vertex_t j) {
            std::string data = std::to_string(i) + "-" + std::to_string(label) + "->" + std::to_string(j);
            CHECK(txn1.get_edge(i, label, j) == data);
        });
        for (label_t label = 0; label < labels; label++)
        {
            for (vertex_t i = 0; i < src_vertices; i++)
            {
                auto riter = txn1.get_edges(i, label, true);
                for (vertex_t j = 0; j < vertices; j++)
                {
                    std::string data = std::to_string(i) + "-" + std::to_string(label) + "->" + std::to_string(j);
                    CHECK(riter.dst_id() == j);
                    CHECK(riter.edge_data() == data);
                    riter.next();
                }
                CHECK(!riter.valid());
                CHECK(riter.dst_id() == (vertex_t)-1);
                CHECK(riter.edge_data() == "");
                auto iter = txn1.get_edges(i, label);
                for (vertex_t j = 0; j < vertices; j++)
                {
                    std::string data =
                        std::to_string(i) + "-" + std::to_string(label) + "->" + std::to_string(vertices - j - 1);
                    CHECK(iter.dst_id() == vertices - j - 1);
                    CHECK(iter.edge_data() == data);
                    iter.next();
                }
                CHECK(!iter.valid());
                CHECK(iter.dst_id() == (vertex_t)-1);
                CHECK(iter.edge_data() == "");
            }
        }
        auto txn2 = graph.begin_transaction();
        loop([&](vertex_t i, label_t label, vertex_t j) {
            std::string data = std::to_string(i) + "-" + std::to_string(label) + "->" + std::to_string(j);
            CHECK(txn2.get_edge(i, label, j) == "");
        });
    }
    {
        auto txn = graph.begin_transaction();
        loop([&](vertex_t i, label_t label, vertex_t j) { CHECK(txn.get_edge(i, label, j) == ""); });
        for (label_t label = 0; label < labels; label++)
        {
            for (vertex_t i = 0; i < src_vertices; i++)
            {
                auto riter = txn.get_edges(i, label, true);
                CHECK(!riter.valid());
                auto iter = txn.get_edges(i, label);
                CHECK(!iter.valid());
            }
        }
    }
    {
        auto txn1 = graph.begin_transaction();
        loop([&](vertex_t i, label_t label, vertex_t j) {
            std::string data = std::to_string(i) + "." + std::to_string(label) + "->" + std::to_string(j);
            txn1.put_edge(i, label, j, data);
        });
        loop([&](vertex_t i, label_t label, vertex_t j) {
            std::string data = std::to_string(i) + "." + std::to_string(label) + "->" + std::to_string(j);
            CHECK(txn1.get_edge(i, label, j) == data);
        });
        auto txn2 = graph.begin_transaction();
        loop([&](vertex_t i, label_t label, vertex_t j) { CHECK(txn2.get_edge(i, label, j) == ""); });
        txn1.commit();
    }
    {
        auto txn = graph.begin_transaction();
        loop([&](vertex_t i, label_t label, vertex_t j) {
            std::string data = std::to_string(i) + "." + std::to_string(label) + "->" + std::to_string(j);
            CHECK(txn.get_edge(i, label, j) == data);
        });
        for (label_t label = 0; label < labels; label++)
        {
            for (vertex_t i = 0; i < src_vertices; i++)
            {
                auto riter = txn.get_edges(i, label, true);
                for (vertex_t j = 0; j < vertices; j++)
                {
                    std::string data = std::to_string(i) + "." + std::to_string(label) + "->" + std::to_string(j);
                    CHECK(riter.dst_id() == j);
                    CHECK(riter.edge_data() == data);
                    riter.next();
                }
                CHECK(!riter.valid());
                CHECK(riter.dst_id() == (vertex_t)-1);
                CHECK(riter.edge_data() == "");
                auto iter = txn.get_edges(i, label);
                for (vertex_t j = 0; j < vertices; j++)
                {
                    std::string data =
                        std::to_string(i) + "." + std::to_string(label) + "->" + std::to_string(vertices - j - 1);
                    CHECK(iter.dst_id() == vertices - j - 1);
                    CHECK(iter.edge_data() == data);
                    iter.next();
                }
                CHECK(!iter.valid());
                CHECK(iter.dst_id() == (vertex_t)-1);
                CHECK(iter.edge_data() == "");
            }
        }
    }

    {
        auto txn = graph.begin_batch_loader();
        loop([&](vertex_t i, label_t label, vertex_t j) {
            std::string data = std::to_string(i) + ".." + std::to_string(label) + "->" + std::to_string(j);
            txn.put_edge(i, label, j, data);
        });
    }
    {
        auto txn = graph.begin_transaction();
        loop([&](vertex_t i, label_t label, vertex_t j) {
            std::string data = std::to_string(i) + ".." + std::to_string(label) + "->" + std::to_string(j);
            CHECK(txn.get_edge(i, label, j) == data);
        });
    }

    {
        auto txn = graph.begin_transaction();
        loop([&](vertex_t i, label_t label, vertex_t j) { CHECK(txn.del_edge(i, label, j)); });
        loop([&](vertex_t i, label_t label, vertex_t j) { CHECK(txn.get_edge(i, label, j) == ""); });
        loop([&](vertex_t i, label_t label, vertex_t j) { CHECK(!txn.del_edge(i, label, j)); });
        loop([&](vertex_t i, label_t label, vertex_t j) {
            std::string data = std::to_string(i) + "." + std::to_string(label) + "->" + std::to_string(j);
            txn.put_edge(i, label, j, data);
        });
        loop([&](vertex_t i, label_t label, vertex_t j) {
            std::string data = std::to_string(i) + "." + std::to_string(label) + "->" + std::to_string(j);
            CHECK(txn.get_edge(i, label, j) == data);
        });
        loop([&](vertex_t i, label_t label, vertex_t j) { CHECK(txn.del_edge(i, label, j)); });
        loop([&](vertex_t i, label_t label, vertex_t j) { CHECK(txn.get_edge(i, label, j) == ""); });
        loop([&](vertex_t i, label_t label, vertex_t j) {
            std::string data = std::to_string(i) + "-" + std::to_string(label) + "->" + std::to_string(j);
            txn.put_edge(i, label, j, data);
        });
        loop([&](vertex_t i, label_t label, vertex_t j) {
            std::string data = std::to_string(i) + "-" + std::to_string(label) + "->" + std::to_string(j);
            CHECK(txn.get_edge(i, label, j) == data);
        });
        auto txn2 = graph.begin_transaction();
        loop([&](vertex_t i, label_t label, vertex_t j) {
            std::string data = std::to_string(i) + ".." + std::to_string(label) + "->" + std::to_string(j);
            CHECK(txn2.get_edge(i, label, j) == data);
        });
        txn.commit();
        loop([&](vertex_t i, label_t label, vertex_t j) {
            std::string data = std::to_string(i) + ".." + std::to_string(label) + "->" + std::to_string(j);
            CHECK(txn2.get_edge(i, label, j) == data);
        });
    }
    {
        auto txn = graph.begin_transaction();
        loop([&](vertex_t i, label_t label, vertex_t j) {
            std::string data = std::to_string(i) + "-" + std::to_string(label) + "->" + std::to_string(j);
            CHECK(txn.get_edge(i, label, j) == data);
        });
    }

    { // Deadlock
        auto txn1 = graph.begin_transaction();
        auto txn2 = graph.begin_transaction();
        txn1.put_edge(0, 0, 1, "AAAA");
        CHECK_THROWS_AS(txn2.put_edge(0, 1, 2, "aaaa"), Transaction::RollbackExcept);
        txn2.del_edge(1, 0, 3);
        CHECK_THROWS_AS(txn1.del_edge(1, 1, 4), Transaction::RollbackExcept);
    }

    { // Write-write conflict
        auto txn1 = graph.begin_transaction();
        auto txn2 = graph.begin_transaction();
        txn1.put_edge(0, 0, 1, "AAAA");
        txn1.del_edge(1, 0, 3);
        txn1.commit();
        txn2.put_edge(0, 1, 2, "aaaa");
        CHECK_THROWS_AS(txn2.put_edge(0, 0, 3, "aaaa"), Transaction::RollbackExcept);
        CHECK(txn2.del_edge(1, 1, 4));
        CHECK_THROWS_AS(txn2.del_edge(1, 0, 4), Transaction::RollbackExcept);
    }
    {
        auto txn = graph.begin_read_only_transaction();
        CHECK_THROWS_AS(txn.put_edge(0, 0, 1, "aaaa"), std::invalid_argument);
        CHECK_THROWS_AS(txn.del_edge(0, 0, 1), std::invalid_argument);
        CHECK(txn.get_edge(0, 0, 1) == "AAAA");
    }
    {
        auto txn = graph.begin_transaction();
        CHECK_THROWS_AS(txn.put_edge(0, 0, vertices, "aaaa"), std::invalid_argument);
        CHECK_THROWS_AS(txn.put_edge(vertices, 0, 0, "aaaa"), std::invalid_argument);
        CHECK_THROWS_AS(txn.del_edge(0, 0, vertices), std::invalid_argument);
        CHECK_THROWS_AS(txn.del_edge(vertices, 0, 0), std::invalid_argument);
        txn.abort();
        CHECK_THROWS_AS(txn.put_edge(0, 0, 1, "aaaa"), std::invalid_argument);
        CHECK_THROWS_AS(txn.del_edge(0, 0, 1), std::invalid_argument);
        CHECK_THROWS_AS(txn.get_edge(0, 0, 1), std::invalid_argument);
    }
}

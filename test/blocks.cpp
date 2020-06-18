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

#include "core/blocks.hpp"

using namespace livegraph;

TEST_CASE("testing the BlockHeader")
{
    BlockHeader header;

    header.set_order(5);
    header.set_type(BlockHeader::Type::SPECIAL);
    CHECK(header.get_type() == BlockHeader::Type::SPECIAL);
    CHECK(header.get_order() == 5);
    CHECK(header.get_block_size() == 1ul << 5);
}

TEST_CASE("testing the N2OBlockHeader")
{
    N2OBlockHeader header;

    header.set_order(5);
    header.set_type(N2OBlockHeader::Type::SPECIAL);

    header.set_vertex_id(233);
    CHECK(header.get_vertex_id() == 233);
    header.set_vertex_id(0xffffffff);
    CHECK(header.get_vertex_id() == 0xffffffff);
    header.set_vertex_id(0x1234abcdeful);
    CHECK(header.get_vertex_id() == 0x1234abcdef);

    header.set_creation_time(0xaabbccddeefful);
    CHECK(header.get_creation_time() == 0xaabbccddeeffl);
    auto creation_time_pointer = header.get_creation_time_pointer();
    CHECK(*creation_time_pointer == 0xaabbccddeeffl);
    *creation_time_pointer = 0xffeeddccbbaal;
    CHECK(header.get_creation_time() == 0xffeeddccbbaal);

    header.set_prev_pointer(0x1122334455667788ul);
    CHECK(header.get_prev_pointer() == 0x1122334455667788ul);

    CHECK(header.get_type() == N2OBlockHeader::Type::SPECIAL);
    CHECK(header.get_order() == 5);
    CHECK(header.get_block_size() == 1ul << 5);
    CHECK(header.get_vertex_id() == 0x1234abcdef);
    CHECK(header.get_creation_time() == 0xffeeddccbbaal);
    CHECK(header.get_prev_pointer() == 0x1122334455667788ul);
}

TEST_CASE("testing the VertexBlockHeader")
{
    const order_t log_size = 6; // size = 64 bytes, left 24 bytes
    auto buf = malloc(1ul << log_size);
    VertexBlockHeader &header = *(VertexBlockHeader *)buf;

    header.set_order(log_size);
    header.set_type(VertexBlockHeader::Type::VERTEX);

    header.set_vertex_id(233);
    CHECK(header.get_vertex_id() == 233);
    header.set_vertex_id(0xffffffff);
    CHECK(header.get_vertex_id() == 0xffffffff);
    header.set_vertex_id(0x1234abcdeful);
    CHECK(header.get_vertex_id() == 0x1234abcdef);

    header.set_creation_time(0xaabbccddeefful);
    CHECK(header.get_creation_time() == 0xaabbccddeeffl);
    auto creation_time_pointer = header.get_creation_time_pointer();
    CHECK(*creation_time_pointer == 0xaabbccddeeffl);
    *creation_time_pointer = 0xffeeddccbbaal;
    CHECK(header.get_creation_time() == 0xffeeddccbbaal);

    header.set_prev_pointer(0x1122334455667788ul);
    CHECK(header.get_prev_pointer() == 0x1122334455667788ul);

    const char *str = "Hello World!";
    CHECK(header.set_data(str, strlen(str)));
    CHECK(header.get_length() == strlen(str));
    CHECK(std::string(header.get_data(), header.get_length()) == str);
    CHECK(!header.set_data(str, (1ul << log_size) - sizeof(header) + 1));
    CHECK(std::string(header.get_data(), header.get_length()) == str);

    CHECK(header.get_type() == VertexBlockHeader::Type::VERTEX);
    CHECK(header.get_order() == log_size);
    CHECK(header.get_block_size() == 1ul << log_size);
    CHECK(header.get_vertex_id() == 0x1234abcdef);
    CHECK(header.get_creation_time() == 0xffeeddccbbaal);
    CHECK(header.get_prev_pointer() == 0x1122334455667788ul);
    CHECK(header.get_length() == strlen(str));
    CHECK(std::string(header.get_data(), header.get_length()) == str);

    free(buf);
}

TEST_CASE("testing the EdgeLabelEntry")
{
    EdgeLabelEntry entry;

    entry.set_label(1234);
    entry.set_pointer(0x123123321321ul);
    CHECK(entry.get_label() == 1234);
    CHECK(entry.get_pointer() == 0x123123321321ul);
}

TEST_CASE("testing the EdgeLabelBlockHeader")
{
    const order_t log_size = 6; // size = 64 bytes, left 32 bytes (2 entry)
    auto buf = malloc(1ul << log_size);
    EdgeLabelBlockHeader &header = *(EdgeLabelBlockHeader *)buf;

    header.set_order(log_size);
    header.set_type(EdgeLabelBlockHeader::Type::EDGE_LABEL);

    header.set_vertex_id(233);
    CHECK(header.get_vertex_id() == 233);
    header.set_vertex_id(0xffffffff);
    CHECK(header.get_vertex_id() == 0xffffffff);
    header.set_vertex_id(0x1234abcdeful);
    CHECK(header.get_vertex_id() == 0x1234abcdef);

    header.set_creation_time(0xaabbccddeefful);
    CHECK(header.get_creation_time() == 0xaabbccddeeffl);
    auto creation_time_pointer = header.get_creation_time_pointer();
    CHECK(*creation_time_pointer == 0xaabbccddeeffl);
    *creation_time_pointer = 0xffeeddccbbaal;
    CHECK(header.get_creation_time() == 0xffeeddccbbaal);

    header.set_prev_pointer(0x1122334455667788ul);
    CHECK(header.get_prev_pointer() == 0x1122334455667788ul);

    header.clear();
    EdgeLabelEntry entry;
    entry.set_label(1111);
    entry.set_pointer(0x1111111111111111ul);
    CHECK(header.append(entry));
    entry.set_label(2222);
    entry.set_pointer(0x2222222222222222ul);
    CHECK(header.append(entry));
    entry.set_label(3333);
    entry.set_pointer(0x3333333333333333ul);
    CHECK(!header.append(entry));
    CHECK(header.get_num_entries() == 2);
    CHECK(header.get_entries()[0].get_label() == 1111);
    CHECK(header.get_entries()[0].get_pointer() == 0x1111111111111111ul);
    CHECK(header.get_entries()[1].get_label() == 2222);
    CHECK(header.get_entries()[1].get_pointer() == 0x2222222222222222ul);

    CHECK(header.get_type() == EdgeLabelBlockHeader::Type::EDGE_LABEL);
    CHECK(header.get_order() == log_size);
    CHECK(header.get_block_size() == 1ul << log_size);
    CHECK(header.get_vertex_id() == 0x1234abcdef);
    CHECK(header.get_creation_time() == 0xffeeddccbbaal);
    CHECK(header.get_prev_pointer() == 0x1122334455667788ul);

    free(buf);
}

TEST_CASE("testing the EdgeEntry")
{
    EdgeEntry entry;
    entry.set_dst(233);
    CHECK(entry.get_dst() == 233);
    entry.set_dst(0xffffffff);
    CHECK(entry.get_dst() == 0xffffffff);
    entry.set_dst(0x1234abcdeful);
    CHECK(entry.get_dst() == 0x1234abcdef);

    entry.set_creation_time(0xaabbccddeefful);
    CHECK(entry.get_creation_time() == 0xaabbccddeeffl);
    auto creation_time_pointer = entry.get_creation_time_pointer();
    CHECK(*creation_time_pointer == 0xaabbccddeeffl);
    *creation_time_pointer = 0xffeeddccbbaal;
    CHECK(entry.get_creation_time() == 0xffeeddccbbaal);

    entry.set_deletion_time(0xabcdefabcdeful);
    CHECK(entry.get_deletion_time() == 0xabcdefabcdeful);
    auto deletion_time_pointer = entry.get_deletion_time_pointer();
    CHECK(*deletion_time_pointer == 0xabcdefabcdeful);
    *deletion_time_pointer = 0x1234432112344321l;
    CHECK(entry.get_deletion_time() == 0x1234432112344321l);

    entry.set_length(0x1122ul);
    CHECK(entry.get_length() == 0x1122ul);

    CHECK(entry.get_dst() == 0x1234abcdef);
    CHECK(entry.get_creation_time() == 0xffeeddccbbaal);
    CHECK(entry.get_deletion_time() == 0x1234432112344321l);
    CHECK(entry.get_length() == 0x1122ul);
}

TEST_CASE("testing the EdgeLabelBlockHeader")
{
    const order_t log_size = 6; // size = 64 bytes, left 32 bytes
    auto buf = malloc(1ul << log_size);
    EdgeLabelBlockHeader &header = *(EdgeLabelBlockHeader *)buf;

    header.set_order(log_size);
    header.set_type(EdgeLabelBlockHeader::Type::EDGE_LABEL);

    header.set_vertex_id(233);
    CHECK(header.get_vertex_id() == 233);
    header.set_vertex_id(0xffffffff);
    CHECK(header.get_vertex_id() == 0xffffffff);
    header.set_vertex_id(0x1234abcdeful);
    CHECK(header.get_vertex_id() == 0x1234abcdef);

    header.set_creation_time(0xaabbccddeefful);
    CHECK(header.get_creation_time() == 0xaabbccddeeffl);
    auto creation_time_pointer = header.get_creation_time_pointer();
    CHECK(*creation_time_pointer == 0xaabbccddeeffl);
    *creation_time_pointer = 0xffeeddccbbaal;
    CHECK(header.get_creation_time() == 0xffeeddccbbaal);

    header.set_prev_pointer(0x1122334455667788ul);
    CHECK(header.get_prev_pointer() == 0x1122334455667788ul);

    header.clear();
    EdgeLabelEntry entry;
    entry.set_label(1111);
    entry.set_pointer(0x1111111111111111ul);
    CHECK(header.append(entry));
    entry.set_label(2222);
    entry.set_pointer(0x2222222222222222ul);
    CHECK(header.append(entry));
    entry.set_label(3333);
    entry.set_pointer(0x3333333333333333ul);
    CHECK(!header.append(entry));
    CHECK(header.get_num_entries() == 2);
    CHECK(header.get_entries()[0].get_label() == 1111);
    CHECK(header.get_entries()[0].get_pointer() == 0x1111111111111111ul);
    CHECK(header.get_entries()[1].get_label() == 2222);
    CHECK(header.get_entries()[1].get_pointer() == 0x2222222222222222ul);

    CHECK(header.get_type() == EdgeLabelBlockHeader::Type::EDGE_LABEL);
    CHECK(header.get_order() == log_size);
    CHECK(header.get_block_size() == 1ul << log_size);
    CHECK(header.get_vertex_id() == 0x1234abcdef);
    CHECK(header.get_creation_time() == 0xffeeddccbbaal);
    CHECK(header.get_prev_pointer() == 0x1122334455667788ul);

    free(buf);
}

TEST_CASE("testing the EdgeEntry")
{
    EdgeEntry entry;
    entry.set_dst(233);
    CHECK(entry.get_dst() == 233);
    entry.set_dst(0xffffffff);
    CHECK(entry.get_dst() == 0xffffffff);
    entry.set_dst(0x1234abcdeful);
    CHECK(entry.get_dst() == 0x1234abcdef);

    entry.set_creation_time(0xaabbccddeefful);
    CHECK(entry.get_creation_time() == 0xaabbccddeeffl);
    auto creation_time_pointer = entry.get_creation_time_pointer();
    CHECK(*creation_time_pointer == 0xaabbccddeeffl);
    *creation_time_pointer = 0xffeeddccbbaal;
    CHECK(entry.get_creation_time() == 0xffeeddccbbaal);

    entry.set_deletion_time(0xabcdefabcdeful);
    CHECK(entry.get_deletion_time() == 0xabcdefabcdeful);
    auto deletion_time_pointer = entry.get_deletion_time_pointer();
    CHECK(*deletion_time_pointer == 0xabcdefabcdeful);
    *deletion_time_pointer = 0x1234432112344321l;
    CHECK(entry.get_deletion_time() == 0x1234432112344321l);

    entry.set_length(0x1122ul);
    CHECK(entry.get_length() == 0x1122ul);

    CHECK(entry.get_dst() == 0x1234abcdef);
    CHECK(entry.get_creation_time() == 0xffeeddccbbaal);
    CHECK(entry.get_deletion_time() == 0x1234432112344321l);
    CHECK(entry.get_length() == 0x1122ul);
}

TEST_CASE("testing the EdgeBlockHeader")
{
    SUBCASE("EdgeBlockHeader without BloomFilter")
    {
        const order_t log_size = 7; // size = 128 bytes, left 80 bytes, 3 edges (2 bytes data)
        auto buf = malloc(1ul << log_size);
        EdgeBlockHeader &header = *(EdgeBlockHeader *)buf;

        header.set_order(log_size);
        header.set_type(EdgeLabelBlockHeader::Type::EDGE);

        CHECK(!header.get_bloom_filter().valid());

        header.set_vertex_id(233);
        CHECK(header.get_vertex_id() == 233);
        header.set_vertex_id(0xffffffff);
        CHECK(header.get_vertex_id() == 0xffffffff);
        header.set_vertex_id(0x1234abcdeful);
        CHECK(header.get_vertex_id() == 0x1234abcdef);

        header.set_creation_time(0xaabbccddeefful);
        CHECK(header.get_creation_time() == 0xaabbccddeeffl);
        auto creation_time_pointer = header.get_creation_time_pointer();
        CHECK(*creation_time_pointer == 0xaabbccddeeffl);
        *creation_time_pointer = 0xffeeddccbbaal;
        CHECK(header.get_creation_time() == 0xffeeddccbbaal);

        header.set_prev_pointer(0x1122334455667788ul);
        CHECK(header.get_prev_pointer() == 0x1122334455667788ul);

        header.set_committed_time(0xabcdefabcdeful);
        CHECK(header.get_committed_time() == 0xabcdefabcdeful);
        auto committed_time_pointer = header.get_committed_time_pointer();
        CHECK(*committed_time_pointer == 0xabcdefabcdeful);
        *committed_time_pointer = 0x1234432112344321l;
        CHECK(header.get_committed_time() == 0x1234432112344321l);

        header.clear();
        for (int i = 0; i < 4; i++)
        {
            std::string data = std::to_string(i) + ".";
            EdgeEntry entry;
            entry.set_creation_time(i);
            entry.set_deletion_time(-i);
            entry.set_dst(i * 1234);
            entry.set_length(data.size());
            CHECK((bool)header.append(entry, data.c_str()) == (i < 3));
        }
        CHECK(header.get_num_entries() == 3);
        CHECK(header.get_data_length() == 6);

        auto data = header.get_data();
        auto entries = header.get_entries();
        for (int i = 0; i < 3; i++)
        {
            std::string ref = std::to_string(i) + ".";
            entries--;
            CHECK(entries->get_creation_time() == i);
            CHECK(entries->get_deletion_time() == -i);
            CHECK(entries->get_dst() == i * 1234);
            CHECK(entries->get_length() == ref.size());
            CHECK(std::string(data, entries->get_length()) == ref);
            data += entries->get_length();
        }

        CHECK(header.get_type() == EdgeLabelBlockHeader::Type::EDGE);
        CHECK(header.get_order() == log_size);
        CHECK(header.get_block_size() == 1ul << log_size);
        CHECK(header.get_vertex_id() == 0x1234abcdef);
        CHECK(header.get_creation_time() == 0xffeeddccbbaal);
        CHECK(header.get_prev_pointer() == 0x1122334455667788ul);
        CHECK(header.get_committed_time() == 0x1234432112344321l);

        free(buf);
    }

    SUBCASE("EdgeBlockHeader with BloomFilter")
    {
        const order_t log_size = 10; // size = 1024 bytes, left 912 bytes, 31 edges (5 bytes data)
        auto buf = aligned_alloc(32, 1ul << log_size);
        EdgeBlockHeader &header = *(EdgeBlockHeader *)buf;

        header.set_order(log_size);
        header.set_type(EdgeLabelBlockHeader::Type::EDGE);

        CHECK(header.get_bloom_filter().valid());

        header.set_vertex_id(233);
        CHECK(header.get_vertex_id() == 233);
        header.set_vertex_id(0xffffffff);
        CHECK(header.get_vertex_id() == 0xffffffff);
        header.set_vertex_id(0x1234abcdeful);
        CHECK(header.get_vertex_id() == 0x1234abcdef);

        header.set_creation_time(0xaabbccddeefful);
        CHECK(header.get_creation_time() == 0xaabbccddeeffl);
        auto creation_time_pointer = header.get_creation_time_pointer();
        CHECK(*creation_time_pointer == 0xaabbccddeeffl);
        *creation_time_pointer = 0xffeeddccbbaal;
        CHECK(header.get_creation_time() == 0xffeeddccbbaal);

        header.set_prev_pointer(0x1122334455667788ul);
        CHECK(header.get_prev_pointer() == 0x1122334455667788ul);

        header.set_committed_time(0xabcdefabcdeful);
        CHECK(header.get_committed_time() == 0xabcdefabcdeful);
        auto committed_time_pointer = header.get_committed_time_pointer();
        CHECK(*committed_time_pointer == 0xabcdefabcdeful);
        *committed_time_pointer = 0x1234432112344321l;
        CHECK(header.get_committed_time() == 0x1234432112344321l);

        auto filter = header.get_bloom_filter();
        header.clear();
        for (int i = 0; i < 32; i++)
        {
            std::string data = std::to_string(i);
            while (data.size() < 5)
                data += ".";
            EdgeEntry entry;
            entry.set_creation_time(i);
            entry.set_deletion_time(-i);
            entry.set_dst(i * 1234);
            entry.set_length(data.size());
            if (i % 2)
            {
                CHECK((bool)header.append(entry, data.c_str(), filter) == (i < 31));
            }
            else
            {
                CHECK((bool)header.append(entry, data.c_str()) == (i < 31));
            }
        }
        CHECK(header.get_num_entries() == 31);
        CHECK(header.get_data_length() == 5 * 31);

        auto data = header.get_data();
        auto entries = header.get_entries();
        for (int i = 0; i < 31; i++)
        {
            std::string ref = std::to_string(i);
            while (ref.size() < 5)
                ref += ".";
            entries--;
            CHECK(entries->get_creation_time() == i);
            CHECK(entries->get_deletion_time() == -i);
            CHECK(entries->get_dst() == i * 1234);
            CHECK(entries->get_length() == ref.size());
            CHECK(std::string(data, entries->get_length()) == ref);
            CHECK(header.get_bloom_filter().find(entries->get_dst()));
            data += entries->get_length();
        }

        header.set_num_entries_data_length_atomic(123, 321);
        CHECK(header.get_num_entries() == 123);
        CHECK(header.get_data_length() == 321);
        CHECK(header.get_num_entries_data_length_atomic() == std::pair<size_t, size_t>{123, 321});

        CHECK(header.get_type() == EdgeLabelBlockHeader::Type::EDGE);
        CHECK(header.get_order() == log_size);
        CHECK(header.get_block_size() == 1ul << log_size);
        CHECK(header.get_vertex_id() == 0x1234abcdef);
        CHECK(header.get_creation_time() == 0xffeeddccbbaal);
        CHECK(header.get_prev_pointer() == 0x1122334455667788ul);
        CHECK(header.get_committed_time() == 0x1234432112344321l);

        free(buf);
    }
}

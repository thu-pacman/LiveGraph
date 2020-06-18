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

#include "blocks.cpp"
#include "core/block_manager.hpp"
#include "core/utils.hpp"

using namespace livegraph;

TEST_CASE("testing the BlockManager")
{
    BlockManager manager("");
    size_t size = sizeof(EdgeLabelBlockHeader);
    order_t order = size_to_order(size);
    CHECK(size == 32);
    CHECK(order == 5);
    auto pointer = manager.alloc(order);

    EdgeLabelBlockHeader *block = manager.convert<EdgeLabelBlockHeader>(pointer);
    block->set_order(order);
    manager.free(pointer, order);

    CHECK(manager.convert<char>(manager.NULLPOINTER) == nullptr);
}

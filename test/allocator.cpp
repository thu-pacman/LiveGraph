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

#include <memory>

#include "core/allocator.hpp"

using namespace livegraph;

TEST_CASE("testing the SparseArrayAllocator")
{
    SparseArrayAllocator<void> allocator;
    auto allocator_for_int = std::allocator_traits<decltype(allocator)>::rebind_alloc<int>(allocator);
    auto int_data = allocator_for_int.allocate(1000);
    allocator_for_int.deallocate(int_data, 1000);
}

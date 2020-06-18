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

#include "core/utils.hpp"

using namespace livegraph;

TEST_CASE("testing the size_to_order")
{
    CHECK(size_to_order(0) == 0);
    CHECK(size_to_order(1) == 0);
    CHECK(size_to_order(2) == 1);
    CHECK(size_to_order(3) == 2);
    CHECK(size_to_order(4) == 2);
    CHECK(size_to_order(5) == 3);
    CHECK(size_to_order(6) == 3);
    CHECK(size_to_order(7) == 3);
    CHECK(size_to_order(8) == 3);
}

TEST_CASE("testing cmp_timestamp")
{
    timestamp_t a = -10, b = 10;
    CHECK(cmp_timestamp(&a, 10) > 0);
    CHECK(cmp_timestamp(&a, 0) > 0);
    CHECK(cmp_timestamp(&b, 0) > 0);
    CHECK(cmp_timestamp(&b, 10) == 0);
    CHECK(cmp_timestamp(&b, 15) < 0);
    CHECK(cmp_timestamp(&a, 10, 10) == 0);
    CHECK(cmp_timestamp(&a, 10, 123) > 0);
    CHECK(cmp_timestamp(&a, 0, 123) > 0);
    CHECK(cmp_timestamp(&b, 0, 123) > 0);
    CHECK(cmp_timestamp(&b, 10, 123) == 0);
    CHECK(cmp_timestamp(&b, 15, 123) < 0);
}

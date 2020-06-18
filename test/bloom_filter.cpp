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

#include <cstdlib>
#include <random>
#include <unordered_set>

#include "core/bloom_filter.hpp"

using namespace livegraph;

TEST_CASE("testing the BloomFilter")
{
    SUBCASE("empty BloomFilter")
    {
        BloomFilter empty_filter;
        CHECK(!empty_filter.valid());
    }

    SUBCASE("insert and find")
    {
        const size_t log_size = 15;
        const size_t size = 1ul << log_size;
        const size_t range = 1ul << log_size;
        const size_t num_checks = 1ul << (log_size + 3);
        const size_t log_bytes_per_elements = 2;
        std::unordered_set<uint64_t> ref;
        std::mt19937_64 random;

        void *buf = aligned_alloc(4096, size * (1ul << log_bytes_per_elements));
        BloomFilter filter(log_size + log_bytes_per_elements, buf);
        CHECK(filter.valid());

        for (size_t i = 0; i < size; i++)
        {
            auto data = random() % range;
            ref.emplace(data);
            filter.insert(data);
        }

        size_t num_false_positive = 0;
        size_t num_negetive = 0;
        for (size_t i = 0; i < num_checks; i++)
        {
            auto data = random() % range;
            if (ref.find(data) != ref.end())
            {
                CHECK(filter.find(data));
            }
            else
            {
                num_negetive++;
                if (filter.find(data))
                    num_false_positive++;
            }
        }

        CHECK(num_false_positive * 10 < num_negetive); // should less than 0.1

        free(buf);
    }
}

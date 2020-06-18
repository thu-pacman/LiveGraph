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

#include <string>

#include "types.hpp"

namespace livegraph
{
    inline void compiler_fence() { asm volatile("" ::: "memory"); }

    inline order_t size_to_order(size_t size)
    {
        order_t order = (order_t)((size & (size - 1)) != 0);
        while (size > 1)
        {
            order += 1;
            size >>= 1;
        }
        return order;
    }

    inline int cmp_timestamp(const timestamp_t *xp, timestamp_t y) // y > 0
    {
        timestamp_t x = *xp;
        if (x < 0)
            return 1;
        if (x < y)
            return -1;
        if (x == y)
            return 0;
        return 1;
    }

    inline int cmp_timestamp(const timestamp_t *xp, timestamp_t y,
                             timestamp_t local_txn_id) // y > 0
    {
        timestamp_t x = *xp;
        if (-x == local_txn_id)
            return 0;
        if (x < 0)
            return 1;
        if (x < y)
            return -1;
        if (x == y)
            return 0;
        return 1;
    }

} // namespace livegraph

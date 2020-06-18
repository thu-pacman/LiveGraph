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

#include <mutex>
#include <random>
#include <thread>
#include <vector>

#include <tbb/parallel_for.h>

#include "core/futex.hpp"

using namespace livegraph;

TEST_CASE("testing the Futex")
{
    SUBCASE("lock and unlock")
    {
        const size_t size = 1ul << 15;
        std::mt19937_64 random;
        std::vector<uint64_t> data(size);
        uint64_t sum = 0, min = UINT64_MAX;
        uint64_t lock_sum = 0, lock_min = UINT64_MAX;
        Futex mutex;

        mutex.lock();
        mutex.unlock();
        mutex.lock();
        mutex.clear();

        for (size_t i = 0; i < size; i++)
        {
            data[i] = random();
            sum += data[i];
            if (min > data[i])
                min = data[i];
        }

        #pragma omp parallel for
        for (size_t i = 0; i < size; i++)
        {
            std::lock_guard<Futex> lock(mutex);
            lock_sum += data[i];
            if (lock_min > data[i])
                lock_min = data[i];
        }

        CHECK(sum == lock_sum);
        CHECK(min == lock_min);
    }

    SUBCASE("try_lock_for")
    {
        Futex futex;
        std::thread thread1([&]() {
            futex.lock();
            std::this_thread::sleep_for(std::chrono::seconds(2));
            futex.unlock();
        });
        std::thread thread2([&]() {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            CHECK(!futex.try_lock_for(std::chrono::milliseconds(1)));
            CHECK(futex.try_lock_for(std::chrono::seconds(2)));
            futex.unlock();
        });
        thread1.join();
        thread2.join();
        futex.lock();
        futex.unlock();
    }
}

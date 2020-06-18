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

#include <cstdlib>
#include <limits>
#include <new>

#include <sys/mman.h>

namespace livegraph
{

    template <typename T> struct SparseArrayAllocator
    {
        using value_type = T;

        SparseArrayAllocator() = default;
        template <class U> constexpr SparseArrayAllocator(const SparseArrayAllocator<U> &) noexcept {}
        T *allocate(size_t n)
        {
            size_t size = n * sizeof(T);
            if (n > std::numeric_limits<std::size_t>::max() / sizeof(T))
                throw std::bad_alloc();
            auto data = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
            if (data == MAP_FAILED)
                throw std::bad_alloc();
            return static_cast<T *>(data);
        }

        void deallocate(T *data, size_t n) noexcept
        {

            size_t size = n * sizeof(T);
            munmap(data, size);
        }

        template <class U> bool operator==(const SparseArrayAllocator<U> &) { return true; }
        template <class U> bool operator!=(const SparseArrayAllocator<U> &) { return false; }
    };

} // namespace livegraph

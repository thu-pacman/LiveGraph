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

// Copied from Apache Impala (incubating), usable under the terms in the Apache
// License, Version 2.0.

// This is a block Bloom filter (from Putze et al.'s "Cache-, Hash- and
// Space-EfficientBloom Filters") with some twists:
//
// 1. Each block is a split Bloom filter - see Section 2.1 of Broder and
// Mitzenmacher's "Network Applications of Bloom Filters: A Survey".
//
// 2. The number of bits set per insert() is contant in order to take advantage
// of SIMD instructions.

#include <algorithm>
#include <cstdint>
#include <cstring>

#include <immintrin.h>

namespace livegraph
{

    class BloomFilter
    {
    public:
        BloomFilter() : log_num_buckets(0), directory(nullptr), directory_mask(0) {}

        BloomFilter(size_t log_size, void *buffer)
            : log_num_buckets(std::max(1ul, log_size - LOG_BUCKET_BYTE_SIZE)),
              directory((bucket_t *)buffer),
              directory_mask((1ul << log_num_buckets) - 1)
        {
        }

        BloomFilter(const BloomFilter &) = default;
        BloomFilter(BloomFilter &&) = default;
        BloomFilter &operator=(const BloomFilter &) = default;
        BloomFilter &operator=(BloomFilter &&a) = default;
        ~BloomFilter() = default;

        bool valid() const { return log_num_buckets != 0; }

        void clear() { memset(directory, 0, (1ul << log_num_buckets) * sizeof(bucket_t)); }

        size_t size() const
        {
            if (valid())
                return 1ul << (log_num_buckets + LOG_BUCKET_BYTE_SIZE);
            else
                return 0;
        }

        [[gnu::always_inline]] inline static uint64_t get_hash(uint64_t key)
        {
            const uint64_t SEED[4] = {0x818c3f78ull, 0x672f4a3aull, 0xabd04d69ull, 0x12b51f95ull};
            const unsigned __int128 m = *reinterpret_cast<const unsigned __int128 *>(&SEED[0]);
            const unsigned __int128 a = *reinterpret_cast<const unsigned __int128 *>(&SEED[2]);
            return (a + m * key) >> 64;
        }

        [[gnu::always_inline]] inline static __m256i get_mask(const uint32_t hash)
        {
            const __m256i ones = _mm256_set1_epi32(1);
            // Odd contants for hashing:
            const __m256i rehash = _mm256_setr_epi32(0x47b6137bU, 0x44974d91U, 0x8824ad5bU, 0xa2b7289dU, 0x705495c7U,
                                                     0x2df1424bU, 0x9efc4947U, 0x5c6bfb31U);
            // Load hash into a YMM register, repeated eight times
            __m256i hash_data = _mm256_set1_epi32(hash);
            // Multiply-shift hashing ala Dietzfelbinger et al.: multiply 'hash'
            // by eight different odd constants, then keep the 5 most
            // significant bits from each product.
            hash_data = _mm256_mullo_epi32(rehash, hash_data);
            hash_data = _mm256_srli_epi32(hash_data, 27);
            // Use these 5 bits to shift a single bit to a location in each
            // 32-bit lane
            return _mm256_sllv_epi32(ones, hash_data);
        }

        [[gnu::always_inline]] inline void insert(size_t id)
        {
            const uint64_t hash = get_hash(id);
            const uint32_t bucket_id = hash & directory_mask;
            const __m256i mask = get_mask(hash >> log_num_buckets);
            __m256i *const bucket = &reinterpret_cast<__m256i *>(directory)[bucket_id];
            _mm256_store_si256(bucket, _mm256_or_si256(*bucket, mask));
        }

        [[gnu::always_inline]] inline bool find(size_t id) const
        {
            const uint64_t hash = get_hash(id);
            const uint32_t bucket_idx = hash & directory_mask;
            const __m256i mask = get_mask(hash >> log_num_buckets);
            const __m256i bucket = reinterpret_cast<__m256i *>(directory)[bucket_idx];
            // We should return true if 'bucket' has a one wherever 'mask' does.
            // _mm256_testc_si256 takes the negation of its first argument and
            // ands that with its second argument. In our case, the result is
            // zero everywhere iff there is a one in 'bucket' wherever 'mask' is
            // one. testc returns 1 if the result is 0 everywhere and returns 0
            // otherwise.
            return _mm256_testc_si256(bucket, mask);
        }

    private:
        using bucket_t = uint32_t[8];
        static constexpr size_t LOG_BUCKET_BYTE_SIZE = 5;
        size_t log_num_buckets;
        bucket_t *directory;
        uint32_t directory_mask;
    };
} // namespace livegraph

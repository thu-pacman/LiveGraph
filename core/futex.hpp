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

#include <chrono>
#include <stdexcept>

#include <errno.h>
#include <linux/futex.h>
#include <sys/syscall.h>
#include <unistd.h>

namespace livegraph
{
    class Futex
    {
    public:
        void lock()
        {
            while (true)
            {
                __sync_fetch_and_add(&num_using, 1);
                if (__sync_bool_compare_and_swap(&futexp, 0, 1))
                    break;
                int ret = futex(&futexp, FUTEX_WAIT, 1, nullptr, nullptr, 0);
                __sync_fetch_and_sub(&num_using, 1);
                if (ret == -1 && errno != EAGAIN)
                    throw std::runtime_error("Futex wait error.");
            }
        }

        template <class Rep, class Period> bool try_lock_for(const std::chrono::duration<Rep, Period> &timeout_duration)
        {
            const struct timespec timeout = {.tv_sec = timeout_duration / std::chrono::seconds(1),
                                             .tv_nsec = (timeout_duration % std::chrono::seconds(1)) /
                                                        std::chrono::nanoseconds(1)};
            while (true)
            {
                __sync_fetch_and_add(&num_using, 1);
                if (__sync_bool_compare_and_swap(&futexp, 0, 1))
                    break;
                int ret = futex(&futexp, FUTEX_WAIT, 1, &timeout, nullptr, 0);
                __sync_fetch_and_sub(&num_using, 1);
                if (ret == -1 && errno != EAGAIN && errno != ETIMEDOUT)
                    throw std::runtime_error("Futex wait error.");
                if (ret != 0 && errno == ETIMEDOUT)
                    return false;
            }
            return true;
        }

        void unlock()
        {
            if (__sync_bool_compare_and_swap(&futexp, 1, 0))
            {
                if (__sync_sub_and_fetch(&num_using, 1) == 0)
                    return;
                int ret = futex(&futexp, FUTEX_WAKE, 1, nullptr, nullptr, 0);
                if (ret == -1)
                    throw std::runtime_error("Futex wake error.");
            }
        }

        void clear()
        {
            futexp = 0;
            num_using = 0;
        }

        Futex() : futexp(0), num_using(0) {}

    private:
        int futexp;
        int num_using;
        inline static int
        futex(int *uaddr, int futex_op, int val, const struct timespec *timeout, int *uaddr2, int val3)
        {
            return syscall(SYS_futex, uaddr, futex_op, val, timeout, uaddr2, val3);
        }
    };
} // namespace livegraph

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

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <thread>

#include <fcntl.h>
#include <unistd.h>

#include "types.hpp"

namespace livegraph
{

    class CommitManager
    {
    public:
        CommitManager(std::string path, std::atomic<timestamp_t> &_global_epoch_id)
            : fd(EMPTY_FD),
              seq_front{0, 0},
              seq_rear{0, 0},
              mutex(),
              client_mutex(),
              cv_server(),
              cv_client(),
              global_client_mutex(0),
              used_size(0),
              file_size(0),
              global_epoch_id(_global_epoch_id),
              writing_epoch_id(global_epoch_id),
              unfinished_epoch_id(),
              queue(),
              closed(false),
              server_thread([&] { server_loop(); })
        {
            if (!path.empty())
            {
                fd = open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0640);
                if (fd == EMPTY_FD)
                    throw std::runtime_error("open wal file error.");
                if (ftruncate(fd, FILE_TRUNC_SIZE) != 0)
                    throw std::runtime_error("ftruncate wal file error.");
            }
            file_size = FILE_TRUNC_SIZE;
        }

        ~CommitManager()
        {
            closed.store(true);
            cv_server.notify_one();
            server_thread.join();
            if (fd != EMPTY_FD)
                close(fd);
        }

        std::pair<timestamp_t, std::atomic<int> *> register_commit(std::string_view wal)
        {
            while (true)
            {
                auto local_client_mutex = global_client_mutex.load();
                std::unique_lock<std::mutex> lock(mutex[local_client_mutex]);
                if (local_client_mutex != global_client_mutex.load())
                    continue;

                timestamp_t local_commit_epoch_id;
                std::atomic<int> *local_num_unfinished;

                queue[local_client_mutex].emplace(wal, &local_commit_epoch_id, &local_num_unfinished);
                auto my_seq = seq_rear[local_client_mutex]++;

                lock.unlock();
                cv_server.notify_one();

                std::unique_lock<std::mutex> client_lock(client_mutex[local_client_mutex]);
                if (seq_front[local_client_mutex] <= my_seq)
                {
                    cv_client[local_client_mutex].wait(client_lock,
                                                       [&]() { return seq_front[local_client_mutex] > my_seq; });
                }

                return {local_commit_epoch_id, local_num_unfinished};
            }
        }

        void finish_commit(timestamp_t local_commit_epoch_id, std::atomic<int> *local_num_unfinished, bool wait)
        {
            local_num_unfinished->fetch_sub(1);
            while (wait && global_epoch_id < local_commit_epoch_id)
            {
                cv_server.notify_one();
                std::this_thread::yield();
            }
        }

    private:
        int fd;
        size_t seq_front[2];                  //(server) increment after fsync() is finished
        size_t seq_rear[2];                   // (client) increment after push() is finished
        std::mutex mutex[2];                  // (server/clients) serialize queue operations
        std::mutex client_mutex[2];           // (clients) wait for fsync() to finish
        std::condition_variable cv_server;    // (server) wait when the queue is empty
        std::condition_variable cv_client[2]; // (clients) wait for fsync() to finish
        std::atomic<int> global_client_mutex;
        size_t used_size;
        size_t file_size;
        std::atomic<timestamp_t> &global_epoch_id;
        timestamp_t writing_epoch_id;
        std::queue<std::pair<timestamp_t, std::atomic<int>>> unfinished_epoch_id;
        std::queue<std::tuple<std::string_view, timestamp_t *, std::atomic<int> **>>
            queue[2]; // wal, epoch_id, unfinished
        std::atomic<bool> closed;
        std::thread server_thread;

        constexpr static size_t FILE_TRUNC_SIZE = 1ul << 30; // 1GB
        constexpr static int EMPTY_FD = -1;
        constexpr static auto SERVER_SPIN_INTERVAL = std::chrono::microseconds(100);

        void check_unfinished_epoch_id()
        {
            while (!unfinished_epoch_id.empty())
            {
                auto &[current_epoch_id, num_unfinished] = unfinished_epoch_id.front();
                if (num_unfinished.load() == 0)
                {
                    global_epoch_id = current_epoch_id;
                    unfinished_epoch_id.pop();
                }
                else
                {
                    break;
                }
            }
        }

        void server_loop()
        {
            while (true)
            {
                check_unfinished_epoch_id();
                int local_client_mutex = global_client_mutex.load();
                std::unique_lock<std::mutex> lock(mutex[local_client_mutex]);
                auto &local_queue = queue[local_client_mutex];
                while (local_queue.empty() && !closed.load())
                {
                    cv_server.wait_for(lock, SERVER_SPIN_INTERVAL, [&]() {
                        check_unfinished_epoch_id();
                        return !local_queue.empty() || closed.load();
                    });
                    check_unfinished_epoch_id();
                    cv_client[local_client_mutex ^ 1].notify_all();
                }
                std::unique_lock<std::mutex> client_lock(client_mutex[local_client_mutex]);

                global_client_mutex ^= 1;

                size_t num_txns = local_queue.size();

                if (!num_txns)
                    break;

                ++writing_epoch_id;

                unfinished_epoch_id.emplace(writing_epoch_id, 0);

                auto &num_unfinished = unfinished_epoch_id.back().second;

                std::string group_wal;
                group_wal.append(reinterpret_cast<char *>(&writing_epoch_id), sizeof(writing_epoch_id));
                group_wal.append(reinterpret_cast<char *>(&num_txns), sizeof(num_txns));

                for (size_t i = 0; i < num_txns; i++)
                {
                    auto &[wal, ret_epoch_id, ret_num] = local_queue.front();

                    group_wal.append(wal);
                    *ret_epoch_id = writing_epoch_id;
                    *ret_num = &num_unfinished;
                    ++num_unfinished;
                    local_queue.pop();
                }

                auto expected_size = used_size + group_wal.size();
                if (expected_size > file_size)
                {
                    size_t new_file_size = (expected_size / FILE_TRUNC_SIZE + 1) * FILE_TRUNC_SIZE;
                    if (fd != EMPTY_FD)
                    {
                        if (ftruncate(fd, new_file_size) != 0)
                            throw std::runtime_error("ftruncate wal file error.");
                    }
                    file_size = new_file_size;
                }

                used_size += group_wal.size();

                if (fd != EMPTY_FD)
                {
                    if ((size_t)write(fd, group_wal.c_str(), group_wal.size()) != group_wal.size())
                        std::runtime_error("write wal file error.");
                }

                if (fd != EMPTY_FD)
                {
                    if (fdatasync(fd) != 0)
                        std::runtime_error("fdatasync wal file error.");
                }

                ++num_unfinished;

                lock.unlock();
                seq_front[local_client_mutex] += num_txns;
                cv_client[local_client_mutex].notify_all();
                client_lock.unlock();

                --num_unfinished;
            }
        }
    };

} // namespace livegraph

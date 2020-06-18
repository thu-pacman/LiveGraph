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

#include "blocks.hpp"
#include "graph.hpp"
#include "utils.hpp"

namespace livegraph
{
    class EdgeIterator
    {
    public:
        EdgeIterator(EdgeEntry *_entries,
                     char *_data,
                     size_t _num_entries,
                     size_t _data_length,
                     timestamp_t _read_epoch_id,
                     timestamp_t _local_txn_id,
                     bool _reverse)
            : entries(_entries),
              data(_data),
              num_entries(_num_entries),
              data_length(_data_length),
              read_epoch_id(_read_epoch_id),
              local_txn_id(_local_txn_id),
              reverse(_reverse)
        {
            if (!reverse)
            {
                entries_cursor = entries - num_entries; // at the begining
                data_cursor = data + data_length;       // at the end
            }
            else
            {
                entries_cursor = entries; // at the end
                data_cursor = data;       // at the begining
            }

            if (!reverse)
            {
                while (valid())
                {
                    if (cmp_timestamp(entries_cursor->get_creation_time_pointer(), read_epoch_id, local_txn_id) <= 0 &&
                        cmp_timestamp(entries_cursor->get_deletion_time_pointer(), read_epoch_id, local_txn_id) > 0)
                    {
                        break;
                    }
                    data_cursor -= entries_cursor->get_length();
                    entries_cursor++;
                }
            }
            else
            {
                while (valid())
                {
                    if (cmp_timestamp((entries_cursor - 1)->get_creation_time_pointer(), read_epoch_id, local_txn_id) <=
                            0 &&
                        cmp_timestamp((entries_cursor - 1)->get_deletion_time_pointer(), read_epoch_id, local_txn_id) >
                            0)
                    {
                        break;
                    }
                    data_cursor += (entries_cursor - 1)->get_length();
                    entries_cursor--;
                }
            }
        }

        EdgeIterator(const EdgeIterator &) = default;

        EdgeIterator(EdgeIterator &&) = default;

        bool valid() const
        {
            if (!reverse)
                return !(entries_cursor == entries);
            else
                return !(entries_cursor == entries - num_entries);
        }

        void next()
        {
            if (!reverse)
            {
                while (valid())
                {
                    data_cursor -= entries_cursor->get_length();
                    entries_cursor++;
                    if (cmp_timestamp(entries_cursor->get_creation_time_pointer(), read_epoch_id, local_txn_id) <= 0 &&
                        cmp_timestamp(entries_cursor->get_deletion_time_pointer(), read_epoch_id, local_txn_id) > 0)
                    {
                        break;
                    }
                }
            }
            else
            {
                while (valid())
                {
                    data_cursor += (entries_cursor - 1)->get_length();
                    entries_cursor--;
                    if (cmp_timestamp((entries_cursor - 1)->get_creation_time_pointer(), read_epoch_id, local_txn_id) <=
                            0 &&
                        cmp_timestamp((entries_cursor - 1)->get_deletion_time_pointer(), read_epoch_id, local_txn_id) >
                            0)
                    {
                        break;
                    }
                }
            }
        }

        vertex_t dst_id() const
        {
            if (!valid())
                return Graph::VERTEX_TOMBSTONE;
            if (!reverse)
                return entries_cursor->get_dst();
            else
                return (entries_cursor - 1)->get_dst();
        }

        std::string_view edge_data() const
        {
            if (!valid())
                return std::string_view();
            if (!reverse)
                return std::string_view(data_cursor - entries_cursor->get_length(), entries_cursor->get_length());
            else
                return std::string_view(data_cursor, (entries_cursor - 1)->get_length());
        }

    private:
        EdgeEntry *entries;
        char *data;
        size_t num_entries;
        size_t data_length;
        timestamp_t read_epoch_id;
        timestamp_t local_txn_id;
        bool reverse;
        EdgeEntry *entries_cursor;
        char *data_cursor;
    };
} // namespace livegraph

/**
 * \file server_meta_storage.h
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef STDB_METASTORAGE_SERVER_META_STORAGE_H_
#define STDB_METASTORAGE_SERVER_META_STORAGE_H_

#include <cstddef>
#include <memory>
#include <vector>
#include <mutex>
#include <condition_variable>
#include <boost/optional.hpp>

#include "stdb/metastorage/meta_storage.h"

namespace stdb {

/** Sqlite3 backed storage for server-side metadata.
 * Metadata includes:
 * - Configuration data
 * - Key to id mapping
 */
struct ServerMetaStorage : public MetaStorage {
 public:
  /** Create new or open existing db.
   * @throw std::runtime_error in a case of error
   */
  ServerMetaStorage(const char* db, bool is_moving);

  /** Read larges series id */
  boost::optional<i64> get_prev_largest_id();

  /** load matcher data into SeriesMatcherBase */
  common::Status load_matcher_data(SeriesMatcher &matcher);

  // Synchronization
  void sync_with_metadata_storage(std::function<void(std::vector<SeriesT>*)> pull_new_series);
  void sync_with_metadata_storage(std::function<void(std::vector<SeriesT>*, std::vector<Location>*)> pull_new_series);

 private:
  /** Add new series to the metadata storage (generate sql query and execute it). */
  void insert_new_series(std::vector<SeriesT>&& items, std::vector<Location>&& locations);

  /** Add new series to the metadata storage */
  void insert_new_series(std::vector<SeriesT>&& items);

  /** Create tables if database is empty
   * @throw std::runtime_error in a case of error
   */
  void create_server_tables();

  // is moving
  bool is_moving_;
  // Synchronization
  mutable std::mutex tran_lock_;
};

}  // namespace stdb

#endif  // STDB_METASTORAGE_SERVER_META_STORAGE_H_

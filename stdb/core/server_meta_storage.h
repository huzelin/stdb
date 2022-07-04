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
#ifndef STDB_CORE_SERVER_META_STORAGE_H_
#define STDB_CORE_SERVER_META_STORAGE_H_

#include <cstddef>
#include <memory>
#include <vector>
#include <mutex>
#include <condition_variable>
#include <boost/optional.hpp>

#include <apr.h>
#include <apr_dbd.h>

#include "stdb/common/apr_utils.h"
#include "stdb/common/basic.h"
#include "stdb/common/status.h"
#include "stdb/index/seriesparser.h"
#include "stdb/storage/volume_registry.h"

namespace stdb {

/** Sqlite3 backed storage for metadata.
 * Metadata includes:
 * - Configuration data
 * - Key to id mapping
 */
struct ServerMetaStorage {
  // Typedefs
  typedef std::unique_ptr<apr_pool_t, decltype(&delete_apr_pool)> PoolT;
  typedef const apr_dbd_driver_t* DriverT;
  typedef std::unique_ptr<apr_dbd_t, AprHandleDeleter> HandleT;
  typedef apr_dbd_prepared_t* PreparedT;
  typedef PlainSeriesMatcher::SeriesNameT SeriesT;

  // Members
  PoolT           pool_;
  DriverT         driver_;
  HandleT         handle_;
  PreparedT       insert_;

  // Synchronization
  mutable std::mutex                                sync_lock_;
  mutable std::mutex                                tran_lock_;
  std::condition_variable                           sync_cvar_;

  /** Create new or open existing db.
   * @throw std::runtime_error in a case of error
   */
  ServerMetaStorage(const char* db);

  /** Create tables if database is empty
   * @throw std::runtime_error in a case of error
   */
  void create_tables();

  /** Initialize config 
   * @throw std::runtime_error in a case of error
   */
  void init_config(const char* db_name,
                   const char* creation_datetime,
                   const char* bstore_type);

  /**
   * @brief Get value of the configuration parameter
   * @param param_name is a name of the configuration parameter
   * @param value is a pointer that should receive configuration value
   * @return true on succes, false otherwise
   */
  bool get_config_param(const std::string param_name, std::string* value);

  /** Read larges series id */
  boost::optional<i64> get_prev_largest_id();

  common::Status load_matcher_data(SeriesMatcherBase &matcher);

  // Synchronization
  virtual std::string get_dbname();

  common::Status wait_for_sync_request(int timeout_us);

  void sync_with_metadata_storage(std::function<void(std::vector<SeriesT>*, std::vector<Location>*)> pull_new_series);

  //! Forces `wait_for_sync_request` to return immediately
  void force_sync();

  /** Add new series to the metadata storage (generate sql query and execute it).
   */
  void insert_new_series(std::vector<SeriesT>&& items, std::vector<Location>&& locations);

 private:
  void begin_transaction();
  void end_transaction();

  /** Execute query that doesn't return anything.
   * @throw std::runtime_error in a case of error
   * @return number of rows changed
   */
  int execute_query(std::string query);

  typedef std::vector<std::string> UntypedTuple;

  /** Execute select query and return untyped results.
   * @throw std::runtime_error in a case of error
   * @return bunch of strings with results
   */
  std::vector<UntypedTuple> select_query(const char* query) const;
};

}  // namespace stdb

#endif  // STDB_CORE_SERVER_META_STORAGE_H_

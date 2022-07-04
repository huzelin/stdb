/**
 * \file server_meta_storage.cc
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
 *
 */
#include "stdb/core/server_meta_storage.h"

#include <sstream>

#include <boost/lexical_cast.hpp>
#include <boost/exception/diagnostic_information.hpp>

#include <sqlite3.h>  // to set trace callback
#include "stdb/common/exception.h"

namespace stdb {

static void callback_adapter(void*, const char* input) {
  std::string msg;
  size_t len_limit = std::min(size_t(0x2000u), msg.max_size());
  size_t len = std::min(strlen(input), len_limit);
  msg.assign(input, input + len);
  if (len == len_limit) {
    msg.append("...;");
  }
  LOG(INFO) << msg;
}

ServerMetaStorage::ServerMetaStorage(const char* db)
    : pool_(nullptr, &delete_apr_pool)
    , driver_(nullptr)
    , handle_(nullptr, AprHandleDeleter(nullptr)) {
  apr_pool_t *pool = nullptr;
  auto status = apr_pool_create(&pool, NULL);
  if (status != APR_SUCCESS) {
    // report error (can't return error from c-tor)
    LOG(FATAL) << "Can't create memory pool";
  }
  pool_.reset(pool);

  status = apr_dbd_get_driver(pool, "sqlite3", &driver_);
  if (status != APR_SUCCESS) {
    LOG(FATAL) << "Can't load driver, maybe libaprutil1-dbd-sqlite3 isn't installed";
  }

  apr_dbd_t *handle = nullptr;
  status = apr_dbd_open(driver_, pool, db, &handle);
  if (status != APR_SUCCESS) {
    LOG(FATAL) << "Can't open database, check file path:" << db;
  }
  handle_ = HandleT(handle, AprHandleDeleter(driver_));

  auto sqlite_handle = apr_dbd_native_handle(driver_, handle);
  sqlite3_trace(static_cast<sqlite3*>(sqlite_handle), callback_adapter, nullptr);

  create_tables();

  // Create prepared statement
  const char* query = "INSERT INTO stdb_series (series_id, keyslist, storage_id, lon, lat) VALUES (%s, %s, %d, %f, %f)";
  status = apr_dbd_prepare(driver_, pool_.get(), handle_.get(), query, "INSERT_SERIES_NAME", &insert_);
  if (status != 0) {
    LOG(ERROR) << "Error creating prepared statement";
    LOG(FATAL) << apr_dbd_error(driver_, handle_.get(), status);
  }
}

void ServerMetaStorage::create_tables() {
  const char* query = nullptr;

  // Create configuration table (key-value-commmentary)
  query =
      "CREATE TABLE IF NOT EXISTS stdb_configuration("
      "name TEXT UNIQUE,"
      "value TEXT,"
      "comment TEXT"
      ");";
  execute_query(query);

  query =
      "CREATE TABLE IF NOT EXISTS stdb_series("
      "id INTEGER PRIMARY KEY UNIQUE,"
      "series_id TEXT,"
      "keyslist TEXT,"
      "storage_id INTEGER UNIQUE,"
      "lon REAL,"
      "lat REAL"
      ");";
  execute_query(query);
}

void ServerMetaStorage::init_config(const char* db_name,
                                  const char* creation_datetime,
                                  const char* bstore_type) {
  // Create table and insert data into it
  std::stringstream insert;
  insert << "INSERT INTO stdb_configuration (name, value, comment)" << std::endl;
  insert << "\tVALUES ('creation_datetime', '" << creation_datetime << "', " << "'DB creation time.'), "
      << "('blockstore_type', '" << bstore_type << "', " << "'Type of block storage used.'),"
#ifdef STDB_VERSION
      << "('storage_version', '" << STDB_VERSION << "', " << "'STDB version used to create the database.'),"
#endif
      << "('db_name', '" << db_name << "', " << "'Name of DB instance.');"
      << std::endl;
  std::string insert_query = insert.str();
  execute_query(insert_query);
}

bool ServerMetaStorage::get_config_param(const std::string name, std::string* result) {
  // Read requested config
  std::stringstream query;
  query << "SELECT value FROM stdb_configuration WHERE name='" << name << "'";
  auto results = select_query(query.str().c_str());
  if (results.size() != 1) {
    LOG(INFO) << "Can't find configuration parameter `" + name + "`";
    return false;
  }
  auto tuple = results.at(0);
  if (tuple.size() != 1) {
    LOG(FATAL) << "Invalid configuration query (" + name + ")";
  }
  // This value can be encoded as dobule by the sqlite engine
  *result = tuple.at(0);
  return true;
}

boost::optional<i64> ServerMetaStorage::get_prev_largest_id() {
  auto query_max = "SELECT max(abs(storage_id)) FROM stdb_series;";
  i64 max_id = 0;
  try {
    auto results = select_query(query_max);
    auto row = results.at(0);
    if (row.empty()) {
      LOG(FATAL) << "Can't get max storage id";
    }
    auto id = row.at(0);
    if (id == "") {
      // Table is empty
      return boost::optional<i64>();
    }
    max_id = boost::lexical_cast<i64>(id);
  } catch(...) {
    LOG(ERROR) << boost::current_exception_diagnostic_information().c_str();
    LOG(FATAL) << "Can't get max storage id";
  }
  return max_id;
}

std::string ServerMetaStorage::get_dbname() {
  std::string dbname;
  bool success = get_config_param("db_name", &dbname);
  if (!success) {
    LOG(FATAL) << "Configuration parameter 'db_name' is missing";
  }
  return dbname;
}

common::Status ServerMetaStorage::wait_for_sync_request(int timeout_us) {
  std::unique_lock<std::mutex> lock(sync_lock_);
  auto res = sync_cvar_.wait_for(lock, std::chrono::microseconds(timeout_us));
  if (res == std::cv_status::timeout) {
    return common::Status::Timeout();
  }
  return common::Status::Retry();
}

void ServerMetaStorage::sync_with_metadata_storage(
    std::function<void(std::vector<SeriesT>*, std::vector<Location>*)> pull_new_series) {
  // Make temporary copies under the lock
  std::vector<PlainSeriesMatcher::SeriesNameT>           newnames;
  std::vector<Location>                 locations;
  
  pull_new_series(&newnames, &locations);

  // This lock is needed to prevent race condition during log replay.
  // When log replay completes, recovery procedure have to start synchronization
  // from another thread. If previous transaction wasn't finished yet it will
  // try to spaun another one but sqlite doesn't support nested transaction so
  // the result will be an error.
  // The performance shouldn't degrade during normal operation since the mutex
  // is locked only from a single thread and there is no contention at all.
  std::unique_lock<std::mutex> lock(tran_lock_);

  // Save new series
  begin_transaction();

  insert_new_series(std::move(newnames), std::move(locations));

  end_transaction();
}

void ServerMetaStorage::force_sync() {
  sync_cvar_.notify_one();
}

struct LightweightString {
  const char* str;
  int len;
};

static inline std::ostream& operator << (std::ostream& s, LightweightString val) {
  s.write(val.str, val.len);
  return s;
}

static bool split_series(const char* str, int n, LightweightString* outname, LightweightString* outkeys) {
  int len = 0;
  while(len < n && str[len] != ' ' && str[len] != '\t') {
    len++;
  }
  if (len >= n) {
    // Error
    return false;
  }
  outname->str = str;
  outname->len = len;
  // Skip space
  auto end = str + n;
  str = str + len;
  while (str < end && (*str == ' ' || *str == '\t')) {
    str++;
  }
  if (str == end) {
    // Error (no keys present)
    return false;
  }
  outkeys->str = str;
  outkeys->len = end - str;
  return true;
}

void ServerMetaStorage::insert_new_series(std::vector<SeriesT> &&items, std::vector<Location>&& locations) {
  if (items.size() == 0) {
    return;
  }
  // Write all data
  std::stringstream query;
  while (!items.empty()) {
    const size_t batchsize = 500; // This limit is defined by SQLITE_MAX_COMPOUND_SELECT
    const size_t newsize = items.size() > batchsize ? items.size() - batchsize : 0;
    std::vector<ServerMetaStorage::SeriesT> batch(items.begin() + static_cast<ssize_t>(newsize), items.end());
    items.resize(newsize);

    const size_t location_newsize = locations.size() > batchsize ? locations.size() - batchsize : 0;
    std::vector<Location> batch_location(locations.begin() + static_cast<ssize_t>(location_newsize), locations.end());
    locations.resize(location_newsize);

    query << "INSERT INTO stdb_series (series_id, keyslist, storage_id, lon, lat)" << std::endl;
    bool first = true;
    for (size_t i = 0; i < batch.size(); ++i) {
      auto& item = batch[i];
      LightweightString name, keys;
      auto lon = i < batch_location.size() ? batch_location[i].lon : 0.0;
      auto lat = i < batch_location.size() ? batch_location[i].lat : 0.0;
      auto stid = std::get<2>(item);
      if (split_series(std::get<0>(item), std::get<1>(item), &name, &keys)) {
        if (first) {
          query << "\tSELECT '" << name << "' as series_id, '"
              << keys << "' as keyslist, "
              << stid << "  as storage_id, "
              << lon << " as lon, "
              << lat << " as lat"
              << std::endl;
          first = false;
        } else {
          query << "\tUNION "
              <<   "SELECT '" << name << "', '"
              << keys << "', "
              << stid << ", "
              << lon << ", "
              << lat
              << std::endl;
        }
      }
    }
    query << ";\n";
  }
  std::string full_query = query.str();
  execute_query(full_query);
}

void ServerMetaStorage::begin_transaction() {
  execute_query("BEGIN TRANSACTION;");
}

void ServerMetaStorage::end_transaction() {
  execute_query("END TRANSACTION;");
}

int ServerMetaStorage::execute_query(std::string query) {
  int nrows = -1;
  int status = apr_dbd_query(driver_, handle_.get(), &nrows, query.c_str());
  if (status != 0 && status != 21) {
    // generate error and throw
    LOG(ERROR) << "Error executing query";
    LOG(FATAL) << apr_dbd_error(driver_, handle_.get(), status);
  }
  return nrows;
}

std::vector<ServerMetaStorage::UntypedTuple> ServerMetaStorage::select_query(const char* query) const {
  std::vector<UntypedTuple> tuples;
  apr_dbd_results_t *results = nullptr;
  int status = apr_dbd_select(driver_, pool_.get(), handle_.get(), &results, query, 0);
  if (status != 0) {
    LOG(ERROR) << "Error executing query";
    LOG(FATAL) << apr_dbd_error(driver_, handle_.get(), status);
  }
  // get rows
  int ntuples = apr_dbd_num_tuples(driver_, results);
  int ncolumns = apr_dbd_num_cols(driver_, results);
  for (int i = ntuples; i --> 0;) {
    apr_dbd_row_t *row = nullptr;
    status = apr_dbd_get_row(driver_, pool_.get(), results, &row, -1);
    if (status != 0) {
      LOG(ERROR) << "Error getting row from resultset";
      LOG(FATAL) << apr_dbd_error(driver_, handle_.get(), status);
    }
    UntypedTuple tup;
    for (int col = 0; col < ncolumns; col++) {
      const char* entry = apr_dbd_get_entry(driver_, row, col);
      if (entry) {
        tup.emplace_back(entry);
      } else {
        tup.emplace_back();
      }
    }
    tuples.push_back(std::move(tup));
  }
  return tuples;
}

common::Status ServerMetaStorage::load_matcher_data(SeriesMatcherBase& matcher) {
  auto query = "SELECT series_id || ' ' || keyslist, storage_id, lon, lat FROM stdb_series;";
  try {
    auto results = select_query(query);
    for(auto row: results) {
      if (row.size() != 2) {
        continue;
      }
      auto series = row.at(0);
      auto id = boost::lexical_cast<i64>(row.at(1));
      Location location;
      location.lon = boost::lexical_cast<LocationType>(row.at(2));
      location.lat = boost::lexical_cast<LocationType>(row.at(3));
      matcher._add(series, id, location);
    }
  } catch(...) {
    LOG(ERROR) << boost::current_exception_diagnostic_information().c_str();
    return common::Status::General();
  }
  return common::Status::Ok();
}

}  // namespace stdb

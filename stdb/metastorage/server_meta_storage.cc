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
#include "stdb/metastorage/server_meta_storage.h"

#include <sstream>

#include <boost/lexical_cast.hpp>
#include <boost/exception/diagnostic_information.hpp>

#include <sqlite3.h>  // to set trace callback
#include "stdb/common/exception.h"

namespace stdb {

ServerMetaStorage::ServerMetaStorage(const char* db, bool is_moving)
    : MetaStorage(db), is_moving_(is_moving) {
  create_server_tables();

  /*
  // Create prepared statement
  const char* query = "INSERT INTO stdb_series (series_id, keyslist, storage_id, lon, lat) VALUES (%s, %s, %d, %f, %f)";
  auto status = apr_dbd_prepare(driver_, pool_.get(), handle_.get(), query, "INSERT_SERIES_NAME", &insert_);
  if (status != 0) {
    LOG(ERROR) << "Error creating prepared statement";
    LOG(FATAL) << apr_dbd_error(driver_, handle_.get(), status);
  }*/
}

void ServerMetaStorage::create_server_tables() {
  const char* query = nullptr;
  if (is_moving_) {
    query =
        "CREATE TABLE IF NOT EXISTS stdb_series("
        "id INTEGER PRIMARY KEY UNIQUE,"
        "series_id TEXT,"
        "keyslist TEXT,"
        "storage_id INTEGER UNIQUE"
        ");";
  } else {
    query =
        "CREATE TABLE IF NOT EXISTS stdb_series("
        "id INTEGER PRIMARY KEY UNIQUE,"
        "series_id TEXT,"
        "keyslist TEXT,"
        "storage_id INTEGER UNIQUE,"
        "lon REAL,"
        "lat REAL"
        ");";
  }
  execute_query(query);
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

void ServerMetaStorage::sync_with_metadata_storage(
    std::function<void(std::vector<SeriesT>*)> pull_new_series) {
  std::vector<SeriesMatcher::SeriesNameT>           newnames;

  pull_new_series(&newnames);
  if (newnames.empty()) return;

  std::unique_lock<std::mutex> lock(tran_lock_);

  // Save new series
  begin_transaction();
  
  insert_new_series(std::move(newnames));

  end_transaction();
}

void ServerMetaStorage::sync_with_metadata_storage(
    std::function<void(std::vector<SeriesT>*, std::vector<Location>*)> pull_new_series) {
  // Make temporary copies under the lock
  std::vector<SeriesMatcher::SeriesNameT>           newnames;
  std::vector<Location>                 locations;
  
  pull_new_series(&newnames, &locations);
  if (newnames.empty() || newnames.size() != locations.size()) {
    return;
  }

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

void ServerMetaStorage::insert_new_series(std::vector<SeriesT>&& items) {
  if (items.empty()) {
    return;
  }
  std::stringstream query;
  while (!items.empty()) {
    const size_t batchsize = 500; // This limit is defined by SQLITE_MAX_COMPOUND_SELECT
    const size_t newsize = items.size() > batchsize ? items.size() - batchsize : 0;
    std::vector<MetaStorage::SeriesT> batch(items.begin() + static_cast<ssize_t>(newsize), items.end());
    items.resize(newsize);

    query << "INSERT INTO stdb_series (series_id, keyslist, storage_id)" << std::endl;
    bool first = true;
    for (size_t i = 0; i < batch.size(); ++i) {
      auto& item = batch[i];
      LightweightString name, keys;
      auto stid = std::get<2>(item);
      if (split_series(std::get<0>(item), std::get<1>(item), &name, &keys)) {
        if (first) {
          query << "\tSELECT '" << name << "' as series_id, '"
              << keys << "' as keyslist, "
              << stid << "  as storage_id"
              << std::endl;
          first = false;
        } else {
          query << "\tUNION "
              <<   "SELECT '" << name << "', '"
              << keys << "', "
              << stid
              << std::endl;
        }
      }
    }
    query << ";\n";
  }
  std::string full_query = query.str();
  execute_query(full_query);
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
    std::vector<MetaStorage::SeriesT> batch(items.begin() + static_cast<ssize_t>(newsize), items.end());
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

common::Status ServerMetaStorage::load_matcher_data(SeriesMatcher& matcher) {
  const char* query = nullptr;
  if (is_moving_) {
    query = "SELECT series_id || ' ' || keyslist, storage_id FROM stdb_series;";
  } else {
    query = "SELECT series_id || ' ' || keyslist, storage_id, lon, lat FROM stdb_series;";
  }
  try {
    auto results = select_query(query);
    LOG(INFO) << "load matcher data size=" << results.size();
    for(auto row: results) {
      if (is_moving_) {
        if (row.size() != 2) continue;
      } else {
        if (row.size() != 4) continue;
      }
      auto series = row.at(0);
      auto id = boost::lexical_cast<i64>(row.at(1));
      if (is_moving_) {
        matcher._add(series, id);
      } else {
        Location location;
        location.lon = boost::lexical_cast<LocationType>(row.at(2));
        location.lat = boost::lexical_cast<LocationType>(row.at(3));
        matcher._add(series, location, id);
      }
    }
  } catch(...) {
    LOG(ERROR) << boost::current_exception_diagnostic_information().c_str();
    return common::Status::General();
  }
  return common::Status::Ok();
}

}  // namespace stdb

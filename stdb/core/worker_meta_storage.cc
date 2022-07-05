/**
 * \file worker_meta_storage.cc
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
#include "stdb/core/worker_meta_storage.h"

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

WorkerMetaStorage::WorkerMetaStorage(const char* db, const char* db_name, std::shared_ptr<Synchronization>& synchronization)
    : pool_(nullptr, &delete_apr_pool)
      , driver_(nullptr)
      , handle_(nullptr, AprHandleDeleter(nullptr))
      , db_name_(db_name)
      , synchronization_(synchronization) {
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
}

void WorkerMetaStorage::create_tables() {
  const char* query = nullptr;

  // Create volumes table
  query =
      "CREATE TABLE IF NOT EXISTS stdb_volumes("
      "id INTEGER UNIQUE,"
      "path TEXT UNIQUE,"
      // Content of the metadata volume moved to sqlite
      "version INTEGER,"
      "nblocks INTEGER,"
      "capacity INTEGER,"
      "generation INTEGER"
      ");";
  execute_query(query);

  query =
      "CREATE TABLE IF NOT EXISTS stdb_rescue_points("
      "storage_id INTEGER PRIMARY KEY UNIQUE,"
      "addr0 INTEGER,"
      "addr1 INTEGER,"
      "addr2 INTEGER,"
      "addr3 INTEGER,"
      "addr4 INTEGER,"
      "addr5 INTEGER,"
      "addr6 INTEGER,"
      "addr7 INTEGER"
      ");";
  execute_query(query);
}

void WorkerMetaStorage::init_volumes(const std::vector<VolumeDesc>& volumes) {
  std::stringstream query;
  query << "INSERT INTO stdb_volumes (id, path, version, nblocks, capacity, generation)" << std::endl;
  bool first = true;
  for (auto desc: volumes) {
    if (first) {
      query << "\tSELECT "
          << desc.id << " as id, '"
          << desc.path << "' as path, '"
          << desc.version << "' as version, "
          << desc.nblocks << " as nblocks, "
          << desc.capacity << " as capacity, "
          << desc.generation << " as generation"
          << std::endl;
      first = false;
    } else {
      query << "\tUNION SELECT "
          << desc.id << ", '"
          << desc.path << "', '"
          << desc.version << "', "
          << desc.nblocks << ", "
          << desc.capacity << ", "
          << desc.generation
          << std::endl;
    }
  }
  std::string full_query = query.str();
  execute_query(full_query);
}

std::vector<WorkerMetaStorage::VolumeDesc> WorkerMetaStorage::get_volumes() {
  const char* query =
      "SELECT id, path, version, nblocks, capacity, generation FROM stdb_volumes;";
  std::vector<VolumeDesc> tuples;
  std::vector<UntypedTuple> untyped = select_query(query);
  // get rows
  auto ntuples = untyped.size();
  for (size_t i = 0; i < ntuples; i++) {
    VolumeDesc desc;
    desc.id = boost::lexical_cast<u32>(untyped.at(i).at(0));
    desc.path = untyped.at(i).at(1);
    desc.version = boost::lexical_cast<u32>(untyped.at(i).at(2));
    desc.nblocks = boost::lexical_cast<u32>(untyped.at(i).at(3));
    desc.capacity = boost::lexical_cast<u32>(untyped.at(i).at(4));
    desc.generation = boost::lexical_cast<u32>(untyped.at(i).at(5));
    tuples.push_back(desc);
  }
  return tuples;
}

void WorkerMetaStorage::add_volume(const VolumeDesc &vol) {
  std::string query =
      "INSERT INTO stdb_volumes (id, path, version, nblocks, capacity, generation) VALUES ";
  query += "(" + std::to_string(vol.id) + ", \"" + vol.path + "\", "
      + std::to_string(vol.version) + ", "
      + std::to_string(vol.nblocks) + ", "
      + std::to_string(vol.capacity) + ", "
      + std::to_string(vol.generation) + ");\n";
  LOG(INFO) << "Execute query:" << query;
  int rows = execute_query(query);
  if (rows == 0) {
    LOG(ERROR) << "Insert query failed: " + query + " - can't save the volume.";
  }
}

void WorkerMetaStorage::update_volume(const VolumeDesc& vol) {
  synchronization_->signal_action([&]() {
    pending_volumes_[vol.id] = vol;
  });
}

std::string WorkerMetaStorage::get_dbname() {
  return db_name_;
}

common::Status WorkerMetaStorage::load_rescue_points(std::unordered_map<u64, std::vector<u64>>& mapping) {
  auto query =
      "SELECT storage_id, addr0, addr1, addr2, addr3,"
      " addr4, addr5, addr6, addr7 "
      "FROM stdb_rescue_points;";
  try {
    auto results = select_query(query);
    for (auto row: results) {
      if (row.size() != 9) {
        continue;
      }
      auto series_id = boost::lexical_cast<u64>(row.at(0));
      if (errno == ERANGE) {
        LOG(ERROR) << "Can't parse series id, database corrupted";
        return common::Status::BadData();
      }
      std::vector<u64> addrlist;
      for (size_t i = 0; i < 8; i++) {
        auto addr = row.at(1 + i);
        if (addr.empty()) {
          break;
        } else {
          u64 uaddr;
          i64 iaddr = boost::lexical_cast<i64>(addr);
          if (iaddr < 0) {
            uaddr = ~0ull;
          } else {
            uaddr = static_cast<u64>(iaddr);
          }
          addrlist.push_back(uaddr);
        }
      }
      mapping[series_id] = std::move(addrlist);
    }
  } catch(...) {
    LOG(ERROR) << boost::current_exception_diagnostic_information().c_str();
    return common::Status::General();
  }
  return common::Status::Ok();
}

void WorkerMetaStorage::add_rescue_point(ParamId id, const std::vector<u64>& val) {
  synchronization_->signal_action([&]() {
    pending_rescue_points_[id] = val;                    
  });
}

void WorkerMetaStorage::sync_with_metadata_storage() {
  // Make temporary copies under the lock
  std::unordered_map<ParamId, std::vector<u64>> rescue_points;
  std::unordered_map<u32, VolumeDesc>               volume_records;
  {
    synchronization_->lock_action([&]() {
      std::swap(rescue_points, pending_rescue_points_);
      std::swap(volume_records, pending_volumes_);
    });
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

  // Save rescue points
  upsert_rescue_points(std::move(rescue_points));

  // Save volume records
  upsert_volume_records(std::move(volume_records));

  end_transaction();
}

void WorkerMetaStorage::upsert_rescue_points(std::unordered_map<ParamId, std::vector<u64>>&& input) {
  if (input.empty()) {
    return;
  }
  std::stringstream query;
  typedef std::pair<ParamId, std::vector<u64>> ValueT;
  std::vector<ValueT> items(input.begin(), input.end());
  while (!items.empty()) {
    const size_t batchsize = 500;  // This limit is defined by SQLITE_MAX_COMPOUND_SELECT
    const size_t newsize = items.size() > batchsize ? items.size() - batchsize : 0;
    std::vector<ValueT> batch(items.begin() + static_cast<ssize_t>(newsize), items.end());
    items.resize(newsize);
    query <<
        "INSERT OR REPLACE INTO stdb_rescue_points (storage_id, addr0, addr1, addr2, addr3, addr4, addr5, addr6, addr7) VALUES ";
    size_t ix = 0;
    for (auto const& kv: batch) {
      query << "( " << static_cast<i64>(kv.first);
      for (auto id: kv.second) {
        if (id == ~0ull) {
          // Values that big can't be represented in SQLite, -1 value should be interpreted as EMPTY_ADDR,
          query << ", -1";
        } else {
          query << ", " << id;
        }
      }
      for(auto i = kv.second.size(); i < 8; i++) {
        query << ", null";
      }
      query << ")";
      ix++;
      if (ix == batch.size()) {
        query << ";\n";
      } else {
        query << ",";
      }
    }
  }
  execute_query(query.str());
}

void WorkerMetaStorage::upsert_volume_records(std::unordered_map<u32, VolumeDesc>&& input) {
  if (input.empty()) {
    return;
  }
  std::stringstream query;
  std::vector<VolumeDesc> items;
  for (const auto& kv: input) {
    items.push_back(kv.second);
  }
  while (!items.empty()) {
    const size_t batchsize = 500;  // This limit is defined by SQLITE_MAX_COMPOUND_SELECT
    const size_t newsize = items.size() > batchsize ? items.size() - batchsize : 0;
    std::vector<VolumeDesc> batch(items.begin() + static_cast<ssize_t>(newsize), items.end());
    items.resize(newsize);
    query << "INSERT OR REPLACE INTO stdb_volumes (id, path, version, nblocks, capacity, generation) VALUES ";
    size_t ix = 0;
    for (auto const& vol: batch) {
      query << "(" << std::to_string(vol.id)         << ", '"
          << vol.path                       << "', "
          << std::to_string(vol.version)    << ", "
          << std::to_string(vol.nblocks)    << ", "
          << std::to_string(vol.capacity)   << ", "
          << std::to_string(vol.generation) << ")";
      ix++;
      if (ix == batch.size()) {
        query << ";\n";
      } else {
        query << ",";
      }
    }
  }
  execute_query(query.str());
}

void WorkerMetaStorage::begin_transaction() {
  execute_query("BEGIN TRANSACTION;");
}

void WorkerMetaStorage::end_transaction() {
  execute_query("END TRANSACTION;");
}

int WorkerMetaStorage::execute_query(std::string query) {
  int nrows = -1;
  int status = apr_dbd_query(driver_, handle_.get(), &nrows, query.c_str());
  if (status != 0 && status != 21) {
    // generate error and throw
    LOG(ERROR) << "Error executing query";
    LOG(FATAL) << apr_dbd_error(driver_, handle_.get(), status);
  }
  return nrows;
}

std::vector<WorkerMetaStorage::UntypedTuple> WorkerMetaStorage::select_query(const char* query) const {
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
    
}  // namespace stdb

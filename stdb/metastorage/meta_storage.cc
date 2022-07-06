/*!
 * \file meta_storage.cc
 */
#include "stdb/metastorage/meta_storage.h"

#include <sqlite3.h>  // to set trace callback

#include "stdb/common/exception.h"
#include "stdb/common/logging.h"

namespace stdb {

void MetaStorage::create_tables() {
  const char* query = nullptr;

  // Create configuration table (key-value-commmentary)
  query =
      "CREATE TABLE IF NOT EXISTS stdb_configuration("
      "name TEXT UNIQUE,"
      "value TEXT,"
      "comment TEXT"
      ");";
  execute_query(query);
}

void MetaStorage::begin_transaction() {
  execute_query("BEGIN TRANSACTION;");
}

void MetaStorage::end_transaction() {
  execute_query("END TRANSACTION;");
}

int MetaStorage::execute_query(std::string query) {
  int nrows = -1;
  int status = apr_dbd_query(driver_, handle_.get(), &nrows, query.c_str());
  if (status != 0 && status != 21) {
    // generate error and throw
    LOG(ERROR) << "Error executing query";
    LOG(FATAL) << apr_dbd_error(driver_, handle_.get(), status);
  }
  return nrows;
}

std::vector<MetaStorage::UntypedTuple> MetaStorage::select_query(const char* query) const {
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

MetaStorage::MetaStorage(const char* db)
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
}

MetaStorage::~MetaStorage() { }

void MetaStorage::init_config(const char* db_name,
                              const char* creation_datetime,
                              const char* bstore_type) {
  // Create table and insert data into it
  std::stringstream insert;
  insert << "INSERT INTO stdb_configuration (name, value, comment)" << std::endl;
  insert << "\tVALUES ('creation_datetime', '" << creation_datetime << "', " << "'DB creation time.'), "
      << "('blockstore_type', '" << bstore_type << "', " << "'Type of block storage used.'),"
      << "('storage_version', '" << STDB_VERSION << "', " << "'STDB version used to create the database.'),"
      << "('db_name', '" << db_name << "', " << "'Name of DB instance.');"
      << std::endl;
  std::string insert_query = insert.str();
  execute_query(insert_query);
}

bool MetaStorage::set_config_param(const std::string& param_name, const std::string& value, const std::string& comment) {
  std::stringstream insert;
  insert << "INSERT INTO stdb_configuration (name, value, comment)" << std::endl;
  insert << "\tVALUES ('" << param_name << "', '" << value << "', '" << comment << "');" << std::endl;
  std::string insert_query = insert.str();
  auto nrows = execute_query(insert_query);
  return nrows == 1;
}

bool MetaStorage::get_config_param(const std::string& name, std::string* result) {
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

std::string MetaStorage::get_database_name() {
  std::string dbname;
  bool success = get_config_param("db_name", &dbname);
  if (!success) {
    STDB_THROW("Configuration parameter 'db_name' is missing");
  }
  return dbname;
}

std::string MetaStorage::get_creation_datetime() {
  std::string creation_datetime;
  bool success = get_config_param("creation_datetime", &creation_datetime);
  if (!success) {
    STDB_THROW("Configuration parameter 'creation_datetime' is missing");
  }
  return creation_datetime;
}

std::string MetaStorage::get_bstore_type() {
  std::string bstore_type;
  bool success = get_config_param("blockstore_type", &bstore_type);
  if (!success) {
    STDB_THROW("Configuration parameter 'blockstore_type' is missing");
  }
  return bstore_type;
}

}  // namespace stdb

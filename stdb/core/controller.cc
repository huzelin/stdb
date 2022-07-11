/*
 * \file controller.cc
 */
#include "stdb/core/controller.h"

#include <sqlite3.h>  // to set trace callback

#include <boost/lexical_cast.hpp>
#include "stdb/core/standalone_database.h"

namespace stdb {

void Controller::start_sync_worker() {
  std::thread sync_worker_thread([this]() { this->start_sync_worker_loop(); });
  sync_worker_thread.detach();
}

void Controller::start_sync_worker_loop() {
  enum {
    SYNC_REQUEST_TIMEOUT = 10000,
  };
  while (done_.load() == 0) {
    auto status = synchronization_->wait_for(SYNC_REQUEST_TIMEOUT);
    if (!status.IsOk()) {
      DLOG(INFO) << "sync_worker_loop timeout";
      continue;
    }
    sync_action();
  }
  close_barrier_.wait();
}

void Controller::sync_action() {
  std::vector<std::shared_ptr<Database>> all_databases = get_alldatabases();
  for (auto database : all_databases) {
    if (database) {
      database->sync();
    }
  }
  sync_waiter_->notify_all();
}

std::shared_ptr<Database> Controller::open_standalone_database(const char* dbname) {
  std::lock_guard<std::mutex> lck(mutex_);

  auto database = get_database(dbname);
  if (database) return database;

  std::string metadata_path;
  bool ismoving = false;
  if (!get_config_database(dbname, &metadata_path, &ismoving)) {
    return database;
  }

  std::string server_path = metadata_path + "/server/" + dbname + ".stdb";
  std::string worker_path = metadata_path + "/worker/" + dbname + ".stdb";
  std::string input_log_path = metadata_path + "/inputlog/" + dbname;

  FineTuneParams fine_tune_params;
  fine_tune_params.input_log_path = input_log_path.c_str();

  std::shared_ptr<Database> new_db(
      new StandaloneDatabase(server_path.c_str(),
                             worker_path.c_str(),
                             fine_tune_params,
                             synchronization_,
                             sync_waiter_,
                             ismoving));
  new_db->initialize(fine_tune_params);
  register_database(dbname, new_db);

  return new_db;
}

void Controller::close_database(const char* dbname) {
  std::lock_guard<std::mutex> lck(mutex_);
  auto database = get_database(dbname);
  if (database) {
    database->close();
    unregister_database(dbname);
  }
}

common::Status Controller::new_standalone_database(
    bool ismoving,
    const char* base_file_name,
    const char* metadata_path,
    const char* volumes_path,
    i32 num_volumes,
    u64 volume_size,
    bool allocate) {
  auto status = StandaloneDatabase::new_database(
      ismoving, base_file_name, metadata_path, volumes_path, num_volumes, volume_size, allocate);
  if (!status.IsOk()) return status;

  if (!set_config_database(ismoving, base_file_name, metadata_path)) {
    return common::Status::Internal();
  }
  return common::Status::Ok();
}

void Controller::close() {
  done_.store(1);
  sync_action();
  close_barrier_.wait();

  std::vector<std::shared_ptr<Database>> all_databases = get_alldatabases();
  for (auto database : all_databases) {
    if (database) {
      database->close();
    }
  }
}

std::vector<std::shared_ptr<Database>> Controller::get_alldatabases() {
  std::lock_guard<std::mutex> lck(mutex_);
  return database_;
}

common::Status Controller::register_database(const std::string& db_name, std::shared_ptr<Database> database) {
  name2index_[db_name] = database_.size();
  database_.emplace_back(database);
  return common::Status::Ok();
}

void Controller::unregister_database(const std::string& db_name) {
  auto iter = name2index_.find(db_name);
  if (iter != name2index_.end()) {
    database_[iter->second] = std::shared_ptr<Database>();
  }
  name2index_.erase(db_name);
}

std::shared_ptr<Database> Controller::get_database(const std::string& db_name) {
  auto iter = name2index_.find(db_name);
  if (iter == name2index_.end()) {
    static std::shared_ptr<Database> kNull;
    return kNull;
  } else {
    return database_[iter->second];
  }
}

bool Controller::set_config_database(bool ismoving, const char* dbname, const char* meta_path) {
  std::stringstream insert;
  insert << "INSERT INTO stdb_databases (dbname, metapath, ismoving)" << std::endl;
  insert << "\tVALUES ('" << dbname << "', '" << meta_path << "', '" << (ismoving ? 1 : 0) << "');" << std::endl;
  std::string insert_query = insert.str();
  auto nrows = execute_query(insert_query);
  return nrows == 1;
}

bool Controller::get_config_database(const char* dbname, std::string* meta_path, bool* ismoving) {
  std::stringstream query;
  query << "SELECT metapath, ismoving FROM stdb_databases WHERE dbname='" << dbname << "'";
  auto results = select_query(query.str().c_str());
  if (results.size() != 1) {
    LOG(INFO) << "Can't find stdb_databases parameter `" << dbname << "`";
    return false;
  }
  auto tuple = results.at(0);
  if (tuple.size() != 2) {
    LOG(FATAL) << "Invalid configuration query (" << dbname << ")";
  }
  // This value can be encoded as dobule by the sqlite engine
  *meta_path = tuple.at(0);
  *ismoving = boost::lexical_cast<i64>(tuple.at(1));
  return true;
}

void Controller::create_tables() {
  const char* query = nullptr;

  // Create configuration table (key-value-commmentary)
  query =
      "CREATE TABLE IF NOT EXISTS stdb_databases("
      "dbname TEXT UNIQUE,"
      "metapath TEXT,"
      "ismoving INTEGER"
      ");";
  execute_query(query);
}

void Controller::begin_transaction() {
  execute_query("BEGIN TRANSACTION;");
}

void Controller::end_transaction() {
  execute_query("END TRANSACTION;");
}

int Controller::execute_query(std::string query) {
  int nrows = -1;
  int status = apr_dbd_query(driver_, handle_.get(), &nrows, query.c_str());
  if (status != 0 && status != 21) {
    // generate error and throw
    LOG(ERROR) << "Error executing query";
    LOG(FATAL) << apr_dbd_error(driver_, handle_.get(), status);
  }
  return nrows;
}

std::vector<Controller::UntypedTuple> Controller::select_query(const char* query) const {
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

Controller::Controller() :
    pool_(nullptr, &delete_apr_pool),
    driver_(nullptr),
    handle_(nullptr, AprHandleDeleter(nullptr)),
    done_(0),
    close_barrier_(2) { }

void Controller::init(const char* config_path) {
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

  std::string db_path = common::GetHomeDir() + "/.stdbrc";
  status = apr_dbd_open(driver_, pool, db_path.c_str() , &handle);
  if (status != APR_SUCCESS) {
    LOG(FATAL) << "Can't open database, check file path:" << db_path;
  }
  handle_ = HandleT(handle, AprHandleDeleter(driver_));

  auto sqlite_handle = apr_dbd_native_handle(driver_, handle);
  sqlite3_trace(static_cast<sqlite3*>(sqlite_handle), callback_adapter, nullptr);

  create_tables();
      
  synchronization_.reset(new Synchronization());
  sync_waiter_.reset(new SyncWaiter());
  start_sync_worker();
}

}  // namespace stdb

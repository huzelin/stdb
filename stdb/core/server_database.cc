/*!
 * \file server_database.cc
 */
#include "stdb/core/server_database.h"

#include "stdb/core/recovery_visitor.h"

namespace stdb {

ServerDatabase::ServerDatabase(bool is_moving) : Database(is_moving) {
  metadata_.reset(new ServerMetaStorage(":memory", is_moving));
}

ServerDatabase::ServerDatabase(const char* path, const FineTuneParams &params, bool is_moving)
  : Database(is_moving) {
  metadata_.reset(new ServerMetaStorage(path, is_moving));

  // Update series matcher
  auto baseline = metadata_->get_prev_largest_id();
  if (baseline) {
    global_matcher_.series_id = baseline.get() + 1;
  }
  auto status = metadata_->load_matcher_data(global_matcher_);
  if (!status.IsOk()) {
    LOG(FATAL) << "Cann't read series names";
  }
}

bool ServerDatabase::init_series_id(const char* begin,
                                    const char* end,
                                    u64* sid,
                                    PlainSeriesMatcher* local_matcher,
                                    storage::InputLog* ilog,
                                    SyncWaiter* sync_waiter) {
  u64 id = 0;
  bool create_new = false;
  {
    std::lock_guard<std::mutex> guard(lock_);
    id = static_cast<u64>(global_matcher_.match(begin, end));
    if (id == 0) {
      // create new series
      id = static_cast<u64>(global_matcher_.add(begin, end));
      create_new = true;
      // write wal for server meta storage.
      write_wal(ilog, id, begin, end - begin, sync_waiter);
    }
  }
  *sid = id;
  local_matcher->_add(begin, end, id);
  return create_new;
}

bool ServerDatabase::init_series_id(const char* begin,
                                    const char* end,
                                    const Location& location,
                                    u64* sid,
                                    PlainSeriesMatcher* local_matcher,
                                    storage::InputLog* ilog,
                                    SyncWaiter* sync_waiter) {

  u64 id = 0;
  bool create_new = false;
  {
    std::lock_guard<std::mutex> guard(lock_);
    id = static_cast<u64>(global_matcher_.match(begin, end));
    if (id == 0) {
      // create new series
      id = static_cast<u64>(global_matcher_.add(begin, end, location));
      create_new = true;
      // write wal for server meta storage
      write_wal(ilog, id, begin, end - begin, sync_waiter);
    }
  }
  *sid = id;
  local_matcher->_add(begin, end, location, id);
  return create_new;
}

int ServerDatabase::get_series_name(ParamId id, char* buffer, size_t buffer_size, PlainSeriesMatcher *local_matcher) {
  auto str = global_matcher_.id2str(static_cast<i64>(id));
  if (str.first == nullptr) {
    return 0;
  }
  // copy value to local matcher
  local_matcher->_add(str.first, str.first + str.second, static_cast<i64>(id));
  // copy the string to out buffer
  if (str.second > buffer_size) {
    return -1 * static_cast<int>(str.second);
  }
  memcpy(buffer, str.first, static_cast<size_t>(str.second));
  return static_cast<int>(str.second);
}

void ServerDatabase::trigger_meta_sync() {
  if (is_moving()) {
    auto get_names = [this](std::vector<PlainSeriesMatcher::SeriesNameT>* names) {
      std::lock_guard<std::mutex> guard(lock_);
      global_matcher_.pull_new_series(names);
    };
    metadata_->sync_with_metadata_storage(get_names);
  } else {
    auto get_names_and_locations = [this](std::vector<PlainSeriesMatcher::SeriesNameT>* names,
                            std::vector<Location>* locations) {
      std::lock_guard<std::mutex> guard(lock_);
      global_matcher_.pull_new_series(names, locations);
    };
    metadata_->sync_with_metadata_storage(get_names_and_locations);
  }
}

void ServerDatabase::close() {
  this->trigger_meta_sync();
}

void ServerDatabase::sync() {
  this->trigger_meta_sync();
}

common::Status ServerDatabase::new_database(bool is_moving,
                                            const char* base_file_name,
                                            const char* metadata_path,
                                            const char* bstore_type) {
  boost::filesystem::path metpath(metadata_path);
  metpath = boost::filesystem::absolute(metpath);

  std::string sqlitebname = std::string(base_file_name) + ".stdb";
  boost::filesystem::path sqlitepath = metpath / sqlitebname;

  if (!boost::filesystem::exists(metpath)) {
    LOG(INFO) << std::string(metadata_path) + " doesn't exists, trying to create directory";
    boost::filesystem::create_directories(metpath);
  } else {
    if (!boost::filesystem::is_directory(metpath)) {
      LOG(ERROR) << metadata_path << " is not a directory";
      return common::Status::BadArg();
    }
  }

  if (boost::filesystem::exists(sqlitepath)) {
    LOG(ERROR) << "Database is already exists";
    return common::Status::BadArg();
  }

  try {
    auto storage = std::make_shared<ServerMetaStorage>(sqlitepath.c_str(), is_moving);

    auto now = apr_time_now();
    char date_time[0x100];
    apr_rfc822_date(date_time, now);

    storage->init_config(base_file_name, date_time, bstore_type);
  } catch (std::exception const& err) {
    LOG(ERROR) << "Can't create metadata file " << sqlitepath << ", the error is: " << err.what();
    return common::Status::BadArg();
  }
  return common::Status::Ok();
}

common::Status ServerDatabase::remove_database(const char* file_name, const char* wal_path) {
  auto perms = boost::filesystem::status(file_name).permissions();
  if ((perms & boost::filesystem::owner_write) == 0) {
    return common::Status::EAccess();
  }
  if (!boost::filesystem::remove(file_name)) {
    LOG(ERROR) << file_name << " file is not deleted!";
  } else {
    LOG(INFO) << file_name << " was deleted";
  }

  common::Status status;
  int card;
  std::tie(status, card) = storage::ShardedInputLog::find_logs(wal_path);
  if (status.IsOk() && card >  0) {
    auto ilog = std::make_shared<storage::ShardedInputLog>(card, wal_path);
    ilog->delete_files();
  }
  return common::Status::Ok();
}

void ServerDatabase::write_wal(storage::InputLog* ilog, ParamId id, const char* begin, u32 size, SyncWaiter* sync_waiter) {
  if (ilog == nullptr) return;

  std::vector<ParamId> staleids;
  auto status = ilog->append(id, begin, size, &staleids);
  if (status.Code() == common::Status::kOverflow) {
    if (!staleids.empty()) {
      // before roate(delete) the oldest wal file, we must wait meta data is
      // backedup.
      std::promise<void> barrier;
      std::future<void> future = barrier.get_future();
      sync_waiter->add_sync_barrier(std::move(barrier));
      future.wait();
    }
    ilog->rotate();
  }
}

void ServerDatabase::run_recovery(const FineTuneParams &params, Database* database) {
  int ccr = 0;
  if (!wal_recovery_is_enabled(params, &ccr)) {
    return;
  }

  std::vector<ParamId> new_ids;
  auto ilog = std::make_shared<storage::ShardedInputLog>(ccr, params.input_log_path);
  run_inputlog_metadata_recovery(ilog.get(), &new_ids, database);

  sync();
}

void ServerDatabase::run_inputlog_metadata_recovery(
    storage::ShardedInputLog* ilog,
    std::vector<ParamId>* restored_ids,
    Database* database) {
  ServerRecoveryVisitor visitor;
  visitor.server_database = database;
  visitor.restored_ids = restored_ids;
  bool proceed = true;
  size_t nitems = 0x1000;
  u64 nsegments = 0;
  std::vector<storage::InputLogRow> rows(nitems);
  LOG(INFO) << "WAL meta data recovery started";

  while (proceed) {
    common::Status status;
    u32 outsize;
    std::tie(status, outsize) = ilog->read_next(nitems, rows.data());
    if (status.IsOk() || (status.Code() == common::Status::kNoData && outsize > 0)) {
      for (u32 ix = 0; ix < outsize; ix++) {
        const storage::InputLogRow& row = rows.at(ix);
        visitor.reset(row.id);
        proceed = row.payload.apply_visitor(visitor);
      }
      nsegments++;
    } else if (status.Code() == common::Status::kNoData) {
      LOG(INFO) << "WAL metadata recovery completed";
      LOG(INFO) << std::to_string(nsegments) + " segments scanned";
      proceed = false;
    } else {
      LOG(ERROR) << "WAL recovery error: " << status.ToString();
      proceed = false;
    }
  }
  ilog->reopen();
  ilog->delete_files();
}

}  // namespace stdb

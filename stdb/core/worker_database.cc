/*!
 * \file worker_database.cc
 */
#include "stdb/core/worker_database.h"

#include "stdb/core/recovery_visitor.h"

namespace stdb {

static apr_status_t create_metadata_page(
    const char* db_name,
    const char* file_name,
    std::vector<std::string> const& page_file_names,
    std::vector<u32> const& capacities,
    const char* bstore_type ) {
  try {
    std::shared_ptr<Synchronization> null_sync;
    auto storage = std::make_shared<WorkerMetaStorage>(file_name, null_sync);

    auto now = apr_time_now();
    char date_time[0x100];
    apr_rfc822_date(date_time, now);

    storage->init_config(db_name, date_time, bstore_type);

    std::vector<WorkerMetaStorage::VolumeDesc> desc;
    u32 ix = 0;
    for(auto str: page_file_names) {
      WorkerMetaStorage::VolumeDesc volume;
      volume.path = str;
      volume.generation = ix;
      volume.capacity = capacities[ix];
      volume.id = ix;
      volume.nblocks = 0;
      volume.version = STDB_VERSION;
      desc.push_back(volume);
      ix++;
    }
    storage->init_volumes(desc);

  } catch (std::exception const& err) {
    LOG(ERROR) << "Can't create metadata file " << file_name << ", the error is: " << err.what();
    return APR_EGENERAL;
  }
  return APR_SUCCESS;
}

WorkerDatabase::WorkerDatabase(std::shared_ptr<Synchronization> synchronization, bool is_moving)
  : Database(is_moving) {
  metadata_.reset(new WorkerMetaStorage(":memory:", synchronization));

  bstore_ = storage::BlockStoreBuilder::create_memstore();
  cstore_ = std::make_shared<storage::ColumnStore>(bstore_);
}

WorkerDatabase::WorkerDatabase(
    const char* path,
    const FineTuneParams& params,
    std::shared_ptr<Synchronization> synchronization,
    bool is_moving)
  : Database(is_moving) {
  metadata_.reset(new WorkerMetaStorage(path, synchronization));

  std::string bstore_type = "FixedSizeFileStorage";
  std::string db_name = "db";
  metadata_->get_config_param("blockstore_type", &bstore_type);
  metadata_->get_config_param("db_name", &db_name);
  if (bstore_type == "FixedSizeFileStorage") {
    LOG(INFO) << "Open as fixed size storage";
    bstore_ = storage::FixedSizeFileStorage::open(metadata_);
  } else if (bstore_type == "ExpandableFileStorage") {
    LOG(INFO) << "Open as expandable storage";
    bstore_ = storage::ExpandableFileStorage::open(metadata_);
  } else {
    LOG(FATAL) << "Unknown blockstore type (" + bstore_type + ")";
  }
  cstore_ = std::make_shared<storage::ColumnStore>(bstore_);
}

void WorkerDatabase::close() {
  // Close column store
  auto mapping = cstore_->close();
  if (!mapping.empty()) {
    for (auto kv: mapping) {
      u64 id; 
      std::vector<u64> vals;
      std::tie(id, vals) = kv;
      update_rescue_point(id, std::move(vals));
    }
    metadata_->sync_with_metadata_storage();
  }
  bstore_->flush();

  // Delete WAL volumes
  inputlog_.reset();
  if (!input_log_path_.empty()) {
    int ccr = 0;
    common::Status status;
    std::tie(status, ccr) = storage::ShardedInputLog::find_logs(input_log_path_.c_str());
    if (status.IsOk() && ccr > 0) {
      auto ilog = std::make_shared<storage::ShardedInputLog>(ccr, input_log_path_.c_str());
      ilog->delete_files();
    }
  }
}

void WorkerDatabase::sync() {
  metadata_->sync_with_metadata_storage();
}

void WorkerDatabase::update_rescue_point(ParamId id, std::vector<storage::LogicAddr>&& rpoints) {
  metadata_->add_rescue_point(id, rpoints);
}

void WorkerDatabase::close_specific_columns(const std::vector<u64>& ids) {
  LOG(INFO) << "Going to close " << ids.size() << " ids";
  auto mapping = cstore_->close(ids);
  LOG(INFO) << ids.size() + " ids were closed";
  if (!mapping.empty()) {
    for (auto kv: mapping) {
      u64 id;
      std::vector<u64> vals;
      std::tie(id, vals) = kv;
      update_rescue_point(id, std::move(vals));
    }
  }
}

common::Status WorkerDatabase::new_database(bool is_moving,
                                            const char* base_file_name,
                                            const char* metadata_path,
                                            const char* volumes_path,
                                            i32 num_volumes,
                                            u64 volume_size,
                                            bool allocate) {
  // Check for max volume size
  const u64 MAX_SIZE = 0x100000000ull * 4096 - 1;  // 15TB
  const u64 MIN_SIZE = 0x100000;  // 1MB
  if (volume_size > MAX_SIZE) {
    LOG(ERROR) << "Volume size is too big: " << volume_size << ", it can't be greater than 15TB";
    return common::Status::BadArg();
  } else if (volume_size < MIN_SIZE) {
    LOG(ERROR) << "Volume size is too small: " << volume_size << ", it can't be less than 1MB";
    return common::Status::BadArg();
  }
  // Create volumes and metapage
  u32 volsize = static_cast<u32>(volume_size / 4096ull);

  boost::filesystem::path volpath(volumes_path);
  boost::filesystem::path metpath(metadata_path);
  volpath = boost::filesystem::absolute(volpath);
  metpath = boost::filesystem::absolute(metpath);
  std::string sqlitebname = std::string(base_file_name) + ".stdb";
  boost::filesystem::path sqlitepath = metpath / sqlitebname;

  if (!boost::filesystem::exists(volpath)) {
    LOG(INFO) << volumes_path << " doesn't exists, trying to create directory";
    boost::filesystem::create_directories(volpath);
  } else {
    if (!boost::filesystem::is_directory(volpath)) {
      LOG(ERROR) << volumes_path << " is not a directory";
      return common::Status::BadArg();
    }
  }

  if (!boost::filesystem::exists(metpath)) {
    LOG(INFO) << metadata_path << " doesn't exists, trying to create directory";
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

  i32 actual_nvols = (num_volumes == 0) ? 1 : num_volumes;
  std::vector<std::tuple<u32, std::string>> paths;
  for (i32 i = 0; i < actual_nvols; i++) {
    std::string basename = std::string(base_file_name) + "_" + std::to_string(i) + ".vol";
    boost::filesystem::path p = volpath / basename;
    paths.push_back(std::make_tuple(volsize, p.string()));
  }

  storage::FileStorage::create(paths);

  if (allocate) {
    for (const auto& path: paths) {
      const auto& p = std::get<1>(path);
      int fd = open(p.c_str(), O_WRONLY);
      if (fd < 0) {
        boost::system::error_code error(errno, boost::system::system_category());
        LOG(ERROR) << "Can't open file '" << p << "' reason: " << error.message() << ". Skip.";
        break;
      }
      int ret = posix_fallocate(fd, 0, std::get<0>(path));
      ::close(fd);
      if (ret == 0) {
        LOG(INFO) << "Disk space for " << p << " preallocated";
      } else {
        boost::system::error_code error(ret, boost::system::system_category());
        LOG(ERROR) << "posix_fallocate fail: " + error.message();
      }
    }
  }

  // Create sqlite database for metadata
  std::vector<std::string> mpaths;
  std::vector<u32> msizes;
  for (auto p: paths) {
    msizes.push_back(std::get<0>(p));
    mpaths.push_back(std::get<1>(p));
  }
  if (num_volumes == 0) {
    LOG(INFO) << "Creating expandable file storage";
    if (APR_SUCCESS != create_metadata_page(base_file_name, sqlitepath.c_str(), mpaths, msizes, "ExpandableFileStorage")) {
      return common::Status::Internal();
    }
  } else {
    LOG(INFO) << "Creating fixed file storage";
    if (APR_SUCCESS != create_metadata_page(base_file_name, sqlitepath.c_str(), mpaths, msizes, "FixedSizeFileStorage")) {
      return common::Status::Internal();
    }
  }
  return common::Status::Ok();
}

void WorkerDatabase::run_recovery(
    const FineTuneParams &params,
    Database* database) {
  LOG(INFO) << "WAL meta data recovery started";
  std::unordered_map<ParamId, std::vector<storage::LogicAddr>> mapping;
  auto status = metadata_->load_rescue_points(mapping);
  if (!status.IsOk()) {
    LOG(FATAL) << "Can't read rescue points";
  }

  int ccr = 0;
  auto run_wal_recovery = wal_recovery_is_enabled(params, &ccr);

  common::Status restore_status;
  std::vector<ParamId> restored_ids;
  std::tie(restore_status, restored_ids) = cstore_->open_or_restore(mapping, params.input_log_path == nullptr);
  if (run_wal_recovery) {
    auto ilog = std::make_shared<storage::ShardedInputLog>(ccr, params.input_log_path);
    run_input_log_recovery(ilog.get(), restored_ids, &mapping, database);

    sync();
  }
  LOG(INFO) << "WAL metadata recovery completed";
}

void WorkerDatabase::run_input_log_recovery(storage::ShardedInputLog* ilog, const std::vector<ParamId>& ids2restore,
                                            std::unordered_map<ParamId, std::vector<storage::LogicAddr>>* mapping,
                                            Database* database) {
  WorkerRecoveryVisitor visitor;
  visitor.worker_database = database;
  visitor.mapping = mapping;
  visitor.top_addr = bstore_->get_top_address();
  bool proceed = true;
  size_t nitems = 0x1000;
  u64 nsegments = 0;
  std::vector<storage::InputLogRow> rows(nitems);
  LOG(INFO) << "WAL recovery started";
  std::unordered_set<ParamId> idfilter(ids2restore.begin(), ids2restore.end());

  while (proceed) {
    common::Status status;
    u32 outsize;
    std::tie(status, outsize) = ilog->read_next(nitems, rows.data());
    if (status.IsOk() || (status.Code() == common::Status::kNoData && outsize > 0)) {
      for (u32 ix = 0; ix < outsize; ix++) {
        const storage::InputLogRow& row = rows.at(ix);
        if (idfilter.count(row.id)) {
          visitor.reset(row.id);
          proceed = row.payload.apply_visitor(visitor);
        }
      }
      nsegments++;
    } else if (status.Code() == common::Status::kNoData) {
      LOG(INFO) << "WAL recovery completed";
      LOG(INFO) << nsegments << " segments scanned";
      LOG(INFO) << visitor.nsamples << " samples recovered";
      LOG(INFO) << visitor.nlost << " samples lost";
      proceed = false;
    } else {
      LOG(ERROR) << "WAL recovery error: " << status.ToString();
      proceed = false;
    }
  }

  // Close column store.
  // Some columns were restored using the NBTree crash recovery algorithm
  // and WAL replay. To delete old WAL volumes we have to close these columns.
  // Another problem that is solved here is memory usage. WAL has a mechanism
  // that is used to offload columns that was opened and didn't received any
  // updates for a while. If all columns will be opened at start, this will be
  // meaningless.
  auto cmapping = cstore_->close();
  if (!cmapping.empty()) {
    for (auto kv: cmapping) {
      u64 id;
      std::vector<u64> vals;
      std::tie(id, vals) = kv;
      metadata_->add_rescue_point(id, std::move(vals));
    }
  }
  bstore_->flush();

  ilog->reopen();
  ilog->delete_files();
}

}  // namespace stdb

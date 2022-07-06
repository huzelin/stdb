/*!
 * \file standalone_database.cc
 */
#include "stdb/core/standalone_database.h"

#include <memory>

#include "stdb/core/standalone_database_session.h"

namespace stdb {

StandaloneDatabase::StandaloneDatabase(std::shared_ptr<Synchronization> synchronization) {
  worker_database_.reset(new WorkerDatabase(synchronization));
  server_database_.reset(new ServerDatabase());
}

StandaloneDatabase::StandaloneDatabase(
    const char* server_path, const char* worker_path,
    const FineTuneParams& params, std::shared_ptr<Synchronization> synchronization) {
  server_database_.reset(new ServerDatabase(server_path, params, this));
  worker_database_.reset(new WorkerDatabase(worker_path, params, synchronization, this));
}

void StandaloneDatabase::initialize_input_log(const FineTuneParams& params) {
  Database::initialize_input_log(params);

  server_database_->set_input_log(inputlog(), input_log_path());
  worker_database_->set_input_log(inputlog(), input_log_path());
}

void StandaloneDatabase::close() {
  server_database_->close();
  worker_database_->close();
}

void StandaloneDatabase::sync() {
  server_database_->sync();
  worker_database_->sync();
}

std::shared_ptr<DatabaseSession> StandaloneDatabase::create_session() {
  std::shared_ptr<storage::CStoreSession> session =
      std::make_shared<storage::CStoreSession>(worker_database_->cstore());
  return std::make_shared<StandaloneDatabaseSession>(shared_from_this(),
                                                     session,
                                                     inputlog_.get());
}

common::Status StandaloneDatabase::new_database(
    const char* base_file_name,
    const char* metadata_path,
    const char* volumes_path,
    i32 num_volumes,
    u64 volume_size,
    bool allocate) {
  std::string server_metadata_path = std::string(metadata_path) + "/server";
  auto status = ServerDatabase::new_database(
      base_file_name, server_metadata_path.c_str(),
      num_volumes == 0 ? "ExpandableFileStorage" : "FixedSizeFileStorage");
  if (!status.IsOk()) {
    return status;
  }

  std::string worker_metadata_path = std::string(metadata_path) + "/worker";
  status = WorkerDatabase::new_database(
      base_file_name, worker_metadata_path.c_str(),
      volumes_path,
      num_volumes,
      volume_size,
      allocate);
  return status;
}

// Create new column store.
void StandaloneDatabase::recovery_create_new_column(ParamId id) {

}

// Update rescue points
void StandaloneDatabase::recovery_update_rescue_points(ParamId id, const std::vector<storage::LogicAddr>& addrs) {

}

// Recovery write.
storage::NBTreeAppendResult StandaloneDatabase::recovery_write(Sample const& sample, bool allow_duplicates) {

}
  
SeriesMatcher* StandaloneDatabase::global_matcher() {
  return server_database_->global_matcher();
}

}  // namespace stdb

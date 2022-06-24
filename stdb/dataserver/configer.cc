/*!
 * \file configer.cc
 */
#include "stdb/dataserver/configer.h"

#include "stdb/common/proto_configure.h"

#include "stdb/core/storage_api.h"

namespace {
std::string kConfigPath = "~/.stdb_ds.conf";
}  // namespace

namespace stdb {

Configer::Configer() {
  common::ProtoConfigure proto_configure;
  auto ret = proto_configure.Init("proto.DsConfig", kConfigPath);
  if (ret == common::ProtoConfigure::kOK) {
    ds_config_ = *(dynamic_cast<const proto::DsConfig*>(proto_configure.config()));
  }
}

void Configer::disable_wal(const std::string& db_name) {
  std::lock_guard<std::mutex> lck(mutex_);
  for (auto& database_config : *ds_config_.mutable_database_config()) {
    if (db_name.empty() || database_config.db_name() == db_name) {
      database_config.mutable_wal_config()->set_input_log_path("");
    }
  }
}

void Configer::set_wal(const std::string& db_name, const std::string& wal_path) {
  std::lock_guard<std::mutex> lck(mutex_);
  for (auto& database_config : *ds_config_.mutable_database_config()) {
    if (db_name == database_config.db_name()) {
      database_config.mutable_wal_config()->set_input_log_path(wal_path);
    }
  }
}

common::Status Configer::create_database_ex(const char* db_name,
                                            const char* metadata_path,
                                            const char* volumes_path,
                                            i32 num_volumes,
                                            u64 volume_size,
                                            bool allocate) {
  std::lock_guard<std::mutex> lck(mutex_);
  if (has_db(db_name)) {
    return common::Status::BadArg();
  }

  auto status = STDBConnection::create_database_ex(
      db_name,
      metadata_path,
      volumes_path,
      num_volumes,
      volume_size,
      allocate);
  if (!status.IsOk()) return status;

  auto database_config = ds_config_.add_database_config();
  database_config->set_db_name(db_name);
  database_config->set_base_file_name(db_name);
  database_config->set_metadata_path(metadata_path);
  database_config->set_volumes_path(volumes_path);
  database_config->set_num_volumes(num_volumes);
  database_config->set_volume_size(volume_size);
  database_config->set_allocate(allocate);

  auto wal_config = database_config->mutable_wal_config();
  wal_config->set_input_log_concurrency(MAX_THREADS);
  wal_config->set_input_log_volume_size(256UL * 1024 * 1024);
  wal_config->set_input_log_volume_numb(2);

  return status;
}

common::Status Configer::delete_database(const char* db_name, bool force) {
  std::lock_guard<std::mutex> lck(mutex_);
  if (!has_db(db_name)) {
    return common::Status::BadArg();
  }

  common::Status status;
  std::vector<proto::DatabaseConfig> database_configs;
  for (auto& database_config : ds_config_.database_config()) {
    database_configs.emplace_back(database_config);
  }

  ds_config_.clear_database_config();
  for (auto& database_config : database_configs) {
    if (database_config.db_name() == db_name) {
      auto meta_path = get_meta_path(database_config);
      auto wal_path = database_config.wal_config().input_log_path();
      status = STDBConnection::delete_database(meta_path.c_str(), wal_path.c_str(), force);
    } else {
      *ds_config_.add_database_config() = database_config;
    }
  }
  return status;
}

bool Configer::has_db(const char* db_name) {
  for (auto& database_config : ds_config_.database_config()) {
    if (database_config.db_name() == db_name) {
      return true;
    }
  }
  return false;
}

std::string Configer::get_meta_path(const proto::DatabaseConfig& database_config) const {
  auto meta_path = database_config.metadata_path() + "/" +
      database_config.base_file_name() + ".stdb";
  return meta_path;
}

}  // namespace stdb

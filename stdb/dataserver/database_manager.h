/*!
 * \file configer.h
 */
#ifndef STDB_DATASERVER_DATABASE_MANAGER_H_
#define STDB_DATASERVER_DATABASE_MANAGER_H_

#include <mutex>

#include "stdb/common/singleton.h"
#include "stdb/common/status.h"
#include "stdb/common/basic.h"
#include "stdb/core/storage_api.h"
#include "stdb/dataserver/ds_config.pb.h"

namespace stdb {

class DatabaseManager : public common::Singleton<DatabaseManager> {
 public:
  DatabaseManager();

  void disable_wal(const std::string& db_name);
  void set_wal(const std::string& db_name, const std::string& wal_path);

  // If num_volumes is 0, it will be ExpandableFileStorage. 
  common::Status create_database_ex(const char* db_name,
                                    const char* metadata_path,
                                    const char* volumes_path,
                                    i32 num_volumes,
                                    u64 volume_size,
                                    bool allocate);
  common::Status delete_database(const char* db_name, bool force);

  std::shared_ptr<DbConnection> get_connection(const std::string& db_name);
  void clear_connection();

 protected:
  common::Status init_connection(const proto::DatabaseConfig& databse_config);
  void remove_connection(const char* db_name);

  bool has_db(const char* db_name);
  std::string get_meta_path(const proto::DatabaseConfig& database_config) const;
  void save_ds_config();

  std::map<std::string, std::shared_ptr<DbConnection>> conns_;
  proto::DsConfig ds_config_;
  std::mutex mutex_;
};

}  // namespace stdb

#endif  // STDB_DATASERVER_DATABASE_MANAGER_H_

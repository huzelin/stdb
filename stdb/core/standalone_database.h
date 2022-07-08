/*!
 * \file standalone_database.h
 */
#ifndef STDB_CORE_STANDALONE_DATABASE_H_
#define STDB_CORE_STANDALONE_DATABASE_H_

#include "stdb/core/server_database.h"
#include "stdb/core/worker_database.h"

namespace stdb {

class StandaloneDatabase : public Database, public std::enable_shared_from_this<StandaloneDatabase> {
 protected:
  std::shared_ptr<SyncWaiter> sync_waiter_;
  std::shared_ptr<ServerDatabase> server_database_;
  std::shared_ptr<WorkerDatabase> worker_database_;
 
 public:
  // Create empty in-memory database
  StandaloneDatabase(std::shared_ptr<Synchronization> synchronization,
                     std::shared_ptr<SyncWaiter> sync_waiter,
                     bool is_moving);

  /* @brief Open storage engine
   * @param path is a path to main files
   */
  StandaloneDatabase(const char* server_path, const char* worker_path,
                     const FineTuneParams& params,
                     std::shared_ptr<Synchronization> synchronization,
                     std::shared_ptr<SyncWaiter> sync_waiter,
                     bool is_moving);

  std::shared_ptr<ServerDatabase> server_database() { return server_database_; }
  std::shared_ptr<WorkerDatabase> worker_database() { return worker_database_; }

  void initialize(const FineTuneParams& params) override;

  // Close operation
  void close() override;
  // Sync operation
  void sync() override;
  // create write session
  std::shared_ptr<DatabaseSession> create_session() override;

  /* Create empty database from scratch.
   * @param ismoving If is moving database
   * @param base_file_name is database name (excl suffix)
   * @param metadata_path is a path to metadata storage
   * @param volumes_path is a path to volumes storage
   * @param num_volumes defines how many volumes should be crated
   * @param volume_size is a size of the volume in bytes
   * @return operation status
   */
  static common::Status new_database(bool ismoving,
                                     const char* base_file_name,
                                     const char* metadata_path,
                                     const char* volumes_path,
                                     i32 num_volumes,
                                     u64 volume_size,
                                     bool allocate);

 protected:
  // Create new column store.
  void recovery_create_new_column(ParamId id) override;

  // Update rescue points
  void recovery_update_rescue_points(ParamId id, const std::vector<storage::LogicAddr>& addrs) override;

  // Recovery write.
  storage::NBTreeAppendResult recovery_write(Sample const& sample, bool allow_duplicates) override;
  
  // Return series matcher
  SeriesMatcher* global_matcher() override;
};

}  // namespace stdb

#endif  // STDB_CORE_STANDALONE_DATABASE_H_

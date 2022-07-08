/*!
 * \file worker_database.h
 */
#ifndef STDB_CORE_WORKER_DATABASE_H_
#define STDB_CORE_WORKER_DATABASE_H_

#include "stdb/core/database.h"

#include "stdb/metastorage/worker_meta_storage.h"
#include "stdb/storage/input_log.h"
#include "stdb/storage/column_store.h"

namespace stdb {

class WorkerDatabase : public Database {
 protected:
  std::shared_ptr<WorkerMetaStorage> metadata_;
  std::shared_ptr<storage::ColumnStore> cstore_;
  std::shared_ptr<storage::BlockStore> bstore_;

 public:
  // Create empty in-memory database
  WorkerDatabase(std::shared_ptr<Synchronization> synchronization, bool is_moving);

  WorkerDatabase(const char* path, const FineTuneParams &params,
                 std::shared_ptr<Synchronization> synchronization,
                 bool is_moving);

  // Return cstore
  std::shared_ptr<storage::ColumnStore> cstore() const { return cstore_; }

  /**
   * @brief called before object destructor, all ingestion sessions should be
   * stooped first.
   */
  void close() override;
  // Sync
  void sync() override;

  // update rescue point
  void update_rescue_point(ParamId id, std::vector<storage::LogicAddr>&& rpoints);

  /**
   * @brief Flush and close every column in the list
   * @param ids list of column ids
   */
  void close_specific_columns(const std::vector<u64>& ids);

  /* Create empty database from scratch.
   * @param is_moving If moving database
   * @param base_file_name is database name (excl suffix)
   * @param metadata_path is a path to metadata storage
   * @param volumes_path is a path to volumes storage
   * @param num_volumes defines how many volumes should be crated
   * @param volume_size is a size of the volume in bytes
   * @return operation status
   */
  static common::Status new_database(bool is_moving,
                                     const char* base_file_name,
                                     const char* metadata_path,
                                     const char* volumes_path,
                                     i32 num_volumes,
                                     u64 volume_size,
                                     bool allocate);

  void run_recovery(const FineTuneParams &params, Database* database);

 protected:
  // recovery from inputlog
  void run_input_log_recovery(storage::ShardedInputLog* ilog, const std::vector<ParamId>& ids2restore,
                              std::unordered_map<ParamId, std::vector<storage::LogicAddr>>* mapping,
                              Database* database);
};

}  // namespace stdb

#endif  // STDB_CORE_WORKER_DATABASE_H_
